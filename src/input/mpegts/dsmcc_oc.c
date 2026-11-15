/*
 *  Tvheadend - DSM-CC object carousel
 *
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "tvheadend.h"

#if ENABLE_ZLIB

#include <zlib.h>
#include "dsmcc_oc.h"

#define OC_MAX_MODULES   1024
#define OC_MAX_MODULE    (64 * 1024 * 1024)  /* module, also decompressed */
/* all module buffers being collected at once (DoS cap): a module is not
 * started while the sum would go over this, so a carousel that lists many
 * huge modules cannot make us allocate OC_MAX_MODULES * OC_MAX_MODULE. Real
 * EPG carousels are a few MiB; completed modules free their buffer, so a
 * larger legitimate carousel still makes progress across repetitions. */
#define OC_MAX_BYTES     (256 * 1024 * 1024)
#define OC_MAX_KEY       16
#define OC_MAX_NAME      128
#define OC_MAX_DEPTH     16
#define OC_MAX_PATH      1024
#define OC_MAX_FAILS     3     /* give up on a module after this many decompress fails */

#define DSMCC_DII        0x1002
#define DSMCC_DDB        0x1003
#define DSMCC_DSI        0x1006

#define TAG_BIOP         0x49534F06  /* BIOPProfileBody */
#define TAG_OBJECT_LOC   0x49534F50  /* BIOP::ObjectLocation */

typedef struct oc_ref {
  int      valid;
  uint16_t module;
  uint8_t  keylen;
  uint8_t  key[OC_MAX_KEY];
} oc_ref_t;

typedef struct oc_module {
  uint16_t id;
  uint8_t  version;
  uint8_t  done;           /* read and its objects parsed */
  uint8_t  compressed;
  uint8_t  fails;          /* decompression failures of this version */
  uint16_t block_size;     /* of the DII that lists the module */
  uint32_t size;           /* in the carousel */
  uint32_t orig_size;      /* decompressed (compressed_module_descriptor) */
  uint32_t nblocks, have;
  uint8_t *data;           /* blocks while collecting */
  uint8_t *got;            /* one byte per block */
} oc_module_t;

typedef struct oc_object {
  struct oc_object *next;
  oc_ref_t ref;
  char     kind[4];        /* "srg", "dir", "fil", ... */
  uint8_t *body;           /* dir/srg: the bindings; fil: content until delivered */
  size_t   len;
  int      delivered;
  uint32_t walk;           /* generation of the last walk that entered it */
} oc_object_t;

struct dsmcc_oc {
  dsmcc_oc_ops_t ops;
  void          *opaque;
  size_t         probe_limit;
  int            root;          /* 0 unknown, 1 wanted, -1 not wanted */
  oc_ref_t       gateway;       /* service gateway (DSI) */
  int            missing;       /* objects of the tree not read yet (last walk) */
  uint32_t       walk;          /* generation of the current/last tree walk */
  size_t         bytes;         /* module buffers allocated right now */
  int            nmodules;
  oc_module_t    modules[OC_MAX_MODULES];
  oc_object_t   *objects;
};

static inline uint16_t get16(const uint8_t *p) { return (p[0] << 8) | p[1]; }
static inline uint32_t get32(const uint8_t *p)
{
  return ((uint32_t)p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
}

/* ------------------------------------------------------------------------
 * Objects
 * ----------------------------------------------------------------------*/

static int ref_eq ( const oc_ref_t *a, const oc_ref_t *b )
{
  return a->module == b->module && a->keylen == b->keylen &&
         !memcmp(a->key, b->key, a->keylen);
}

static oc_object_t *object_find ( dsmcc_oc_t *oc, const oc_ref_t *ref )
{
  oc_object_t *o;
  for (o = oc->objects; o; o = o->next)
    if (ref_eq(&o->ref, ref))
      return o;
  return NULL;
}

/* the objects of a module go when the module changes */
static void objects_drop_module ( dsmcc_oc_t *oc, uint16_t module )
{
  oc_object_t **pp = &oc->objects, *o;
  while ((o = *pp) != NULL) {
    if (o->ref.module == module) {
      *pp = o->next;
      free(o->body);
      free(o);
    } else {
      pp = &o->next;
    }
  }
}

/*
 * IOR -> object location (module, object key) of a BIOPProfileBody;
 * returns the bytes used, -1 on an error
 */
static int parse_ior ( const uint8_t *p, size_t len, oc_ref_t *ref )
{
  size_t o = 0, tl, n, i;
  memset(ref, 0, sizeof(*ref));
  if (len < 4) return -1;
  tl = get32(p); o = 4 + tl;
  if (o + 4 > len) return -1;
  n = get32(p + o); o += 4;
  for (i = 0; i < n; i++) {
    uint32_t tag, plen;
    if (o + 8 > len) return -1;
    tag = get32(p + o); plen = get32(p + o + 4); o += 8;
    if (plen > len - o) return -1;
    if (tag == TAG_BIOP && plen >= 2) {
      const uint8_t *b = p + o;
      size_t q = 2, c, cnt = b[1];
      for (c = 0; c < cnt; c++) {
        uint32_t ctag;
        size_t clen;
        if (q + 5 > plen) break;
        ctag = get32(b + q); clen = b[q + 4]; q += 5;
        if (q + clen > plen) break;
        if (ctag == TAG_OBJECT_LOC && clen >= 9) {
          const uint8_t *l = b + q;
          size_t kl = l[8];
          if (kl <= OC_MAX_KEY && 9 + kl <= clen) {
            ref->valid = 1;
            ref->module = get16(l + 4);
            ref->keylen = kl;
            memcpy(ref->key, l + 9, kl);
          }
        }
        q += clen;
      }
    }
    o += plen;
  }
  return o;
}

/* the BIOP messages of a complete module */
static void parse_module_objects ( dsmcc_oc_t *oc, uint16_t module,
                                   const uint8_t *d, size_t len )
{
  size_t o = 0;
  while (o + 12 <= len && !memcmp(d + o, "BIOP", 4)) {
    size_t msize = get32(d + o + 8), end = o + 12 + msize, q = o + 12;
    size_t kl, kindl, oil, sci, i, blen;
    oc_object_t *obj;
    if (msize > len - o - 12) break;
    if (q + 1 > end) break;
    kl = d[q]; q++;
    if (kl > OC_MAX_KEY || q + kl + 4 > end) break;
    obj = calloc(1, sizeof(*obj));
    if (!obj) break;
    obj->ref.valid = 1;
    obj->ref.module = module;
    obj->ref.keylen = kl;
    memcpy(obj->ref.key, d + q, kl); q += kl;
    kindl = get32(d + q); q += 4;
    if (kindl > end - q) { free(obj); break; }
    memcpy(obj->kind, d + q, kindl < 3 ? kindl : 3); q += kindl;
    if (q + 2 > end) { free(obj); break; }
    oil = get16(d + q); q += 2 + oil;
    if (q + 1 > end) { free(obj); break; }
    sci = d[q]; q++;
    for (i = 0; i < sci; i++) {
      if (q + 6 > end) break;
      q += 6 + get16(d + q + 4);
    }
    if (q + 4 > end) { free(obj); break; }
    blen = get32(d + q); q += 4;
    if (blen > end - q) { free(obj); break; }
    if (!strcmp(obj->kind, "fil")) {
      /* content_length + content */
      if (blen >= 4) {
        size_t cl = get32(d + q);
        if (cl <= blen - 4) {
          obj->body = malloc(cl ? cl : 1);
          if (obj->body) { memcpy(obj->body, d + q + 4, cl); obj->len = cl; }
        }
      }
    } else if (!strcmp(obj->kind, "srg") || !strcmp(obj->kind, "dir")) {
      obj->body = malloc(blen ? blen : 1);
      if (obj->body) { memcpy(obj->body, d + q, blen); obj->len = blen; }
    }
    obj->next = oc->objects;
    oc->objects = obj;
    o = end;
  }
}

/* ------------------------------------------------------------------------
 * Directory tree
 * ----------------------------------------------------------------------*/

typedef void (*binding_cb)(dsmcc_oc_t *oc, void *aux, const char *name,
                           const char *kind, const oc_ref_t *ref);

static void dir_bindings ( dsmcc_oc_t *oc, const oc_object_t *dir,
                           binding_cb cb, void *aux )
{
  const uint8_t *b = dir->body;
  size_t len = dir->len, q = 2, n, i;
  if (!b || len < 2) return;
  n = get16(b);
  for (i = 0; i < n; i++) {
    char name[OC_MAX_NAME] = "", kind[8] = "";
    size_t nc, c;
    oc_ref_t ref;
    int r;
    if (q + 1 > len) return;
    nc = b[q]; q++;
    for (c = 0; c < nc; c++) {
      size_t il, kl;
      if (q + 1 > len) return;
      il = b[q]; q++;
      if (q + il > len) return;
      if (il && il < sizeof(name)) {
        memcpy(name, b + q, il); name[il] = '\0';
      }
      q += il;
      if (q + 1 > len) return;
      kl = b[q]; q++;
      if (q + kl > len) return;
      if (kl && kl < sizeof(kind)) {
        memcpy(kind, b + q, kl); kind[kl] = '\0';
      }
      q += kl;
    }
    if (q + 1 > len) return;
    q++; /* bindingType */
    r = parse_ior(b + q, len - q, &ref);
    if (r < 0) return;
    q += r;
    if (q + 2 > len) return;
    q += 2 + get16(b + q); /* objectInfo */
    /* names with a path separator or "." / ".." are not followed */
    if (!name[0] || strchr(name, '/') || !strcmp(name, ".") || !strcmp(name, ".."))
      continue;
    cb(oc, aux, name, kind, &ref);
  }
}

typedef struct root_names {
  const char *names[256];
  char        buf[256][OC_MAX_NAME];
  int         count;
} root_names_t;

static void root_cb ( dsmcc_oc_t *oc, void *aux, const char *name,
                      const char *kind, const oc_ref_t *ref )
{
  root_names_t *rn = aux;
  if (rn->count >= 256) return;
  strcpy(rn->buf[rn->count], name);
  rn->names[rn->count] = rn->buf[rn->count];
  rn->count++;
}

typedef struct walk {
  char path[OC_MAX_PATH];
  int  depth;
  int  missing;   /* reachable objects not read yet */
} walk_t;

static void walk_cb ( dsmcc_oc_t *oc, void *aux, const char *name,
                      const char *kind, const oc_ref_t *ref )
{
  walk_t *w = aux;
  oc_object_t *o;
  size_t l = strlen(w->path);
  if (!ref->valid) return;
  if (l + 1 + strlen(name) + 1 > sizeof(w->path)) return;
  o = object_find(oc, ref);
  if (!o) { w->missing++; return; }
  if (l) w->path[l++] = '/';
  strcpy(w->path + l, name);
  if (!strcmp(o->kind, "dir")) {
    /* enter a directory once per walk: a directory reachable over more than
     * one path (a DAG, or a cycle within the depth limit) is otherwise
     * re-expanded, and the work grows with fanout^depth */
    if (o->walk != oc->walk && w->depth < OC_MAX_DEPTH) {
      o->walk = oc->walk;
      w->depth++;
      dir_bindings(oc, o, walk_cb, w);
      w->depth--;
    }
  } else if (!strcmp(o->kind, "fil") && !o->delivered && o->body) {
    o->delivered = 1;
    if (oc->ops.file)
      oc->ops.file(oc->opaque, w->path, o->body, o->len);
    free(o->body);
    o->body = NULL;
  }
  w->path[l ? l - 1 : 0] = '\0';
}

/* look at the root once it is there, deliver what is complete */
static void update_tree ( dsmcc_oc_t *oc )
{
  oc_object_t *srg;
  walk_t w;
  if (!oc->gateway.valid || oc->root < 0) return;
  srg = object_find(oc, &oc->gateway);
  if (!srg) return;
  if (oc->root == 0) {
    root_names_t *rn = calloc(1, sizeof(*rn));
    int r;
    if (!rn) return;
    dir_bindings(oc, srg, root_cb, rn);
    r = oc->ops.root ? oc->ops.root(oc->opaque, rn->names, rn->count) : 0;
    free(rn);
    oc->root = r < 0 ? -1 : 1;
    if (oc->root < 0) return;
  }
  memset(&w, 0, sizeof(w));
  if (++oc->walk == 0) oc->walk = 1;   /* a fresh generation, never 0 */
  srg->walk = oc->walk;
  dir_bindings(oc, srg, walk_cb, &w);
  oc->missing = w.missing;
}

/* ------------------------------------------------------------------------
 * Modules
 * ----------------------------------------------------------------------*/

/* free the collected blocks of a module and account the freed bytes */
static void module_free_data ( dsmcc_oc_t *oc, oc_module_t *m )
{
  if (m->data) {
    oc->bytes -= m->size;
    free(m->data);
    m->data = NULL;
  }
  free(m->got);
  m->got = NULL;
}

static void module_reset ( dsmcc_oc_t *oc, oc_module_t *m )
{
  module_free_data(oc, m);
  m->have = 0;
  m->done = 0;
}

static oc_module_t *module_find ( dsmcc_oc_t *oc, uint16_t id )
{
  int i;
  for (i = 0; i < oc->nmodules; i++)
    if (oc->modules[i].id == id)
      return &oc->modules[i];
  return NULL;
}

static int module_wanted ( dsmcc_oc_t *oc, const oc_module_t *m )
{
  if (oc->root < 0) return 0;
  if (oc->root > 0) return 1;
  return m->size <= oc->probe_limit;
}

static void module_complete ( dsmcc_oc_t *oc, oc_module_t *m )
{
  uint8_t *d = m->data;
  size_t len = m->size;
  uint8_t *out = NULL;
  if (m->compressed) {
    z_stream z;
    uLongf olen = m->orig_size;
    if (!olen || olen > OC_MAX_MODULE) goto fail;
    out = malloc(olen);
    if (!out) goto fail;
    memset(&z, 0, sizeof(z));
    if (inflateInit(&z) != Z_OK) goto fail;
    z.next_in = d; z.avail_in = len;
    z.next_out = out; z.avail_out = olen;
    if (inflate(&z, Z_FINISH) != Z_STREAM_END) { inflateEnd(&z); goto fail; }
    len = olen - z.avail_out;
    inflateEnd(&z);
    d = out;
  }
  objects_drop_module(oc, m->id);
  parse_module_objects(oc, m->id, d, len);
  free(out);
  module_free_data(oc, m);
  m->done = 1;
  update_tree(oc);
  return;
fail:
  free(out);
  if (++m->fails >= OC_MAX_FAILS) {
    /* a module that never decompresses (wrong/garbage compressed data) would
     * be collected and fail again on every repetition and keep
     * dsmcc_oc_complete from ever returning; give up on it (the OTA/probe
     * timeout still bounds the read if the tree really needed it) */
    module_free_data(oc, m);
    m->done = 1;
  } else {
    module_reset(oc, m);  /* collect it again */
  }
}

/* ------------------------------------------------------------------------
 * Messages
 * ----------------------------------------------------------------------*/

static void handle_dsi ( dsmcc_oc_t *oc, const uint8_t *m, size_t len )
{
  oc_ref_t ref;
  size_t q = 20, cl;
  if (q + 2 > len) return;
  cl = get16(m + q); q += 2 + cl;
  if (q + 2 > len) return;
  q += 2; /* privateDataLength: ServiceGatewayInfo follows */
  if (parse_ior(m + q, len - q, &ref) < 0 || !ref.valid) return;
  if (oc->gateway.valid && ref_eq(&oc->gateway, &ref)) return;
  oc->gateway = ref;
  update_tree(oc);
}

static void handle_dii ( dsmcc_oc_t *oc, const uint8_t *m, size_t len )
{
  size_t q = 4, cl, n, i;
  uint16_t bs;
  if (q + 2 + 10 > len) return;
  bs = get16(m + q); q += 2;
  q += 1 + 1 + 4 + 4; /* windowSize ackPeriod tCDownloadWindow tCDownloadScenario */
  if (q + 2 > len) return;
  cl = get16(m + q); q += 2 + cl;
  if (q + 2 > len) return;
  n = get16(m + q); q += 2;
  for (i = 0; i < n; i++) {
    uint16_t id;
    uint32_t size, orig = 0;
    uint8_t ver, il;
    int comp = 0;
    oc_module_t *mod;
    if (q + 8 > len) return;
    id = get16(m + q); size = get32(m + q + 2); ver = m[q + 6]; il = m[q + 7];
    if (q + 8 + il > len) return;
    /* BIOP::ModuleInfo: three timeouts, taps, userInfo descriptors */
    {
      const uint8_t *mi = m + q + 8;
      size_t t = 12, tc, k, ul, ue;
      if (t + 1 <= il) {
        tc = mi[t]; t++;
        for (k = 0; k < tc && t + 7 <= il; k++)
          t += 7 + mi[t + 6]; /* id use association_tag selector */
        if (t + 1 <= il) {
          ul = mi[t]; t++; ue = t + ul;
          while (t + 2 <= ue && ue <= il) {
            if (mi[t] == 0x09 && mi[t + 1] >= 5 && t + 7 <= ue) {
              comp = 1;
              orig = get32(mi + t + 3);
            }
            t += 2 + mi[t + 1];
          }
        }
      }
    }
    q += 8 + il;
    if (!size || size > OC_MAX_MODULE) continue;
    mod = module_find(oc, id);
    if (!mod) {
      if (oc->nmodules >= OC_MAX_MODULES) continue;
      mod = &oc->modules[oc->nmodules++];
      memset(mod, 0, sizeof(*mod));
      mod->id = id;
    } else if (mod->version == ver && mod->size == size && mod->block_size == bs) {
      continue;
    } else {
      /* also another block size: the blocks collected so far do not fit */
      module_reset(oc, mod);
      objects_drop_module(oc, id);
    }
    mod->version = ver;
    mod->size = size;
    mod->compressed = comp;
    mod->orig_size = orig;
    mod->block_size = bs;
    mod->fails = 0;    /* a new module version may well decompress */
    mod->nblocks = bs ? (size + bs - 1) / bs : 0;
  }
}

static void handle_ddb ( dsmcc_oc_t *oc, const uint8_t *m, size_t len )
{
  oc_module_t *mod;
  uint16_t id, bno;
  uint8_t ver;
  size_t off, n;
  if (len < 6) return;
  id = get16(m); ver = m[2]; bno = get16(m + 4);
  mod = module_find(oc, id);
  if (!mod || mod->done || mod->version != ver || bno >= mod->nblocks) return;
  if (!module_wanted(oc, mod)) return;
  /* the block size of the module's DII: bno < nblocks keeps off < size */
  off = (size_t)bno * mod->block_size;
  n = bno + 1 < mod->nblocks ? mod->block_size : mod->size - off;
  if (len - 6 < n || (bno + 1 < mod->nblocks && len - 6 != n)) return;
  if (!mod->data) {
    uint8_t *data, *got;
    /* keep the sum of the module buffers under the cap; the module is just
     * not started here and is retried on the next carousel repetition once
     * others have completed and freed theirs */
    if (oc->bytes + mod->size > OC_MAX_BYTES) return;
    data = malloc(mod->size);
    got = calloc(mod->nblocks, 1);
    if (!data || !got) { free(data); free(got); return; }
    mod->data = data;
    mod->got = got;
    oc->bytes += mod->size;
  }
  if (mod->got[bno]) return;
  memcpy(mod->data + off, m + 6, n);
  mod->got[bno] = 1;
  if (++mod->have == mod->nblocks)
    module_complete(oc, mod);
}

void
dsmcc_oc_section ( dsmcc_oc_t *oc, const uint8_t *sec, size_t len )
{
  const uint8_t *h;
  size_t hl, alen, mlen;
  uint16_t msg;
  if (!oc || oc->root < 0 || len < 8 + 12) return;
  if (sec[0] != 0x3B && sec[0] != 0x3C) return;
  h = sec + 8; hl = len - 8;
  /* dsmccMessageHeader / dsmccDownloadDataHeader */
  if (h[0] != 0x11 || h[1] != 0x03) return;
  msg = get16(h + 2);
  alen = h[9];
  mlen = get16(h + 10);
  if (12 + alen > hl) return;
  if (mlen > hl - 12 - alen) mlen = hl - 12 - alen;
  h += 12 + alen;
  if (sec[0] == 0x3B && msg == DSMCC_DSI)
    handle_dsi(oc, h, mlen);
  else if (sec[0] == 0x3B && msg == DSMCC_DII)
    handle_dii(oc, h, mlen);
  else if (sec[0] == 0x3C && msg == DSMCC_DDB)
    handle_ddb(oc, h, mlen);
}

int
dsmcc_oc_complete ( dsmcc_oc_t *oc )
{
  int i;
  if (!oc || oc->root <= 0 || !oc->nmodules || oc->missing) return 0;
  for (i = 0; i < oc->nmodules; i++)
    if (!oc->modules[i].done) return 0;
  return 1;
}

void
dsmcc_oc_stats ( dsmcc_oc_t *oc, int *modules, int *read, int *missing )
{
  int i;
  *modules = *read = *missing = 0;
  if (!oc) return;
  *modules = oc->nmodules;
  for (i = 0; i < oc->nmodules; i++) {
    if (oc->modules[i].done)
      (*read)++;
    else if (oc->modules[i].data)
      *missing += oc->modules[i].nblocks - oc->modules[i].have;
    else
      *missing += oc->modules[i].nblocks;
  }
}

dsmcc_oc_t *
dsmcc_oc_create ( const dsmcc_oc_ops_t *ops, void *opaque, size_t probe_limit )
{
  dsmcc_oc_t *oc = calloc(1, sizeof(*oc));
  if (!oc) return NULL;
  if (ops) oc->ops = *ops;
  oc->opaque = opaque;
  oc->probe_limit = probe_limit;
  return oc;
}

void
dsmcc_oc_destroy ( dsmcc_oc_t *oc )
{
  oc_object_t *o;
  int i;
  if (!oc) return;
  for (i = 0; i < oc->nmodules; i++)
    module_reset(oc, &oc->modules[i]);
  while ((o = oc->objects) != NULL) {
    oc->objects = o->next;
    free(o->body);
    free(o);
  }
  free(oc);
}

#endif /* ENABLE_ZLIB */
