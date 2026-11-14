/*
 *  Tvheadend - EIT sections in the files of a DSM-CC object carousel
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

/*
 * Some providers send their EPG as files: Sky Deutschland puts ZIP files with
 * the EIT schedule sections of each service (two weeks, many free channels
 * too) into the directories "mh_epg" and "mh_epgex" of an object carousel.
 * An EIT module with a "carousel" entry in its configuration looks for such a
 * carousel on every transponder it gets: the stream_type 0x0B components of
 * the PMTs are probed (root directory only) and a carousel whose root holds
 * one of the configured directories is read; its ZIP files are unpacked and
 * the sections go to the EIT parser like received ones. The transponder is
 * then marked for this module.
 */

#include "tvheadend.h"

#if ENABLE_ZLIB

#include <zlib.h>
#include "epg.h"
#include "epggrab.h"
#include "epggrab/private.h"
#include "subscriptions.h"
#include "input.h"
#include "input/mpegts/dsmcc_oc.h"

#define EITOC_MAX_DIRS     8
#define EITOC_MAX_PIDS     64
#define EITOC_PROBE_LIMIT  (64 * 1024)
#define EITOC_PROBE_TIME   60        /* seconds to find the root directories */
#define EITOC_MAX_ENTRY    (32 * 1024 * 1024)

typedef struct eitoc_config {
  char *dirs[EITOC_MAX_DIRS];
  int   ndirs;
} eitoc_config_t;

typedef struct eitoc_mux eitoc_mux_t;

typedef struct eitoc_oc {
  LIST_ENTRY(eitoc_oc) link;
  eitoc_mux_t    *em;
  mpegts_table_t *mt;
  dsmcc_oc_t     *oc;
  uint16_t        pid;
  int             state;     /* 0 probe, 1 EPG carousel, -1 not wanted, 2 complete */
  int             files, sections;
} eitoc_oc_t;

struct eitoc_mux {
  int                   refcount;
  epggrab_ota_map_t    *map;
  epggrab_module_ota_t *mod;
  eitoc_config_t       *conf;
  mpegts_mux_t         *mm;
  epggrab_ota_mux_t    *ota;
  uint16_t              pids[EITOC_MAX_PIDS];
  int                   npids;
  int                   pmts;      /* PMT tables still open */
  int                   epg;       /* EPG carousels */
  int                   done;      /* complete / given up */
  mtimer_t              timer;
  LIST_HEAD(, eitoc_oc) ocs;
};

/* ------------------------------------------------------------------------
 * Configuration
 * ----------------------------------------------------------------------*/

void *
eitoc_config_load ( htsmsg_t *conf )
{
  eitoc_config_t *c;
  htsmsg_t *l;
  htsmsg_field_t *f;
  const char *s;

  if (!conf || !(l = htsmsg_get_list(conf, "dirs")))
    return NULL;
  c = calloc(1, sizeof(*c));
  HTSMSG_FOREACH(f, l) {
    if ((s = htsmsg_field_get_str(f)) == NULL || !*s) continue;
    if (c->ndirs >= EITOC_MAX_DIRS) break;
    c->dirs[c->ndirs++] = strdup(s);
  }
  if (!c->ndirs) {
    free(c);
    return NULL;
  }
  return c;
}

void
eitoc_config_free ( void *p )
{
  eitoc_config_t *c = p;
  int i;
  if (!c) return;
  for (i = 0; i < c->ndirs; i++)
    free(c->dirs[i]);
  free(c);
}

static int
eitoc_dir_wanted ( eitoc_config_t *c, const char *name, size_t len )
{
  int i;
  for (i = 0; i < c->ndirs; i++)
    if (strlen(c->dirs[i]) == len && !strncmp(c->dirs[i], name, len))
      return 1;
  return 0;
}

/* ------------------------------------------------------------------------
 * State
 * ----------------------------------------------------------------------*/

static void
eitoc_mux_unref ( eitoc_mux_t *em )
{
  lock_assert(&global_lock);
  if (--em->refcount > 0) return;
  mtimer_disarm(&em->timer);
  free(em);
}

/*
 * Done with this transponder: complete our map of the OTA mux. This is needed
 * on a pure OTA run, where nothing else subscribes the mux: once we probe a
 * carousel (om_first cleared in eitoc_add_carousel) our map holds up the mux
 * completion, so we must release it here whether or not we found an EPG
 * carousel, otherwise the mux would wait for the full OTA timeout. A mux with
 * no carousel to probe never cleared om_first and is not affected (the
 * completion check ignores a map that never received data).
 */
static void
eitoc_finish ( eitoc_mux_t *em, const char *why )
{
  epggrab_ota_mux_t *ota;
  if (em->done) return;
  em->done = 1;
  mtimer_disarm(&em->timer);
  tvhdebug(em->mod->subsys, "%s: %s - %s", em->mod->id, em->mm->mm_nicename, why);
  /* em->ota is set once a carousel was found; otherwise find the mux we were
   * registered on at grab start. epggrab_ota_complete marks only our map and
   * ends the grab solely when every module of the mux is done, so completing
   * without a carousel cannot cut another module's grab short. */
  ota = em->ota ?: epggrab_ota_find_mux(em->mm);
  if (ota)
    epggrab_ota_complete(em->mod, ota);
}

/* everything decided and read? */
static void
eitoc_check ( eitoc_mux_t *em )
{
  eitoc_oc_t *o;
  if (em->done || em->pmts) return;
  LIST_FOREACH(o, &em->ocs, link)
    if (o->state == 0 || o->state == 1) return;
  eitoc_finish(em, em->epg ? "EPG carousel read" : "no EPG carousel");
}

/* ------------------------------------------------------------------------
 * ZIP files of EIT sections
 * ----------------------------------------------------------------------*/

static inline uint16_t le16 ( const uint8_t *p ) { return p[0] | (p[1] << 8); }
static inline uint32_t le32 ( const uint8_t *p )
{
  return p[0] | (p[1] << 8) | (p[2] << 16) | ((uint32_t)p[3] << 24);
}

/* sections of one unpacked file: whole EIT sections one after another */
static int
eitoc_sections ( eitoc_mux_t *em, const uint8_t *d, size_t len )
{
  size_t o = 0, l;
  int n = 0;
  while (o + 3 <= len) {
    if (d[o] < 0x4e || d[o] > 0x6f) break;
    l = 3 + (((d[o + 1] & 0x0f) << 8) | d[o + 2]);
    if (l > len - o) break;
    /* the broadcast EIT path checks the section CRC (MT_CRC); the carousel
     * path must too, there is no transport CRC on a "stored" ZIP entry */
    if (!tvh_crc32(d + o, l, 0xffffffff)) {
      eit_carousel_section(em->map, em->mm, d + o, l);
      n++;
    }
    o += l;
  }
  return n;
}

/* the entries of a ZIP file (central directory; stored or deflated) */
static int
eitoc_zip ( eitoc_mux_t *em, const char *path, const uint8_t *d, size_t len )
{
  size_t eocd, cd, i, n, o;
  int sections = 0;

  if (len < 22) return -1;
  for (eocd = len - 22; ; eocd--) {
    if (le32(d + eocd) == 0x06054b50) break;
    if (eocd == 0 || len - eocd > 22 + 65535) return -1;
  }
  n  = le16(d + eocd + 10);
  cd = le32(d + eocd + 16);
  for (i = 0, o = cd; i < n; i++) {
    uint16_t method, nlen, xlen, clen;
    uint32_t csize, usize, loff;
    const uint8_t *data;
    uint8_t *out;
    /* a broken central directory after some entries were already fed: keep
     * the sections we got instead of reporting the whole file "not a ZIP" */
    if (o + 46 > len || le32(d + o) != 0x02014b50)
      return i ? sections : -1;
    method = le16(d + o + 10);
    csize  = le32(d + o + 20);
    usize  = le32(d + o + 24);
    nlen   = le16(d + o + 28);
    xlen   = le16(d + o + 30);
    clen   = le16(d + o + 32);
    loff   = le32(d + o + 42);
    o += 46 + nlen + xlen + clen;
    if (loff + 30 > len || le32(d + loff) != 0x04034b50) continue;
    data = d + loff + 30 + le16(d + loff + 26) + le16(d + loff + 28);
    if (data < d || data + csize > d + len || usize > EITOC_MAX_ENTRY) continue;
    if (method == 0) {
      if (csize != usize) continue;
      sections += eitoc_sections(em, data, usize);
    } else if (method == 8) {
      z_stream z;
      int r;
      if ((out = malloc(usize ? usize : 1)) == NULL) continue;
      memset(&z, 0, sizeof(z));
      if (inflateInit2(&z, -MAX_WBITS) != Z_OK) { free(out); continue; }
      z.next_in = (Bytef *)data; z.avail_in = csize;
      z.next_out = out; z.avail_out = usize;
      r = inflate(&z, Z_FINISH);
      inflateEnd(&z);
      if (r == Z_STREAM_END && z.avail_out == 0)
        sections += eitoc_sections(em, out, usize);
      else
        tvhwarn(em->mod->subsys, "%s: %s: entry %u not unpacked (%d)",
                em->mod->id, path, (unsigned)i, r);
      free(out);
    }
  }
  return sections;
}

/* ------------------------------------------------------------------------
 * Carousel
 * ----------------------------------------------------------------------*/

static int
eitoc_oc_root ( void *opaque, const char *const *names, int count )
{
  eitoc_oc_t *o = opaque;
  eitoc_mux_t *em = o->em;
  int i;

  for (i = 0; i < count; i++)
    if (eitoc_dir_wanted(em->conf, names[i], strlen(names[i])))
      break;
  if (i >= count) {
    tvhtrace(em->mod->subsys, "%s: %s pid %d - not an EPG carousel",
             em->mod->id, em->mm->mm_nicename, o->pid);
    o->state = -1;
    return -1;
  }
  tvhinfo(em->mod->subsys, "%s: %s pid %d - EPG carousel (%s)",
          em->mod->id, em->mm->mm_nicename, o->pid, names[i]);
  o->state = 1;
  em->epg++;
  if (!em->ota)
    em->ota = epggrab_ota_register(em->mod, NULL, em->mm);
  em->map->om_first = 0;  /* data: the grab of this mux waits for the carousel */
  mpegts_mux_set_epg_module(em->mm, em->mod->id);
  return 0;
}

static void
eitoc_oc_file ( void *opaque, const char *path, const uint8_t *data, size_t len )
{
  eitoc_oc_t *o = opaque;
  eitoc_mux_t *em = o->em;
  const char *slash = strchr(path, '/');
  size_t l = strlen(path);
  int r;

  if (!slash || !eitoc_dir_wanted(em->conf, path, slash - path)) return;
  if (l < 4 || strcasecmp(path + l - 4, ".zip")) return;
  r = eitoc_zip(em, path, data, len);
  if (r < 0) {
    tvhwarn(em->mod->subsys, "%s: %s: not a ZIP file", em->mod->id, path);
    return;
  }
  o->files++;
  o->sections += r;
  tvhdebug(em->mod->subsys, "%s: %s - %d sections", em->mod->id, path, r);
}

static const dsmcc_oc_ops_t eitoc_oc_ops = {
  .root = eitoc_oc_root,
  .file = eitoc_oc_file,
};

static void
eitoc_oc_destroy ( mpegts_table_t *mt )
{
  eitoc_oc_t *o = mt->mt_opaque;
  eitoc_mux_t *em = o->em;
  if (o->state == 1) {
    int modules, read, missing;
    dsmcc_oc_stats(o->oc, &modules, &read, &missing);
    tvhdebug(em->mod->subsys, "%s: %s pid %d - stopped with %d of %d modules "
             "(%d blocks missing), %d files, %d EIT sections",
             em->mod->id, em->mm->mm_nicename, o->pid, read, modules, missing,
             o->files, o->sections);
  }
  LIST_REMOVE(o, link);
  dsmcc_oc_destroy(o->oc);
  free(o);
  eitoc_mux_unref(em);
}

static int
eitoc_oc_callback
  ( mpegts_table_t *mt, const uint8_t *ptr, int len, int tableid )
{
  eitoc_oc_t *o = mt->mt_opaque;
  eitoc_mux_t *em = o->em;
  th_subscription_t *ths;

  if (!epggrab_ota_running || o->state < 0 || o->state == 2)
    return -1;
  ths = mpegts_mux_find_subscription_by_name(mt->mt_mux, "epggrab");
  if (ths) {
    subscription_add_bytes_in(ths, len);
    subscription_add_bytes_out(ths, len);
  }
  if (tableid != 0x3B && tableid != 0x3C)
    return 0;
  dsmcc_oc_section(o->oc, ptr, len);
  if (o->state < 0 || (o->state == 1 && dsmcc_oc_complete(o->oc))) {
    if (o->state == 1) {
      o->state = 2;
      tvhinfo(em->mod->subsys, "%s: %s pid %d - %d files, %d EIT sections",
              em->mod->id, em->mm->mm_nicename, o->pid, o->files, o->sections);
    }
    em->refcount++;  /* the table may take o and the last reference with it */
    mpegts_table_destroy(mt);
    eitoc_check(em);
    eitoc_mux_unref(em);
  }
  return 0;
}

static void
eitoc_add_carousel ( eitoc_mux_t *em, uint16_t pid )
{
  eitoc_oc_t *o;
  mpegts_table_t *mt;
  int i;

  if (em->done) return;
  for (i = 0; i < em->npids; i++)
    if (em->pids[i] == pid) return;
  if (em->npids >= EITOC_MAX_PIDS) return;
  em->pids[em->npids++] = pid;

  o = calloc(1, sizeof(*o));
  o->em = em;
  o->pid = pid;
  o->oc = dsmcc_oc_create(&eitoc_oc_ops, o, EITOC_PROBE_LIMIT);
  mt = mpegts_table_add(em->mm, 0x38, 0xF8, eitoc_oc_callback, o,
                        em->mod->id, em->mod->subsys, MT_CRC | MT_FULL,
                        pid, MPS_WEIGHT_EIT);
  if (!mt || mt->mt_destroy) {
    dsmcc_oc_destroy(o->oc);
    free(o);
    return;
  }
  o->mt = mt;
  mt->mt_destroy = eitoc_oc_destroy;
  em->refcount++;
  LIST_INSERT_HEAD(&em->ocs, o, link);
  /* This mux has an object carousel to probe. Mark our map as receiving so
   * the OTA grab of the mux does not complete (when the other modules, e.g.
   * the broadcast EIT, finish first) before we know whether this is an EPG
   * carousel - a map with om_first still set is ignored by the completion
   * check. The hold is released in eitoc_finish, bounded by EITOC_PROBE_TIME:
   * either the carousel is read (completes our map) or the probe gives up. */
  em->map->om_first = 0;
  tvhtrace(em->mod->subsys, "%s: %s - probe carousel pid %d",
           em->mod->id, em->mm->mm_nicename, pid);
}

/* ------------------------------------------------------------------------
 * PMT: object carousel components (stream_type 0x0B)
 * ----------------------------------------------------------------------*/

static void
eitoc_pmt_destroy ( mpegts_table_t *mt )
{
  eitoc_mux_unref(mt->mt_opaque);
}

static int
eitoc_pmt_callback
  ( mpegts_table_t *mt, const uint8_t *ptr, int len, int tableid )
{
  eitoc_mux_t *em = mt->mt_opaque;
  int o, il, el;

  if (!epggrab_ota_running || tableid != 0x02 || len < 9)
    return -1;
  /* program_number(2) version(1) section(1) last(1) PCR_PID(2) program_info_length(2) */
  il = ((ptr[7] & 0x0f) << 8) | ptr[8];
  o = 9 + il;
  while (o + 5 <= len) {
    uint8_t  st  = ptr[o];
    uint16_t pid = ((ptr[o + 1] & 0x1f) << 8) | ptr[o + 2];
    el = ((ptr[o + 3] & 0x0f) << 8) | ptr[o + 4];
    if (st == 0x0B)
      eitoc_add_carousel(em, pid);
    o += 5 + el;
  }
  if (em->pmts > 0) em->pmts--;
  em->refcount++;
  mpegts_table_destroy(mt);
  eitoc_check(em);
  eitoc_mux_unref(em);
  return 0;
}

static void
eitoc_timeout ( void *aux )
{
  eitoc_mux_t *em = aux;
  eitoc_oc_t *o, *n;

  if (em->done) return;
  /* carousels without a root directory in time are not looked at further;
   * a found EPG carousel is read on (OTA timeout) */
  em->refcount++;
  for (o = LIST_FIRST(&em->ocs); o; o = n) {
    n = LIST_NEXT(o, link);
    if (o->state == 0) {
      o->state = -1;
      if (o->mt) mpegts_table_destroy(o->mt);
    }
  }
  em->pmts = 0;
  if (em->epg)
    eitoc_check(em);
  else
    eitoc_finish(em, "no EPG carousel");
  eitoc_mux_unref(em);
}

/* ------------------------------------------------------------------------
 * Module
 * ----------------------------------------------------------------------*/

int
eitoc_start ( epggrab_ota_map_t *map, mpegts_mux_t *mm )
{
  return 0;
}

void
eitoc_handlers ( epggrab_ota_map_t *map, mpegts_mux_t *mm )
{
  epggrab_module_ota_t *mod = map->om_module;
  eitoc_config_t *conf = eit_module_carousel(mod);
  mpegts_service_t *s;
  mpegts_table_t *mt;
  eitoc_mux_t *em;

  if (!conf) return;
  em = calloc(1, sizeof(*em));
  em->refcount = 1;    /* until the end of this function */
  em->map = map;
  em->mod = mod;
  em->conf = conf;
  em->mm = mm;
  LIST_INIT(&em->ocs);

  LIST_FOREACH(s, &mm->mm_services, s_dvb_mux_link) {
    uint16_t pmt = s->s_components.set_pmt_pid;
    if (!pmt || pmt >= 0x1fff) continue;
    mt = mpegts_table_add(mm, 0x02, 0xff, eitoc_pmt_callback, em,
                          mod->id, mod->subsys, MT_CRC,
                          pmt, MPS_WEIGHT_EIT);
    if (!mt || mt->mt_destroy) continue;
    mt->mt_destroy = eitoc_pmt_destroy;
    em->pmts++;
    em->refcount++;
  }
  tvhtrace(mod->subsys, "%s: %s - %d PMTs", mod->id, mm->mm_nicename, em->pmts);
  /* the timer has no reference: it is disarmed when em goes */
  if (em->pmts)
    mtimer_arm_rel(&em->timer, eitoc_timeout, em, sec2mono(EITOC_PROBE_TIME));
  eitoc_mux_unref(em);
}

int
eitoc_tune ( epggrab_ota_map_t *map, epggrab_ota_mux_t *om, mpegts_mux_t *mm )
{
  /* not looked at yet, or the carousel was found here */
  if (!om->om_complete)
    return 1;
  return mm->mm_epg_module_id && !strcmp(mm->mm_epg_module_id, map->om_module->id);
}

#endif /* ENABLE_ZLIB */
