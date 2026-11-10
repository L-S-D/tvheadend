/*
 *  Tvheadend - instant channel switching, configuration
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "dvbbuffer_private.h"
#include "settings.h"
#include "api.h"

dvbbuffer_conf_t dvbbuffer_conf;

CLASS_DOC(dvbbuffer)

/* default backlog: live clients need the newest keyframe and the audio up to
 * it (3 s - 2 s misses the keyframe of 1.2-1.4 s GOPs behind the audio-video
 * offset of the mux) */
#define DVBBUFFER_BUFFER_SEC_DEFAULT 3

static void dvbbuffer_conf_slots_apply(void);
static void dvbbuffer_conf_ecm_apply(void);

static void
dvbbuffer_conf_fixup(void)
{
  if (dvbbuffer_conf.buffer_sec < 2)
    dvbbuffer_conf.buffer_sec = 2;
  if (dvbbuffer_conf.buffer_sec > 30)   /* the ring is sized from it: RAM per warm mux */
    dvbbuffer_conf.buffer_sec = 30;
  if (dvbbuffer_conf.max_bitrate_kbps < 1000)
    dvbbuffer_conf.max_bitrate_kbps = 1000;
  if (dvbbuffer_conf.max_bitrate_kbps > 250000)  /* above any DVB transponder */
    dvbbuffer_conf.max_bitrate_kbps = 250000;
  /* below the weights of viewers and recordings, whatever is configured */
  if (dvbbuffer_conf.warm_weight < 1)
    dvbbuffer_conf.warm_weight = 1;
  if (dvbbuffer_conf.warm_weight >= SUBSCRIPTION_PRIO_MIN)
    dvbbuffer_conf.warm_weight = SUBSCRIPTION_PRIO_MIN - 1;
  if (dvbbuffer_conf.lru_weight < 1)
    dvbbuffer_conf.lru_weight = 1;
  if (dvbbuffer_conf.lru_weight >= SUBSCRIPTION_PRIO_MIN)
    dvbbuffer_conf.lru_weight = SUBSCRIPTION_PRIO_MIN - 1;
  if (dvbbuffer_conf.warm_max > 16)     /* one field per warm mux on the page */
    dvbbuffer_conf.warm_max = 16;
  if (dvbbuffer_conf.start_lag_ms > 60000)
    dvbbuffer_conf.start_lag_ms = 60000;
}

uint32_t
dvbbuffer_backlog_packets(void)
{
  /* 25 Mbit/s is above any single service; 1504 bits per packet */
  return dvbbuffer_running ? dvbbuffer_conf.buffer_sec * (25000000u / 1504) : 0;
}

static void
dvbbuffer_conf_class_changed(idnode_t *self)
{
  dvbbuffer_conf_fixup();
  dvbbuffer_conf_slots_apply();
  dvbbuffer_conf_ecm_apply();
  /* buffer size / bitrate apply to muxes started from now on */
  dvbbuffer_warm_reconcile();
  dvbbuffer_camd35_reconf();
}

/*
 * The muxes kept warm, chosen on this page: one field per warm mux (as
 * many as "Maximum warm muxes", more when more muxes are marked), each a
 * view of the mux property "prebuffer" (Configuration -> DVB Inputs ->
 * Muxes); nothing of it is stored here. The order of the fields is kept
 * while tvh runs, muxes marked elsewhere fill the free fields by name.
 * IPTV networks have thousands of muxes: their muxes are offered only
 * once marked at the mux.
 */
#define DVBBUFFER_MUX_SLOTS 16

static char dvbbuffer_conf_slot[DVBBUFFER_MUX_SLOTS][UUID_HEX_SIZE];
static char dvbbuffer_conf_slot_new[DVBBUFFER_MUX_SLOTS][UUID_HEX_SIZE];
static int  dvbbuffer_conf_slot_dirty;

static int
dvbbuffer_conf_mux_offered(mpegts_mux_t *mm, const idclass_t *iptv)
{
  if (mm->mm_prebuffer)
    return 1;
  return iptv == NULL || !idnode_is_instance(&mm->mm_network->mn_id, iptv);
}

static mpegts_mux_t *
dvbbuffer_conf_slot_mux(const char *uuid)
{
  mpegts_mux_t *mm;

  if (uuid == NULL || uuid[0] == '\0')
    return NULL;
  mm = mpegts_mux_find(uuid);
  return mm && mm->mm_prebuffer ? mm : NULL;
}

static int
dvbbuffer_conf_mux_name_cmp(const void *a, const void *b)
{
  return strcasecmp((*(mpegts_mux_t * const *)a)->mm_nicename ?: "",
                    (*(mpegts_mux_t * const *)b)->mm_nicename ?: "");
}

/* the fields as shown: the kept order of the marked muxes, then the
 * other marked muxes by name; returns the number of marked muxes */
static int
dvbbuffer_conf_slots_view(char view[DVBBUFFER_MUX_SLOTS][UUID_HEX_SIZE])
{
  mpegts_network_t *mn;
  mpegts_mux_t *mm, *rest[DVBBUFFER_MUX_SLOTS];
  int i, j, n = 0, nrest = 0, count = 0;

  memset(view, 0, DVBBUFFER_MUX_SLOTS * UUID_HEX_SIZE);
  for (i = 0; i < DVBBUFFER_MUX_SLOTS; i++) {
    if (dvbbuffer_conf_slot_mux(dvbbuffer_conf_slot[i]) == NULL)
      continue;
    for (j = 0; j < i; j++)
      if (!strcmp(dvbbuffer_conf_slot[j], dvbbuffer_conf_slot[i]))
        break;
    if (j == i)
      strcpy(view[i], dvbbuffer_conf_slot[i]);
  }
  LIST_FOREACH(mn, &mpegts_network_all, mn_global_link)
    LIST_FOREACH(mm, &mn->mn_muxes, mm_network_link) {
      char ubuf[UUID_HEX_SIZE];
      if (!mm->mm_prebuffer)
        continue;
      count++;
      idnode_uuid_as_str(&mm->mm_id, ubuf);
      for (i = 0; i < DVBBUFFER_MUX_SLOTS; i++)
        if (!strcmp(view[i], ubuf))
          break;
      if (i == DVBBUFFER_MUX_SLOTS && nrest < DVBBUFFER_MUX_SLOTS)
        rest[nrest++] = mm;
    }
  qsort(rest, nrest, sizeof(rest[0]), dvbbuffer_conf_mux_name_cmp);
  for (i = 0; i < DVBBUFFER_MUX_SLOTS && n < nrest; i++)
    if (view[i][0] == '\0')
      idnode_uuid_as_str(&rest[n++]->mm_id, view[i]);
  return count;
}

static const void *
dvbbuffer_conf_slot_get(int slot)
{
  static char view[DVBBUFFER_MUX_SLOTS][UUID_HEX_SIZE];
  static const char *ret;

  dvbbuffer_conf_slots_view(view);
  ret = view[slot];
  return &ret;
}

static int
dvbbuffer_conf_slot_set(int slot, const void *v)
{
  const char *uuid = v ?: "";

  /* the form sends the changed fields only: the others stay as shown */
  if (!dvbbuffer_conf_slot_dirty) {
    dvbbuffer_conf_slots_view(dvbbuffer_conf_slot_new);
    dvbbuffer_conf_slot_dirty = 1;
  }
  if (!strcmp(dvbbuffer_conf_slot_new[slot], uuid))
    return 0;
  strlcpy(dvbbuffer_conf_slot_new[slot], uuid, UUID_HEX_SIZE);
  return 1;
}

static uint32_t
dvbbuffer_conf_slot_opts(int slot, uint32_t opts)
{
  char view[DVBBUFFER_MUX_SLOTS][UUID_HEX_SIZE];
  int n = dvbbuffer_conf_slots_view(view);

  if (n < (int)dvbbuffer_conf.warm_max)
    n = dvbbuffer_conf.warm_max;
  return slot < n ? opts : opts | PO_HIDDEN | PO_NOUI;
}

/* the changed fields to the muxes (from dvbbuffer_conf_class_changed) */
static void
dvbbuffer_conf_slots_apply(void)
{
  mpegts_network_t *mn;
  mpegts_mux_t *mm;
  char ubuf[UUID_HEX_SIZE];
  int i, want;

  if (!dvbbuffer_conf_slot_dirty)
    return;
  dvbbuffer_conf_slot_dirty = 0;
  LIST_FOREACH(mn, &mpegts_network_all, mn_global_link)
    LIST_FOREACH(mm, &mn->mn_muxes, mm_network_link) {
      idnode_uuid_as_str(&mm->mm_id, ubuf);
      want = 0;
      for (i = 0; i < DVBBUFFER_MUX_SLOTS; i++)
        if (!strcmp(dvbbuffer_conf_slot_new[i], ubuf))
          want = 1;
      if (!want == !mm->mm_prebuffer)
        continue;
      mm->mm_prebuffer = want;
      idnode_changed(&mm->mm_id);
      dvbbuffer_mux_prebuffer_notify(mm, NULL);
    }
  memcpy(dvbbuffer_conf_slot, dvbbuffer_conf_slot_new, sizeof(dvbbuffer_conf_slot));
}

#define DVBBUFFER_SLOT_FUNCS(i) \
  static const void *dvbbuffer_conf_slot_get_##i(void *o) \
    { return dvbbuffer_conf_slot_get(i); } \
  static int dvbbuffer_conf_slot_set_##i(void *o, const void *v) \
    { return dvbbuffer_conf_slot_set(i, v); } \
  static uint32_t dvbbuffer_conf_slot_opts_##i(void *o, uint32_t opts) \
    { return dvbbuffer_conf_slot_opts(i, opts); }
DVBBUFFER_SLOT_FUNCS(0)  DVBBUFFER_SLOT_FUNCS(1)  DVBBUFFER_SLOT_FUNCS(2)
DVBBUFFER_SLOT_FUNCS(3)  DVBBUFFER_SLOT_FUNCS(4)  DVBBUFFER_SLOT_FUNCS(5)
DVBBUFFER_SLOT_FUNCS(6)  DVBBUFFER_SLOT_FUNCS(7)  DVBBUFFER_SLOT_FUNCS(8)
DVBBUFFER_SLOT_FUNCS(9)  DVBBUFFER_SLOT_FUNCS(10) DVBBUFFER_SLOT_FUNCS(11)
DVBBUFFER_SLOT_FUNCS(12) DVBBUFFER_SLOT_FUNCS(13) DVBBUFFER_SLOT_FUNCS(14)
DVBBUFFER_SLOT_FUNCS(15)

/*
 * ECM prefetch per warm mux: one field below each warm mux, a choice of
 * the CA PIDs of its services; the same as "ECM prefetch PIDs" at the mux
 * (nothing stored here). The options are those of the mux shown when the
 * page was loaded: after choosing another mux, save and reload.
 */
static char dvbbuffer_conf_ecm_view[DVBBUFFER_MUX_SLOTS][UUID_HEX_SIZE];
static htsmsg_t *dvbbuffer_conf_ecm_new[DVBBUFFER_MUX_SLOTS];
static int  dvbbuffer_conf_ecm_dirty;

static mpegts_mux_t *
dvbbuffer_conf_ecm_mux(int slot)
{
  char view[DVBBUFFER_MUX_SLOTS][UUID_HEX_SIZE];

  dvbbuffer_conf_slots_view(view);
  return dvbbuffer_conf_slot_mux(view[slot]);
}

/* the PIDs of the mux property as a list of decimal strings */
static htsmsg_t *
dvbbuffer_conf_ecm_pids(const char *str)
{
  htsmsg_t *l = htsmsg_create_list();
  char buf[8];
  char *end;
  long v;

  while (str && *str) {
    while (*str && (*str == ' ' || *str == ',' || *str == ';'))
      str++;
    if (!*str)
      break;
    v = strtol(str, &end, 0);
    if (end == str)
      break;
    if (v > 0 && v < 0x1fff) {
      snprintf(buf, sizeof(buf), "%ld", v);
      htsmsg_add_str(l, NULL, buf);
    }
    str = end;
  }
  return l;
}

static const void *
dvbbuffer_conf_ecm_get(int slot)
{
  mpegts_mux_t *mm = dvbbuffer_conf_ecm_mux(slot);

  return dvbbuffer_conf_ecm_pids(mm ? mm->mm_ecm_prefetch : NULL);
}

static int
dvbbuffer_conf_ecm_set(int slot, const void *v)
{
  if (!dvbbuffer_conf_ecm_dirty) {
    dvbbuffer_conf_slots_view(dvbbuffer_conf_ecm_view);
    dvbbuffer_conf_ecm_dirty = 1;
  }
  htsmsg_destroy(dvbbuffer_conf_ecm_new[slot]);
  dvbbuffer_conf_ecm_new[slot] = v ? htsmsg_copy((htsmsg_t *)v) : htsmsg_create_list();
  return 1;
}

/* the CA PIDs of the services of the mux in this field, one entry per PID:
 * "CAID 0D98, PID 2501: ORF2W HD, ORF2N HD, ...", sorted by CAID and PID
 * (regional services usually share one ECM PID) */
#define DVBBUFFER_ECM_ENUM_MAX 64

typedef struct {
  uint16_t pid;
  uint16_t caid;     /* the first one, for the order */
  char     caids[48];
  char     names[200];
} dvbbuffer_conf_ecm_entry_t;

static int
dvbbuffer_conf_ecm_entry_cmp(const void *a, const void *b)
{
  const dvbbuffer_conf_ecm_entry_t *x = a, *y = b;

  if (x->caid != y->caid)
    return x->caid < y->caid ? -1 : 1;
  return (int)x->pid - (int)y->pid;
}

/* "a, b, c", ending with "..." when the next one does not fit */
static void
dvbbuffer_conf_ecm_append(char *buf, size_t size, const char *sep, const char *str)
{
  size_t len = strlen(buf), need = (len ? strlen(sep) : 0) + strlen(str);

  if (len >= 3 && !strcmp(buf + len - 3, "..."))
    return;
  if (len + need + 4 < size)
    snprintf(buf + len, size - len, "%s%s", len ? sep : "", str);
  else if (len + 5 < size)
    strcpy(buf + len, len ? ", ..." : "...");
}

static htsmsg_t *
dvbbuffer_conf_ecm_enum(int slot)
{
  mpegts_mux_t *mm = dvbbuffer_conf_ecm_mux(slot);
  mpegts_service_t *s;
  elementary_stream_t *st;
  caid_t *c;
  dvbbuffer_conf_ecm_entry_t *e = NULL;
  htsmsg_t *l = htsmsg_create_list();
  int i, n = 0;
  char key[8], val[300], buf[8];

  if (mm == NULL)
    return l;
  e = calloc(DVBBUFFER_ECM_ENUM_MAX, sizeof(*e));
  if (e == NULL)
    return l;
  LIST_FOREACH(s, &mm->mm_services, s_dvb_mux_link) {
    tvh_mutex_lock(&s->s_stream_mutex);
    TAILQ_FOREACH(st, &s->s_components.set_all, es_link) {
      if (st->es_type != SCT_CA)
        continue;
      for (i = 0; i < n; i++)
        if (e[i].pid == st->es_pid)
          break;
      if (i == n) {
        if (n == DVBBUFFER_ECM_ENUM_MAX)
          continue;
        n++;
        e[i].pid = st->es_pid;
        e[i].caid = 0xffff;
        LIST_FOREACH(c, &st->es_caids, link) {
          if (e[i].caid == 0xffff)
            e[i].caid = c->caid;
          snprintf(buf, sizeof(buf), "%04X", c->caid);
          dvbbuffer_conf_ecm_append(e[i].caids, sizeof(e[i].caids), "/", buf);
        }
      }
      if (s->s_dvb_svcname)
        dvbbuffer_conf_ecm_append(e[i].names, sizeof(e[i].names), ", ", s->s_dvb_svcname);
    }
    tvh_mutex_unlock(&s->s_stream_mutex);
  }
  qsort(e, n, sizeof(*e), dvbbuffer_conf_ecm_entry_cmp);
  for (i = 0; i < n; i++) {
    snprintf(key, sizeof(key), "%d", e[i].pid);
    snprintf(val, sizeof(val), "CAID %s, PID %d: %s",
             e[i].caids[0] ? e[i].caids : "?", e[i].pid, e[i].names[0] ? e[i].names : "?");
    htsmsg_add_msg(l, NULL, htsmsg_create_key_val(key, val));
  }
  free(e);
  return l;
}

static uint32_t
dvbbuffer_conf_ecm_opts(int slot, uint32_t opts)
{
  opts = dvbbuffer_conf_slot_opts(slot, opts);
  return dvbbuffer_conf_ecm_mux(slot) ? opts : opts | PO_HIDDEN | PO_NOUI;
}

/* the changed fields to the muxes, only where the field's mux is still
 * the one shown (from dvbbuffer_conf_class_changed) */
static void
dvbbuffer_conf_ecm_apply(void)
{
  char view[DVBBUFFER_MUX_SLOTS][UUID_HEX_SIZE];
  mpegts_mux_t *mm;
  htsmsg_field_t *f;
  char *str;
  size_t len, size;
  int i;

  if (!dvbbuffer_conf_ecm_dirty)
    return;
  dvbbuffer_conf_ecm_dirty = 0;
  dvbbuffer_conf_slots_view(view);
  for (i = 0; i < DVBBUFFER_MUX_SLOTS; i++) {
    htsmsg_t *l = dvbbuffer_conf_ecm_new[i];
    dvbbuffer_conf_ecm_new[i] = NULL;
    if (l == NULL)
      continue;
    mm = dvbbuffer_conf_slot_mux(view[i]);
    if (mm && !strcmp(view[i], dvbbuffer_conf_ecm_view[i])) {
      str = NULL;
      len = size = 0;
      HTSMSG_FOREACH(f, l) {
        const char *pid = htsmsg_field_get_str(f);
        size_t n = pid ? strlen(pid) : 0;
        if (n == 0 || len + n + 2 > 512)
          continue;
        if (len + n + 2 > size) {
          size = 512;
          if ((str = realloc(str, size)) == NULL)
            break;
        }
        len += snprintf(str + len, size - len, "%s%s", len ? "," : "", pid);
      }
      if (strcmp(str ?: "", mm->mm_ecm_prefetch ?: "")) {
        free(mm->mm_ecm_prefetch);
        mm->mm_ecm_prefetch = str;
        str = NULL;
        idnode_changed(&mm->mm_id);
        dvbbuffer_mux_prefetch_notify(mm, NULL);
      }
      free(str);
    }
    htsmsg_destroy(l);
  }
}

#define DVBBUFFER_ECM_FUNCS(i) \
  static const void *dvbbuffer_conf_ecm_get_##i(void *o) \
    { return dvbbuffer_conf_ecm_get(i); } \
  static int dvbbuffer_conf_ecm_set_##i(void *o, const void *v) \
    { return dvbbuffer_conf_ecm_set(i, v); } \
  static uint32_t dvbbuffer_conf_ecm_opts_##i(void *o, uint32_t opts) \
    { return dvbbuffer_conf_ecm_opts(i, opts); } \
  static htsmsg_t *dvbbuffer_conf_ecm_enum_##i(void *o, const char *lang) \
    { return dvbbuffer_conf_ecm_enum(i); }
DVBBUFFER_ECM_FUNCS(0)  DVBBUFFER_ECM_FUNCS(1)  DVBBUFFER_ECM_FUNCS(2)
DVBBUFFER_ECM_FUNCS(3)  DVBBUFFER_ECM_FUNCS(4)  DVBBUFFER_ECM_FUNCS(5)
DVBBUFFER_ECM_FUNCS(6)  DVBBUFFER_ECM_FUNCS(7)  DVBBUFFER_ECM_FUNCS(8)
DVBBUFFER_ECM_FUNCS(9)  DVBBUFFER_ECM_FUNCS(10) DVBBUFFER_ECM_FUNCS(11)
DVBBUFFER_ECM_FUNCS(12) DVBBUFFER_ECM_FUNCS(13) DVBBUFFER_ECM_FUNCS(14)
DVBBUFFER_ECM_FUNCS(15)

static htsmsg_t *
dvbbuffer_conf_class_muxes_enum(void *o, const char *lang)
{
  htsmsg_t *m = htsmsg_create_map();

  htsmsg_add_str(m, "type", "api");
  htsmsg_add_str(m, "uri",  "dvbbuffer/mux/list");
  htsmsg_add_str(m, "event", "mpegts_mux");
  return m;
}

typedef struct {
  char key[UUID_HEX_SIZE];
  char val[256];
} dvbbuffer_conf_mux_entry_t;

static int
dvbbuffer_conf_mux_entry_cmp(const void *a, const void *b)
{
  return strcasecmp(((const dvbbuffer_conf_mux_entry_t *)a)->val,
                    ((const dvbbuffer_conf_mux_entry_t *)b)->val);
}

/* the muxes to choose from, "network: mux" sorted by name */
static int
dvbbuffer_conf_api_mux_list
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
  const idclass_t *iptv = idclass_find("iptv_network");
  mpegts_network_t *mn;
  mpegts_mux_t *mm;
  dvbbuffer_conf_mux_entry_t *e = NULL;
  htsmsg_t *l;
  size_t n = 0, size = 0, i, len;
  char nbuf[128];

  tvh_mutex_lock(&global_lock);
  LIST_FOREACH(mn, &mpegts_network_all, mn_global_link) {
    nbuf[0] = '\0';
    if (mn->mn_display_name)
      mn->mn_display_name(mn, nbuf, sizeof(nbuf));
    LIST_FOREACH(mm, &mn->mn_muxes, mm_network_link) {
      if (!dvbbuffer_conf_mux_offered(mm, iptv))
        continue;
      if (n == size) {
        size = size ? size * 2 : 256;
        e = realloc(e, size * sizeof(*e));
        if (e == NULL) {
          tvh_mutex_unlock(&global_lock);
          return ENOMEM;
        }
      }
      idnode_uuid_as_str(&mm->mm_id, e[n].key);
      len = snprintf(e[n].val, sizeof(e[n].val), "%s: ", nbuf);
      if (len < sizeof(e[n].val) && mm->mm_display_name)
        mm->mm_display_name(mm, e[n].val + len, sizeof(e[n].val) - len);
      n++;
    }
  }
  tvh_mutex_unlock(&global_lock);

  if (n)
    qsort(e, n, sizeof(*e), dvbbuffer_conf_mux_entry_cmp);
  l = htsmsg_create_list();
  for (i = 0; i < n; i++)
    htsmsg_add_msg(l, NULL, htsmsg_create_key_val(e[i].key, e[i].val));
  free(e);
  *resp = htsmsg_create_map();
  htsmsg_add_msg(*resp, "entries", l);
  return 0;
}

static htsmsg_t *
dvbbuffer_conf_class_save(idnode_t *self, char *filename, size_t fsize)
{
  htsmsg_t *m = htsmsg_create_map();
  idnode_save(&dvbbuffer_conf.idnode, m);
  if (filename)
    snprintf(filename, fsize, "dvbbuffer/config");
  return m;
}

const idclass_t dvbbuffer_conf_class = {
  .ic_snode      = &dvbbuffer_conf.idnode,
  .ic_class      = "dvbbuffer",
  .ic_caption    = N_("Instant zapping"),
  .ic_event      = "dvbbuffer",
  .ic_doc        = tvh_doc_dvbbuffer_class,
  .ic_perm_def   = ACCESS_ADMIN,
  .ic_changed    = dvbbuffer_conf_class_changed,
  .ic_save       = dvbbuffer_conf_class_save,
  .ic_groups     = (const property_group_t[]) {
    { .name = N_("General"),                 .number = 1 },
    { .name = N_("Recently used muxes"),     .number = 2 },
    { .name = N_("Live start"),              .number = 3 },
    { .name = N_("ECM prefetch (camd35)"),   .number = 4 },
    {}
  },
  .ic_properties = (const property_t[]){
    {
      .type   = PT_BOOL,
      .id     = "enabled",
      .name   = N_("Enabled"),
      .desc   = N_("Keep the muxes marked \"Prebuffer\" tuned, each with a "
                   "ring buffer of its whole transport stream."),
      .off    = offsetof(dvbbuffer_conf_t, enabled),
      .group  = 1,
    },
    {
      .type   = PT_U32,
      .id     = "buffer_sec",
      .name   = N_("Buffer (seconds)"),
      .desc   = N_("History of the whole transport stream kept in RAM per "
                   "warm mux; a service started on it gets this much "
                   "backlog. Keep it below the shortest key period of the "
                   "pay TV services (7 s, e.g. HD+ and Sky): the backlog "
                   "then holds one key change at most. Live clients need "
                   "about 3 s (newest keyframe with its audio). Applies to "
                   "muxes started afterwards."),
      .off    = offsetof(dvbbuffer_conf_t, buffer_sec),
      .group  = 1,
    },
    {
      .type   = PT_U32,
      .id     = "max_bitrate_kbps",
      .name   = N_("Maximum mux bitrate (kbit/s)"),
      .desc   = N_("The ring buffer is sized for the bitrate of the mux "
                   "computed from its tuning parameters, at most this "
                   "much; for this bitrate if they are unknown."),
      .off    = offsetof(dvbbuffer_conf_t, max_bitrate_kbps),
      .opts   = PO_ADVANCED,
      .group  = 1,
    },
    {
      .type   = PT_U32,
      .id     = "warm_weight",
      .name   = N_("Warm mux weight"),
      .desc   = N_("Subscription weight of the warm muxes (1 to 9). Up to "
                   "3 the OTA EPG grabber may take their tuners, 4 and "
                   "above blocks it; from 5 on initial scans cannot take "
                   "them either. Viewers and recordings always win."),
      .off    = offsetof(dvbbuffer_conf_t, warm_weight),
      .opts   = PO_ADVANCED,
      .group  = 1,
    },
    {
      .type   = PT_U32,
      .id     = "warm_max",
      .name   = N_("Maximum warm muxes"),
      .desc   = N_("At most this many muxes are kept warm (up to 16)."),
      .off    = offsetof(dvbbuffer_conf_t, warm_max),
      .group  = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_1",
      .name     = N_("Warm mux 1"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_0,
      .set      = dvbbuffer_conf_slot_set_0,
      .get_opts = dvbbuffer_conf_slot_opts_0,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_2",
      .name     = N_("Warm mux 2"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_1,
      .set      = dvbbuffer_conf_slot_set_1,
      .get_opts = dvbbuffer_conf_slot_opts_1,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_3",
      .name     = N_("Warm mux 3"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_2,
      .set      = dvbbuffer_conf_slot_set_2,
      .get_opts = dvbbuffer_conf_slot_opts_2,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_4",
      .name     = N_("Warm mux 4"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_3,
      .set      = dvbbuffer_conf_slot_set_3,
      .get_opts = dvbbuffer_conf_slot_opts_3,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_5",
      .name     = N_("Warm mux 5"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_4,
      .set      = dvbbuffer_conf_slot_set_4,
      .get_opts = dvbbuffer_conf_slot_opts_4,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_6",
      .name     = N_("Warm mux 6"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_5,
      .set      = dvbbuffer_conf_slot_set_5,
      .get_opts = dvbbuffer_conf_slot_opts_5,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_7",
      .name     = N_("Warm mux 7"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_6,
      .set      = dvbbuffer_conf_slot_set_6,
      .get_opts = dvbbuffer_conf_slot_opts_6,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_8",
      .name     = N_("Warm mux 8"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_7,
      .set      = dvbbuffer_conf_slot_set_7,
      .get_opts = dvbbuffer_conf_slot_opts_7,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_9",
      .name     = N_("Warm mux 9"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_8,
      .set      = dvbbuffer_conf_slot_set_8,
      .get_opts = dvbbuffer_conf_slot_opts_8,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_10",
      .name     = N_("Warm mux 10"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_9,
      .set      = dvbbuffer_conf_slot_set_9,
      .get_opts = dvbbuffer_conf_slot_opts_9,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_11",
      .name     = N_("Warm mux 11"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_10,
      .set      = dvbbuffer_conf_slot_set_10,
      .get_opts = dvbbuffer_conf_slot_opts_10,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_12",
      .name     = N_("Warm mux 12"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_11,
      .set      = dvbbuffer_conf_slot_set_11,
      .get_opts = dvbbuffer_conf_slot_opts_11,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_13",
      .name     = N_("Warm mux 13"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_12,
      .set      = dvbbuffer_conf_slot_set_12,
      .get_opts = dvbbuffer_conf_slot_opts_12,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_14",
      .name     = N_("Warm mux 14"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_13,
      .set      = dvbbuffer_conf_slot_set_13,
      .get_opts = dvbbuffer_conf_slot_opts_13,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_15",
      .name     = N_("Warm mux 15"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_14,
      .set      = dvbbuffer_conf_slot_set_14,
      .get_opts = dvbbuffer_conf_slot_opts_14,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type     = PT_STR,
      .id       = "prebuffer_mux_16",
      .name     = N_("Warm mux 16"),
      .desc     = N_("A mux kept warm, the same as \"Prebuffer (instant "
                     "zapping)\" at the mux (Configuration -> DVB Inputs -> "
                     "Muxes). There is one field per warm mux (\"Maximum warm "
                     "muxes\", save and reload for more). Muxes of IPTV "
                     "networks are listed only when marked there."),
      .get      = dvbbuffer_conf_slot_get_15,
      .set      = dvbbuffer_conf_slot_set_15,
      .get_opts = dvbbuffer_conf_slot_opts_15,
      .list     = dvbbuffer_conf_class_muxes_enum,
      .opts     = PO_NOSAVE,
      .group    = 1,
    },
    {
      .type   = PT_U32,
      .id     = "lru_max",
      .name   = N_("Recently used muxes kept warm"),
      .desc   = N_("Besides the prebuffer muxes, this many of the muxes "
                   "last watched stay tuned with a ring buffer (it starts "
                   "while watching), so zapping back starts from the "
                   "buffer. 0 disables it."),
      .off    = offsetof(dvbbuffer_conf_t, lru_max),
      .group  = 2,
    },
    {
      .type   = PT_U32,
      .id     = "lru_weight",
      .name   = N_("Recently used mux weight"),
      .desc   = N_("Subscription weight of the recently used muxes, "
                   "normally below the warm mux weight: they give their "
                   "tuners away first."),
      .off    = offsetof(dvbbuffer_conf_t, lru_weight),
      .opts   = PO_ADVANCED,
      .group  = 2,
    },
    {
      .type   = PT_U32,
      .id     = "start_lag_ms",
      .name   = N_("Live start behind live (ms)"),
      .desc   = N_("After zapping, live clients (profiles with \"Instant "
                   "zapping: start live\") get the stream from the newest "
                   "keyframe at least this far behind live instead of the "
                   "whole backlog. 0 = the newest keyframe; more gives the "
                   "player that much buffer at once."),
      .off    = offsetof(dvbbuffer_conf_t, start_lag_ms),
      .group  = 3,
    },
    {
      .type   = PT_BOOL,
      .id     = "camd35_enabled",
      .name   = N_("ECM prefetch (camd35)"),
      .desc   = N_("Send the ECMs of the PIDs listed at the prebuffer muxes "
                   "(\"ECM prefetch PIDs\") to this camd35 (UDP) server as "
                   "they change, the answers are not used: the keys of these "
                   "services are in the server's cache when they start."),
      .off    = offsetof(dvbbuffer_conf_t, camd35_enabled),
      .opts   = PO_ADVANCED,
      .group  = 4,
    },
    {
      .type   = PT_STR,
      .id     = "camd35_host",
      .name   = N_("camd35 host"),
      .off    = offsetof(dvbbuffer_conf_t, camd35_host),
      .opts   = PO_ADVANCED,
      .group  = 4,
    },
    {
      .type   = PT_U32,
      .id     = "camd35_port",
      .name   = N_("camd35 port (UDP)"),
      .off    = offsetof(dvbbuffer_conf_t, camd35_port),
      .opts   = PO_ADVANCED,
      .group  = 4,
    },
    {
      .type   = PT_STR,
      .id     = "camd35_user",
      .name   = N_("camd35 user"),
      .off    = offsetof(dvbbuffer_conf_t, camd35_user),
      .opts   = PO_ADVANCED,
      .group  = 4,
    },
    {
      .type   = PT_STR,
      .id     = "camd35_pass",
      .name   = N_("camd35 password"),
      .off    = offsetof(dvbbuffer_conf_t, camd35_pass),
      .opts   = PO_ADVANCED | PO_PASSWORD,
      .group  = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_1",
      .name     = N_("Warm mux 1: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 1 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_0,
      .set      = dvbbuffer_conf_ecm_set_0,
      .get_opts = dvbbuffer_conf_ecm_opts_0,
      .list     = dvbbuffer_conf_ecm_enum_0,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_2",
      .name     = N_("Warm mux 2: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 2 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_1,
      .set      = dvbbuffer_conf_ecm_set_1,
      .get_opts = dvbbuffer_conf_ecm_opts_1,
      .list     = dvbbuffer_conf_ecm_enum_1,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_3",
      .name     = N_("Warm mux 3: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 3 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_2,
      .set      = dvbbuffer_conf_ecm_set_2,
      .get_opts = dvbbuffer_conf_ecm_opts_2,
      .list     = dvbbuffer_conf_ecm_enum_2,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_4",
      .name     = N_("Warm mux 4: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 4 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_3,
      .set      = dvbbuffer_conf_ecm_set_3,
      .get_opts = dvbbuffer_conf_ecm_opts_3,
      .list     = dvbbuffer_conf_ecm_enum_3,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_5",
      .name     = N_("Warm mux 5: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 5 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_4,
      .set      = dvbbuffer_conf_ecm_set_4,
      .get_opts = dvbbuffer_conf_ecm_opts_4,
      .list     = dvbbuffer_conf_ecm_enum_4,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_6",
      .name     = N_("Warm mux 6: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 6 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_5,
      .set      = dvbbuffer_conf_ecm_set_5,
      .get_opts = dvbbuffer_conf_ecm_opts_5,
      .list     = dvbbuffer_conf_ecm_enum_5,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_7",
      .name     = N_("Warm mux 7: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 7 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_6,
      .set      = dvbbuffer_conf_ecm_set_6,
      .get_opts = dvbbuffer_conf_ecm_opts_6,
      .list     = dvbbuffer_conf_ecm_enum_6,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_8",
      .name     = N_("Warm mux 8: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 8 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_7,
      .set      = dvbbuffer_conf_ecm_set_7,
      .get_opts = dvbbuffer_conf_ecm_opts_7,
      .list     = dvbbuffer_conf_ecm_enum_7,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_9",
      .name     = N_("Warm mux 9: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 9 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_8,
      .set      = dvbbuffer_conf_ecm_set_8,
      .get_opts = dvbbuffer_conf_ecm_opts_8,
      .list     = dvbbuffer_conf_ecm_enum_8,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_10",
      .name     = N_("Warm mux 10: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 10 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_9,
      .set      = dvbbuffer_conf_ecm_set_9,
      .get_opts = dvbbuffer_conf_ecm_opts_9,
      .list     = dvbbuffer_conf_ecm_enum_9,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_11",
      .name     = N_("Warm mux 11: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 11 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_10,
      .set      = dvbbuffer_conf_ecm_set_10,
      .get_opts = dvbbuffer_conf_ecm_opts_10,
      .list     = dvbbuffer_conf_ecm_enum_10,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_12",
      .name     = N_("Warm mux 12: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 12 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_11,
      .set      = dvbbuffer_conf_ecm_set_11,
      .get_opts = dvbbuffer_conf_ecm_opts_11,
      .list     = dvbbuffer_conf_ecm_enum_11,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_13",
      .name     = N_("Warm mux 13: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 13 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_12,
      .set      = dvbbuffer_conf_ecm_set_12,
      .get_opts = dvbbuffer_conf_ecm_opts_12,
      .list     = dvbbuffer_conf_ecm_enum_12,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_14",
      .name     = N_("Warm mux 14: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 14 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_13,
      .set      = dvbbuffer_conf_ecm_set_13,
      .get_opts = dvbbuffer_conf_ecm_opts_13,
      .list     = dvbbuffer_conf_ecm_enum_13,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_15",
      .name     = N_("Warm mux 15: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 15 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_14,
      .set      = dvbbuffer_conf_ecm_set_14,
      .get_opts = dvbbuffer_conf_ecm_opts_14,
      .list     = dvbbuffer_conf_ecm_enum_14,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {
      .type     = PT_STR,
      .islist   = 1,
      .id       = "ecm_prefetch_16",
      .name     = N_("Warm mux 16: ECM prefetch"),
      .desc     = N_("The CA PIDs of warm mux 16 whose ECMs are sent to the "
                     "camd35 server, the same as \"ECM prefetch PIDs\" at "
                     "the mux. After choosing another warm mux, save and "
                     "reload for its PIDs."),
      .get      = dvbbuffer_conf_ecm_get_15,
      .set      = dvbbuffer_conf_ecm_set_15,
      .get_opts = dvbbuffer_conf_ecm_opts_15,
      .list     = dvbbuffer_conf_ecm_enum_15,
      .opts     = PO_NOSAVE | PO_ADVANCED,
      .group    = 4,
    },
    {}
  }
};

void
dvbbuffer_conf_init(void)
{
  static api_hook_t ah[] = {
    { "dvbbuffer/config/load", ACCESS_ADMIN, api_idnode_load_simple, &dvbbuffer_conf },
    { "dvbbuffer/config/save", ACCESS_ADMIN, api_idnode_save_simple, &dvbbuffer_conf },
    { "dvbbuffer/mux/list",    ACCESS_ADMIN, dvbbuffer_conf_api_mux_list, NULL },
    { NULL },
  };
  htsmsg_t *m;

  memset(&dvbbuffer_conf, 0, sizeof(dvbbuffer_conf));
  dvbbuffer_conf.idnode.in_class  = &dvbbuffer_conf_class;
  dvbbuffer_conf.enabled          = 1;
  dvbbuffer_conf.buffer_sec       = DVBBUFFER_BUFFER_SEC_DEFAULT;
  dvbbuffer_conf.max_bitrate_kbps = 80000;
  dvbbuffer_conf.warm_weight      = 5;
  dvbbuffer_conf.warm_max         = 4;
  dvbbuffer_conf.lru_max          = 0;  /* holds tuners: opt in */
  dvbbuffer_conf.lru_weight       = 3;

  idclass_register(&dvbbuffer_conf_class);

  if ((m = hts_settings_load("dvbbuffer/config"))) {
    idnode_load(&dvbbuffer_conf.idnode, m);
    htsmsg_destroy(m);
  }
  dvbbuffer_conf_fixup();

  api_register_all(ah);
}
