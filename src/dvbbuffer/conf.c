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
  /* buffer size / bitrate apply to muxes started from now on */
  dvbbuffer_warm_reconcile();
  dvbbuffer_camd35_reconf();
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
      .desc   = N_("At most this many muxes are kept warm."),
      .off    = offsetof(dvbbuffer_conf_t, warm_max),
      .group  = 1,
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
    {}
  }
};

void
dvbbuffer_conf_init(void)
{
  static api_hook_t ah[] = {
    { "dvbbuffer/config/load", ACCESS_ADMIN, api_idnode_load_simple, &dvbbuffer_conf },
    { "dvbbuffer/config/save", ACCESS_ADMIN, api_idnode_save_simple, &dvbbuffer_conf },
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
