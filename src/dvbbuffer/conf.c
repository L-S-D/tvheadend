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
#include "streaming.h"

dvbbuffer_conf_t dvbbuffer_conf;

static void
dvbbuffer_conf_fixup(void)
{
  if (dvbbuffer_conf.buffer_sec < 2)
    dvbbuffer_conf.buffer_sec = 2;
  if (dvbbuffer_conf.buffer_sec > 120)
    dvbbuffer_conf.buffer_sec = 120;
  if (dvbbuffer_conf.max_bitrate_kbps < 1000)
    dvbbuffer_conf.max_bitrate_kbps = 1000;
  if (dvbbuffer_conf.warm_weight < 1)
    dvbbuffer_conf.warm_weight = 1;
  if (dvbbuffer_conf.burst_kb < 20)
    dvbbuffer_conf.burst_kb = 20;
  if (dvbbuffer_conf.pace_factor < 1)
    dvbbuffer_conf.pace_factor = 1;
  if (dvbbuffer_conf.max_age_ms < 500)
    dvbbuffer_conf.max_age_ms = 500;
  if (dvbbuffer_conf.keyframe_back < 1)
    dvbbuffer_conf.keyframe_back = 1;
  if (dvbbuffer_conf.hls_port < 1 || dvbbuffer_conf.hls_port > 65535)
    dvbbuffer_conf.hls_port = 8890;
  if (dvbbuffer_conf.hls_target_duration < 1)
    dvbbuffer_conf.hls_target_duration = 1;

  /* output queues (HTSP/HTTP) take the burst of a start without dropping */
  if (dvbbuffer_conf.enabled && dvbbuffer_ctx) {
    streaming_start_reserve      = (size_t)dvbbuffer_conf.start_reserve_kb * 1000;
    streaming_start_reserve_time = sec2mono(dvbbuffer_conf.start_reserve_sec);
  } else {
    streaming_start_reserve      = 0;
    streaming_start_reserve_time = 0;
  }
}

static void
dvbbuffer_conf_class_changed(idnode_t *self)
{
  dvbbuffer_conf_fixup();
  /* buffer size / bitrate apply to muxes started from now on */
  dvbbuffer_warm_reconcile();
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
  .ic_perm_def   = ACCESS_ADMIN,
  .ic_changed    = dvbbuffer_conf_class_changed,
  .ic_save       = dvbbuffer_conf_class_save,
  .ic_groups     = (const property_group_t[]) {
    {
      .name   = N_("General"),
      .number = 1,
    },
    {
      .name   = N_("Kodi / tvh streaming"),
      .number = 2,
    },
    {
      .name   = N_("HLS server (webOS)"),
      .number = 3,
    },
    {}
  },
  .ic_properties = (const property_t[]){
    {
      .type   = PT_BOOL,
      .id     = "enabled",
      .name   = N_("Enabled"),
      .desc   = N_("Keep the muxes marked \"Prebuffer\" warm and start "
                   "their services from the ring buffer."),
      .off    = offsetof(dvbbuffer_conf_t, enabled),
      .group  = 1,
    },
    {
      .type   = PT_U32,
      .id     = "buffer_sec",
      .name   = N_("Buffer (seconds)"),
      .desc   = N_("History of the whole transport stream kept in RAM "
                   "per warm mux (applies to muxes started afterwards)."),
      .off    = offsetof(dvbbuffer_conf_t, buffer_sec),
      .group  = 1,
    },
    {
      .type   = PT_U32,
      .id     = "max_bitrate_kbps",
      .name   = N_("Maximum mux bitrate (kbit/s)"),
      .desc   = N_("The ring buffer is sized for this bitrate."),
      .off    = offsetof(dvbbuffer_conf_t, max_bitrate_kbps),
      .opts   = PO_ADVANCED,
      .group  = 1,
    },
    {
      .type   = PT_U32,
      .id     = "warm_weight",
      .name   = N_("Warm mux weight"),
      .desc   = N_("Subscription weight of the warm muxes. Up to 3 the "
                   "OTA EPG grabber may take their tuners, 4 and above "
                   "blocks it. Viewers and recordings always win."),
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
      .id     = "burst_kb",
      .name   = N_("First burst (kB)"),
      .desc   = N_("Backlog delivered at once on service start, the rest "
                   "is paced. Keep it below the start queue reserve."),
      .off    = offsetof(dvbbuffer_conf_t, burst_kb),
      .opts   = PO_ADVANCED,
      .group  = 2,
    },
    {
      .type   = PT_U32,
      .id     = "pace_factor",
      .name   = N_("Catch-up factor"),
      .desc   = N_("After the first burst the backlog is delivered with up "
                   "to this multiple of the live rate."),
      .off    = offsetof(dvbbuffer_conf_t, pace_factor),
      .opts   = PO_ADVANCED,
      .group  = 2,
    },
    {
      .type   = PT_U32,
      .id     = "max_age_ms",
      .name   = N_("Maximum keyframe age (ms)"),
      .desc   = N_("Start from the last keyframe only if it is at most "
                   "this old, otherwise start live."),
      .off    = offsetof(dvbbuffer_conf_t, max_age_ms),
      .opts   = PO_ADVANCED,
      .group  = 2,
    },
    {
      .type   = PT_U32,
      .id     = "keyframe_back",
      .name   = N_("Start keyframe"),
      .desc   = N_("Keyframe to start from, counted back over the complete ones: "
                   "1 = newest complete one, 2 = the one before, ... "
                   "(a keyframe still on air never counts)."),
      .off    = offsetof(dvbbuffer_conf_t, keyframe_back),
      .opts   = PO_EXPERT,
      .group  = 2,
    },
    {
      .type   = PT_U32,
      .id     = "start_reserve_kb",
      .name   = N_("Start queue reserve (kB)"),
      .desc   = N_("A start from the buffer comes as a burst. HTSP and HTTP "
                   "output queues accept this much more data before they "
                   "drop, for the first seconds after the stream started."),
      .off    = offsetof(dvbbuffer_conf_t, start_reserve_kb),
      .opts   = PO_ADVANCED,
      .group  = 2,
    },
    {
      .type   = PT_U32,
      .id     = "start_reserve_sec",
      .name   = N_("Start queue reserve (seconds)"),
      .desc   = N_("How long after the first data of a stream the queue "
                   "reserve applies."),
      .off    = offsetof(dvbbuffer_conf_t, start_reserve_sec),
      .opts   = PO_ADVANCED,
      .group  = 2,
    },
    {
      .type   = PT_U32,
      .id     = "key_wait_ms",
      .name   = N_("Key wait (ms)"),
      .desc   = N_("Encrypted services: wait at most this long for the "
                   "descrambler key before starting live."),
      .off    = offsetof(dvbbuffer_conf_t, key_wait_ms),
      .opts   = PO_ADVANCED,
      .group  = 2,
    },
    {
      .type   = PT_BOOL,
      .id     = "inject_dvr",
      .name   = N_("Also for recordings"),
      .desc   = N_("Start recordings from the last keyframe as well."),
      .off    = offsetof(dvbbuffer_conf_t, inject_dvr),
      .opts   = PO_ADVANCED,
      .group  = 2,
    },
    {
      .type   = PT_BOOL,
      .id     = "hls_enabled",
      .name   = N_("HLS server"),
      .desc   = N_("Own HTTP server with HLS for the webOS app: "
                   "/hls/<channel uuid>/index.m3u8 (changes need a restart)."),
      .off    = offsetof(dvbbuffer_conf_t, hls_enabled),
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_port",
      .name   = N_("HLS port"),
      .desc   = N_("TCP port of the HLS server (changes need a restart)."),
      .off    = offsetof(dvbbuffer_conf_t, hls_port),
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_weight",
      .name   = N_("HLS subscription weight"),
      .desc   = N_("Weight of the mux subscription of an HLS client."),
      .off    = offsetof(dvbbuffer_conf_t, hls_weight),
      .opts   = PO_ADVANCED,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_start_back_ms",
      .name   = N_("HLS start back (ms)"),
      .desc   = N_("A stream starts this far in the past (at a keyframe) - "
                   "the backlog is delivered at once."),
      .off    = offsetof(dvbbuffer_conf_t, hls_start_back_ms),
      .opts   = PO_ADVANCED,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_initial_target_ms",
      .name   = N_("HLS backlog segment length (ms)"),
      .desc   = N_("Minimum length of the segments from the buffer."),
      .off    = offsetof(dvbbuffer_conf_t, hls_initial_target_ms),
      .opts   = PO_EXPERT,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_target_ms",
      .name   = N_("HLS live segment length (ms)"),
      .desc   = N_("Minimum length of the live segments."),
      .off    = offsetof(dvbbuffer_conf_t, hls_target_ms),
      .opts   = PO_EXPERT,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_target_duration",
      .name   = N_("HLS target duration (s)"),
      .desc   = N_("EXT-X-TARGETDURATION, constant."),
      .off    = offsetof(dvbbuffer_conf_t, hls_target_duration),
      .opts   = PO_EXPERT,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_initial_segments",
      .name   = N_("HLS first playlist: segments"),
      .desc   = N_("The first playlist is delivered with at least this "
                   "many segments ..."),
      .off    = offsetof(dvbbuffer_conf_t, hls_initial_segments),
      .opts   = PO_ADVANCED,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_initial_duration_ms",
      .name   = N_("HLS first playlist: duration (ms)"),
      .desc   = N_("... and at least this much content."),
      .off    = offsetof(dvbbuffer_conf_t, hls_initial_duration_ms),
      .opts   = PO_ADVANCED,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_initial_timeout_ms",
      .name   = N_("HLS first playlist: timeout (ms)"),
      .desc   = N_("After this, the first playlist comes with what is there."),
      .off    = offsetof(dvbbuffer_conf_t, hls_initial_timeout_ms),
      .opts   = PO_EXPERT,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_window_ms",
      .name   = N_("HLS playlist window (ms)"),
      .off    = offsetof(dvbbuffer_conf_t, hls_window_ms),
      .opts   = PO_EXPERT,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_keep_ms",
      .name   = N_("HLS segments kept (ms)"),
      .off    = offsetof(dvbbuffer_conf_t, hls_keep_ms),
      .opts   = PO_EXPERT,
      .group  = 3,
    },
    {
      .type   = PT_U32,
      .id     = "hls_idle_ms",
      .name   = N_("HLS idle timeout (ms)"),
      .desc   = N_("A stream without requests ends after this."),
      .off    = offsetof(dvbbuffer_conf_t, hls_idle_ms),
      .opts   = PO_EXPERT,
      .group  = 3,
    },
    {
      .type   = PT_INT,
      .id     = "hls_start_offset_ms",
      .name   = N_("HLS start offset (ms)"),
      .desc   = N_("EXT-X-START TIME-OFFSET (0 = none, negative = from the end)."),
      .off    = offsetof(dvbbuffer_conf_t, hls_start_offset_ms),
      .opts   = PO_EXPERT,
      .group  = 3,
    },
    {
      .type   = PT_BOOL,
      .id     = "hls_blocking_reload",
      .name   = N_("HLS blocking playlist reload"),
      .off    = offsetof(dvbbuffer_conf_t, hls_blocking_reload),
      .opts   = PO_EXPERT,
      .group  = 3,
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
  dvbbuffer_conf.buffer_sec       = 10;
  dvbbuffer_conf.max_bitrate_kbps = 80000;
  dvbbuffer_conf.warm_weight      = 5;
  dvbbuffer_conf.warm_max         = 4;
  dvbbuffer_conf.burst_kb         = 8000;
  dvbbuffer_conf.pace_factor      = 3;
  dvbbuffer_conf.max_age_ms       = 5000;
  dvbbuffer_conf.keyframe_back    = 1;
  dvbbuffer_conf.key_wait_ms      = 3000;
  dvbbuffer_conf.start_reserve_kb  = 8000;
  dvbbuffer_conf.start_reserve_sec = 5;
  dvbbuffer_conf.hls_enabled             = 1;
  dvbbuffer_conf.hls_port                = 8890;
  dvbbuffer_conf.hls_weight              = 150;
  dvbbuffer_conf.hls_start_back_ms       = 8000;
  dvbbuffer_conf.hls_initial_target_ms   = 1000;
  dvbbuffer_conf.hls_target_ms           = 2000;
  dvbbuffer_conf.hls_target_duration     = 4;
  dvbbuffer_conf.hls_initial_segments    = 4;
  dvbbuffer_conf.hls_initial_duration_ms = 6000;
  dvbbuffer_conf.hls_initial_timeout_ms  = 8000;
  dvbbuffer_conf.hls_window_ms           = 30000;
  dvbbuffer_conf.hls_keep_ms             = 60000;
  dvbbuffer_conf.hls_idle_ms             = 30000;
  dvbbuffer_conf.hls_start_offset_ms     = 0;
  dvbbuffer_conf.hls_blocking_reload     = 1;

  idclass_register(&dvbbuffer_conf_class);

  if ((m = hts_settings_load("dvbbuffer/config"))) {
    idnode_load(&dvbbuffer_conf.idnode, m);
    htsmsg_destroy(m);
  }
  dvbbuffer_conf_fixup();

  api_register_all(ah);
}
