/*
 *  Tvheadend - instant channel switching (libdvbbuffer glue), internals
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#ifndef __TVH_DVBBUFFER_PRIVATE_H__
#define __TVH_DVBBUFFER_PRIVATE_H__

#include "tvheadend.h"
#include "idnode.h"
#include "input.h"
#include "tvh_dvbbuffer.h"

#include <dvbbuffer/dvbbuffer.h>

/*
 * Configuration (dvbbuffer/config), global_lock
 */
typedef struct dvbbuffer_conf {
  idnode_t idnode;
  int      enabled;
  uint32_t buffer_sec;        /* history kept per warm mux */
  uint32_t max_bitrate_kbps;  /* ring is sized for this rate */
  uint32_t warm_weight;       /* subscription weight of warm muxes */
  uint32_t warm_max;          /* max. number of warm muxes */
  uint32_t lru_max;           /* recently used muxes kept warm too */
  uint32_t lru_weight;        /* ... with this subscription weight */
  uint32_t burst_kb;          /* first injection burst */
  uint32_t pace_factor;       /* then up to k x live bytes */
  uint32_t max_age_ms;        /* keyframe may be at most this old */
  uint32_t keyframe_back;     /* start keyframe: 1 newest, 2 the one before */
  uint32_t key_wait_ms;       /* pay-TV: wait for tvh's key, then live */
  uint32_t start_reserve_kb;  /* extra output queue room at stream start */
  uint32_t start_reserve_sec; /* ... for this long after the first data */
  int      inject_dvr;        /* also inject for recordings */
  /* HLS server (webOS path) */
  int      hls_enabled;
  uint32_t hls_port;
  uint32_t hls_weight;        /* mux subscription weight of HLS clients */
  uint32_t hls_start_back_ms;
  uint32_t hls_initial_target_ms;
  uint32_t hls_target_ms;
  uint32_t hls_target_duration;
  uint32_t hls_initial_segments;
  uint32_t hls_initial_duration_ms;
  uint32_t hls_initial_timeout_ms;
  uint32_t hls_window_ms;
  uint32_t hls_keep_ms;
  uint32_t hls_idle_ms;
  int      hls_start_offset_ms;
  int      hls_blocking_reload;
  char    *hls_audio_langs;   /* preferred audio languages, "ger,eng" */
  uint32_t hls_cold_backlog_ms;     /* cold start: less backlog than this */
  uint32_t hls_cold_segments;       /* ... first playlist after this many segments */
  uint32_t hls_cold_short_segments; /* ... this many short live segments */
  uint32_t hls_cold_first_ms;       /* ... first segment cut early (0 = off) */
  int      hls_timeshift;           /* always record every HLS stream */
  uint32_t hls_timeshift_min;       /* ... this many minutes per stream */
  char    *hls_timeshift_dir;       /* ... beyond the RAM budget on disk here */
  uint32_t hls_timeshift_ram_mb;    /* RAM budget, all streams (0 = auto) */
  uint32_t hls_timeshift_disk_gb;   /* disk budget, all streams */
  char    *hls_oscam_host;    /* scrambled channels: OSCam dvbapi (protocol 2) */
  uint32_t hls_oscam_port;
} dvbbuffer_conf_t;

extern dvbbuffer_conf_t dvbbuffer_conf;
extern const idclass_t dvbbuffer_conf_class;
extern dvbbuf_ctx *dvbbuffer_ctx;

void dvbbuffer_conf_init(void);

/*
 * Ring buffer of one running mux (mux.c)
 *
 * Created on mux start for prebuffer muxes, published in mm_dvbbuffer under
 * mi_output_lock. Reference counted under global_lock: the mux itself holds
 * one reference while attached, every service with a raw reader one more.
 * The library object is only used from the input thread (feed, raw reads)
 * and under global_lock (status), and destroyed with the last reference.
 */
typedef struct dvbbuffer_mux {
  dvbbuf_mux *dm_lib;
  int         dm_refcount;
  char       *dm_name;
} dvbbuffer_mux_t;

void dvbbuffer_mux_init(void);
void dvbbuffer_mux_done(void);
void dvbbuffer_mux_attach(mpegts_mux_t *mm);
void dvbbuffer_mux_detach(mpegts_mux_t *mm);
void dvbbuffer_mux_ref(dvbbuffer_mux_t *dm);
void dvbbuffer_mux_unref(dvbbuffer_mux_t *dm);
int  dvbbuffer_mux_wanted(mpegts_mux_t *mm);
int  dvbbuffer_mux_prebuffer(mpegts_mux_t *mm);

/*
 * Warm mux manager (warm.c), global_lock
 */
void dvbbuffer_warm_init(void);
void dvbbuffer_warm_done(void);
void dvbbuffer_warm_reconcile(void);
void dvbbuffer_warm_mux_delete(mpegts_mux_t *mm);
void dvbbuffer_warm_used(mpegts_mux_t *mm, int delta);
int  dvbbuffer_warm_lru(mpegts_mux_t *mm);

/*
 * HLS server bridge (hlsbridge.c)
 */
void dvbbuffer_hls_init(void);   /* global_lock */
void dvbbuffer_hls_done(void);   /* WITHOUT global_lock */

#endif /* __TVH_DVBBUFFER_PRIVATE_H__ */
