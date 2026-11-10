/*
 *  Tvheadend - instant channel switching, internals
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
#include "memoryinfo.h"
#include "input.h"
#include "tvh_dvbbuffer.h"

#include "ring.h"

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
  uint32_t start_lag_ms;      /* live start: at least this far behind live */
  int      camd35_enabled;    /* ECM prefetch over camd35 (UDP) */
  char    *camd35_host;
  uint32_t camd35_port;
  char    *camd35_user;
  char    *camd35_pass;
} dvbbuffer_conf_t;

extern dvbbuffer_conf_t dvbbuffer_conf;
extern memoryinfo_t dvbbuffer_memoryinfo;
extern const idclass_t dvbbuffer_conf_class;

void dvbbuffer_conf_init(void);

/*
 * Ring buffer of one running mux (mux.c)
 *
 * Created on mux start for warm muxes (global_lock), published in
 * mm_dvbbuffer under mi_output_lock. The ring is fed and read in the input
 * thread (mi_output_lock); it is destroyed when the mux stops or is no
 * longer kept warm, after it was unpublished.
 */
typedef struct dvbbuffer_prefetch dvbbuffer_prefetch_t;

/* PMT tables of a warm mux at most (one per PMT PID of its services) */
#define DVBBUFFER_PMT_MAX 64

typedef struct dvbbuffer_mux {
  dvbbuffer_ring_t *dm_ring;
  char             *dm_name;
  mpegts_table_t   *dm_pmt[DVBBUFFER_PMT_MAX]; /* the PMTs kept current */
  int               dm_pmt_n;
  dvbbuffer_prefetch_t **dm_prefetch;  /* ECM prefetch PIDs (prefetch.c) */
  int               dm_prefetch_n;
} dvbbuffer_mux_t;

void dvbbuffer_mux_init(void);
void dvbbuffer_mux_done(void);
void dvbbuffer_mux_attach(mpegts_mux_t *mm);
void dvbbuffer_mux_detach(mpegts_mux_t *mm);
int  dvbbuffer_mux_wanted(mpegts_mux_t *mm);
int  dvbbuffer_mux_prebuffer(mpegts_mux_t *mm);
int  dvbbuffer_mux_is_dvb(mpegts_mux_t *mm);
/* upper bound of the TS bitrate from the tuning parameters, kbit/s, 0 = unknown */
uint32_t dvbbuffer_mux_bitrate(mpegts_mux_t *mm);

/*
 * Warm mux manager (warm.c), global_lock
 */
void dvbbuffer_warm_init(void);
void dvbbuffer_warm_done(void);
void dvbbuffer_warm_reconcile(void);
void dvbbuffer_warm_mux_delete(mpegts_mux_t *mm);
void dvbbuffer_warm_tuner_freed(void);
void dvbbuffer_warm_used(mpegts_mux_t *mm, int delta);
int  dvbbuffer_warm_lru(mpegts_mux_t *mm);

/*
 * ECM prefetch (prefetch.c): the ECMs of the PIDs listed at a warm mux go
 * to OSCam over camd35 (UDP), the answers are thrown away - their control
 * words stay in OSCam's cache (global_lock unless noted)
 */
void dvbbuffer_camd35_reconf(void);
void dvbbuffer_camd35_done(void);
void dvbbuffer_prefetch_start(mpegts_mux_t *mm);
void dvbbuffer_prefetch_stop(dvbbuffer_mux_t *dm);
void dvbbuffer_prefetch_check(mpegts_mux_t *mm);
void dvbbuffer_prefetch_init(void);
void dvbbuffer_prefetch_done(void);

#endif /* __TVH_DVBBUFFER_PRIVATE_H__ */
