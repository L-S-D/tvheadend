/*
 *  Tvheadend - instant channel switching
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#ifndef __TVH_DVBBUFFER_H__
#define __TVH_DVBBUFFER_H__

#include "build.h"

#include <stdint.h>

struct mpegts_mux;
struct mpegts_service;
struct streaming_target;

#if ENABLE_DVBBUFFER

void dvbbuffer_init(void);
void dvbbuffer_done(void);

/* 1 while the module runs (capability "dvbbuffer", configuration tab) */
extern uint32_t dvbbuffer_running;

/* the number of muxes with a ring buffer now */
int dvbbuffer_mux_count(void);

/*
 * The ring buffer of a warm mux sits in front of tvh's input processing
 * (mpegts_input_process(), input thread, mi_output_lock held): every chunk
 * goes into the ring first, tvh then processes it from the ring memory.
 */
typedef struct dvbbuffer_span {
  uint64_t seq, end;
} dvbbuffer_span_t;

/* 1 = process the chunk from the ring, piece by piece (dvbbuffer_mux_span),
   0 = process the chunk itself (no ring, or the chunk is too large) */
int dvbbuffer_mux_feed0(struct mpegts_mux *mm, const uint8_t *tsb, int len,
                        int cc_restart, dvbbuffer_span_t *sp);
#define dvbbuffer_mux_feed(mm, tsb, len, cc_restart, sp) \
  ((mm)->mm_dvbbuffer ? dvbbuffer_mux_feed0(mm, tsb, len, cc_restart, sp) : 0)
/* the next piece in one piece of ring memory: bytes, 0 = done */
int dvbbuffer_mux_span(struct mpegts_mux *mm, dvbbuffer_span_t *sp, const uint8_t **data);
/* the ring holds a backlog for new PID subscriptions (not just tuned) */
int dvbbuffer_mux_backlog_ready(struct mpegts_mux *mm);
/* the backlog for new PID subscriptions, oldest first (read with
   dvbbuffer_mux_span); capids: bitmap (8192 bits) of the ECM PIDs of the
   services they belong to, NULL = none - the backlog then starts at the
   first ECM, the packets before it have no key */
void dvbbuffer_mux_backlog(struct mpegts_mux *mm, dvbbuffer_span_t *sp,
                           const uint8_t *capids);

/* mux property "prebuffer" changed, global_lock held */
void dvbbuffer_mux_prebuffer_notify(void *p, const char *lang);
/* mux property "ecm_prefetch" changed, global_lock held */
void dvbbuffer_mux_prefetch_notify(void *p, const char *lang);

/* a mux or network was enabled or disabled, global_lock held */
void dvbbuffer_mux_enabled_notify(void);

/* a service starts (+1) or stops (-1): mux use for the recently used muxes
   kept warm (LRU), global_lock held */
void dvbbuffer_service_used(struct mpegts_service *s, int delta);

/* packets of one service's ring backlog at most (buffer_sec at 25 Mbit/s):
 * the descrambler queues that many until the first key */
uint32_t dvbbuffer_backlog_packets(void);

/* live start in front of tsfix (profile chains): after a service start the
   client gets the stream from the newest keyframe at least start_lag_ms
   behind live, not the whole backlog; live = 0: everything passes */
struct streaming_target *dvbbuffer_start_create(struct streaming_target *output, int live);
void dvbbuffer_start_destroy(struct streaming_target *st);
/* the same for raw TS chains (pass profiles), in front of the queue */
struct streaming_target *dvbbuffer_ts_start_create(struct streaming_target *output, int live);
void dvbbuffer_ts_start_destroy(struct streaming_target *st);

#else

static inline void dvbbuffer_init(void) { }
static inline void dvbbuffer_done(void) { }
static inline struct streaming_target *
dvbbuffer_start_create(struct streaming_target *output, int live) { return output; }
static inline void dvbbuffer_start_destroy(struct streaming_target *st) { }
static inline struct streaming_target *
dvbbuffer_ts_start_create(struct streaming_target *output, int live) { return output; }
static inline void dvbbuffer_ts_start_destroy(struct streaming_target *st) { }
static inline uint32_t dvbbuffer_backlog_packets(void) { return 0; }

#endif

#endif /* __TVH_DVBBUFFER_H__ */
