/*
 *  Tvheadend - instant channel switching (libdvbbuffer glue)
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
struct service;

#if ENABLE_DVBBUFFER

void dvbbuffer_init(void);
void dvbbuffer_done(void);

/*
 * H2 - mpegts_input_process(): every input chunk of a mux, before the
 * per-PID dispatch. Input thread, mi_output_lock held.
 */
void dvbbuffer_mux_input0(struct mpegts_mux *mm, uint64_t pos,
                          const uint8_t *tsb, int len, int cc_restart);

#define dvbbuffer_mux_input(mm, pos, tsb, len, cc_restart) do { \
  if ((mm)->mm_dvbbuffer) \
    dvbbuffer_mux_input0(mm, pos, tsb, len, cc_restart); \
} while (0)

/*
 * H3 - ts_recv_packet1(): live run of one PID of a running service, before
 * the descrambler. Input thread, s_stream_mutex held.
 * Returns 1 when the run was consumed (dropped, it is in the ring buffer),
 * 0 when tvh processes it normally.
 */
int dvbbuffer_service_packet0(struct mpegts_service *t, uint64_t tspos,
                              uint16_t pid, const uint8_t *tsb, int len);

#define dvbbuffer_service_packet(t, tspos, pid, tsb, len) \
  ((t)->s_dvbbuffer ? dvbbuffer_service_packet0(t, tspos, pid, tsb, len) : 0)

/* H4 - mpegts_service_start()/stop(), global_lock held */
void dvbbuffer_service_start(struct mpegts_service *t);
void dvbbuffer_service_stop(struct mpegts_service *t);

/* H5 - descrambler_keys(): tvh obtained a key, s_stream_mutex held (read-only
 * hint). type = DESCRAMBLER_*, even/odd = key data (zero = not in this answer),
 * keylen in bytes, ecm = ICAM ecm byte of the key (csa_ecm) */
void dvbbuffer_service_key(struct service *t, int type, uint16_t pid,
                           const uint8_t *even, const uint8_t *odd,
                           int keylen, uint8_t ecm);

/* H8 - mux property "prebuffer" changed, global_lock held */
void dvbbuffer_mux_prebuffer_notify(void *p, const char *lang);

#else

static inline void dvbbuffer_init(void) { }
static inline void dvbbuffer_done(void) { }

#endif

#endif /* __TVH_DVBBUFFER_H__ */
