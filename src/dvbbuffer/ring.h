/*
 *  Tvheadend - instant channel switching: TS ring buffer of one mux
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#ifndef __TVH_DVBBUFFER_RING_H__
#define __TVH_DVBBUFFER_RING_H__

#include <stddef.h>
#include <stdint.h>

/*
 * The ring keeps the complete transport stream of one mux for the last
 * buffer_ms (packets + per-packet metadata + an arrival time index).
 * It sits in front of tvh's input processing: a chunk goes into the ring
 * first, then tvh processes it from the ring memory (span by span).
 *
 * Threads: everything runs in one context - tvh's input thread under
 * mi_output_lock.
 *
 * Positions: every packet gets an increasing 64-bit sequence number (seq).
 */

typedef struct dvbbuffer_ring dvbbuffer_ring_t;

/* NULL: out of memory (the ring is allocated at once) */
dvbbuffer_ring_t *dvbbuffer_ring_create(uint32_t buffer_ms, uint32_t max_bitrate_kbps,
                                        const char *name);
void dvbbuffer_ring_destroy(dvbbuffer_ring_t *ring);
/* bytes the ring takes (packets, metadata, index) */
size_t dvbbuffer_ring_memory(const dvbbuffer_ring_t *ring);
/* packets the ring holds */
uint64_t dvbbuffer_ring_capacity(const dvbbuffer_ring_t *ring);

/* any length (a partial packet is carried to the next call); mono_us: the
   arrival time */
void dvbbuffer_ring_feed(dvbbuffer_ring_t *ring, const uint8_t *ts, size_t len,
                         int64_t mono_us);
/* input discontinuity (retune, overflow): a partial packet is dropped */
void dvbbuffer_ring_mark_gap(dvbbuffer_ring_t *ring);
/* seq of the next packet the feed stores */
uint64_t dvbbuffer_ring_head(const dvbbuffer_ring_t *ring);
/* arrival time of the oldest data, INT64_MAX = empty */
int64_t dvbbuffer_ring_oldest(const dvbbuffer_ring_t *ring);
/* seq of the first packet that arrived at or after mono_us (floor..head) */
uint64_t dvbbuffer_ring_seq_at(const dvbbuffer_ring_t *ring, int64_t mono_us);
/* the first packet from seq on, below end, of a PID in the bitmap pids
   (8192 bits) with a payload unit start; end = none */
uint64_t dvbbuffer_ring_find_pusi(const dvbbuffer_ring_t *ring, uint64_t seq, uint64_t end,
                                  const uint8_t *pids);
/* the stored packets from seq on, below end, that lie in one piece of ring
   memory (*data): their number, 0 = none (seq >= end or overwritten) */
uint32_t dvbbuffer_ring_span(const dvbbuffer_ring_t *ring, uint64_t seq, uint64_t end,
                             const uint8_t **data);

#endif /* __TVH_DVBBUFFER_RING_H__ */
