/*
 *  Tvheadend - instant channel switching: TS ring buffer, internals
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#ifndef __TVH_DVBBUFFER_RING_PRIVATE_H__
#define __TVH_DVBBUFFER_RING_PRIVATE_H__

#include "tvheadend.h"
#include "ring.h"

/* ---------------------------------------------------------------------------
 * Per-packet metadata, stored parallel to the packets: [12:0] PID, RING_PUSI
 * ------------------------------------------------------------------------- */

#define RING_PUSI    0x2000   /* payload unit start */

typedef uint16_t ring_meta_t;

static inline uint16_t ring_meta_pid(ring_meta_t m) { return m & 0x1FFF; }
static inline int ring_meta_pusi(ring_meta_t m) { return (m & RING_PUSI) != 0; }

/* ---------------------------------------------------------------------------
 * The ring (ring.c)
 * ------------------------------------------------------------------------- */

typedef struct ring_index {
  uint64_t seq;        /* first seq of the chunk */
  uint32_t count;
  int64_t  mono_us;
} ring_index_t;

/* one index entry per commit: tvh feeds at least 100 packets at a time
   (one commit, two at the wrap), so capacity / 64 entries cover the whole
   ring; at least 65536 */
#define RING_INDEX_MIN     65536
#define RING_INDEX_DIV     64

struct dvbbuffer_ring {
  char         *name;
  uint32_t      buffer_ms;
  uint64_t      capacity;      /* packets */
  uint8_t      *data;          /* capacity * 188, mmap */
  ring_meta_t  *meta;          /* capacity, mmap */
  /* positions: packets below head are complete; slots from claim_end -
     capacity on are not being overwritten (all written in the feed context) */
  uint64_t      head;
  uint64_t      claim_end;
  uint64_t      pending_first;
  ring_index_t *index;         /* ring of index_size entries */
  size_t        index_size;
  size_t        index_head;    /* next write position */
  size_t        index_count;
  /* feed state */
  uint8_t       carry[188];
  size_t        carry_len;
  tvhlog_limit_t sync_log;     /* "sync lost" at most every 10 s */
};

/* oldest seq whose slot is not being overwritten */
static inline uint64_t ring_floor(const dvbbuffer_ring_t *r)
  { return r->claim_end > r->capacity ? r->claim_end - r->capacity : 0; }

/* first seq whose chunk arrived at or after mono_us (clamped to floor..head) */
uint64_t ring_seq_at_time(const dvbbuffer_ring_t *r, int64_t mono_us);

#endif /* __TVH_DVBBUFFER_RING_PRIVATE_H__ */
