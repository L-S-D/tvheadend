/*
 *  Tvheadend - instant channel switching: TS ring buffer of one mux
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include <sys/mman.h>

#include "ring_private.h"

#define RING_MAX_CHUNK 5577    /* packets per claim, ~1 MiB */

/* buffer_ms x max bitrate x 1.15 headroom / 1504 bits per packet; at most
 * INT_MAX / 188 packets: the byte count of a span is an int, and on 32 bit
 * the mapping size (size_t) must not wrap - too big a ring just fails to map */
#define RING_MAX_PACKETS ((uint64_t)INT_MAX / 188)

static uint64_t
ring_capacity(uint32_t buffer_ms, uint32_t max_bitrate_kbps)
{
  double pk = (double)buffer_ms / 1000.0 * max_bitrate_kbps * 1000.0 * 1.15 / 1504.0;
  uint64_t n = pk < (double)RING_MAX_PACKETS ? (uint64_t)pk : RING_MAX_PACKETS;
  return n > 16384 ? n : 16384;
}

/*
 * The pages come with the first feed (input thread), not here: the ring is
 * created under global_lock, populating ~120 MB there takes 40-50 ms. No
 * transparent huge pages: their first touch may compact memory in the
 * input thread (stalls of 100 ms seen); 4K pages cost ~80 ms spread over
 * the first buffer_ms, no single feed longer than a millisecond.
 */
static void *
ring_map(const char *name, size_t bytes)
{
  void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (p == MAP_FAILED) {
    tvherror(LS_DVBBUFFER, "%s: mmap(%zu) failed: %s", name, bytes, strerror(errno));
    return NULL;
  }
#ifdef MADV_NOHUGEPAGE
  (void)madvise(p, bytes, MADV_NOHUGEPAGE);   /* an advice: failing is harmless */
#endif
  return p;
}

/* ---------------------------------------------------------------------------
 * Packet store
 * ------------------------------------------------------------------------- */

static inline ring_meta_t
ring_classify(const uint8_t *p)
{
  return (((p[1] & 0x1F) << 8) | p[2]) | ((p[1] & 0x40) ? RING_PUSI : 0);
}

/* contiguous writable span at head: at most max packets, never across the
   wrap; the slots count as overwritten (claim_end) from here on */
static uint32_t
ring_claim(dvbbuffer_ring_t *r, uint32_t max, uint64_t *first)
{
  uint64_t h = r->head, room = r->capacity - h % r->capacity;
  uint32_t n = room < max ? (uint32_t)room : max;

  r->claim_end = h + n;
  r->pending_first = h;
  *first = h;
  return n;
}

static void
ring_commit(dvbbuffer_ring_t *r, uint32_t n, int64_t mono_us)
{
  ring_index_t *e;

  if (n == 0)
    return;
  e = &r->index[r->index_head];
  e->seq = r->pending_first;
  e->count = n;
  e->mono_us = mono_us;
  r->index_head = (r->index_head + 1) % r->index_size;
  if (r->index_count < r->index_size)
    r->index_count++;
  r->head = r->pending_first + n;
}

static inline const ring_index_t *
ring_index_at(const dvbbuffer_ring_t *r, size_t oldest, size_t k)
{
  return &r->index[(oldest + k) % r->index_size];
}

uint64_t
ring_seq_at_time(const dvbbuffer_ring_t *r, int64_t mono_us)
{
  uint64_t fl = ring_floor(r), hd = r->head, seq;
  size_t oldest, lo = 0, hi = r->index_count, mid;

  if (r->index_count == 0)
    return hd;
  oldest = (r->index_head + r->index_size - r->index_count) % r->index_size;
  while (lo < hi) {
    mid = (lo + hi) / 2;
    if (ring_index_at(r, oldest, mid)->mono_us < mono_us)
      lo = mid + 1;
    else
      hi = mid;
  }
  seq = lo == r->index_count ? hd : ring_index_at(r, oldest, lo)->seq;
  if (seq < fl)
    seq = fl;
  if (seq > hd)
    seq = hd;
  return seq;
}

/* ---------------------------------------------------------------------------
 * API
 * ------------------------------------------------------------------------- */

dvbbuffer_ring_t *
dvbbuffer_ring_create(uint32_t buffer_ms, uint32_t max_bitrate_kbps, const char *name)
{
  dvbbuffer_ring_t *r = calloc(1, sizeof(*r));

  if (!r)
    return NULL;
  r->name = strdup(name ?: "");
  r->buffer_ms = buffer_ms;
  r->capacity = ring_capacity(buffer_ms, max_bitrate_kbps);
  r->data = ring_map(r->name, r->capacity * 188);
  r->meta = ring_map(r->name, r->capacity * sizeof(ring_meta_t));
  r->index_size = r->capacity / RING_INDEX_DIV > RING_INDEX_MIN ?
                  r->capacity / RING_INDEX_DIV : RING_INDEX_MIN;
  r->index = calloc(r->index_size, sizeof(ring_index_t));
  if (!r->data || !r->meta || !r->index || !r->name) {
    dvbbuffer_ring_destroy(r);
    return NULL;
  }
  tvhdebug(LS_DVBBUFFER, "%s: ring created: %u ms @ %u kbit/s -> %"PRIu64" packets",
           r->name, buffer_ms, max_bitrate_kbps, r->capacity);
  return r;
}

void
dvbbuffer_ring_destroy(dvbbuffer_ring_t *r)
{
  if (!r)
    return;
  if (r->data)
    munmap(r->data, r->capacity * 188);
  if (r->meta)
    munmap(r->meta, r->capacity * sizeof(ring_meta_t));
  free(r->index);
  free(r->name);
  free(r);
}

size_t
dvbbuffer_ring_memory(const dvbbuffer_ring_t *r)
{
  return r->capacity * 188 + r->capacity * sizeof(ring_meta_t) +
         r->index_size * sizeof(ring_index_t);
}

uint64_t
dvbbuffer_ring_capacity(const dvbbuffer_ring_t *r)
{
  return r->capacity;
}

void
dvbbuffer_ring_mark_gap(dvbbuffer_ring_t *r)
{
  r->carry_len = 0;
}

uint64_t
dvbbuffer_ring_head(const dvbbuffer_ring_t *r)
{
  return r->head;
}

uint64_t
dvbbuffer_ring_seq_at(const dvbbuffer_ring_t *r, int64_t mono_us)
{
  return ring_seq_at_time(r, mono_us);
}

int64_t
dvbbuffer_ring_oldest(const dvbbuffer_ring_t *r)
{
  size_t oldest;

  if (r->index_count == 0)
    return INT64_MAX;
  oldest = (r->index_head + r->index_size - r->index_count) % r->index_size;
  return r->index[oldest].mono_us;
}

uint64_t
dvbbuffer_ring_find_pusi(const dvbbuffer_ring_t *r, uint64_t seq, uint64_t end,
                         const uint8_t *pids)
{
  ring_meta_t m;
  uint16_t pid;

  if (end > r->head)
    end = r->head;
  if (seq < ring_floor(r))
    seq = ring_floor(r);
  for (; seq < end; seq++) {
    m = r->meta[seq % r->capacity];
    pid = ring_meta_pid(m);
    if (ring_meta_pusi(m) && (pids[pid >> 3] & (1 << (pid & 7))))
      return seq;
  }
  return end;
}

uint32_t
dvbbuffer_ring_span(const dvbbuffer_ring_t *r, uint64_t seq, uint64_t end,
                    const uint8_t **data)
{
  uint64_t off, n;

  if (end > r->head)
    end = r->head;
  if (seq >= end || seq < ring_floor(r))
    return 0;
  off = seq % r->capacity;
  n = r->capacity - off;
  if (n > end - seq)
    n = end - seq;
  *data = r->data + off * 188;
  return (uint32_t)n;
}

/* complete a packet carried over from the previous call: bytes taken */
static size_t
ring_feed_carry(dvbbuffer_ring_t *r, const uint8_t *ts, size_t len, int64_t mono_us)
{
  size_t take = MIN(188 - r->carry_len, len);
  uint64_t first;
  uint8_t *dst;

  memcpy(r->carry + r->carry_len, ts, take);
  r->carry_len += take;
  if (r->carry_len < 188)
    return take;
  /* the carry starts with a sync byte (dvbbuffer_ring_feed()); tvh's
   * chunks are whole packets, a carry does not come up with them */
  ring_claim(r, 1, &first);
  dst = r->data + (first % r->capacity) * 188;
  memcpy(dst, r->carry, 188);
  r->meta[first % r->capacity] = ring_classify(dst);
  ring_commit(r, 1, mono_us);
  r->carry_len = 0;
  return take;
}

/* store n packets */
static void
ring_feed_packets(dvbbuffer_ring_t *r, const uint8_t *ts, size_t n, int64_t mono_us)
{
  size_t done, max;
  uint64_t first;
  uint32_t cnt, k;
  uint8_t *dst;

  for (done = 0; done < n; done += cnt) {
    max = MIN(n - done, RING_MAX_CHUNK);
    cnt = ring_claim(r, (uint32_t)max, &first);
    dst = r->data + (first % r->capacity) * 188;
    memcpy(dst, ts + done * 188, (size_t)cnt * 188);
    for (k = 0; k < cnt; k++)
      r->meta[(first + k) % r->capacity] = ring_classify(dst + (size_t)k * 188);
    ring_commit(r, cnt, mono_us);
  }
}

/* sync lost at i: the position of the next sync byte followed by another */
static size_t
ring_resync(dvbbuffer_ring_t *r, const uint8_t *ts, size_t len, size_t i)
{
  size_t j = i + 1;

  while (j < len && !(ts[j] == 0x47 && (j + 188 >= len || ts[j + 188] == 0x47)))
    j++;
  if (tvhlog_limit(&r->sync_log, 10))
    tvhwarn(LS_DVBBUFFER, "%s: sync lost, skipped %zu bytes (%zu times so far)",
            r->name, j - i, r->sync_log.count);
  return j;
}

void
dvbbuffer_ring_feed(dvbbuffer_ring_t *r, const uint8_t *ts, size_t len, int64_t mono_us)
{
  size_t i = 0, n;

  if (r->carry_len > 0) {
    i = ring_feed_carry(r, ts, len, mono_us);
    if (r->carry_len > 0)
      return;
  }
  while (i + 188 <= len) {
    if (ts[i] != 0x47) {
      i = ring_resync(r, ts, len, i);
      continue;
    }
    /* contiguous in-sync packets */
    n = 1;
    while (i + (n + 1) * 188 <= len && ts[i + n * 188] == 0x47)
      n++;
    ring_feed_packets(r, ts + i, n, mono_us);
    i += n * 188;
  }

  if (i < len && ts[i] == 0x47) {  /* keep the partial packet */
    r->carry_len = len - i;
    memcpy(r->carry, ts + i, r->carry_len);
  }
}
