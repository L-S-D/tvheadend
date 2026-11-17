/*
 *  Tvheadend - MPEG-TS: what the PIDs of a mux carry
 *
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Every TS packet counts for its PID: packets, scrambled ones (transport
 * scrambling control), continuity errors. At an unscrambled payload unit
 * start the payload tells what the PID carries:
 *  - 00 00 01 and a stream id: a PES; the start of its elementary stream
 *    after the PES header names it (a video start code, an audio sync word,
 *    the data identifier of teletext or subtitles);
 *  - else sections: the pointer field, then a table id. The first sections
 *    of a PID are put together (up to 4096 bytes) and count when their CRC
 *    is right (long form, TOT), or when they are short form ones of a table
 *    that has that form (TDT, RST, ST, DIT, ECM, EMM); a payload that only
 *    looks like sections (T2-MI, other data) makes none.
 * The PID a demodulator passes BBFrames in (the Neumo driver, for a generic
 * stream) counts the frames by their BBHEADER instead: MATYPE, input
 * streams, data field lengths, normal or high efficiency mode.
 */

#include "mpegts_pidstats.h"

#include <stdlib.h>
#include <string.h>

#define PS_MAX_PIDS   512
#define PS_SECTIONS   8          /* sections put together per PID, at most */
#define PS_SECTION_MAX 4096

typedef struct ps_pid {
  mpegts_pidstats_pid_t pub;
  uint8_t   cc;                  /* the last continuity counter, 0xff none */
  uint8_t  *sec;                 /* a section being put together */
  int       sec_have;
  int       sec_tried;           /* sections put together so far */
} ps_pid_t;

struct mpegts_pidstats {
  int       bb_pid;              /* -1 none */
  mpegts_pidstats_bb_cb_t bb_cb;
  void     *bb_opaque;
  mpegts_pidstats_ts_cb_t ts_cb;
  void     *ts_opaque;
  uint16_t  idx[8192];           /* PID -> slot + 1 */
  ps_pid_t  pid[PS_MAX_PIDS];
  int       count;
  mpegts_pidstats_total_t total;
  int64_t   first, last;
};

/* MPEG-2 CRC32 (as tvh_crc32(), here without tvheadend) */
static uint32_t
ps_crc32 ( const uint8_t *d, int n )
{
  uint32_t crc = 0xffffffff;
  int i;

  while (n-- > 0) {
    crc ^= (uint32_t)*d++ << 24;
    for (i = 0; i < 8; i++)
      crc = (crc & 0x80000000) ? (crc << 1) ^ 0x04c11db7 : crc << 1;
  }
  return crc;
}

mpegts_pidstats_t *
mpegts_pidstats_create ( void )
{
  mpegts_pidstats_t *ps = calloc(1, sizeof(mpegts_pidstats_t));

  if (ps)
    ps->bb_pid = -1;
  return ps;
}

void
mpegts_pidstats_bbframes ( mpegts_pidstats_t *ps, int pid )
{
  ps->bb_pid = pid;
}

void
mpegts_pidstats_bbsink ( mpegts_pidstats_t *ps, mpegts_pidstats_bb_cb_t cb, void *opaque )
{
  ps->bb_cb = cb;
  ps->bb_opaque = opaque;
}

void *
mpegts_pidstats_bbsink_opaque ( mpegts_pidstats_t *ps )
{
  return ps->bb_opaque;
}

void
mpegts_pidstats_tssink ( mpegts_pidstats_t *ps, mpegts_pidstats_ts_cb_t cb, void *opaque )
{
  ps->ts_cb = cb;
  ps->ts_opaque = opaque;
}

void *
mpegts_pidstats_tssink_opaque ( mpegts_pidstats_t *ps )
{
  return ps->ts_opaque;
}

/* the CRC-8 of a BBHEADER (EN 302 307), and the mode it says: 0 normal,
 * 1 high efficiency, -1 not a header */
static int
ps_bbheader_mode ( const uint8_t *h )
{
  uint8_t c = 0;
  int i, j, m;

  for (i = 0; i < 9; i++) {
    c ^= h[i];
    for (j = 0; j < 8; j++)
      c = (c & 0x80) ? (uint8_t)((c << 1) ^ 0xd5) : (uint8_t)(c << 1);
  }
  m = c ^ h[9];
  return m == 0 || m == 1 ? m : -1;
}

/* a packet of the BBFrame PID: Neumo puts the bytes of the frames in the
 * adaptation field (t[5] 0x80, t[7] their number from t[8] on); a frame
 * starts with 0xB8, then its BBHEADER */
static void
ps_bbframe ( ps_pid_t *p, const uint8_t *t )
{
  mpegts_pidstats_bb_t *bb = &p->pub.bb;
  int mode;

  p->pub.bbframes = 1;
  if (t[5] != 0x80 || t[7] < 11 || t[7] > 180 || t[8] != 0xb8 ||
      (mode = ps_bbheader_mode(t + 9)) < 0)
    return;
  bb->frames++;
  if (mode)
    bb->hem++;
  bb->matype = t[9];
  if (!(t[9] & 0x20))
    bb->isis[t[10] >> 3] |= 1 << (t[10] & 7);
  bb->dfl_bits += (t[13] << 8) | t[14];
}

void
mpegts_pidstats_destroy ( mpegts_pidstats_t *ps )
{
  int i;

  if (ps == NULL)
    return;
  for (i = 0; i < ps->count; i++)
    free(ps->pid[i].sec);
  free(ps);
}

static ps_pid_t *
ps_slot ( mpegts_pidstats_t *ps, uint16_t pid )
{
  ps_pid_t *p;

  if (ps->idx[pid])
    return &ps->pid[ps->idx[pid] - 1];
  if (ps->count >= PS_MAX_PIDS)
    return NULL;
  p = &ps->pid[ps->count++];
  ps->idx[pid] = ps->count;
  p->pub.pid = pid;
  p->pub.stream_id = -1;
  p->pub.bb.matype = -1;
  p->cc = 0xff;
  ps->total.pids++;
  return p;
}

/* what an elementary stream starts with (d: after the PES header) */
static mpegts_pidstats_es_t
ps_es ( int stream_id, const uint8_t *d, int n )
{
  int i, b;

  if (stream_id >= 0xe0 && stream_id <= 0xef) {
    /* the first start code: MPEG-2 video, or the header of a NAL unit */
    for (i = 0; i + 3 < n; i++) {
      if (d[i] != 0 || d[i + 1] != 0 || d[i + 2] != 1)
        continue;
      b = d[i + 3];
      if (b == 0xb3 || b == 0xb8 || b == 0x00)
        return MPEGTS_PIDSTATS_ES_MPEG2V;
      if (!(b & 0x80) && ((b & 0x1f) == 9 || (b & 0x1f) == 7))
        return MPEGTS_PIDSTATS_ES_H264;
      if (!(b & 0x81) && (((b >> 1) & 0x3f) == 35 || ((b >> 1) & 0x3f) == 32))
        return MPEGTS_PIDSTATS_ES_HEVC;
      return MPEGTS_PIDSTATS_ES_OTHER;
    }
    return MPEGTS_PIDSTATS_ES_NONE;
  }
  if (n < 2)
    return MPEGTS_PIDSTATS_ES_NONE;
  if (stream_id >= 0xc0 && stream_id <= 0xdf) {
    if (d[0] == 0xff && (d[1] & 0xf0) == 0xf0)
      return ((d[1] >> 1) & 3) == 0 ? MPEGTS_PIDSTATS_ES_ADTS : MPEGTS_PIDSTATS_ES_MPA;
    if (d[0] == 0x56 && (d[1] & 0xe0) == 0xe0)
      return MPEGTS_PIDSTATS_ES_LATM;
    return MPEGTS_PIDSTATS_ES_OTHER;
  }
  if (stream_id == 0xbd) {
    if (d[0] == 0x0b && d[1] == 0x77)
      return MPEGTS_PIDSTATS_ES_AC3;
    if (d[0] == 0xac && (d[1] == 0x40 || d[1] == 0x41))
      return MPEGTS_PIDSTATS_ES_AC4;
    if (d[0] >= 0x10 && d[0] <= 0x1f)
      return MPEGTS_PIDSTATS_ES_TELETEXT;
    if (d[0] == 0x20)
      return MPEGTS_PIDSTATS_ES_SUBTITLES;
    return MPEGTS_PIDSTATS_ES_OTHER;
  }
  return MPEGTS_PIDSTATS_ES_OTHER;
}

/* a section put together: its table id counts if it is one */
static void
ps_section_done ( ps_pid_t *p )
{
  const uint8_t *s = p->sec;
  int len = 3 + (((s[1] & 0x0f) << 8) | s[2]), tid = s[0];

  p->sec_tried++;
  p->sec_have = 0;
  if ((s[1] & 0x80) || tid == 0x73) {
    /* long form (and the TOT): CRC32 over the whole section, 0 when right */
    if (len >= 7 && ps_crc32(s, len) == 0) {
      p->pub.tables[tid >> 3] |= 1 << (tid & 7);
      p->pub.sections++;
    }
  } else if ((tid == 0x70 && len == 8) || tid == 0x71 || tid == 0x72 || tid == 0x7e ||
             (tid >= 0x80 && tid <= 0x8f)) {
    /* the short form ones of EN 300 468 (TDT, RST, ST, DIT) and the
     * conditional access ones (ECM, EMM): nothing to check them by, and
     * other data (T2-MI) does not start like them */
    p->pub.tables_short[tid >> 3] |= 1 << (tid & 7);
    p->pub.sections++;
  }
}

/* bytes of a section (from its start, or more of it) */
static void
ps_section_add ( ps_pid_t *p, const uint8_t *d, int n )
{
  int len, take;

  if (p->sec_tried >= PS_SECTIONS || n <= 0)
    return;
  if (p->sec == NULL && (p->sec = malloc(PS_SECTION_MAX)) == NULL)
    return;
  if (p->sec_have < 3) {
    take = 3 - p->sec_have < n ? 3 - p->sec_have : n;
    memcpy(p->sec + p->sec_have, d, take);
    p->sec_have += take;
    d += take;
    n -= take;
    if (p->sec_have < 3)
      return;
    if (p->sec[0] == 0xff) {             /* stuffing, no section */
      p->sec_have = 0;
      return;
    }
  }
  len = 3 + (((p->sec[1] & 0x0f) << 8) | p->sec[2]);
  if (len > PS_SECTION_MAX) {            /* not a section */
    p->sec_have = 0;
    p->sec_tried++;
    return;
  }
  take = len - p->sec_have < n ? len - p->sec_have : n;
  memcpy(p->sec + p->sec_have, d, take);
  p->sec_have += take;
  if (p->sec_have == len) {
    ps_section_done(p);
    if (p->sec_tried >= PS_SECTIONS) {
      free(p->sec);
      p->sec = NULL;
    }
  }
}

static void
ps_packet ( mpegts_pidstats_t *ps, const uint8_t *t )
{
  uint16_t pid = ((t[1] & 0x1f) << 8) | t[2];
  int afc = (t[3] >> 4) & 3, off = 4, cc, n, ptr;
  const uint8_t *d;
  ps_pid_t *p;

  ps->total.packets++;
  if (t[1] & 0x80) {
    ps->total.tei++;
    return;
  }
  if (pid == 0x1fff) {
    ps->total.nulls++;
    return;
  }
  if ((p = ps_slot(ps, pid)) == NULL)
    return;
  p->pub.packets++;
  if (pid == ps->bb_pid) {
    ps_bbframe(p, t);
    if (ps->bb_cb)
      ps->bb_cb(ps->bb_opaque, t);
    return;
  }
  if (ps->ts_cb)
    ps->ts_cb(ps->ts_opaque, t);
  if (t[3] & 0xc0)
    p->pub.scrambled++;
  if (!(afc & 1))                        /* no payload */
    return;
  cc = t[3] & 0x0f;
  if (p->cc != 0xff && cc != ((p->cc + 1) & 0x0f) && cc != p->cc)
    p->pub.cc_errors++;
  p->cc = cc;
  if (afc == 3)
    off = 5 + t[4];
  if (off >= 188 || (t[3] & 0xc0))
    return;
  d = t + off;
  n = 188 - off;
  if (!(t[1] & 0x40)) {
    /* more of a section */
    if (p->sec_have)
      ps_section_add(p, d, n);
    return;
  }
  p->pub.starts++;
  if (n >= 9 && d[0] == 0 && d[1] == 0 && d[2] == 1 && d[3] >= 0xbc) {
    /* a PES: the elementary stream after its header */
    p->sec_have = 0;
    if (p->pub.stream_id < 0)
      p->pub.stream_id = d[3];
    if (p->pub.es == MPEGTS_PIDSTATS_ES_NONE && d[3] != 0xbe && d[3] != 0xbf &&
        9 + d[8] < n)
      p->pub.es = ps_es(d[3], d + 9 + d[8], n - 9 - d[8]);
    return;
  }
  /* sections: the end of the one before, then a new one */
  ptr = d[0];
  if (1 + ptr > n)
    return;
  if (p->sec_have)
    ps_section_add(p, d + 1, ptr);
  p->sec_have = 0;
  ps_section_add(p, d + 1 + ptr, n - 1 - ptr);
}

void
mpegts_pidstats_feed ( mpegts_pidstats_t *ps, const uint8_t *tsb, int len, int64_t now )
{
  if (ps->first == 0)
    ps->first = now;
  ps->last = now;
  for (; len >= 188; tsb += 188, len -= 188)
    if (tsb[0] == 0x47)
      ps_packet(ps, tsb);
}

static int
ps_cmp ( const void *a, const void *b )
{
  const mpegts_pidstats_pid_t *x = a, *y = b;

  if (x->packets != y->packets)
    return x->packets > y->packets ? -1 : 1;
  return x->pid - y->pid;
}

int
mpegts_pidstats_get ( mpegts_pidstats_t *ps, mpegts_pidstats_total_t *total,
                      mpegts_pidstats_pid_t *out, int n )
{
  mpegts_pidstats_pid_t *all;
  int i;

  *total = ps->total;
  total->duration = ps->last - ps->first;
  if (n <= 0 || ps->count == 0)
    return 0;
  if ((all = malloc(ps->count * sizeof(*all))) == NULL)
    return 0;
  for (i = 0; i < ps->count; i++)
    all[i] = ps->pid[i].pub;
  qsort(all, ps->count, sizeof(*all), ps_cmp);
  if (n > ps->count)
    n = ps->count;
  memcpy(out, all, n * sizeof(*all));
  free(all);
  return n;
}
