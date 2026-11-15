/*
 *  Tvheadend - instant channel switching, live start for raw TS chains
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  The counterpart of start.c for the pass profiles (HTTP/UDP .ts): the
 *  service delivers the ring backlog as one burst of descrambled TS. This
 *  filter in front of the streaming queue holds the burst until the TS
 *  arrives in real time and passes it on from the newest video keyframe at
 *  least start_lag_ms behind live:
 *   - first the last PAT and PMT before the start,
 *   - video and the PCR from the keyframe, audio and subtitles from their
 *     first PES with a PTS at or after it, teletext from its first PES in
 *     the burst (page caches of the clients), the rest from the keyframe.
 *  Keyframes are found on the clear ES after the PES header (parameter sets
 *  + IDR/I slice, IRAP, I picture), the random access indicator otherwise.
 *  Without video (radio) the start is the newest audio PES.
 */

#include "dvbbuffer_private.h"
#include "streaming.h"
#include "packet.h"

#define TSS_GAP         8000        /* us between two TS chunks: real time */
#define TSS_HOLD     2000000        /* us: held at most this long */
#define TSS_MAXBYTES (24 << 20)     /* bytes: held at most this much */
#define TSS_CUT      3000000        /* us after the start: audio still cut */
#define TSS_ES_SCAN     4096        /* ES bytes checked for a keyframe */
#define TSS_CHUNK        100        /* TS packets per message passed on */

enum { K_OTHER = 0, K_VIDEO, K_AUDIO, K_SUB, K_TTX };
enum { V_UNKNOWN = 0, V_MPEG2, V_H264, V_HEVC };
enum { RAP_NO = 0, RAP_YES, RAP_UNKNOWN };

typedef struct dvbbuffer_tsstart {
  streaming_target_t  ts_input;
  streaming_target_t *ts_output;
  int                 ts_live;      /* profile option, not for recordings */
  int                 ts_armed;     /* holding since SMT_START */
  int                 ts_need_key;  /* released, but no keyframe yet */
  int64_t             ts_first, ts_last;
  int                 ts_lead_seen; /* TS of the lead PID came: gaps count from
                                       now on (clear tables come before the keys) */
  struct streaming_message_queue ts_q;
  size_t              ts_qbytes;
  int                 ts_vpid, ts_vtype, ts_lead, ts_apid;
  int                 ts_pmt;
  int                 ts_sid;       /* service id (for a PAT of our own) */
  uint8_t             ts_kind[8192];
  /* after the start: audio/subtitles from their first PES at the start */
  int64_t             ts_cut;       /* PTS, -1 = no cut */
  int64_t             ts_cut_until;
  uint8_t             ts_started[8192 / 8];
} dvbbuffer_tsstart_t;

/* ---------------------------------------------------------------------------
 * TS / PES / ES helpers (keyframe detection taken from the instant-zap ring)
 * ------------------------------------------------------------------------- */

static inline int tss_pid(const uint8_t *p) { return ((p[1] & 0x1f) << 8) | p[2]; }
static inline int tss_pusi(const uint8_t *p) { return (p[1] & 0x40) != 0; }

/* offset of the payload in the TS packet, -1 = none */
static int
tss_payload(const uint8_t *p)
{
  int off = 4;

  if ((p[3] & 0x10) == 0)
    return -1;
  if (p[3] & 0x20)
    off += 1 + p[4];
  return off < 188 ? off : -1;
}

static int
tss_rai(const uint8_t *p)
{
  return (p[3] & 0x20) && p[4] > 0 && (p[5] & 0x40);
}

static inline int64_t
tss_ts_diff(int64_t a, int64_t b)
{
  int64_t d = (a - b) & PTS_MASK;
  return d > PTS_MASK / 2 ? d - PTS_MASK - 1 : d;
}

/* PTS of the PES starting in this packet, -1 = none; *es = offset of the ES */
static int64_t
tss_pes_pts(const uint8_t *p, int *es)
{
  int off = tss_payload(p);
  const uint8_t *b;

  if (off < 0 || off + 14 > 188)
    return -1;
  b = p + off;
  if (b[0] != 0 || b[1] != 0 || b[2] != 1 || b[3] < 0xbc || (b[6] & 0xc0) != 0x80)
    return -1;
  if (es)
    *es = off + 9 + b[8];
  if (!(b[7] & 0x80))
    return -1;
  return ((int64_t)(b[9] & 0x0e) << 29) | ((int64_t)b[10] << 22) |
         ((int64_t)(b[11] & 0xfe) << 14) | ((int64_t)b[12] << 7) | ((int64_t)b[13] >> 1);
}

typedef struct tss_bits {
  const uint8_t *d;
  size_t n, pos;
} tss_bits_t;

static int
tss_bit(tss_bits_t *b)
{
  int v;

  if (b->pos >= b->n * 8)
    return -1;
  v = (b->d[b->pos / 8] >> (7 - b->pos % 8)) & 1;
  b->pos++;
  return v;
}

static int64_t
tss_ue(tss_bits_t *b)
{
  int zeros = 0, bit, i;
  int64_t v = 0;

  for (;;) {
    if ((bit = tss_bit(b)) < 0 || zeros > 31)
      return -1;
    if (bit)
      break;
    zeros++;
  }
  for (i = 0; i < zeros; i++) {
    if ((bit = tss_bit(b)) < 0)
      return -1;
    v = (v << 1) | bit;
  }
  return ((int64_t)1 << zeros) - 1 + v;
}

static size_t
tss_unescape(const uint8_t *src, size_t n, uint8_t *dst, size_t max)
{
  size_t o = 0, zeros = 0, i;

  for (i = 0; i < n && o < max; i++) {
    if (zeros >= 2 && src[i] == 0x03) {
      zeros = 0;
      continue;
    }
    zeros = src[i] == 0 ? zeros + 1 : 0;
    dst[o++] = src[i];
  }
  return o;
}

static size_t
tss_next_start(const uint8_t *es, size_t len, size_t from)
{
  size_t i;

  for (i = from; i + 3 <= len; i++)
    if (es[i] == 0 && es[i + 1] == 0 && es[i + 2] == 1)
      return i + 3;
  return len;
}

static int
tss_rap_mpeg2(const uint8_t *es, size_t len)
{
  int seq_hdr = 0;
  size_t p;

  for (p = tss_next_start(es, len, 0); p < len; p = tss_next_start(es, len, p)) {
    if (es[p] == 0xb3) {
      seq_hdr = 1;
    } else if (es[p] == 0x00) {  /* picture header */
      if (p + 2 >= len)
        return RAP_UNKNOWN;
      if (((es[p + 2] >> 3) & 7) != 1)
        return RAP_NO;
      return seq_hdr ? RAP_YES : RAP_UNKNOWN;
    }
  }
  return RAP_UNKNOWN;
}

/* slice type of a non-IDR slice NAL (payload after the NAL header): I or SI
 * = 1, other = 0, -1 = unknown */
static int
tss_h264_intra_slice(const uint8_t *nal, size_t len)
{
  uint8_t rbsp[32];
  tss_bits_t br;
  int64_t first_mb, t;

  br.d = rbsp;
  br.n = tss_unescape(nal, len, rbsp, sizeof(rbsp));
  br.pos = 0;
  first_mb = tss_ue(&br);
  t = tss_ue(&br);
  if (first_mb < 0 || t < 0)
    return -1;
  t %= 5;
  return t == 2 || t == 4;
}

/* IDR or I slice, with SPS and PPS in front (a keyframe without them is no
 * start point) */
static int
tss_rap_h264(const uint8_t *es, size_t len)
{
  int sps = 0, pps = 0, intra;
  uint8_t nal;
  size_t p;

  for (p = tss_next_start(es, len, 0); p < len; p = tss_next_start(es, len, p)) {
    nal = es[p] & 0x1f;
    if (nal == 7) {
      sps = 1;
    } else if (nal == 8) {
      pps = 1;
    } else if (nal == 5 || nal == 1 || nal == 2) {
      intra = nal == 5 ? 1 : tss_h264_intra_slice(es + p + 1, len - p - 1);
      if (intra < 0)
        return RAP_UNKNOWN;
      return intra && sps && pps ? RAP_YES : RAP_NO;
    }
  }
  return RAP_UNKNOWN;
}

static int
tss_rap_hevc(const uint8_t *es, size_t len)
{
  int vps = 0, sps = 0, pps = 0, nal;
  size_t p;

  for (p = tss_next_start(es, len, 0); p < len; p = tss_next_start(es, len, p)) {
    nal = (es[p] >> 1) & 0x3f;
    if (nal == 32)
      vps = 1;
    else if (nal == 33)
      sps = 1;
    else if (nal == 34)
      pps = 1;
    if (nal >= 16 && nal <= 21)
      return vps && sps && pps ? RAP_YES : RAP_NO;
    if (nal < 16 || (nal >= 22 && nal <= 31))
      return RAP_NO;
  }
  return RAP_UNKNOWN;
}

/* ---------------------------------------------------------------------------
 * Held packets
 * ------------------------------------------------------------------------- */

typedef struct tss_pkts {
  const uint8_t **p;
  int n;
} tss_pkts_t;

/* the held TS packets in stream order; 0 = out of memory */
static int
tss_index(dvbbuffer_tsstart_t *ts, tss_pkts_t *pk)
{
  streaming_message_t *sm;
  pktbuf_t *pb;
  size_t len, i;
  int n = 0;

  pk->n = 0;
  pk->p = malloc((ts->ts_qbytes / 188 + 1) * sizeof(*pk->p));
  if (pk->p == NULL)
    return 0;
  TAILQ_FOREACH(sm, &ts->ts_q, sm_link) {
    pb = sm->sm_data;
    len = pktbuf_len(pb);
    for (i = 0; i + 188 <= len; i += 188)
      if (pktbuf_ptr(pb)[i] == 0x47)
        pk->p[n++] = pktbuf_ptr(pb) + i;
  }
  pk->n = n;
  return 1;
}

/* keyframe at packet i (video PUSI): the ES after the PES header from this
 * and the following packets of the PID */
static int
tss_keyframe(dvbbuffer_tsstart_t *ts, tss_pkts_t *pk, int i)
{
  uint8_t es[TSS_ES_SCAN];
  size_t len = 0, l;
  int j, off = -1, r = RAP_UNKNOWN;

  tss_pes_pts(pk->p[i], &off);
  if (off <= 0 || off >= 188)        /* no PES header (or it goes on) */
    return RAP_NO;
  for (j = i; j < pk->n && len < sizeof(es); j++) {
    if (tss_pid(pk->p[j]) != ts->ts_vpid)
      continue;
    if (j > i) {
      if (tss_pusi(pk->p[j]))
        break;
      off = tss_payload(pk->p[j]);
    }
    if (off < 0 || off >= 188)
      continue;
    l = MIN(188 - off, sizeof(es) - len);
    memcpy(es + len, pk->p[j] + off, l);
    len += l;
  }
  switch (ts->ts_vtype) {
  case V_MPEG2: r = tss_rap_mpeg2(es, len); break;
  case V_H264:  r = tss_rap_h264(es, len); break;
  case V_HEVC:  r = tss_rap_hevc(es, len); break;
  default:      break;
  }
  if (r == RAP_UNKNOWN && ts->ts_vtype == V_UNKNOWN)
    r = tss_rai(pk->p[i]) ? RAP_YES : RAP_NO;
  return r;
}

/* ---------------------------------------------------------------------------
 * Output
 * ------------------------------------------------------------------------- */

typedef struct tss_out {
  dvbbuffer_tsstart_t *ts;
  uint8_t buf[TSS_CHUNK * 188];
  int n, sent;
} tss_out_t;

static void
tss_out_flush(tss_out_t *o)
{
  pktbuf_t *pb;

  if (o->n == 0)
    return;
  if ((pb = pktbuf_alloc(o->buf, o->n * 188)) != NULL)
    streaming_target_deliver2(o->ts->ts_output, streaming_msg_create_data(SMT_MPEGTS, pb));
  o->n = 0;
}

static void
tss_out_add(tss_out_t *o, const uint8_t *p)
{
  memcpy(o->buf + o->n * 188, p, 188);
  o->sent++;
  if (++o->n == TSS_CHUNK)
    tss_out_flush(o);
}

/*
 * A PAT of our own for the start point: a source without a ring has no PAT in
 * front of the start (the table packets of the first chunk pass the service
 * before it listens to them), and a TS player discards the video until it
 * has seen PAT and PMT. One program: the service id on the PMT PID; the pass
 * muxer behind rewrites it for its output.
 */
static void
tss_out_pat(tss_out_t *o, int sid, int pmt_pid)
{
  uint8_t p[188], *b;
  uint32_t crc;

  memset(p, 0xff, sizeof(p));
  p[0] = 0x47;
  p[1] = 0x40;                     /* PUSI, PID 0 */
  p[2] = 0x00;
  p[3] = 0x10;                     /* payload only, CC rewritten by the muxer */
  p[4] = 0x00;                     /* pointer field */
  b = p + 5;
  b[0] = 0x00;                     /* table id: PAT */
  b[1] = 0xb0;
  b[2] = 13;                       /* 5 + one program (4) + CRC (4) */
  b[3] = sid >> 8;                 /* transport stream id */
  b[4] = sid;
  b[5] = 0xc1;                     /* version 0, current */
  b[6] = b[7] = 0;                 /* section 0 of 0 */
  b[8] = sid >> 8;
  b[9] = sid;
  b[10] = 0xe0 | ((pmt_pid >> 8) & 0x1f);
  b[11] = pmt_pid;
  crc = tvh_crc32(b, 12, 0xffffffff);
  b[12] = crc >> 24;
  b[13] = crc >> 16;
  b[14] = crc >> 8;
  b[15] = crc;
  tss_out_add(o, p);
  o->sent--;                       /* not one of the held packets */
}

/* the section starting at packet start (PUSI) of this PID - with its
 * continuation, which may follow the start point (long PMTs) - marked as
 * passed */
static void
tss_out_section(tss_out_t *o, tss_pkts_t *pk, int start, int pid, uint8_t *done)
{
  int i;

  for (i = start; i >= 0 && i < pk->n; i++) {
    if (tss_pid(pk->p[i]) != pid)
      continue;
    if (i > start && tss_pusi(pk->p[i]))
      break;
    tss_out_add(o, pk->p[i]);
    done[i] = 1;
  }
}

static void
tss_flush(dvbbuffer_tsstart_t *ts, int deliver)
{
  streaming_message_t *sm;

  while ((sm = TAILQ_FIRST(&ts->ts_q)) != NULL) {
    TAILQ_REMOVE(&ts->ts_q, sm, sm_link);
    if (deliver)
      streaming_target_deliver2(ts->ts_output, sm);
    else
      streaming_msg_free(sm);
  }
  ts->ts_qbytes = 0;
}

/* first PES at or after the cut on this audio/subtitle PID */
static inline int
tss_starts(dvbbuffer_tsstart_t *ts, const uint8_t *p)
{
  int64_t pts;

  if (!tss_pusi(p))
    return 0;
  pts = tss_pes_pts(p, NULL);
  return pts >= 0 && tss_ts_diff(pts, ts->ts_cut) >= 0;
}

static inline void
tss_set_started(dvbbuffer_tsstart_t *ts, int pid)
{
  ts->ts_started[pid >> 3] |= 1 << (pid & 7);
}

static inline int
tss_is_started(dvbbuffer_tsstart_t *ts, int pid)
{
  return (ts->ts_started[pid >> 3] >> (pid & 7)) & 1;
}

/* live edge: up to where the video and the first audio PID are held (a TS
 * player waits for these, not for further audio tracks); -1 = none */
static int64_t
tss_edge(const dvbbuffer_tsstart_t *ts, const tss_pkts_t *pk)
{
  int64_t vtop = -1, atop = -1, pts;
  int i, pid;

  for (i = 0; i < pk->n; i++) {
    pid = tss_pid(pk->p[i]);
    if ((pid != ts->ts_vpid && pid != ts->ts_apid) || !tss_pusi(pk->p[i]))
      continue;
    if ((pts = tss_pes_pts(pk->p[i], NULL)) < 0)
      continue;
    if (pid == ts->ts_vpid) {
      if (vtop < 0 || tss_ts_diff(pts, vtop) > 0)
        vtop = pts;
    } else if (atop < 0 || tss_ts_diff(pts, atop) > 0) {
      atop = pts;
    }
  }
  if (vtop < 0)
    return atop;
  if (atop < 0 || tss_ts_diff(vtop, atop) < 0)
    return vtop;
  return atop;
}

/* the start: the newest keyframe (audio PES) of the lead PID at least the
 * lag behind the edge, else the oldest one; -1 = none, *kpts its PTS */
static int
tss_find_key(dvbbuffer_tsstart_t *ts, tss_pkts_t *pk, int64_t edge, int64_t *kpts)
{
  int64_t pts, lag = (int64_t)dvbbuffer_conf.start_lag_ms * 90;
  int i, key = -1, oldest = -1, npes = 0, nrap[3] = { 0, 0, 0 }, r;

  for (i = 0; edge >= 0 && i < pk->n; i++) {
    if (tss_pid(pk->p[i]) != ts->ts_lead || !tss_pusi(pk->p[i]) ||
        (pts = tss_pes_pts(pk->p[i], NULL)) < 0)
      continue;
    npes++;
    if (ts->ts_vpid >= 0) {
      r = tss_keyframe(ts, pk, i);
      nrap[r]++;
      if (r != RAP_YES)
        continue;
    }
    if (oldest < 0)
      oldest = i;
    if (tss_ts_diff(edge, pts) >= lag) {
      key = i;
      *kpts = pts;
    }
  }
  if (key < 0 && oldest >= 0) {
    key = oldest;
    *kpts = tss_pes_pts(pk->p[key], NULL);
  }
  if (key < 0)
    tvhtrace(LS_DVBBUFFER, "live start (TS): no start point in %d packets (%d lead PES, "
             "keyframes yes %d no %d unknown %d)", pk->n, npes, nrap[RAP_YES], nrap[RAP_NO],
             nrap[RAP_UNKNOWN]);
  return key;
}

/* the packet passes from the start on: audio and subtitles from their first
 * PES at the start, teletext from its first PES, the rest from the key */
static int
tss_passes(dvbbuffer_tsstart_t *ts, const uint8_t *p, int i, int key, int *ttx)
{
  int pid = tss_pid(p);

  switch (ts->ts_kind[pid]) {
  case K_TTX:
    if (!tss_is_started(ts, pid)) {
      if (!tss_pusi(p))
        return 0;
      tss_set_started(ts, pid);
    }
    (*ttx)++;
    return 1;
  case K_AUDIO:
  case K_SUB:
    if (pid == ts->ts_lead) {
      if (i < key)
        return 0;
      tss_set_started(ts, pid);
      return 1;
    }
    if (!tss_is_started(ts, pid)) {
      if (!tss_starts(ts, p))
        return 0;
      tss_set_started(ts, pid);
    }
    return 1;
  default:
    /* video, PCR, the rest; PAT/PMT before the start went first */
    return i >= key;
  }
}

/* PAT and PMT before the start, then what passes; the number passed, -1 =
 * out of memory; *notes: what was missing (log line) */
static int
tss_emit(dvbbuffer_tsstart_t *ts, tss_pkts_t *pk, int key, int *ttx, char *notes, size_t nsize)
{
  tss_out_t *o;
  uint8_t *done;
  size_t l = 0;
  int i, pid, pat = -1, pmt = -1, sent;

  /* the last PAT and PMT section before it */
  for (i = 0; i < key; i++) {
    if (!tss_pusi(pk->p[i]))
      continue;
    pid = tss_pid(pk->p[i]);
    if (pid == 0)
      pat = i;
    else if (pid == ts->ts_pmt)
      pmt = i;
  }
  o = malloc(sizeof(*o));
  done = calloc(1, pk->n);
  if (o == NULL || done == NULL) {
    free(o);
    free(done);
    return -1;
  }
  o->ts = ts;
  o->n = o->sent = 0;
  notes[0] = '\0';
  if (pat >= 0) {
    tss_out_section(o, pk, pat, 0, done);
  } else if (ts->ts_sid > 0 && ts->ts_pmt > 0 && ts->ts_pmt < 8192) {
    tss_out_pat(o, ts->ts_sid, ts->ts_pmt);
    tvh_strlcatf(notes, nsize, l, ", PAT made");
  } else {
    tvh_strlcatf(notes, nsize, l, ", no PAT");
  }
  if (pmt < 0)
    tvh_strlcatf(notes, nsize, l, ", no PMT");
  tss_out_section(o, pk, pmt, ts->ts_pmt, done);
  for (i = 0; i < pk->n; i++)
    if (!done[i] && tss_passes(ts, pk->p[i], i, key, ttx))
      tss_out_add(o, pk->p[i]);
  tss_out_flush(o);
  sent = o->sent;
  free(o);
  free(done);
  return sent;
}

/*
 * Real time reached: pass the held TS on from the start point.
 * 0 = no keyframe held yet (video), keep holding
 */
static int
tss_release(dvbbuffer_tsstart_t *ts, int64_t now)
{
  tss_pkts_t pk;
  int64_t edge, kpts = -1;
  char notes[32];
  int key, sent, ttx = 0;

  if (!tss_index(ts, &pk)) {
    tss_flush(ts, 1);
    ts->ts_armed = ts->ts_need_key = 0;
    return 1;
  }
  edge = tss_edge(ts, &pk);
  if ((key = tss_find_key(ts, &pk, edge, &kpts)) < 0) {
    free(pk.p);
    return 0;
  }
  ts->ts_cut = kpts;
  ts->ts_cut_until = now + TSS_CUT;
  memset(ts->ts_started, 0, sizeof(ts->ts_started));
  if ((sent = tss_emit(ts, &pk, key, &ttx, notes, sizeof(notes))) < 0) {
    /* out of memory: as it is */
    free(pk.p);
    tss_flush(ts, 1);
    ts->ts_armed = ts->ts_need_key = 0;
    return 1;
  }
  tvhdebug(LS_DVBBUFFER, "live start (TS) at the %s %"PRId64" ms behind live: "
           "%d of %d packets passed (%d teletext)%s, held %"PRId64" ms",
           ts->ts_vpid >= 0 ? "keyframe" : "audio PES",
           tss_ts_diff(edge, kpts) / 90, sent, pk.n, ttx, notes,
           (now - ts->ts_first) / 1000);
  free(pk.p);
  tss_flush(ts, 0);
  ts->ts_armed = ts->ts_need_key = 0;
  return 1;
}

/* just started: audio/subtitle PIDs not begun yet wait for their first PES
 * at the start (sent late in the mux) */
static void
tss_cut_live(dvbbuffer_tsstart_t *ts, streaming_message_t *sm)
{
  pktbuf_t *pb = sm->sm_data;
  const uint8_t *p, *end;
  tss_out_t *o;
  int pid, k, drop = 0;

  end = pktbuf_ptr(pb) + pktbuf_len(pb);
  for (p = pktbuf_ptr(pb); p + 188 <= end; p += 188) {
    pid = tss_pid(p);
    k = ts->ts_kind[pid];
    if ((k == K_AUDIO || k == K_SUB) && !tss_is_started(ts, pid)) {
      if (tss_starts(ts, p))
        tss_set_started(ts, pid);
      else
        drop = 1;
    }
  }
  if (!drop) {
    streaming_target_deliver2(ts->ts_output, sm);
    return;
  }
  /* again, now passing only what goes */
  if ((o = malloc(sizeof(*o))) == NULL) {
    streaming_target_deliver2(ts->ts_output, sm);
    return;
  }
  o->ts = ts;
  o->n = o->sent = 0;
  for (p = pktbuf_ptr(pb); p + 188 <= end; p += 188) {
    pid = tss_pid(p);
    k = ts->ts_kind[pid];
    if ((k == K_AUDIO || k == K_SUB) && !tss_is_started(ts, pid))
      continue;
    tss_out_add(o, p);
  }
  tss_out_flush(o);
  free(o);
  streaming_msg_free(sm);
}

/* 1 = the PID is in it, 2 = a PES of it starts there */
static int
tss_has_pid(pktbuf_t *pb, int pid)
{
  const uint8_t *p = pktbuf_ptr(pb), *end = p + pktbuf_len(pb);
  int r = 0;

  for (; p + 188 <= end; p += 188)
    if (tss_pid(p) == pid) {
      if (tss_pusi(p))
        return 2;
      r = 1;
    }
  return r;
}

static int
tss_vtype(streaming_component_type_t type)
{
  switch (type) {
  case SCT_MPEG2VIDEO: return V_MPEG2;
  case SCT_H264:       return V_H264;
  case SCT_HEVC:       return V_HEVC;
  default:             return V_UNKNOWN;
  }
}

static void
tss_arm(dvbbuffer_tsstart_t *ts, const streaming_start_t *ss)
{
  const streaming_start_component_t *ssc;
  int i, audio = -1;

  tss_flush(ts, 0);
  ts->ts_armed = ts->ts_need_key = 0;
  ts->ts_cut = -1;
  ts->ts_vpid = ts->ts_lead = -1;
  ts->ts_vtype = V_UNKNOWN;
  ts->ts_last = 0;
  ts->ts_lead_seen = 0;
  memset(ts->ts_kind, 0, sizeof(ts->ts_kind));
  /* only a start with the ring backlog (not a restart, not a cold mux) */
  if (!ts->ts_live || !ss->ss_dvbbuffer || !dvbbuffer_running || !dvbbuffer_conf.enabled)
    return;
  ts->ts_pmt = ss->ss_pmt_pid;
  ts->ts_sid = ss->ss_service_id;
  for (i = 0; i < ss->ss_num_components; i++) {
    ssc = &ss->ss_components[i];
    if (ssc->ssc_disabled || ssc->es_pid <= 0 || ssc->es_pid >= 8192)
      continue;
    if (SCT_ISVIDEO(ssc->es_type)) {
      if (ts->ts_vpid < 0) {
        ts->ts_vpid = ssc->es_pid;
        ts->ts_vtype = tss_vtype(ssc->es_type);
      }
      ts->ts_kind[ssc->es_pid] = K_VIDEO;
    } else if (SCT_ISAUDIO(ssc->es_type)) {
      ts->ts_kind[ssc->es_pid] = K_AUDIO;
      if (audio < 0)
        audio = ssc->es_pid;
    } else if (ssc->es_type == SCT_TELETEXT) {
      ts->ts_kind[ssc->es_pid] = K_TTX;
    } else if (SCT_ISSUBTITLE(ssc->es_type)) {
      ts->ts_kind[ssc->es_pid] = K_SUB;
    }
  }
  ts->ts_lead = ts->ts_vpid >= 0 ? ts->ts_vpid : audio;
  ts->ts_apid = audio;
  ts->ts_armed = ts->ts_lead >= 0;
}

/* holding since the start: the TS waits until it arrives in real time */
static void
tss_hold(dvbbuffer_tsstart_t *ts, streaming_message_t *sm)
{
  int64_t now = getmonoclock();
  int lead;

  if (TAILQ_EMPTY(&ts->ts_q))
    ts->ts_first = now;
  TAILQ_INSERT_TAIL(&ts->ts_q, sm, sm_link);
  ts->ts_qbytes += pktbuf_len(sm->sm_data);
  lead = tss_has_pid(sm->sm_data, ts->ts_lead);
  if (ts->ts_need_key) {
    /* a keyframe starts with a PES of the lead */
    if (lead == 2)
      tss_release(ts, now);
  } else if (ts->ts_lead_seen && now - ts->ts_last >= TSS_GAP && !tss_release(ts, now)) {
    ts->ts_need_key = 1;
  }
  if (lead)
    ts->ts_lead_seen = 1;
  ts->ts_last = now;
  /* safety: something else than a backlog burst - pass it as it is */
  if (ts->ts_armed && (now - ts->ts_first > TSS_HOLD || ts->ts_qbytes > TSS_MAXBYTES)) {
    tvhdebug(LS_DVBBUFFER, "live start (TS): no start point after %"PRId64" ms, "
             "%zu kB passed as they are", (now - ts->ts_first) / 1000, ts->ts_qbytes / 1024);
    tss_flush(ts, 1);
    ts->ts_armed = ts->ts_need_key = 0;
  }
}

static void
tss_input(void *opaque, streaming_message_t *sm)
{
  dvbbuffer_tsstart_t *ts = opaque;

  switch (sm->sm_type) {
  case SMT_MPEGTS:
    if (ts->ts_armed) {
      tss_hold(ts, sm);
      return;
    }
    if (ts->ts_cut >= 0 && getmonoclock() > ts->ts_cut_until)
      ts->ts_cut = -1;
    if (ts->ts_cut >= 0) {
      tss_cut_live(ts, sm);
      return;
    }
    break;
  case SMT_START:
    tss_arm(ts, sm->sm_data);
    break;
  case SMT_STOP:
    tss_flush(ts, 0);
    ts->ts_armed = ts->ts_need_key = 0;
    ts->ts_cut = -1;
    break;
  default:
    break;
  }
  streaming_target_deliver2(ts->ts_output, sm);
}

static htsmsg_t *
tss_input_info(void *opaque, htsmsg_t *list)
{
  dvbbuffer_tsstart_t *ts = opaque;
  streaming_target_t *st = ts->ts_output;

  htsmsg_add_str(list, NULL, "dvbbuffer live start (TS) input");
  return st->st_ops.st_info(st->st_opaque, list);
}

static streaming_ops_t tss_input_ops = {
  .st_cb   = tss_input,
  .st_info = tss_input_info
};

streaming_target_t *
dvbbuffer_ts_start_create(streaming_target_t *output, int live)
{
  dvbbuffer_tsstart_t *ts = calloc(1, sizeof(*ts));

  if (ts == NULL)
    return output;    /* no live start (the chain destroys only a filter) */
  ts->ts_output = output;
  ts->ts_live = live;
  ts->ts_vpid = ts->ts_lead = -1;
  ts->ts_cut = -1;
  TAILQ_INIT(&ts->ts_q);
  streaming_target_init(&ts->ts_input, &tss_input_ops, ts, 0);
  return &ts->ts_input;
}

void
dvbbuffer_ts_start_destroy(streaming_target_t *st)
{
  dvbbuffer_tsstart_t *ts = (dvbbuffer_tsstart_t *)st;

  if (ts == NULL)
    return;
  tss_flush(ts, 0);
  free(ts);
}
