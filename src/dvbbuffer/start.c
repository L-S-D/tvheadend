/*
 *  Tvheadend - instant channel switching, live start at the output
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  A service started on a warm mux gets the ring backlog (some seconds) at
 *  once, decrypted and parsed like live data. A live client should not play
 *  from its start: this filter in front of tsfix holds the packets from the
 *  start until the stream arrives in real time, then passes them on from the
 *  newest video keyframe at least start_lag_ms behind live. Video, audio and
 *  subtitles start there; teletext is passed whole (page caches of the
 *  clients). What is kept from before the start goes right after it - tsfix
 *  keeps packets before its reference only when audio or video came early -
 *  with the PCR of the start (tsfix drops packets with a PCR before its
 *  reference). Teletext older than the start - before it and, sent late in
 *  the mux, for a while after it - gets the timestamps of the start: tsfix
 *  drops it otherwise, or times the teletext stream from its oldest packet.
 *  Without video (radio) the start is the newest audio packet.
 *
 *  Real time: the backlog comes in one run of the input thread (backlog pass
 *  or the flush of the descrambler queue), its packets at most a few
 *  milliseconds apart; the first packet of any stream START_GAP after the
 *  previous one is live (live packets come with the chunks of the tuner,
 *  every few milliseconds - the next video frame only every 20-40 ms).
 */

#include "dvbbuffer_private.h"
#include "streaming.h"
#include "packet.h"

#define START_GAP         8000      /* us between two packets: real time */
#define START_HOLD     2000000      /* us: held at most this long */
#define START_MAX        30000      /* packets: held at most this many */
#define START_CUT      3000000      /* us after the start: later packets older
                                       than the start are still dropped */

typedef struct dvbbuffer_start {
  streaming_target_t  ds_input;
  streaming_target_t *ds_output;
  int                 ds_live;      /* profile option: start live */
  int                 ds_armed;     /* holding since SMT_START */
  int                 ds_need_key;  /* released, but no keyframe yet */
  int                 ds_lead;      /* component index of the lead stream */
  int                 ds_lead_video;
  int64_t             ds_first;     /* arrival of the first held packet */
  int64_t             ds_last;      /* arrival of the previous packet */
  int                 ds_lead_seen; /* a lead packet came: gaps count from now on
                                       (clear packets may come before the keys) */
  int64_t             ds_cut;       /* start (90 kHz), PTS_UNSET = none */
  int64_t             ds_cut_until; /* mono us */
  int64_t             ds_k_pts, ds_k_dts, ds_k_pcr; /* of the start packet */
  struct streaming_message_queue ds_q;
  int                 ds_qlen;
} dvbbuffer_start_t;

/* timestamps per component index: more components are not considered */
#define START_COMPONENTS 256

/* a - b of two 33 bit timestamps */
static inline int64_t
start_ts_diff(int64_t a, int64_t b)
{
  int64_t d = (a - b) & PTS_MASK;
  return d > PTS_MASK / 2 ? d - PTS_MASK - 1 : d;
}

static inline int64_t
start_pkt_ts(const th_pkt_t *pkt)
{
  return pkt->pkt_pts != PTS_UNSET ? pkt->pkt_pts : pkt->pkt_dts;
}

/* teletext or AIT (its packets carry the PCR as time) older than the start */
static inline int
start_ttx_old(const dvbbuffer_start_t *ds, const th_pkt_t *pkt)
{
  int64_t ts = pkt->pkt_dts != PTS_UNSET ? pkt->pkt_dts : pkt->pkt_pts;

  return (pkt->pkt_type == SCT_TELETEXT || pkt->pkt_type == SCT_HBBTV) &&
         (ts == PTS_UNSET || start_ts_diff(ts, ds->ds_k_dts) < 0);
}

/* a copy of the packet with the timestamps of the start: the PCR, teletext
 * older than the start also PTS/DTS (the packet may be shared with other
 * subscriptions of the service) */
static streaming_message_t *
start_retime(dvbbuffer_start_t *ds, streaming_message_t *sm, int pcr)
{
  th_pkt_t *pkt = sm->sm_data, *pkt2;
  streaming_message_t *sm2;

  if ((pkt2 = pkt_copy_shallow(pkt)) == NULL)
    return sm;    /* out of memory: as it is */
  if (pcr)
    pkt2->pkt_pcr = ds->ds_k_pcr;
  if (start_ttx_old(ds, pkt)) {
    pkt2->pkt_pts = ds->ds_k_pts;
    pkt2->pkt_dts = ds->ds_k_dts;
  }
  sm2 = streaming_msg_create_pkt(pkt2);
  pkt_ref_dec(pkt2);
  streaming_msg_free(sm);
  return sm2;
}

static void
start_flush(dvbbuffer_start_t *ds, int deliver)
{
  streaming_message_t *sm;

  while ((sm = TAILQ_FIRST(&ds->ds_q)) != NULL) {
    TAILQ_REMOVE(&ds->ds_q, sm, sm_link);
    if (deliver)
      streaming_target_deliver2(ds->ds_output, sm);
    else
      streaming_msg_free(sm);
  }
  ds->ds_qlen = 0;
}

/* the lead stream: the first video component, else the first audio one */
static void
start_arm(dvbbuffer_start_t *ds, const streaming_start_t *ss)
{
  const streaming_start_component_t *ssc;
  int i, audio = -1;

  start_flush(ds, 0);
  ds->ds_armed = ds->ds_need_key = 0;
  ds->ds_cut = PTS_UNSET;
  ds->ds_lead = -1;
  ds->ds_lead_video = 0;
  ds->ds_last = 0;
  ds->ds_lead_seen = 0;
  /* only a start with the ring backlog (not a restart, not a cold mux) */
  if (!ds->ds_live || !ss->ss_dvbbuffer || !dvbbuffer_running || !dvbbuffer_conf.enabled)
    return;
  for (i = 0; i < ss->ss_num_components; i++) {
    ssc = &ss->ss_components[i];
    if (ssc->ssc_disabled)
      continue;
    if (SCT_ISVIDEO(ssc->es_type)) {
      ds->ds_lead = ssc->es_index;
      ds->ds_lead_video = 1;
      break;
    }
    if (audio < 0 && SCT_ISAUDIO(ssc->es_type))
      audio = ssc->es_index;
  }
  if (ds->ds_lead < 0)
    ds->ds_lead = audio;
  ds->ds_armed = ds->ds_lead >= 0;
}

/* live edge: up to where every audio and video stream is held - audio comes
 * later in the mux than video, and globalheaders waits for a packet of each
 * stream; a start beyond it would wait for the audio first */
static int64_t
start_edge(const dvbbuffer_start_t *ds)
{
  const streaming_message_t *sm;
  const th_pkt_t *pkt;
  int64_t top[START_COMPONENTS], edge = PTS_UNSET, ts;
  int i;

  for (i = 0; i < START_COMPONENTS; i++)
    top[i] = PTS_UNSET;
  TAILQ_FOREACH(sm, &ds->ds_q, sm_link) {
    pkt = sm->sm_data;
    i = pkt->pkt_componentindex;
    if (i < 0 || i >= START_COMPONENTS || !SCT_ISAV(pkt->pkt_type) ||
        (ts = start_pkt_ts(pkt)) == PTS_UNSET)
      continue;
    if (top[i] == PTS_UNSET || start_ts_diff(ts, top[i]) > 0)
      top[i] = ts;
  }
  for (i = 0; i < START_COMPONENTS; i++)
    if (top[i] != PTS_UNSET && (edge == PTS_UNSET || start_ts_diff(top[i], edge) < 0))
      edge = top[i];
  return edge;
}

/* the start: the newest keyframe (audio packet) of the lead stream at least
 * the lag behind the edge, else the oldest one; NULL = none held */
static streaming_message_t *
start_find_key(const dvbbuffer_start_t *ds, int64_t edge)
{
  streaming_message_t *sm, *key = NULL, *oldest = NULL;
  const th_pkt_t *pkt;
  int64_t ts, lag = (int64_t)dvbbuffer_conf.start_lag_ms * 90;

  TAILQ_FOREACH(sm, &ds->ds_q, sm_link) {
    pkt = sm->sm_data;
    if (pkt->pkt_componentindex != ds->ds_lead || (ts = start_pkt_ts(pkt)) == PTS_UNSET)
      continue;
    if (ds->ds_lead_video && pkt->v.pkt_frametype != PKT_I_FRAME)
      continue;
    if (oldest == NULL)
      oldest = sm;
    if (edge != PTS_UNSET && start_ts_diff(edge, ts) >= lag)
      key = sm;
  }
  return key ? key : oldest;
}

/* pass the held packets on from the start (key): what is kept from before it
 * goes right after it with its PCR; the number passed, *ttx = teletext */
static int
start_deliver(dvbbuffer_start_t *ds, streaming_message_t *key, int *ttx)
{
  struct streaming_message_queue laterq;
  streaming_message_t *sm, *sm2;
  th_pkt_t *pkt;
  int64_t ts;
  int before = 1, sent = 0, is_key;

  TAILQ_INIT(&laterq);
  while ((sm = TAILQ_FIRST(&ds->ds_q)) != NULL) {
    TAILQ_REMOVE(&ds->ds_q, sm, sm_link);
    pkt = sm->sm_data;
    ts = start_pkt_ts(pkt);
    if (pkt->pkt_type == SCT_TELETEXT) {
      (*ttx)++;
    } else if (pkt->pkt_type == SCT_HBBTV) {
      /* AIT: the parser forwards it only when its version changes - cut
       * here, a subscriber would not see it until the broadcaster changes
       * the AIT (no HbbTV); goes after the start, retimed to it like
       * teletext */
    } else if ((before && pkt->pkt_componentindex == ds->ds_lead && sm != key) ||
               (ts != PTS_UNSET && start_ts_diff(ts, ds->ds_cut) < 0)) {
      streaming_msg_free(sm);
      continue;
    }
    if (before && sm != key) {
      /* after the start, with its PCR (TAILQ macros: no calls inside) */
      sm2 = start_retime(ds, sm, 1);
      TAILQ_INSERT_TAIL(&laterq, sm2, sm_link);
      continue;
    }
    if (start_ttx_old(ds, pkt))
      sm = start_retime(ds, sm, 0);
    sent++;
    /* the message belongs to the output after the delivery: decide first */
    is_key = (sm == key);
    streaming_target_deliver2(ds->ds_output, sm);
    if (is_key) {
      before = 0;
      while ((sm = TAILQ_FIRST(&laterq)) != NULL) {
        TAILQ_REMOVE(&laterq, sm, sm_link);
        sent++;
        streaming_target_deliver2(ds->ds_output, sm);
      }
    }
  }
  return sent;
}

/*
 * Real time reached: pass the held packets on from the start point.
 * 0 = no keyframe held yet (video), keep holding
 */
static int
start_release(dvbbuffer_start_t *ds, int64_t now)
{
  streaming_message_t *key;
  th_pkt_t *kpkt;
  int64_t edge = start_edge(ds);
  int sent, ttx = 0, held = ds->ds_qlen;

  if ((key = start_find_key(ds, edge)) == NULL)
    return 0;
  kpkt = key->sm_data;
  ds->ds_cut = start_pkt_ts(kpkt);
  ds->ds_k_pts = kpkt->pkt_pts;
  ds->ds_k_dts = kpkt->pkt_dts != PTS_UNSET ? kpkt->pkt_dts : kpkt->pkt_pts;
  ds->ds_k_pcr = kpkt->pkt_pcr;
  ds->ds_cut_until = now + START_CUT;
  sent = start_deliver(ds, key, &ttx);
  ds->ds_qlen = 0;
  ds->ds_armed = ds->ds_need_key = 0;
  tvhdebug(LS_DVBBUFFER, "live start at the %s %"PRId64" ms behind live: %d of %d "
           "packets passed (%d teletext), held %"PRId64" ms",
           ds->ds_lead_video ? "keyframe" : "audio packet",
           edge != PTS_UNSET ? start_ts_diff(edge, ds->ds_cut) / 90 : 0, sent, held, ttx,
           (now - ds->ds_first) / 1000);
  return 1;
}

/* not holding: just started, packets older than the start, late in the mux
 * (audio after video), are dropped (teletext and HbbTV are retimed instead) */
static void
start_live_packet(dvbbuffer_start_t *ds, streaming_message_t *sm)
{
  th_pkt_t *pkt = sm->sm_data;
  int64_t ts;

  if (ds->ds_cut != PTS_UNSET && getmonoclock() > ds->ds_cut_until)
    ds->ds_cut = PTS_UNSET;
  if (ds->ds_cut == PTS_UNSET) {
    streaming_target_deliver2(ds->ds_output, sm);
    return;
  }
  if (pkt->pkt_type == SCT_TELETEXT || pkt->pkt_type == SCT_HBBTV) {
    if (start_ttx_old(ds, pkt))
      sm = start_retime(ds, sm, 0);
  } else if ((ts = start_pkt_ts(pkt)) != PTS_UNSET && start_ts_diff(ts, ds->ds_cut) < 0) {
    streaming_msg_free(sm);
    return;
  }
  streaming_target_deliver2(ds->ds_output, sm);
}

static void
start_packet(dvbbuffer_start_t *ds, streaming_message_t *sm)
{
  th_pkt_t *pkt = sm->sm_data;
  int64_t now;
  int lead;

  if (!ds->ds_armed) {
    start_live_packet(ds, sm);
    return;
  }

  now = getmonoclock();
  if (ds->ds_qlen == 0)
    ds->ds_first = now;
  TAILQ_INSERT_TAIL(&ds->ds_q, sm, sm_link);
  ds->ds_qlen++;

  lead = pkt->pkt_componentindex == ds->ds_lead;
  if (ds->ds_need_key) {
    if (lead && (!ds->ds_lead_video || pkt->v.pkt_frametype == PKT_I_FRAME))
      start_release(ds, now);
  } else if (ds->ds_lead_seen && now - ds->ds_last >= START_GAP && !start_release(ds, now)) {
    ds->ds_need_key = 1;
  }
  if (lead)
    ds->ds_lead_seen = 1;
  ds->ds_last = now;
  /* safety: something else than a backlog burst - pass it as it is */
  if (ds->ds_armed && (now - ds->ds_first > START_HOLD || ds->ds_qlen > START_MAX)) {
    tvhdebug(LS_DVBBUFFER, "live start: no start point after %"PRId64" ms, "
             "%d packets passed as they are", (now - ds->ds_first) / 1000, ds->ds_qlen);
    start_flush(ds, 1);
    ds->ds_armed = ds->ds_need_key = 0;
  }
}

static void
start_input(void *opaque, streaming_message_t *sm)
{
  dvbbuffer_start_t *ds = opaque;

  switch (sm->sm_type) {
  case SMT_PACKET:
    start_packet(ds, sm);
    return;
  case SMT_START:
    start_arm(ds, sm->sm_data);
    break;
  case SMT_STOP:
    start_flush(ds, 0);
    ds->ds_armed = ds->ds_need_key = 0;
    ds->ds_cut = PTS_UNSET;
    break;
  default:
    break;
  }
  streaming_target_deliver2(ds->ds_output, sm);
}

static htsmsg_t *
start_input_info(void *opaque, htsmsg_t *list)
{
  dvbbuffer_start_t *ds = opaque;
  streaming_target_t *st = ds->ds_output;

  htsmsg_add_str(list, NULL, "dvbbuffer live start input");
  return st->st_ops.st_info(st->st_opaque, list);
}

static streaming_ops_t start_input_ops = {
  .st_cb   = start_input,
  .st_info = start_input_info
};

streaming_target_t *
dvbbuffer_start_create(streaming_target_t *output, int live)
{
  dvbbuffer_start_t *ds = calloc(1, sizeof(*ds));

  if (ds == NULL)
    return output;    /* no live start (the chain destroys only a filter) */
  ds->ds_output = output;
  ds->ds_live = live;
  ds->ds_lead = -1;
  ds->ds_cut = PTS_UNSET;
  TAILQ_INIT(&ds->ds_q);
  streaming_target_init(&ds->ds_input, &start_input_ops, ds, 0);
  return &ds->ds_input;
}

void
dvbbuffer_start_destroy(streaming_target_t *st)
{
  dvbbuffer_start_t *ds = (dvbbuffer_start_t *)st;

  if (ds == NULL)
    return;
  start_flush(ds, 0);
  free(ds);
}
