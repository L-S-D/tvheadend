/*
 *  Tvheadend - instant channel switching, Kodi/tvh path
 *
 *  On service start the raw (still scrambled) packets of the service since
 *  its last keyframe are taken from the mux ring buffer and injected in
 *  front of tvh's descrambler, paced, until the backlog reaches the live
 *  packet; from there tvh continues live (exact handoff, no gap, no
 *  duplicate). Pay-TV waits until tvh's own descrambler has a key.
 *
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "dvbbuffer_private.h"
#include "subscriptions.h"
#include "input/mpegts/tsdemux.h"
#include "descrambler/descrambler.h"

typedef enum {
  DVBBUFFER_SVC_WAIT,     /* live packets are dropped (they are in the ring) */
  DVBBUFFER_SVC_CATCHUP,  /* backlog injection */
  DVBBUFFER_SVC_LIVE      /* done, tvh processes live packets */
} dvbbuffer_svc_state_t;

typedef struct dvbbuffer_svc {
  /* global_lock */
  dvbbuffer_mux_t *ds_mux;
  mtimer_t         ds_decide_timer;
  /* s_stream_mutex */
  dvbbuf_raw      *ds_raw;
  dvbbuffer_svc_state_t ds_state;
  int              ds_decided;      /* decision timer ran */
  int              ds_allowed;      /* injection allowed for these subscribers */
  int              ds_has_key;      /* H5: tvh obtained a key */
  int64_t          ds_start;        /* mclk() of the service start */
  int64_t          ds_key_wait;     /* key wait limit (mono) */
  uint8_t         *ds_buf;
  uint32_t         ds_buf_packets;
  uint64_t         ds_injected;
  uint64_t         ds_dropped;
} dvbbuffer_svc_t;

/* waiting for subscribers / PMT longer than this -> give up */
#define DVBBUFFER_SVC_LINK_WAIT  sec2mono(2)

static void
dvbbuffer_svc_go_live(mpegts_service_t *t, dvbbuffer_svc_t *ds, const char *why)
{
  ds->ds_state = DVBBUFFER_SVC_LIVE;
  tvhdebug(LS_DVBBUFFER, "%s: live (%s), injected %"PRIu64" packets, "
           "held back %"PRIu64" live packets, %"PRId64" ms after start",
           service_nicename((service_t *)t), why, ds->ds_injected, ds->ds_dropped,
           mono2ms(mclk() - ds->ds_start));
}

/*
 * Decision on the main thread (global_lock): subscriptions are linked to the
 * service right after mpegts_service_start(), so they are visible here.
 */
static void
dvbbuffer_svc_decide(void *aux)
{
  mpegts_service_t *t = aux;
  dvbbuffer_svc_t *ds = t->s_dvbbuffer;
  th_subscription_t *s;
  int allowed = 1;

  if (ds == NULL)
    return;
  if (!dvbbuffer_conf.inject_dvr) {
    LIST_FOREACH(s, &t->s_subscriptions, ths_service_link)
      if (s->ths_title && strncmp(s->ths_title, "DVR: ", 5) == 0)
        allowed = 0;
  }
  tvh_mutex_lock(&t->s_stream_mutex);
  ds->ds_decided = 1;
  ds->ds_allowed = allowed;
  if (!allowed && ds->ds_state == DVBBUFFER_SVC_WAIT)
    dvbbuffer_svc_go_live(t, ds, "recording");
  tvh_mutex_unlock(&t->s_stream_mutex);
}

/*
 * H4 - service start (global_lock), after the mux was started
 */
void
dvbbuffer_service_start(mpegts_service_t *t)
{
  mpegts_mux_t *mm = t->s_dvb_mux;
  dvbbuffer_mux_t *dm = mm ? mm->mm_dvbbuffer : NULL;
  dvbbuffer_svc_t *ds;
  dvbbuf_raw_config cfg;
  dvbbuf_raw *raw;

  lock_assert(&global_lock);

  if (dm == NULL || t->s_dvbbuffer || t->s_type != STYPE_STD ||
      t->s_scrambled_pass || service_id16(t) == 0)
    return;

  memset(&cfg, 0, sizeof(cfg));
  cfg.struct_size = sizeof(cfg);
  cfg.service_id  = service_id16(t);
  cfg.max_age_ms  = dvbbuffer_conf.max_age_ms;
  cfg.burst_bytes = dvbbuffer_conf.burst_kb * 1000;
  cfg.pace_factor = dvbbuffer_conf.pace_factor;
  cfg.keyframe_back = dvbbuffer_conf.keyframe_back;
  if (dvbbuf_raw_open(dm->dm_lib, &cfg, &raw) != DVBBUF_OK) {
    tvherror(LS_DVBBUFFER, "%s: raw reader: %s",
             service_nicename((service_t *)t), dvbbuf_last_error());
    return;
  }

  ds = calloc(1, sizeof(*ds));
  ds->ds_mux = dm;
  dvbbuffer_mux_ref(dm);
  ds->ds_raw = raw;
  ds->ds_state = DVBBUFFER_SVC_WAIT;
  ds->ds_start = mclk();
  ds->ds_key_wait = ds->ds_start + ms2mono(dvbbuffer_conf.key_wait_ms);
  ds->ds_buf_packets = 2048;   /* read in chunks, the reader keeps the budget */
  ds->ds_buf = malloc((size_t)ds->ds_buf_packets * 188);

  tvh_mutex_lock(&t->s_stream_mutex);
  t->s_dvbbuffer = ds;
  tvh_mutex_unlock(&t->s_stream_mutex);

  mtimer_arm_rel(&ds->ds_decide_timer, dvbbuffer_svc_decide, t, 0);
  tvhdebug(LS_DVBBUFFER, "%s: service start, waiting for subscribers%s",
           service_nicename((service_t *)t),
           service_is_encrypted((service_t *)t) ? " and key" : "");
}

/*
 * H4 - service stop (global_lock)
 */
void
dvbbuffer_service_stop(mpegts_service_t *t)
{
  dvbbuffer_svc_t *ds = t->s_dvbbuffer;

  lock_assert(&global_lock);

  if (ds == NULL)
    return;
  mtimer_disarm(&ds->ds_decide_timer);
  tvh_mutex_lock(&t->s_stream_mutex);
  if (ds->ds_state != DVBBUFFER_SVC_LIVE)
    dvbbuffer_svc_go_live(t, ds, "service stop");
  t->s_dvbbuffer = NULL;
  tvh_mutex_unlock(&t->s_stream_mutex);
  dvbbuf_raw_close(ds->ds_raw);
  dvbbuffer_mux_unref(ds->ds_mux);
  free(ds->ds_buf);
  free(ds);
}

/*
 * H5 - descrambler_keys() (s_stream_mutex)
 */
void
dvbbuffer_service_key(service_t *t)
{
  dvbbuffer_svc_t *ds;

  if (t->s_source_type != S_MPEG_TS)
    return;
  ds = ((mpegts_service_t *)t)->s_dvbbuffer;
  if (ds && !ds->ds_has_key) {
    ds->ds_has_key = 1;
    tvhdebug(LS_DVBBUFFER, "%s: tvh has the key after %"PRId64" ms",
             service_nicename(t), mono2ms(mclk() - ds->ds_start));
  }
}

/*
 * Feed backlog packets through tvh's normal path (descrambler/demuxer),
 * in runs of the same PID like mpegts_input_process() does
 */
static void
dvbbuffer_svc_inject(mpegts_service_t *t, const uint8_t *tsb, int len)
{
  int llen, table;
  uint16_t pid;

  while (len > 0) {
    llen = mpegts_word_count(tsb, len, 0xFF9FFFD0);
    pid = ((tsb[1] & 0x1f) << 8) | tsb[2];
    table = pid == 0 || pid == t->s_components.set_pmt_pid ||
            pid == t->s_components.set_pcr_pid;
    ts_recv_packet1_locked(t, pid, tsb, llen, table, (tsb[1] & 0x80) ? 1 : 0);
    tsb += llen;
    len -= llen;
  }
}

/*
 * Is the service ready to receive the backlog?
 * 1 = yes, 0 = keep waiting, -1 = give up (go live, *why set)
 */
static int
dvbbuffer_svc_ready(mpegts_service_t *t, dvbbuffer_svc_t *ds, const char **why)
{
  int64_t now = mclk();

  /* CAM, DD-CI or pass-through: not tvh's software descrambler */
  if (t->s_descramble && t->s_descramble->dr_descramble) {
    *why = "hardware/pass-through descrambling";
    return -1;
  }
  if (!ds->ds_decided || !ds->ds_allowed ||
      t->s_streaming_pad.sp_ntargets == 0 ||
      !elementary_set_has_streams(&t->s_components, 1)) {
    *why = "no subscriber/streams in time";
    return now - ds->ds_start > DVBBUFFER_SVC_LINK_WAIT ? -1 : 0;
  }
  if (service_is_encrypted((service_t *)t) && !ds->ds_has_key) {
    *why = "no key in time";
    return now > ds->ds_key_wait ? -1 : 0;
  }
  return 1;
}

/*
 * H3 - live run of one PID (input thread, s_stream_mutex)
 */
int
dvbbuffer_service_packet0(mpegts_service_t *t, uint64_t tspos,
                          uint16_t pid, const uint8_t *tsb, int len)
{
  dvbbuffer_svc_t *ds = t->s_dvbbuffer;
  elementary_stream_t *st;
  dvbbuf_raw_result res;
  dvbbuf_rap rap;
  const char *why = NULL;
  int r;

  if (ds->ds_state == DVBBUFFER_SVC_LIVE)
    return 0;

  /* the ring buffer of this service was stopped (mux restart) */
  if (t->s_dvb_mux->mm_dvbbuffer != ds->ds_mux) {
    dvbbuffer_svc_go_live(t, ds, "ring buffer stopped");
    return 0;
  }

  /* the elementary streams, PCR, PAT and PMT come from the backlog,
   * ECM and anything else continue live */
  if (pid != t->s_components.set_pcr_pid && pid != 0 &&
      pid != t->s_components.set_pmt_pid) {
    st = elementary_stream_find(&t->s_components, pid);
    if (st == NULL || st->es_type == SCT_CA || st->es_type == SCT_CAT)
      return 0;
  }

  if (ds->ds_state == DVBBUFFER_SVC_WAIT) {
    r = dvbbuffer_svc_ready(t, ds, &why);
    if (r < 0) {
      dvbbuffer_svc_go_live(t, ds, why);
      return 0;
    }
    if (r == 0) {
      ds->ds_dropped += len / 188;
      return 1;
    }
    memset(&rap, 0, sizeof(rap));
    rap.struct_size = sizeof(rap);
    r = dvbbuf_raw_start(ds->ds_raw, &rap);
    if (r == DVBBUF_EAGAIN) {
      ds->ds_dropped += len / 188;
      return 1;
    }
    if (r != DVBBUF_OK) {
      dvbbuffer_svc_go_live(t, ds, "no keyframe in the ring buffer");
      return 0;
    }
    tvhdebug(LS_DVBBUFFER, "%s: injecting from keyframe %"PRId64" ms back "
             "(pid %d, %s), %"PRId64" ms after start",
             service_nicename((service_t *)t), rap.age_us / 1000, rap.pid,
             rap.method == DVBBUF_RAP_ES ? "ES" : "RAI",
             mono2ms(mclk() - ds->ds_start));
    /* subscription_input() drops everything before TSS_PACKETS, which tvh
     * would only set after the first flush of our backlog */
    service_set_streaming_status_flags((service_t *)t, TSS_PACKETS);
    ds->ds_state = DVBBUFFER_SVC_CATCHUP;
  }

  do {
    memset(&res, 0, sizeof(res));
    if (dvbbuf_raw_read(ds->ds_raw, tspos, len, ds->ds_buf,
                        ds->ds_buf_packets, &res) != DVBBUF_OK) {
      dvbbuffer_svc_go_live(t, ds, "raw reader error");
      return 0;
    }
    if (res.packets) {
      dvbbuffer_svc_inject(t, ds->ds_buf, res.packets * 188);
      ds->ds_injected += res.packets;
    }
    /* the pace budget is per live run: count the live bytes only once */
    len = 0;
  } while (res.packets == ds->ds_buf_packets && !res.handoff && !res.lost);
  if (res.handoff || res.lost) {
    dvbbuffer_svc_go_live(t, ds, res.lost ? "backlog lost" : "handoff");
    return 0;
  }
  ds->ds_dropped += len / 188;
  return 1;
}
