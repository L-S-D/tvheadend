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

/* H7: keys per parity kept for the backlog of joining subscribers */
#define DVBBUFFER_SVC_CW_HIST    4
/* H7: subscribers waiting for their backlog at the same time */
#define DVBBUFFER_SVC_JOIN_MAX   4

typedef struct dvbbuffer_svc {
  /* global_lock */
  dvbbuffer_mux_t *ds_mux;
  mtimer_t         ds_decide_timer;
  /* s_stream_mutex */
  dvbbuf_raw      *ds_raw;
  dvbbuffer_svc_state_t ds_state;
  int              ds_decided;      /* decision timer ran */
  int              ds_allowed;      /* injection allowed for these subscribers */
  int              ds_ts_client;    /* raw TS client: start ts_start_ms back */
  int              ds_has_key;      /* H5: tvh obtained a key */
  uint8_t          ds_cw[2][8];     /* H5: DVB-CSA keys (even, odd) so far */
  uint8_t          ds_cw_valid[2];
  uint8_t          ds_cw_ecm;       /* ICAM ecm byte */
  uint8_t          ds_cw_hist[2][DVBBUFFER_SVC_CW_HIST][8]; /* H7: recent distinct keys, */
  uint8_t          ds_cw_hist_n[2];                         /* oldest first */
  uint32_t         ds_cw_gen;       /* key updates handed to the raw reader */
  uint32_t         ds_tried_gen;    /* keys of the last start attempt without result */
  int              ds_tried;
  int64_t          ds_retry_at;     /* next start attempt (mono) */
  uint32_t         ds_ecm_replayed; /* H6: ECMs from the history given to the CA client */
  uint64_t         ds_ecm_ids[8];   /* H6: ... their history ids (each once per start) */
  th_subscription_t *ds_join[DVBBUFFER_SVC_JOIN_MAX]; /* H7: joining subscribers, */
  int              ds_njoin;        /* served by the next live run (input thread) */
  uint32_t         ds_joins;        /* H7: subscribers started from the backlog */
  int64_t          ds_start;        /* mclk() of the service start */
  int64_t          ds_key_wait;     /* key wait limit (mono) */
  uint8_t         *ds_buf;
  uint32_t         ds_buf_packets;
  uint64_t         ds_injected;
  uint64_t         ds_dropped;
} dvbbuffer_svc_t;

/* waiting for subscribers / PMT longer than this -> give up */
#define DVBBUFFER_SVC_LINK_WAIT  sec2mono(2)
/* no start point yet: next search after this */
#define DVBBUFFER_SVC_RETRY      ms2mono(40)
/* TS client: wait at most this long (from the service start) for the key of
 * the older crypto period - VideoGuard gives one key per ECM */
#define DVBBUFFER_SVC_TS_KEY_WAIT ms2mono(1000)

/* remember a key (s_stream_mutex); a known one moves to the end */
static void
dvbbuffer_svc_cw_hist(dvbbuffer_svc_t *ds, int parity, const uint8_t *cw)
{
  uint8_t (*h)[8] = ds->ds_cw_hist[parity];
  int i, n = ds->ds_cw_hist_n[parity];

  for (i = 0; i < n; i++)
    if (memcmp(h[i], cw, 8) == 0)
      break;
  if (i == n && n == DVBBUFFER_SVC_CW_HIST)
    i = 0;                       /* full: drop the oldest */
  else if (i == n)
    n++;
  memmove(h[i], h[i + 1], (size_t)(n - 1 - i) * 8);
  memcpy(h[n - 1], cw, 8);
  ds->ds_cw_hist_n[parity] = n;
}

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
 * A client that gets the raw transport stream over HTTP (pass profile, e.g.
 * a TV): it buffers some seconds before it plays, so it starts further back.
 * HTSP (Kodi) gets parsed packets and starts at once.
 */
static int
dvbbuffer_ts_client(th_subscription_t *s)
{
  return dvbbuffer_conf.ts_start_ms &&
         (s->ths_flags & SUBSCRIPTION_STREAMING) &&
         (s->ths_flags & SUBSCRIPTION_TYPE_MASK) == SUBSCRIPTION_MPEGTS;
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
  int allowed = 1, ts_client = 0;

  if (ds == NULL)
    return;
  LIST_FOREACH(s, &t->s_subscriptions, ths_service_link) {
    if (!dvbbuffer_conf.inject_dvr && s->ths_title && strncmp(s->ths_title, "DVR: ", 5) == 0)
      allowed = 0;
    if (dvbbuffer_ts_client(s))
      ts_client = 1;
  }
  tvh_mutex_lock(&t->s_stream_mutex);
  ds->ds_ts_client = ts_client;
  if (ts_client && ds->ds_state == DVBBUFFER_SVC_WAIT) {
    /* the oldest keyframe within ts_start_ms (keyframe_back large) */
    dvbbuf_raw_set_start(ds->ds_raw, dvbbuffer_conf.ts_start_ms, 1000);
    tvhdebug(LS_DVBBUFFER, "%s: TS client, start up to %u ms back",
             service_nicename((service_t *)t), dvbbuffer_conf.ts_start_ms);
  }
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

  /* a viewer (or recording) uses this mux: LRU */
  if (mm && t->s_type == STYPE_STD && !t->s_dvbbuffer_used) {
    t->s_dvbbuffer_used = 1;
    dvbbuffer_warm_used(mm, 1);
  }

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

  if (t->s_dvbbuffer_used) {
    t->s_dvbbuffer_used = 0;
    if (t->s_dvb_mux)
      dvbbuffer_warm_used(t->s_dvb_mux, -1);
  }
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
 *
 * The keys are also handed to the raw reader: with them the start keyframe
 * is chosen by trial decryption - only where tvh can descramble and a
 * decoder can start (SPS/PPS...). OSCam often answers with one parity only,
 * the keys are collected per parity.
 */
void
dvbbuffer_service_key(service_t *t, int type, uint16_t pid,
                      const uint8_t *even, const uint8_t *odd,
                      int keylen, uint8_t ecm)
{
  static const uint8_t empty[8];
  dvbbuffer_svc_t *ds;
  elementary_stream_t *st;
  const uint8_t *cw[2] = { even, odd };
  int i, r;

  if (t->s_source_type != S_MPEG_TS)
    return;
  ds = ((mpegts_service_t *)t)->s_dvbbuffer;
  if (ds == NULL)
    return;
  if (!ds->ds_has_key) {
    ds->ds_has_key = 1;
    tvhdebug(LS_DVBBUFFER, "%s: tvh has the key after %"PRId64" ms",
             service_nicename(t), mono2ms(mclk() - ds->ds_start));
  }
  /* DVB-CSA only; per-PID keys: only those of the video stream */
  if (type != DESCRAMBLER_CSA_CBC || keylen != 8)
    return;
  if (pid) {
    st = elementary_stream_find(&t->s_components, pid);
    if (st == NULL || !SCT_ISVIDEO(st->es_type))
      return;
  }
  /* collected all the time: joining subscribers get a decrypted backlog (H7) */
  for (i = 0; i < 2; i++)
    if (cw[i] && memcmp(cw[i], empty, 8)) {
      memcpy(ds->ds_cw[i], cw[i], 8);
      ds->ds_cw_valid[i] = 1;
      dvbbuffer_svc_cw_hist(ds, i, cw[i]);
    }
  ds->ds_cw_ecm = ecm;
  if (ds->ds_state != DVBBUFFER_SVC_WAIT)
    return;
  r = dvbbuf_raw_set_cw(ds->ds_raw, ds->ds_cw_valid[0] ? ds->ds_cw[0] : NULL,
                        ds->ds_cw_valid[1] ? ds->ds_cw[1] : NULL, ecm);
  if (r != DVBBUF_OK) {
    tvhwarn(LS_DVBBUFFER, "%s: keys not usable for the keyframe search: %s",
            service_nicename(t), dvbbuf_last_error());
    return;
  }
  ds->ds_cw_gen++;
  tvhdebug(LS_DVBBUFFER, "%s: keys for the keyframe search: even %s, odd %s, ecm %d",
           service_nicename(t), ds->ds_cw_valid[0] ? "yes" : "no",
           ds->ds_cw_valid[1] ? "yes" : "no", ecm);
}

/*
 * H6 - capmt_set_filter() (capmt_mutex held)
 *
 * While the service waits for its start, a new ECM filter of the CA client
 * gets the newest matching ECM of the history at once: the one on air (no
 * wait for the next repetition) and - when OSCam then filters for the other
 * table id - the ECM of the previous crypto period, whose key opens the
 * keyframes in the ring buffer (OSCam answers each ECM with the key of one
 * period only, e.g. VideoGuard).
 */
int
dvbbuffer_service_ecm(mpegts_service_t *t, uint16_t pid,
                      int (*match)(void *opaque, const uint8_t *sec, int len),
                      void *opaque, uint8_t *out, int max)
{
  dvbbuffer_svc_t *ds;
  dvbbuf_ecm_entry *e = NULL;
  uint32_t n = 0, cnt, len, i, j;
  int64_t now = getfastmonoclock(), oldest;
  int r = 0;

  tvh_mutex_lock(&t->s_stream_mutex);
  ds = t->s_dvbbuffer;
  if (ds == NULL || ds->ds_state != DVBBUFFER_SVC_WAIT)
    goto end;
  /* history is oldest first: get the count, then all entries */
  if (dvbbuf_mux_ecm_history(ds->ds_mux->dm_lib, pid, NULL, 0, &cnt) != DVBBUF_OK || cnt == 0)
    goto end;
  cnt += 8;  /* the input thread may add some meanwhile */
  e = malloc(cnt * sizeof(*e));
  if (dvbbuf_mux_ecm_history(ds->ds_mux->dm_lib, pid, e, cnt, &n) != DVBBUF_OK)
    goto end;
  if (n > cnt)
    goto end;  /* grew too fast, the next filter will try again */
  /* only ECMs of the buffered period are of use */
  oldest = now - (int64_t)dvbbuffer_conf.buffer_sec * 1000000;
  for (i = n; i-- > 0; ) {
    if (e[i].last_mono_us < oldest)
      break;
    if (dvbbuf_mux_ecm_section(ds->ds_mux->dm_lib, pid, e[i].id, out, max, &len) != DVBBUF_OK)
      continue;
    if (!match(opaque, out, len))
      continue;
    /* an older ECM than the one on air only while a key parity is missing
     * (one key per answer, e.g. VideoGuard): its answer would otherwise
     * overwrite tvh's key of the next period with an old one */
    if (i != n - 1 && ds->ds_cw_valid[0] && ds->ds_cw_valid[1])
      goto end;
    /* the newest match only, and each ECM once: OSCam re-sets its filter
     * after every section, a second delivery would loop */
    for (j = 0; j < ARRAY_SIZE(ds->ds_ecm_ids); j++)
      if (ds->ds_ecm_ids[j] == e[i].id)
        goto end;
    if (ds->ds_ecm_replayed >= ARRAY_SIZE(ds->ds_ecm_ids))
      goto end;
    ds->ds_ecm_ids[ds->ds_ecm_replayed++] = e[i].id;
    /* tvh's descrambler: ECM start of the parity, ICAM ecm mode of the keys */
    descrambler_ecm_from_buffer((service_t *)t, pid, out, len);
    tvhdebug(LS_DVBBUFFER, "%s: ECM table %02X on pid %d from the buffer (%"PRId64" ms old) "
             "to the CA client", service_nicename((service_t *)t), out[0], pid,
             (now - e[i].last_mono_us) / 1000);
    r = len;
    break;
  }
end:
  tvh_mutex_unlock(&t->s_stream_mutex);
  free(e);
  return r;
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
static int
dvbbuffer_svc_packet(mpegts_service_t *t, uint64_t tspos,
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
    /* no (good) start point so far: try again with new keys at once,
     * otherwise every DVBBUFFER_SVC_RETRY */
    if (ds->ds_tried && ds->ds_tried_gen == ds->ds_cw_gen && mclk() < ds->ds_retry_at) {
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
      /* no complete keyframe (with a known key) yet - e.g. the crypto period
       * just changed or the mux was just tuned: wait for one (at most a GOP
       * normally) instead of going live in the middle of a GOP */
      if (mclk() < ds->ds_key_wait) {
        if (!ds->ds_tried)
          tvhdebug(LS_DVBBUFFER, "%s: no start point yet (keys: even %s, odd %s), waiting",
                   service_nicename((service_t *)t), ds->ds_cw_valid[0] ? "yes" : "no",
                   ds->ds_cw_valid[1] ? "yes" : "no");
        ds->ds_tried = 1;
        ds->ds_tried_gen = ds->ds_cw_gen;
        ds->ds_retry_at = mclk() + DVBBUFFER_SVC_RETRY;
        ds->ds_dropped += len / 188;
        return 1;
      }
      dvbbuffer_svc_go_live(t, ds, "no keyframe in the ring buffer");
      return 0;
    }
    /* a start in an older crypto period only, the key of the newer one is
     * probably on its way (H6): wait for it rather than start far back */
    if (rap.newer_without_key && !(ds->ds_cw_valid[0] && ds->ds_cw_valid[1]) &&
        mclk() < ds->ds_key_wait) {
      if (!ds->ds_tried)
        tvhdebug(LS_DVBBUFFER, "%s: start %"PRId64" ms back in the older crypto period only, "
                 "waiting for the %s key", service_nicename((service_t *)t), rap.age_us / 1000,
                 ds->ds_cw_valid[0] ? "odd" : "even");
      ds->ds_tried = 1;
      ds->ds_tried_gen = ds->ds_cw_gen;
      ds->ds_retry_at = mclk() + DVBBUFFER_SVC_RETRY;
      ds->ds_dropped += len / 188;
      return 1;
    }
    /* a TS client wants seconds of backlog (its pre-buffer): with only one
     * key the older crypto period cannot be descrambled and the start is
     * close to live - wait a little for the other key */
    if (ds->ds_ts_client && rap.age_us < (int64_t)dvbbuffer_conf.ts_start_ms * 500 &&
        !(ds->ds_cw_valid[0] && ds->ds_cw_valid[1]) &&
        mclk() < ds->ds_start + DVBBUFFER_SVC_TS_KEY_WAIT) {
      if (!ds->ds_tried)
        tvhdebug(LS_DVBBUFFER, "%s: TS client start only %"PRId64" ms back, waiting for the %s key",
                 service_nicename((service_t *)t), rap.age_us / 1000,
                 ds->ds_cw_valid[0] ? "odd" : "even");
      ds->ds_tried = 1;
      ds->ds_tried_gen = ds->ds_cw_gen;
      ds->ds_retry_at = mclk() + DVBBUFFER_SVC_RETRY;
      ds->ds_dropped += len / 188;
      return 1;
    }
    tvhdebug(LS_DVBBUFFER, "%s: injecting from keyframe %"PRId64" ms back "
             "(pid %d, %s), %"PRId64" ms after start",
             service_nicename((service_t *)t), rap.age_us / 1000, rap.pid,
             rap.method == DVBBUF_RAP_ES ? "ES" :
             rap.method == DVBBUF_RAP_CW ? "CW" : "RAI",
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

/*
 * H7 - the backlog of a joining subscriber, up to (excluding) the live run at
 * tspos which tvh is about to process (input thread, s_stream_mutex held):
 * the raw reader is used in feed context, as the library requires
 */
static void
dvbbuffer_svc_join(mpegts_service_t *ms, dvbbuffer_svc_t *ds,
                   th_subscription_t *s, uint64_t tspos)
{
  service_t *t = (service_t *)ms;
  th_descrambler_runtime_t *dr;
  dvbbuf_raw_config cfg;
  dvbbuf_raw_result res;
  dvbbuf_rap rap;
  dvbbuf_raw *raw = NULL;
  streaming_message_t sm;
  pktbuf_t *pb;
  uint8_t *buf = NULL;
  size_t len = 0, cap = 0;
  int encrypted, r, i, j;
  const char *why = NULL;

  if (ms->s_dvb_mux->mm_dvbbuffer != ds->ds_mux) {
    why = "ring buffer stopped";
    goto skip;
  }
  dr = t->s_descramble;
  if (dr && dr->dr_descramble) {
    why = "hardware/pass-through descrambling";
    goto skip;
  }
  if (dr && dr->dr_queue_total) {
    why = "descrambler queue not empty";
    goto skip;
  }
  encrypted = service_is_encrypted(t);
  if (encrypted && !(ds->ds_cw_valid[0] || ds->ds_cw_valid[1])) {
    why = "no key yet";
    goto skip;
  }

  memset(&cfg, 0, sizeof(cfg));
  cfg.struct_size   = sizeof(cfg);
  cfg.service_id    = service_id16(t);
  cfg.max_age_ms    = dvbbuffer_conf.max_age_ms;
  cfg.keyframe_back = dvbbuffer_conf.keyframe_back;
  cfg.decrypt       = encrypted ? 1 : 0;
  if (dvbbuf_raw_open(ds->ds_mux->dm_lib, &cfg, &raw) != DVBBUF_OK) {
    why = "raw reader";
    goto skip;
  }
  /* a TS client starts further back (its player time 0 is noted for the
   * subtitle timing, /ttx/<channel>/start) */
  if (dvbbuffer_ts_client(s))
    dvbbuf_raw_set_start(raw, dvbbuffer_conf.ts_start_ms, 1000);
  if (encrypted) {
    /* all recent keys for the parity runs of the backlog (an answer to a
     * replayed older ECM may have replaced the current one), tvh's own last */
    for (i = 0; i < 2; i++)
      for (j = 0; j < ds->ds_cw_hist_n[i]; j++)
        dvbbuf_raw_set_cw(raw, i == 0 ? ds->ds_cw_hist[0][j] : NULL,
                          i == 1 ? ds->ds_cw_hist[1][j] : NULL, ds->ds_cw_ecm);
    dvbbuf_raw_set_cw(raw, ds->ds_cw_valid[0] ? ds->ds_cw[0] : NULL,
                      ds->ds_cw_valid[1] ? ds->ds_cw[1] : NULL, ds->ds_cw_ecm);
  }
  memset(&rap, 0, sizeof(rap));
  rap.struct_size = sizeof(rap);
  if ((r = dvbbuf_raw_start(raw, &rap)) != DVBBUF_OK) {
    why = r == DVBBUF_EAGAIN ? "ring buffer busy" : "no keyframe";
    goto skip;
  }
  /* the backlog up to this live run: everything before it went to the
   * others (flushed when the subscriber was linked) */
  for (;;) {
    if (cap - len < 4096 * 188) {
      cap = cap ? cap * 2 : 8192 * 188;
      buf = realloc(buf, cap);
    }
    memset(&res, 0, sizeof(res));
    if (dvbbuf_raw_read_until(raw, tspos, buf + len,
                              (uint32_t)((cap - len) / 188), &res) != DVBBUF_OK || res.lost) {
      why = encrypted ? "backlog lost or key missing" : "backlog lost";
      goto skip;
    }
    len += (size_t)res.packets * 188;
    if (res.handoff)
      break;
  }
  if (len) {
    pb = pktbuf_alloc(buf, len);
    memset(&sm, 0, sizeof(sm));
    sm.sm_type = SMT_MPEGTS;
    sm.sm_data = pb;
    streaming_target_deliver(&s->ths_input, streaming_msg_clone(&sm));
    pktbuf_ref_dec(pb);
  }
  ds->ds_joins++;
  tvhdebug(LS_DVBBUFFER, "%s: subscriber %s starts from the keyframe %"PRId64" ms back "
           "(%zu packets%s)", service_nicename(t), s->ths_title ?: "?", rap.age_us / 1000,
           len / 188, encrypted ? ", decrypted" : "");
  goto done;

skip:
  tvhdebug(LS_DVBBUFFER, "%s: subscriber %s joins live (%s)", service_nicename(t),
           s->ths_title ?: "?", why);
done:
  if (raw)
    dvbbuf_raw_close(raw);
  free(buf);
}

int
dvbbuffer_service_packet0(mpegts_service_t *t, uint64_t tspos,
                          uint16_t pid, const uint8_t *tsb, int len)
{
  dvbbuffer_svc_t *ds = t->s_dvbbuffer;
  int i;

  /* joining subscribers first: their backlog ends right before this run */
  if (ds->ds_njoin) {
    for (i = 0; i < ds->ds_njoin; i++)
      dvbbuffer_svc_join(t, ds, ds->ds_join[i], tspos);
    ds->ds_njoin = 0;
  }
  return dvbbuffer_svc_packet(t, tspos, pid, tsb, len);
}

/*
 * H7 - a subscriber joins a running service (subscription_link_service(),
 * global_lock + s_stream_mutex): what tvh processed so far goes to the others
 * now, the new one waits for its backlog in the input thread
 */
void
dvbbuffer_service_link_pre(service_t *t)
{
  dvbbuffer_svc_t *ds;

  if (t->s_source_type != S_MPEG_TS)
    return;
  ds = ((mpegts_service_t *)t)->s_dvbbuffer;
  if (ds && ds->ds_state == DVBBUFFER_SVC_LIVE) {
    /* the CSA batches first: their output goes through ts_remux() */
    descrambler_flush_csa(t);
    ts_remux_flush((mpegts_service_t *)t);
  }
}

void
dvbbuffer_service_link(service_t *t, th_subscription_t *s)
{
  mpegts_service_t *ms = (mpegts_service_t *)t;
  dvbbuffer_svc_t *ds;

  if (t->s_source_type != S_MPEG_TS)
    return;
  ds = ms->s_dvbbuffer;
  if (ds == NULL || ds->ds_state != DVBBUFFER_SVC_LIVE)
    return;                      /* not buffered, or the first subscribers (injection) */
  if (ms->s_dvb_mux->mm_dvbbuffer != ds->ds_mux)
    return;
  if (!dvbbuffer_conf.inject_dvr && s->ths_title && strncmp(s->ths_title, "DVR: ", 5) == 0)
    return;
  if (ds->ds_njoin >= DVBBUFFER_SVC_JOIN_MAX) {
    tvhdebug(LS_DVBBUFFER, "%s: subscriber %s joins live (too many joining at once)",
             service_nicename(t), s->ths_title ?: "?");
    return;
  }
  ds->ds_join[ds->ds_njoin++] = s;
}

/* H7 - the subscriber is unlinked (s_stream_mutex held) */
void
dvbbuffer_service_unlink(service_t *t, th_subscription_t *s)
{
  dvbbuffer_svc_t *ds;
  int i;

  if (t->s_source_type != S_MPEG_TS)
    return;
  ds = ((mpegts_service_t *)t)->s_dvbbuffer;
  if (ds == NULL)
    return;
  for (i = 0; i < ds->ds_njoin; i++)
    if (ds->ds_join[i] == s) {
      memmove(&ds->ds_join[i], &ds->ds_join[i + 1],
              (size_t)(ds->ds_njoin - i - 1) * sizeof(ds->ds_join[0]));
      ds->ds_njoin--;
      break;
    }
}
