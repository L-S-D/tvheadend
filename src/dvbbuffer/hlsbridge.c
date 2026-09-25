/*
 *  Tvheadend - instant channel switching, HLS server bridge (webOS path)
 *
 *  libdvbbuffer runs its own HTTP server (CivetWeb, own port) with the HLS
 *  pipeline; it asks tvh through these callbacks, on its server threads and
 *  without holding any library lock:
 *    authorize - tvh access entries (streaming right + channel rights)
 *    resolve   - channel UUID -> service (a buffered mux is preferred)
 *    acquire   - hold the mux with a mux subscription (viewer weight) and a
 *                ring buffer, tune it if needed; not a service subscription,
 *                so tvh's own descrambling is not started for it
 *    release   - give it back
 *
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "dvbbuffer_private.h"
#include "access.h"
#include "channels.h"
#include "subscriptions.h"
#include "streaming.h"
#include "profile.h"
#include "tcp.h"

typedef struct dvbbuffer_hls_hold {
  char                mm_uuid[UUID_HEX_SIZE];
  dvbbuffer_mux_t    *dm;       /* referenced once the ring buffer exists */
  profile_chain_t     prch;
  streaming_target_t  input;
  th_subscription_t  *sub;
} dvbbuffer_hls_hold_t;

static dvbbuf_http *dvbbuffer_http;

/*
 * Subscription output: the data is taken in mpegts_input_process()
 */
static void
dvbbuffer_hls_input(void *opaque, streaming_message_t *sm)
{
  if (sm->sm_type == SMT_STOP || sm->sm_type == SMT_NOSTART)
    tvhdebug(LS_DVBBUFFER, "HLS mux subscription %s (%s)",
             sm->sm_type == SMT_STOP ? "stopped" : "not started",
             streaming_code2txt(sm->sm_code));
  streaming_msg_free(sm);
}

static htsmsg_t *
dvbbuffer_hls_input_info(void *opaque, htsmsg_t *list)
{
  htsmsg_add_str(list, NULL, "dvbbuffer HLS input");
  return list;
}

static streaming_ops_t dvbbuffer_hls_input_ops = {
  .st_cb   = dvbbuffer_hls_input,
  .st_info = dvbbuffer_hls_input_info
};

/*
 * The service of a channel: a mux with a ring buffer first (global_lock)
 */
static mpegts_service_t *
dvbbuffer_hls_service(const char *channel)
{
  channel_t *ch = channel_find_by_uuid(channel);
  idnode_list_mapping_t *ilm;
  mpegts_service_t *ms, *best = NULL;
  service_t *s;

  if (ch == NULL || !ch->ch_enabled)
    return NULL;
  LIST_FOREACH(ilm, &ch->ch_services, ilm_in2_link) {
    s = (service_t *)ilm->ilm_in1;
    if (s->s_source_type != S_MPEG_TS || !s->s_is_enabled(s, 0))
      continue;
    ms = (mpegts_service_t *)s;
    if (ms->s_dvb_mux == NULL)
      continue;
    if (ms->s_dvb_mux->mm_dvbbuffer)
      return ms;
    if (best == NULL)
      best = ms;
  }
  return best;
}

static int
dvbbuffer_hls_verify(void *aux, const char *passwd)
{
  const char *given = aux;
  return given && passwd && strcmp(given, passwd) == 0;
}

static int
dvbbuffer_hls_authorize(void *user, const char *channel, const char *username,
                        const char *password, const char *peer)
{
  struct sockaddr_storage ss;
  access_t *a;
  channel_t *ch;
  int ok = 0;

  memset(&ss, 0, sizeof(ss));
  if (tcp_get_ip_from_str(peer, &ss) == NULL)
    return DVBBUF_EACCES;
  tvh_mutex_lock(&global_lock);
  a = access_get(&ss, username && *username ? username : NULL,
                 dvbbuffer_hls_verify, (void *)password);
  ch = channel_find_by_uuid(channel);
  if (a && !access_verify2(a, ACCESS_STREAMING) && ch && channel_access(ch, a, 0))
    ok = 1;
  access_destroy(a);
  tvh_mutex_unlock(&global_lock);
  if (!ok)
    tvhinfo(LS_DVBBUFFER, "HLS: access denied for %s@%s to channel %s",
            username && *username ? username : "(anonymous)", peer, channel);
  return ok ? DVBBUF_OK : DVBBUF_EACCES;
}

static int
dvbbuffer_hls_resolve(void *user, const char *channel, dvbbuf_http_source *src)
{
  mpegts_service_t *ms;
  channel_t *ch;
  int r = DVBBUF_ENOENT;

  tvh_mutex_lock(&global_lock);
  ms = dvbbuffer_hls_service(channel);
  if (ms) {
    ch = channel_find_by_uuid(channel);
    src->service_id = service_id16(ms);
    src->scrambled = service_is_encrypted((service_t *)ms) ? 1 : 0;
    strlcpy(src->name, channel_get_name(ch, ""), sizeof(src->name));
    r = DVBBUF_OK;
  }
  tvh_mutex_unlock(&global_lock);
  return r;
}

static void
dvbbuffer_hls_hold_free(dvbbuffer_hls_hold_t *h)
{
  mpegts_mux_t *mm = mpegts_mux_find(h->mm_uuid);

  lock_assert(&global_lock);
  if (h->sub)
    subscription_unsubscribe(h->sub, UNSUBSCRIBE_FINAL);
  if (h->dm)
    dvbbuffer_mux_unref(h->dm);
  if (mm) {
    if (mm->mm_dvbbuffer_hold > 0) {
      mm->mm_dvbbuffer_hold--;
      dvbbuffer_warm_used(mm, -1);
    }
    if (!dvbbuffer_mux_wanted(mm))
      dvbbuffer_mux_detach(mm);
  }
  free(h);
}

static int
dvbbuffer_hls_acquire(void *user, const char *channel, const char *peer,
                      dvbbuf_mux **mux, void **handle)
{
  dvbbuffer_hls_hold_t *h = *handle;
  mpegts_service_t *ms;
  mpegts_service_t *raw;
  mpegts_apids_t pids;
  mpegts_mux_t *mm;
  int r = DVBBUF_EAGAIN;

  tvh_mutex_lock(&global_lock);
  if (h == NULL) {
    ms = dvbbuffer_hls_service(channel);
    if (ms == NULL) {
      r = DVBBUF_ENOENT;
      goto end;
    }
    mm = ms->s_dvb_mux;
    h = calloc(1, sizeof(*h));
    idnode_uuid_as_str(&mm->mm_id, h->mm_uuid);
    mm->mm_dvbbuffer_hold++;
    dvbbuffer_warm_used(mm, 1);
    streaming_target_init(&h->input, &dvbbuffer_hls_input_ops, h, 0);
    h->prch.prch_id = mm;
    h->prch.prch_st = &h->input;
    h->sub = subscription_create_from_mux(&h->prch, NULL, dvbbuffer_conf.hls_weight,
                                          "dvbbuffer HLS", SUBSCRIPTION_MINIMAL,
                                          peer, NULL, "dvbbuffer HLS", NULL);
    if (h->sub == NULL) {
      dvbbuffer_hls_hold_free(h);
      r = DVBBUF_EBUSY;
      goto end;
    }
    raw = (mpegts_service_t *)h->sub->ths_service;
    mpegts_pid_init(&pids);
    pids.all = 1;
    raw->s_update_pids(raw, &pids);
    mpegts_pid_done(&pids);
    /* a mux which runs already gets its ring buffer now, a tuned one on start */
    dvbbuffer_mux_attach(mm);
    *handle = h;
    tvhinfo(LS_DVBBUFFER, "HLS: %s holds %s for channel %s", peer, mm->mm_nicename, channel);
  }
  mm = mpegts_mux_find(h->mm_uuid);
  if (mm == NULL) {
    r = DVBBUF_ENOENT;
    goto end;
  }
  if (mm->mm_dvbbuffer) {
    if (h->dm == NULL) {
      h->dm = mm->mm_dvbbuffer;
      dvbbuffer_mux_ref(h->dm);
    }
    *mux = h->dm->dm_lib;
    r = DVBBUF_OK;
  }
end:
  tvh_mutex_unlock(&global_lock);
  return r;
}

static void
dvbbuffer_hls_release(void *user, void *handle)
{
  dvbbuffer_hls_hold_t *h = handle;

  if (h == NULL)
    return;
  tvh_mutex_lock(&global_lock);
  tvhinfo(LS_DVBBUFFER, "HLS: mux %s released", h->mm_uuid);
  dvbbuffer_hls_hold_free(h);
  tvh_mutex_unlock(&global_lock);
}

/*
 * Start the server (global_lock held, the callbacks run later on server threads)
 */
void
dvbbuffer_hls_init(void)
{
  dvbbuf_http_config cfg;
  char port[16];

  if (dvbbuffer_ctx == NULL || !dvbbuffer_conf.hls_enabled)
    return;
  memset(&cfg, 0, sizeof(cfg));
  cfg.struct_size = sizeof(cfg);
  snprintf(port, sizeof(port), "%u", dvbbuffer_conf.hls_port);
  cfg.listen = port;
  cfg.hls.struct_size             = sizeof(cfg.hls);
  cfg.hls.start_back_ms           = dvbbuffer_conf.hls_start_back_ms;
  cfg.hls.initial_target_ms       = dvbbuffer_conf.hls_initial_target_ms;
  cfg.hls.target_ms               = dvbbuffer_conf.hls_target_ms;
  cfg.hls.target_duration_s       = dvbbuffer_conf.hls_target_duration;
  cfg.hls.initial_segments_min    = dvbbuffer_conf.hls_initial_segments;
  cfg.hls.initial_duration_min_ms = dvbbuffer_conf.hls_initial_duration_ms;
  cfg.hls.initial_timeout_ms      = dvbbuffer_conf.hls_initial_timeout_ms;
  cfg.hls.window_ms               = dvbbuffer_conf.hls_window_ms;
  cfg.hls.keep_ms                 = dvbbuffer_conf.hls_keep_ms;
  cfg.hls.idle_ms                 = dvbbuffer_conf.hls_idle_ms;
  cfg.hls.start_offset_ms         = dvbbuffer_conf.hls_start_offset_ms;
  cfg.hls.no_blocking_reload      = !dvbbuffer_conf.hls_blocking_reload;
  cfg.hls.audio_langs             = dvbbuffer_conf.hls_audio_langs;
  cfg.cb.authorize = dvbbuffer_hls_authorize;
  cfg.cb.resolve   = dvbbuffer_hls_resolve;
  cfg.cb.acquire   = dvbbuffer_hls_acquire;
  cfg.cb.release   = dvbbuffer_hls_release;
  if (dvbbuffer_conf.hls_oscam_host && *dvbbuffer_conf.hls_oscam_host &&
      dvbbuffer_conf.hls_oscam_port > 0 && dvbbuffer_conf.hls_oscam_port < 65536) {
    cfg.oscam_host = dvbbuffer_conf.hls_oscam_host;
    cfg.oscam_port = (uint16_t)dvbbuffer_conf.hls_oscam_port;
  }
  if (dvbbuf_http_start(dvbbuffer_ctx, &cfg, &dvbbuffer_http) != DVBBUF_OK) {
    tvherror(LS_DVBBUFFER, "HLS server on port %s: %s", port, dvbbuf_last_error());
    dvbbuffer_http = NULL;
    return;
  }
  tvhinfo(LS_DVBBUFFER, "HLS server on port %s: /hls/<channel uuid>/index.m3u8, scrambled channels %s%s%s",
          port, cfg.oscam_host ? "via OSCam " : "refused", cfg.oscam_host ? cfg.oscam_host : "",
          cfg.oscam_host ? "" : "");
}

/*
 * Stop the server - WITHOUT global_lock: request threads may wait for it
 */
void
dvbbuffer_hls_done(void)
{
  if (dvbbuffer_http) {
    dvbbuf_http_stop(dvbbuffer_http);
    dvbbuffer_http = NULL;
  }
}
