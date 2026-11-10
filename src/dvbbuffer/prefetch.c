/*
 *  Tvheadend - instant channel switching, ECM prefetch
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  For the ECM PIDs listed at a warm mux every new ECM goes to OSCam over
 *  camd35 (UDP, OSCam "cs357x") and the answer is thrown away: the control
 *  words of these services stay in OSCam's cache, a start of the service
 *  (capmt, the same ECM) is answered from there at once.
 *
 *  camd35: 4 bytes CRC32(MD5(user)) in clear, then AES-128-ECB with the key
 *  MD5(password) over a 20 byte header and the ECM, padded with 0xff to 16
 *  bytes; header: [0] command (0 = ECM), [1] ECM length, [4..7] CRC32 of the
 *  ECM, [8..9] SID, [10..11] CAID, [12..15] provider, [16..17] index,
 *  [18..19] 0xff (OSCam module-camd35.c)
 */

#include <netdb.h>
#include <sys/socket.h>
#include <openssl/evp.h>

#include "dvbbuffer_private.h"

#define PREFETCH_MAX      16      /* ECM PIDs per mux */
#define PREFETCH_SEEN     4       /* recent ECMs per PID, not sent again */

/* owned by its table: freed with the table's last reference (mt_destroy), the
 * input thread may still be in the callback when the table is destroyed. We
 * hold a reference of our own (start/stop): mpegts_mux_stop() destroys all
 * tables of the mux before ml_mux_stop reaches dvbbuffer_prefetch_stop() */
struct dvbbuffer_prefetch {
  mpegts_table_t   *dp_table;
  uint16_t          dp_pid;
  /* SID/CAID/provider of the service of this ECM PID (resolved under
   * global_lock, read in the input thread once dp_resolved is set) */
  uint16_t          dp_sid;
  uint16_t          dp_caid;
  uint32_t          dp_prid;
  int               dp_resolved;
  uint32_t          dp_seen[PREFETCH_SEEN];
  int               dp_seen_n;
  uint32_t          dp_sent;
  int64_t           dp_last;      /* last ECM section (mclk, input thread) */
  int64_t           dp_sent_at;   /* last ECM sent (mclk, input thread) */
  int               dp_quiet;     /* warned: nothing sent for PREFETCH_QUIET */
};

#define PREFETCH_QUIET    sec2mono(30)

/* ---------------------------------------------------------------------------
 * camd35 over UDP
 * ------------------------------------------------------------------------- */

static tvh_mutex_t camd35_lock = TVH_THREAD_MUTEX_INITIALIZER;
static int         camd35_fd = -1;
static struct sockaddr_storage camd35_sa;
static socklen_t   camd35_salen;
static uint8_t     camd35_key[16];
static uint8_t     camd35_ucrc[4];
static uint16_t    camd35_idx;
static tvhlog_limit_t camd35_log;
static tvhlog_limit_t camd35_resolve_log;
static int64_t     camd35_retry;    /* mclk() of the next try to resolve */

/* CRC-32 as zlib's crc32() (reflected, 0xEDB88320) */
static uint32_t
camd35_crc32(uint32_t crc, const uint8_t *p, size_t len)
{
  int k;

  crc = ~crc;
  while (len--) {
    crc ^= *p++;
    for (k = 0; k < 8; k++)
      crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
  }
  return ~crc;
}

static int
camd35_md5(const char *s, uint8_t out[16])
{
  unsigned int len = 16;

  return EVP_Digest(s, strlen(s), out, &len, EVP_md5(), NULL) == 1 ? 0 : -1;
}

static void
camd35_put(uint8_t *p, uint32_t v, int n)
{
  while (n--) {
    p[n] = v & 0xff;
    v >>= 8;
  }
}

/* the settings the socket was opened for (host, port, user, password) */
static char *camd35_cfg;

/* (re)open the socket for the configured server (global_lock). Only when a
 * camd35 setting changed - this runs on every save of the settings page -
 * and resolving and setting up run without camd35_lock: the input thread
 * takes it to send, a slow DNS lookup must not hold it up */
void
dvbbuffer_camd35_reconf(void)
{
  dvbbuffer_conf_t *c = &dvbbuffer_conf;
  struct addrinfo hints, *ai = NULL;
  struct sockaddr_storage sa;
  socklen_t salen = 0;
  uint8_t md5[16], key[16] = { 0 }, ucrc[4] = { 0 };
  char port[16], cfg[1024];
  int fd = -1, old;

  lock_assert(&global_lock);
  memset(&sa, 0, sizeof(sa));
  cfg[0] = '\0';
  if (dvbbuffer_running && c->camd35_enabled && c->camd35_host && *c->camd35_host &&
      c->camd35_port && c->camd35_user && c->camd35_pass)
    snprintf(cfg, sizeof(cfg), "%s\n%u\n%s\n%s", c->camd35_host, c->camd35_port,
             c->camd35_user, c->camd35_pass);
  if (camd35_cfg && !strcmp(camd35_cfg, cfg))
    return;
  free(camd35_cfg);
  camd35_cfg = strdup(cfg);
  if (!cfg[0])
    goto swap;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  snprintf(port, sizeof(port), "%u", c->camd35_port);
  if (getaddrinfo(c->camd35_host, port, &hints, &ai) || ai == NULL) {
    if (tvhlog_limit(&camd35_resolve_log, 600))
      tvherror(LS_DVBBUFFER, "ECM prefetch: unknown host %s", c->camd35_host);
    /* not resolved (network not up yet): tried again from the timer */
    free(camd35_cfg);
    camd35_cfg = NULL;
    camd35_retry = mclk() + sec2mono(60);
    goto swap;
  }
  fd = socket(ai->ai_family, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0 || ai->ai_addrlen > sizeof(sa) ||
      camd35_md5(c->camd35_pass, key) || camd35_md5(c->camd35_user, md5)) {
    tvherror(LS_DVBBUFFER, "ECM prefetch: no socket for %s:%u", c->camd35_host, c->camd35_port);
    if (fd >= 0)
      close(fd);
    fd = -1;
    goto swap;
  }
  memcpy(&sa, ai->ai_addr, ai->ai_addrlen);
  salen = ai->ai_addrlen;
  camd35_put(ucrc, camd35_crc32(0, md5, 16), 4);
  tvhinfo(LS_DVBBUFFER, "ECM prefetch: camd35 (UDP) to %s:%u as %s",
          c->camd35_host, c->camd35_port, c->camd35_user);
swap:
  if (ai)
    freeaddrinfo(ai);
  tvh_mutex_lock(&camd35_lock);
  old = camd35_fd;
  camd35_fd = fd;
  if (fd >= 0) {
    memcpy(&camd35_sa, &sa, salen);
    camd35_salen = salen;
    memcpy(camd35_key, key, sizeof(camd35_key));
    memcpy(camd35_ucrc, ucrc, sizeof(camd35_ucrc));
  }
  tvh_mutex_unlock(&camd35_lock);
  if (old >= 0)
    close(old);
}

static int
camd35_send(uint16_t sid, uint16_t caid, uint32_t prid, const uint8_t *ecm, int len)
{
  uint8_t pkt[20 + 255 + 16], out[4 + sizeof(pkt)], drain[512];
  EVP_CIPHER_CTX *ctx;
  int l, outl = 0, ok = 0;

  if (len < 3 || len > 255)     /* [1] is the length: one byte */
    return -1;
  l = (20 + len + 15) & ~15;
  memset(pkt, 0, 20);
  memset(pkt + 20, 0xff, l - 20);
  pkt[1] = len & 0xff;
  camd35_put(pkt + 8, sid, 2);
  camd35_put(pkt + 10, caid, 2);
  camd35_put(pkt + 12, prid, 4);
  pkt[18] = pkt[19] = 0xff;
  memcpy(pkt + 20, ecm, len);
  camd35_put(pkt + 4, camd35_crc32(0, pkt + 20, len), 4);

  tvh_mutex_lock(&camd35_lock);
  if (camd35_fd < 0)
    goto end;
  camd35_put(pkt + 16, ++camd35_idx, 2);
  memcpy(out, camd35_ucrc, 4);
  if ((ctx = EVP_CIPHER_CTX_new()) == NULL)
    goto end;
  if (EVP_EncryptInit_ex(ctx, EVP_aes_128_ecb(), NULL, camd35_key, NULL) == 1 &&
      EVP_CIPHER_CTX_set_padding(ctx, 0) == 1 &&
      EVP_EncryptUpdate(ctx, out + 4, &outl, pkt, l) == 1 && outl == l)
    ok = 1;
  EVP_CIPHER_CTX_free(ctx);
  if (!ok)
    goto end;
  if (sendto(camd35_fd, out, 4 + l, MSG_DONTWAIT,
             (struct sockaddr *)&camd35_sa, camd35_salen) != 4 + l) {
    if (tvhlog_limit(&camd35_log, 30))
      tvhwarn(LS_DVBBUFFER, "ECM prefetch: send failed: %s", strerror(errno));
    ok = 0;
  }
  /* the answers are not needed: only keep the socket buffer empty */
  while (recv(camd35_fd, drain, sizeof(drain), MSG_DONTWAIT) > 0)
    ;
end:
  tvh_mutex_unlock(&camd35_lock);
  return ok ? 0 : -1;
}

void
dvbbuffer_camd35_done(void)
{
  tvh_mutex_lock(&camd35_lock);
  if (camd35_fd >= 0)
    close(camd35_fd);
  camd35_fd = -1;
  tvh_mutex_unlock(&camd35_lock);
  free(camd35_cfg);
  camd35_cfg = NULL;
}

/* ---------------------------------------------------------------------------
 * ECM PIDs of a warm mux
 * ------------------------------------------------------------------------- */

/* SID, CAID and provider of the service whose CA stream is this PID
 * (global_lock) */
static void
prefetch_resolve(mpegts_mux_t *mm, dvbbuffer_prefetch_t *dp)
{
  mpegts_service_t *s;
  elementary_stream_t *st;
  caid_t *c;

  if (atomic_get(&dp->dp_resolved))
    return;
  LIST_FOREACH(s, &mm->mm_services, s_dvb_mux_link) {
    tvh_mutex_lock(&s->s_stream_mutex);
    TAILQ_FOREACH(st, &s->s_components.set_all, es_link) {
      if (st->es_type != SCT_CA || st->es_pid != dp->dp_pid)
        continue;
      LIST_FOREACH(c, &st->es_caids, link)
        if (c->use)
          break;
      if (c == NULL)
        c = LIST_FIRST(&st->es_caids);
      if (c) {
        dp->dp_sid = service_id16(s);
        dp->dp_caid = c->caid;
        dp->dp_prid = c->providerid;
        atomic_set(&dp->dp_resolved, 1);
      }
      break;
    }
    tvh_mutex_unlock(&s->s_stream_mutex);
    if (atomic_get(&dp->dp_resolved)) {
      tvhdebug(LS_DVBBUFFER, "%s: ECM prefetch pid %d = %s (sid %d, caid %04X, provider %06X)",
               mm->mm_nicename, dp->dp_pid, s->s_dvb_svcname ?: "?",
               dp->dp_sid, dp->dp_caid, dp->dp_prid);
      return;
    }
  }
}

/* an ECM section of a listed PID (input thread) */
static int
prefetch_ecm(mpegts_table_t *mt, const uint8_t *sec, int len, int tableid)
{
  dvbbuffer_prefetch_t *dp = mt->mt_opaque;
  uint32_t crc;
  int i;

  dp->dp_last = mclk();
  if (len < 3 || (sec[0] & 0xfe) != 0x80 || !atomic_get(&dp->dp_resolved))
    return 0;
  crc = camd35_crc32(0, sec, len);
  for (i = 0; i < dp->dp_seen_n; i++)
    if (dp->dp_seen[i] == crc)
      return 0;
  /* an ECM that could not be sent (no server, no connection) is not
   * marked as seen: its repetition is sent when the server is back */
  if (camd35_send(dp->dp_sid, dp->dp_caid, dp->dp_prid, sec, len) != 0)
    return 0;
  if (dp->dp_seen_n < PREFETCH_SEEN)
    dp->dp_seen_n++;
  memmove(dp->dp_seen + 1, dp->dp_seen, (PREFETCH_SEEN - 1) * sizeof(uint32_t));
  dp->dp_seen[0] = crc;
  dp->dp_sent++;
  dp->dp_sent_at = mclk();
  tvhtrace(LS_DVBBUFFER, "ECM prefetch pid %d table %02X (sid %d, caid %04X) sent",
           dp->dp_pid, sec[0], dp->dp_sid, dp->dp_caid);
  return 0;
}

/* the PIDs of the mux property "ECM prefetch": numbers, 0x for hex */
static int
prefetch_parse(const char *s, uint16_t *pids, int max)
{
  char *end;
  long v;
  int n = 0;

  while (s && *s && n < max) {
    while (*s && (*s == ' ' || *s == ',' || *s == ';'))
      s++;
    if (!*s)
      break;
    v = strtol(s, &end, 0);
    if (end == s)
      break;
    if (v > 0 && v < 0x1fff)
      pids[n++] = v;
    s = end;
  }
  return n;
}

static void
prefetch_table_destroy(mpegts_table_t *mt)
{
  free(mt->mt_opaque);
}

/* open the tables of the listed ECM PIDs (global_lock, ring attached) */
void
dvbbuffer_prefetch_start(mpegts_mux_t *mm)
{
  dvbbuffer_mux_t *dm = mm->mm_dvbbuffer;
  dvbbuffer_prefetch_t *dp;
  uint16_t pids[PREFETCH_MAX];
  int i, n;

  lock_assert(&global_lock);
  if (dm == NULL || dm->dm_prefetch_n)
    return;
  n = prefetch_parse(mm->mm_ecm_prefetch, pids, PREFETCH_MAX);
  if (n == 0)
    return;
  dm->dm_prefetch = calloc(n, sizeof(*dm->dm_prefetch));
  if (dm->dm_prefetch == NULL)
    return;
  for (i = 0; i < n; i++) {
    if ((dp = calloc(1, sizeof(*dp))) == NULL)
      break;
    dp->dp_pid = pids[i];
    dp->dp_last = dp->dp_sent_at = mclk();
    prefetch_resolve(mm, dp);
    dp->dp_table = mpegts_table_add(mm, 0x80, 0xfe, prefetch_ecm, dp, "dvbbuffer-ecm",
                                    LS_DVBBUFFER, MT_FULL | MT_DEFER | MT_FAST,
                                    pids[i], MPS_WEIGHT_CA);
    dp->dp_table->mt_destroy = prefetch_table_destroy;
    mpegts_table_grab(dp->dp_table);
    dm->dm_prefetch[dm->dm_prefetch_n++] = dp;
    if (!atomic_get(&dp->dp_resolved))
      tvhdebug(LS_DVBBUFFER, "%s: ECM prefetch pid %d: no service with this ECM PID (yet)",
               mm->mm_nicename, pids[i]);
  }
  tvhinfo(LS_DVBBUFFER, "%s: ECM prefetch for %d PID(s)", mm->mm_nicename, dm->dm_prefetch_n);
}

/* close them (global_lock) */
void
dvbbuffer_prefetch_stop(dvbbuffer_mux_t *dm)
{
  mpegts_table_t *mt;
  int i;

  lock_assert(&global_lock);
  /* the entries go with their tables (prefetch_table_destroy): destroy (a no-op
   * when mpegts_mux_stop() did it already), then drop our reference */
  for (i = 0; i < dm->dm_prefetch_n; i++) {
    mt = dm->dm_prefetch[i]->dp_table;
    mpegts_table_destroy(mt);
    mpegts_table_release(mt);
  }
  free(dm->dm_prefetch);
  dm->dm_prefetch = NULL;
  dm->dm_prefetch_n = 0;
}

/* PIDs not resolved yet (service list or PMT came later): retry (global_lock) */
void
dvbbuffer_prefetch_check(mpegts_mux_t *mm)
{
  dvbbuffer_mux_t *dm = mm->mm_dvbbuffer;
  int i;

  if (dm == NULL)
    return;
  for (i = 0; i < dm->dm_prefetch_n; i++)
    prefetch_resolve(mm, dm->dm_prefetch[i]);
}

/* a listed PID without ECMs sent (they change every few seconds): say so
 * once, with the table state (global_lock) */
static void
prefetch_watch(mpegts_mux_t *mm, int active)
{
  dvbbuffer_mux_t *dm = mm->mm_dvbbuffer;
  dvbbuffer_prefetch_t *dp;
  int64_t now = mclk(), last;
  int i;

  if (dm == NULL)
    return;
  for (i = 0; i < dm->dm_prefetch_n; i++) {
    dp = dm->dm_prefetch[i];
    if (!active)
      dp->dp_sent_at = now;       /* counts from the (re)start of camd35 */
    last = dp->dp_sent_at;
    if (now - last > PREFETCH_QUIET && !dp->dp_quiet) {
      dp->dp_quiet = 1;
      tvhwarn(LS_DVBBUFFER, "%s: ECM prefetch pid %d: nothing sent for %"PRId64" s, "
              "last section %"PRId64" s ago (resolved %d, table subscribed %d, "
              "waiting for the ring %d, sent %u)",
              mm->mm_nicename, dp->dp_pid, mono2sec(now - last),
              mono2sec(now - dp->dp_last), atomic_get(&dp->dp_resolved),
              dp->dp_table->mt_subscribed, dp->dp_table->mt_backlog, dp->dp_sent);
    } else if (now - last <= PREFETCH_QUIET && dp->dp_quiet) {
      dp->dp_quiet = 0;
      tvhinfo(LS_DVBBUFFER, "%s: ECM prefetch pid %d: sending again", mm->mm_nicename,
              dp->dp_pid);
    }
  }
}

static mtimer_t prefetch_timer;

static void
prefetch_timer_cb(void *aux)
{
  mpegts_network_t *mn;
  mpegts_mux_t *mm;
  int active;

  tvh_mutex_lock(&camd35_lock);
  active = camd35_fd >= 0;
  tvh_mutex_unlock(&camd35_lock);
  if (!active && camd35_cfg == NULL && camd35_retry <= mclk())
    dvbbuffer_camd35_reconf();
  LIST_FOREACH(mn, &mpegts_network_all, mn_global_link)
    LIST_FOREACH(mm, &mn->mn_muxes, mm_network_link) {
      dvbbuffer_prefetch_check(mm);
      prefetch_watch(mm, active);
    }
  mtimer_arm_rel(&prefetch_timer, prefetch_timer_cb, NULL, sec2mono(15));
}

void
dvbbuffer_prefetch_init(void)
{
  dvbbuffer_camd35_reconf();
  mtimer_arm_rel(&prefetch_timer, prefetch_timer_cb, NULL, sec2mono(15));
}

void
dvbbuffer_prefetch_done(void)
{
  mtimer_disarm(&prefetch_timer);
  dvbbuffer_camd35_done();
  free(dvbbuffer_conf.camd35_host);
  free(dvbbuffer_conf.camd35_user);
  free(dvbbuffer_conf.camd35_pass);
  dvbbuffer_conf.camd35_host = dvbbuffer_conf.camd35_user = dvbbuffer_conf.camd35_pass = NULL;
}
