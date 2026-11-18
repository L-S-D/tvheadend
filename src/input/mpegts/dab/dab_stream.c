/*
 *  DAB Streaming - Digital Audio Broadcasting decapsulation
 *  Wrapper around libdvbdab streamer for tvheadend integration
 *
 *  Copyright (C) 2025 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "tvheadend.h"

#if ENABLE_DVBDAB

#include "dab_stream.h"
#include "input.h"
#include "input/mpegts/mpegts_dvb.h"
#include <dvbdab/dvbdab_c.h>
#ifdef DVBDAB_HAVE_FORMAT_IPV4
#include "input/mpegts/mpe.h"
#endif

/*
 * libdvbdab logs through tvhlog (by default it writes everything, with names
 * from the stream, to stderr)
 */
static void
dab_log_cb(int level, const char *tag, const char *msg)
{
  switch (level) {
  case DVBDAB_LOG_ERROR: tvherror(LS_MPEGTS, "dvbdab: %s: %s", tag, msg); break;
  case DVBDAB_LOG_WARN:  tvhwarn(LS_MPEGTS, "dvbdab: %s: %s", tag, msg); break;
  case DVBDAB_LOG_TRACE: tvhtrace(LS_MPEGTS, "dvbdab: %s: %s", tag, msg); break;
  default:               tvhdebug(LS_MPEGTS, "dvbdab: %s: %s", tag, msg); break;
  }
}

static pthread_once_t dab_log_once = PTHREAD_ONCE_INIT;

static void
dab_log_setup(void)
{
  dvbdab_set_log_callback(dab_log_cb);
}

void
dab_log_init(void)
{
  pthread_once(&dab_log_once, dab_log_setup);
  /* the library formats every message it lets through, per packet at debug:
   * only errors and warnings unless tvheadend itself logs debug; no trace */
  dvbdab_set_log_level(atomic_get(&tvhlog_level) >= LOG_DEBUG ? DVBDAB_LOG_DEBUG
                                                              : DVBDAB_LOG_WARN);
}

struct dab_stream_ctx {
  dvbdab_streamer_t      *streamer;
  dab_stream_output_cb    callback;
  void                   *opaque;
  int                     started;
  /* the PIDs the clients of the sub-mux take (mpegts_mux_decap_pids), for
   * the thread that feeds the streamer: count -1 = all */
  tvh_mutex_t             pids_lock;
  uint16_t               *pids;
  int                     pids_count;
  int                     pids_changed;
#ifdef DVBDAB_HAVE_FORMAT_IPV4
  /* MPE: tvheadend takes the datagrams out, the IPv4 ones go on */
  mpe_t                  *mpe;
  int                     mpe_pid;
#endif
};

#ifdef DVBDAB_HAVE_FORMAT_IPV4
static void
dab_stream_datagram_cb(void *opaque, const mpe_datagram_t *d)
{
  dab_stream_ctx_t *ctx = opaque;

  if (d->ethertype == 0x0800)
    dvbdab_streamer_feed(ctx->streamer, d->data, d->len);
}
#endif

/*
 * libdvbdab output callback - receives TS packets
 */
static void
dab_stream_ts_cb(void *opaque, const uint8_t *data, size_t len)
{
  dab_stream_ctx_t *ctx = opaque;

  if (ctx && ctx->callback && data && len > 0)
    ctx->callback(ctx->opaque, data, len);
}

dab_stream_ctx_t *
dab_stream_create(const dab_stream_config_t *config,
                   dab_stream_output_cb cb, void *opaque)
{
  dab_stream_ctx_t *ctx;
  dvbdab_streamer_config_t scfg;

  if (!config || !cb)
    return NULL;

  dab_log_init();

  ctx = calloc(1, sizeof(*ctx));
  if (!ctx)
    return NULL;

  /* Set up libdvbdab streamer config */
  memset(&scfg, 0, sizeof(scfg));
  scfg.pid = config->pid;
#ifdef DVBDAB_HAVE_AAC_OUTPUT
  /* DAB+ AUs have 960 samples; in ADTS decoders take 1024: the audio plays
   * 6.7 % slow and about a semitone low. Decoded and encoded again as AAC-LC
   * (libdvbdab with faad2 and libavcodec) it plays right everywhere. */
  if (dvbdab_aac_transcode())
    scfg.aac_output = DVBDAB_AAC_TRANSCODE;
#endif

  switch (config->format) {
  case 0: /* ETI-NA */
    scfg.format = DVBDAB_FORMAT_ETI_NA;
    scfg.eti_padding = config->eti_padding;
    scfg.eti_bit_offset = config->eti_bit_offset;
    scfg.eti_inverted = config->eti_inverted;
    break;
  case 1: /* MPE */
#ifdef DVBDAB_HAVE_FORMAT_IPV4
    /* the datagrams of the PID are taken out here, the IPv4 ones go on */
    scfg.format = DVBDAB_FORMAT_IPV4;
    ctx->mpe = mpe_create();
    ctx->mpe_pid = config->pid;
    if (ctx->mpe == NULL) {
      free(ctx);
      return NULL;
    }
#else
    scfg.format = DVBDAB_FORMAT_MPE;
#endif
    scfg.filter_ip = config->filter_ip;
    scfg.filter_port = config->filter_port;
    break;
  case 4: /* TSNI - TS NI V.11 */
    scfg.format = DVBDAB_FORMAT_TSNI;
    /* TSNI only needs PID, no IP/port filtering */
    break;
  default:
    free(ctx);
    return NULL;
  }

  /* Create libdvbdab streamer */
  ctx->streamer = dvbdab_streamer_create(&scfg);
  if (!ctx->streamer) {
#ifdef DVBDAB_HAVE_FORMAT_IPV4
    mpe_destroy(ctx->mpe);
#endif
    free(ctx);
    return NULL;
  }

  /* Set output callback */
  dvbdab_streamer_set_output(ctx->streamer, dab_stream_ts_cb, ctx);

  ctx->callback = cb;
  ctx->opaque = opaque;
  ctx->started = 0;
  tvh_mutex_init(&ctx->pids_lock, NULL);
  ctx->pids_count = -1;

  return ctx;
}

void
dab_stream_destroy(dab_stream_ctx_t *ctx)
{
  if (!ctx)
    return;

  if (ctx->streamer)
    dvbdab_streamer_destroy(ctx->streamer);
#ifdef DVBDAB_HAVE_FORMAT_IPV4
  mpe_destroy(ctx->mpe);
#endif

  tvh_mutex_destroy(&ctx->pids_lock);
  free(ctx->pids);
  free(ctx);
}

/* the PIDs the clients take, to the streamer (it is fed from one thread) */
static void
dab_stream_apply_pids(dab_stream_ctx_t *ctx)
{
#ifdef DVBDAB_HAVE_WANTED_PIDS
  if (!atomic_get(&ctx->pids_changed))
    return;
  tvh_mutex_lock(&ctx->pids_lock);
  atomic_set(&ctx->pids_changed, 0);
  dvbdab_streamer_set_wanted_pids(ctx->streamer, ctx->pids, ctx->pids_count);
  tvh_mutex_unlock(&ctx->pids_lock);
#else
  (void)ctx;
#endif
}

int
dab_stream_feed(dab_stream_ctx_t *ctx, const uint8_t *data, size_t len)
{
  if (!ctx || !ctx->streamer || !data)
    return -1;

  /* Feed to libdvbdab (MPE: the IPv4 datagrams of the PID) */
  dab_stream_apply_pids(ctx);
#ifdef DVBDAB_HAVE_FORMAT_IPV4
  if (ctx->mpe)
    mpe_feed(ctx->mpe, data, len, ctx->mpe_pid, dab_stream_datagram_cb, ctx);
  else
#endif
  dvbdab_streamer_feed(ctx->streamer, data, len);

  /* Auto-start all services once ready */
  if (!ctx->started && dvbdab_streamer_is_basic_ready(ctx->streamer)) {
    dvbdab_streamer_start_all(ctx->streamer);
    ctx->started = 1;
  }

  return 0;
}

/* **************************************************************************
 * Decapsulation of a DAB sub-mux: while its instance runs, the packets of the
 * input are fed to libdvbdab, its TS goes on as the stream of the mux
 * *************************************************************************/

static void
dab_decap_output_cb(void *opaque, const uint8_t *pkt, size_t len)
{
  mpegts_mux_instance_t *mmi = opaque;

  sbuf_append(&mmi->mmi_decap_out, pkt, len);
}

static int
dab_decap_format(const mpegts_mux_t *mm)
{
  switch (mm->mm_type) {
  case MM_TYPE_DAB_ETI:  return 0;
  case MM_TYPE_DAB_MPE:  return 1;
  case MM_TYPE_DAB_TSNI: return 4;
  default:               return -1;
  }
}

int
mpegts_mux_decap_start(mpegts_mux_instance_t *mmi)
{
  mpegts_mux_t *mm = mmi->mmi_mux;
  const dvb_mux_conf_t *dmc;
  dab_stream_config_t cfg;
  int format = dab_decap_format(mm);

  if (format < 0)
    return 0;   /* nothing to decapsulate */

  dmc = &((dvb_mux_t *)mm)->lm_tuning;
  memset(&cfg, 0, sizeof(cfg));
  cfg.format = format;
  cfg.pid = dmc->dmc_fe_pid;
  if (cfg.pid == 0) {
    tvherror(LS_MPEGTS, "%s: DAB mux has no PID configured", mm->mm_nicename);
    return -1;
  }
  if (format == 0) {            /* ETI-NA */
    cfg.eti_padding = dmc->dmc_dab_eti_padding;
    cfg.eti_bit_offset = dmc->dmc_dab_eti_bit_offset;
    cfg.eti_inverted = dmc->dmc_dab_eti_inverted;
  } else if (format == 1) {     /* MPE */
    cfg.filter_ip = dmc->dmc_dab_ip;
    cfg.filter_port = dmc->dmc_dab_port;
  }                             /* TS NI: the PID is all it needs */

  mmi->mmi_decap = dab_stream_create(&cfg, dab_decap_output_cb, mmi);
  if (mmi->mmi_decap == NULL) {
    tvherror(LS_MPEGTS, "%s: failed to create the DAB streaming context",
             mm->mm_nicename);
    return -1;
  }
  sbuf_init(&mmi->mmi_decap_out);
  tvh_mutex_lock(&mmi->mmi_input->mi_output_lock);
  mpegts_mux_decap_pids(mmi);
  tvh_mutex_unlock(&mmi->mmi_input->mi_output_lock);
  tvhdebug(LS_MPEGTS, "%s: DAB streaming initialized for PID 0x%04X (format %d)",
           mm->mm_nicename, cfg.pid, format);
  return 0;
}

/* the PIDs clients of the sub-mux take the packets of (not only its
 * tables): their number, and into pids unless it is NULL */
static int
dab_pids_taken(mpegts_mux_t *mm, uint16_t *pids)
{
  mpegts_pid_t *mp;
  int n = 0;

  RB_FOREACH(mp, &mm->mm_pids, mp_link)
    if (mp->mp_pid < MPEGTS_FULLMUX_PID &&
        (mp->mp_type & (MPS_SERVICE | MPS_RAW | MPS_STREAM)) != 0) {
      if (pids)
        pids[n] = mp->mp_pid;
      n++;
    }
  return n;
}

/* the PIDs of the sub-mux its clients take (with the output lock of its
 * input; a full mux takes all): libdvbdab transcodes the DAB+ audio of
 * their services only */
void
mpegts_mux_decap_pids(mpegts_mux_instance_t *mmi)
{
  dab_stream_ctx_t *ctx = mmi->mmi_decap;
  mpegts_mux_t *mm = mmi->mmi_mux;
  mpegts_pid_sub_t *mps;
  uint16_t *pids;
  int all = 0;
  int n = 0;

  lock_assert(&mmi->mmi_input->mi_output_lock);
  if (ctx == NULL)
    return;
  LIST_FOREACH(mps, &mm->mm_all_subs, mps_svcraw_link)
    if (mps->mps_type & MPS_ALL)
      all = 1;
  if (!all)
    n = dab_pids_taken(mm, NULL);
  tvh_mutex_lock(&ctx->pids_lock);
  if (n > 0) {
    pids = realloc(ctx->pids, n * sizeof(*pids));
    if (pids != NULL) {
      ctx->pids = pids;
      n = dab_pids_taken(mm, pids);
    } else {
      all = 1;          /* without the memory for the list: all */
    }
  }
  ctx->pids_count = all ? -1 : n;
  atomic_set(&ctx->pids_changed, 1);
  tvh_mutex_unlock(&ctx->pids_lock);
}

void
mpegts_mux_decap_stop(mpegts_mux_instance_t *mmi)
{
  if (mmi->mmi_decap == NULL)
    return;
  dab_stream_destroy(mmi->mmi_decap);
  mmi->mmi_decap = NULL;
  sbuf_free(&mmi->mmi_decap_out);
}

void
mpegts_mux_decap_feed(mpegts_mux_instance_t *mmi, sbuf_t *sb)
{
  const uint8_t *data = sb->sb_data;
  size_t len = sb->sb_ptr;
  size_t i = 0;
  size_t j;

  /* whole TS packets only: a lost sync byte is searched for again, the
   * start of a packet at the end waits for the next read */
  while (i + 188 <= len) {
    if (data[i] != 0x47) {
      i++;
      continue;
    }
    for (j = i; j + 188 <= len && data[j] == 0x47; j += 188)
      ;
    dab_stream_feed(mmi->mmi_decap, data + i, j - i);
    i = j;
  }
  sbuf_cut(sb, i);

  if (mmi->mmi_decap_out.sb_ptr > 0)
    mpegts_input_recv_packets(mmi, &mmi->mmi_decap_out, MPEGTS_DATA_DECAPSULATED, NULL);
}

#endif /* ENABLE_DVBDAB */
