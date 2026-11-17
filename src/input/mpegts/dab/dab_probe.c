/*
 *  DAB Probe - Detection of DAB over DVB (EN 301 192)
 *  Copyright (C) 2025 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "tvheadend.h"

#if ENABLE_DVBDAB

#include "input.h"
#include "input/mpegts/mpegts_dvb.h"
#include <dvbdab/dvbdab_c.h>
#include "dab_stream.h"
#ifdef DVBDAB_HAVE_FORMAT_IPV4
/* MPE: tvheadend takes the datagrams out of the datagram sections of a PID
 * (mpe_pids_t), the IPv4 ones go to a libdvbdab streamer of the PID in
 * discovery mode */
#include "input/mpegts/mpe.h"
#endif

/*
 * DAB probe context - simple callback-based scanner
 */
typedef struct dab_probe_ctx {
  mpegts_mux_t      *mm;
  dvbdab_scanner_t  *scanner;
  mtimer_t           wait_timer;  /* the end of the scan waits for the scanner */
  int64_t            started;
  int                result;      /* the result the scan ended with */
  int                weight;
  int                waiting;
#ifdef DVBDAB_HAVE_FORMAT_IPV4
  mpe_pids_t         mpe;         /* the input thread, mi_output_lock; user:
                                     the streamer of the PID */
#endif
} dab_probe_ctx_t;

/* Scanner timeout in milliseconds */
#ifndef DAB_PROBE_TIMEOUT_MS
#define DAB_PROBE_TIMEOUT_MS  5000
#endif

/* MPE PIDs without an ensemble after this long carry no DAB (MPE carries
 * other IP services as well) */
#ifndef DAB_PROBE_MPE_NODAB_MS
#define DAB_PROBE_MPE_NODAB_MS 2000
#endif

/* The longest a scan that ended waits for the scanner */
#ifndef DAB_PROBE_MAX_MS
#define DAB_PROBE_MAX_MS      (DAB_PROBE_TIMEOUT_MS + 3000)
#endif

/*
 * Secondary scan callback - receives all TS packets
 * Called from mi-table thread context during scan
 */
#ifdef DVBDAB_HAVE_FORMAT_IPV4
static void
dab_probe_mpe_datagram(void *opaque, mpe_pid_t *p, const mpe_datagram_t *d)
{
  dvbdab_streamer_config_t cfg;

  (void)opaque;
  if (d->ethertype != 0x0800)
    return;
  if (p->user == NULL) {
    memset(&cfg, 0, sizeof(cfg));
    cfg.format = DVBDAB_FORMAT_IPV4;
    cfg.pid = p->pid;
    if ((p->user = dvbdab_streamer_create(&cfg)) == NULL)
      return;
  }
  dvbdab_streamer_feed(p->user, d->data, d->len);
}

static void
dab_probe_mpe_streamer_free(void *streamer)
{
  dvbdab_streamer_destroy(streamer);
}

/* the ensembles of the MPE PIDs have their labels and services; an MPE
 * PID without any ensemble after DAB_PROBE_MPE_NODAB_MS carries no DAB, the
 * time of the scanner ends the rest */
static int
dab_probe_mpe_done(dab_probe_ctx_t *dctx)
{
  dvbdab_ensemble_t *e;
  int64_t elapsed = mclk() - dctx->started;
  int i, j, n, done = 1;

  if (elapsed >= ms2mono(DAB_PROBE_TIMEOUT_MS))
    return 1;
  for (i = 0; i < dctx->mpe.n && done; i++) {
    n = 0;
    e = dctx->mpe.p[i].user ? dvbdab_streamer_get_all_ensembles(dctx->mpe.p[i].user, &n) : NULL;
    if (n == 0 && elapsed < ms2mono(DAB_PROBE_MPE_NODAB_MS))
      done = 0;
    for (j = 0; j < n; j++)
      if (!e[j].label[0] || e[j].service_count == 0)
        done = 0;
    if (e)
      dvbdab_streamer_free_all_ensembles(e, n);
  }
  return done;
}
#endif

static void
dab_probe_packet_cb(void *ctx, const uint8_t *pkt, int len)
{
  dab_probe_ctx_t *dctx = ctx;

  if (!dctx || !dctx->scanner)
    return;

  /* Feed data to libdvbdab scanner */
  dvbdab_scanner_feed(dctx->scanner, pkt, len);
#ifdef DVBDAB_HAVE_FORMAT_IPV4
  mpe_pids_feed(&dctx->mpe, pkt, len, dab_probe_mpe_datagram, dctx);
#endif
}

/*
 * Format IP address to string
 */
static void
format_ip_str(char *buf, size_t len, uint32_t ip)
{
  snprintf(buf, len, "%d.%d.%d.%d",
           (ip >> 24) & 0xFF, (ip >> 16) & 0xFF,
           (ip >> 8) & 0xFF, ip & 0xFF);
}

/*
 * Initialize new DAB mux common fields and queue for scanning
 */
static void
init_new_dab_mux(dvb_mux_t *dab_mux, const char *label)
{
  dvb_mux_set_dab_ensemble((mpegts_mux_t *)dab_mux, label);
  mpegts_mux_update_nice_name((mpegts_mux_t *)dab_mux);

  dab_mux->mm_scan_first = gclk();
  dab_mux->mm_scan_last_seen = dab_mux->mm_scan_first;
  idnode_changed(&dab_mux->mm_id);

  mpegts_network_scan_queue_add((mpegts_mux_t *)dab_mux,
                                 SUBSCRIPTION_PRIO_SCAN_INIT,
                                 SUBSCRIPTION_INITSCAN, 10);
}

/*
 * Update existing mux timestamp and optionally the ensemble label
 */
static void
update_existing_mux(dvb_mux_t *dab_mux, const char *label)
{
  dvb_mux_set_dab_ensemble((mpegts_mux_t *)dab_mux, label);
  dab_mux->mm_scan_last_seen = gclk();
  idnode_changed(&dab_mux->mm_id);
}

/*
 * Process a single DAB ensemble from scanner results
 */
static int
process_ensemble(mpegts_mux_t *mm, dvb_network_t *ln,
                 const dvb_mux_t *outer_dm, const dvbdab_ensemble_t *ens)
{
  dvb_mux_conf_t dmc;
  dvb_mux_t *dab_mux;
  const char *type_str;
  char location[64];
  char ip_str[20];
  int is_etina = ens->is_etina;
  int is_tsni = ens->is_tsni;

  /* Determine type and location string */
  if (is_etina) {
    type_str = "ETI-NA";
    snprintf(location, sizeof(location), "ETI-NA PID %d", ens->source_pid);
  } else if (is_tsni) {
    type_str = "DAB-TSNI";
    snprintf(location, sizeof(location), "TSNI PID %d", ens->source_pid);
  } else {
    type_str = "DAB-MPE";
    format_ip_str(ip_str, sizeof(ip_str), ens->source_ip);
    snprintf(location, sizeof(location), "%s:%d", ip_str, ens->source_port);
  }

  tvhdebug(LS_MPEGTS, "mux %s: %s ensemble EID=0x%04X \"%s\" at %s with %d service(s)",
           mm->mm_nicename, type_str, ens->eid, ens->label, location, ens->service_count);

  /* Copy outer mux tuning parameters and set type-specific fields */
  dmc = outer_dm->lm_tuning;
  dmc.dmc_fe_pid = ens->source_pid;
  dmc.dmc_dab_eti_padding = 0;
  dmc.dmc_dab_eti_bit_offset = 0;
  dmc.dmc_dab_eti_inverted = 0;
  dmc.dmc_dab_ip = 0;
  dmc.dmc_dab_port = 0;

  if (is_etina) {
    dmc.dmc_dab_eti_padding = ens->etina_padding;
    dmc.dmc_dab_eti_bit_offset = ens->etina_bit_offset;
    dmc.dmc_dab_eti_inverted = ens->etina_inverted;
    dab_mux = dvb_network_find_mux_dab_eti(ln, &dmc);
  } else if (is_tsni) {
    dab_mux = dvb_network_find_mux_dab_tsni(ln, &dmc);
  } else {
    dmc.dmc_dab_ip = ens->source_ip;
    dmc.dmc_dab_port = ens->source_port;
    dab_mux = dvb_network_find_mux_dab_mpe(ln, &dmc);
  }

  /* Existing mux found - update timestamp and label */
  if (dab_mux) {
    update_existing_mux(dab_mux, ens->label);
    return 1;
  }

  /* Create new mux */
  dab_mux = dvb_mux_create0(ln, MPEGTS_ONID_NONE, ens->eid, &dmc, NULL, NULL);
  if (!dab_mux)
    return 0;

  if (is_etina)
    dab_mux->mm_type = MM_TYPE_DAB_ETI;
  else if (is_tsni)
    dab_mux->mm_type = MM_TYPE_DAB_TSNI;
  else
    dab_mux->mm_type = MM_TYPE_DAB_MPE;
  dvb_mux_submux_link(dab_mux, (dvb_mux_t *)mm);

  tvhinfo(LS_MPEGTS, "mux %s: created %s child mux \"%s\" (EID=0x%04X, PID=0x%04X)",
          mm->mm_nicename, type_str, ens->label, ens->eid, dmc.dmc_fe_pid);

  init_new_dab_mux(dab_mux, ens->label);
  return 1;
}

/*
 * Process a single ETI-NA stream from scanner results
 */
static int
process_etina_stream(mpegts_mux_t *mm, dvb_network_t *ln,
                     const dvb_mux_t *outer_dm, const dvbdab_etina_info_t *etina)
{
  dvb_mux_conf_t dmc;
  dvb_mux_t *dab_mux;

  /* Setup DMC for ETI-NA */
  dmc = outer_dm->lm_tuning;
  dmc.dmc_fe_pid = etina->pid;
  dmc.dmc_dab_eti_padding = etina->padding_bytes;
  dmc.dmc_dab_eti_bit_offset = etina->sync_bit_offset;
  dmc.dmc_dab_eti_inverted = etina->inverted ? 1 : 0;
  dmc.dmc_dab_ip = 0;
  dmc.dmc_dab_port = 0;

  dab_mux = dvb_network_find_mux_dab_eti(ln, &dmc);

  /* Existing mux found - update timestamp only (keep existing label) */
  if (dab_mux) {
    update_existing_mux(dab_mux, NULL);
    return 1;
  }

  /* Create new mux: TSID and services come from its scan */
  dab_mux = dvb_mux_create0(ln, MPEGTS_ONID_NONE, MPEGTS_TSID_NONE, &dmc, NULL, NULL);
  if (!dab_mux)
    return 0;

  dab_mux->mm_type = MM_TYPE_DAB_ETI;
  dvb_mux_submux_link(dab_mux, (dvb_mux_t *)mm);

  tvhinfo(LS_MPEGTS, "mux %s: created ETI-NA child mux on PID %d",
          mm->mm_nicename, etina->pid);

  /* the label comes from the scan of the ETI mux */
  init_new_dab_mux(dab_mux, NULL);
  return 1;
}

/*
 * Process scanner results - create child muxes for discovered ensembles
 */
static int
dab_probe_process_results(mpegts_mux_t *mm, dvbdab_results_t *results)
{
  int i;
  dvb_network_t *ln;
  const dvb_mux_t *outer_dm;
  int found_dab = 0;

  if (!results)
    return 0;

  ln = (dvb_network_t *)mm->mm_network;
  outer_dm = (dvb_mux_t *)mm;

  tvhdebug(LS_MPEGTS, "mux %s: DAB probe found %d ensemble(s), %d ETI-NA stream(s)",
           mm->mm_nicename, results->ensemble_count, results->etina_count);

  /* Process DAB ensembles (MPE, ETI-NA, or TSNI); MPE ones come from the
   * datagrams tvheadend takes out (mpegts_dab_probe_complete()) */
  for (i = 0; i < results->ensemble_count; i++) {
#ifdef DVBDAB_HAVE_FORMAT_IPV4
    if (!results->ensembles[i].is_etina && !results->ensembles[i].is_tsni)
      continue;
#endif
    if (process_ensemble(mm, ln, outer_dm, &results->ensembles[i]))
      found_dab = 1;
  }

  /* Process ETI-NA streams */
  for (i = 0; i < results->etina_count; i++) {
    if (process_etina_stream(mm, ln, outer_dm, &results->etina_streams[i]))
      found_dab = 1;
  }

  return found_dab;
}

/*
 * Install or remove the secondary packet callback of a mux. The input
 * thread calls it under mi_output_lock (mpegts_input_process).
 */
static void
dab_probe_set_cb(mpegts_mux_t *mm, void *ctx,
                 void (*cb)(void *ctx, const uint8_t *pkt, int len))
{
  mpegts_mux_instance_t *mmi = mm->mm_active;
  mpegts_input_t *mi = mmi ? mmi->mmi_input : NULL;

  if (mi)
    tvh_mutex_lock(&mi->mi_output_lock);
  mm->mm_secondary_ctx = ctx;
  mm->mm_secondary_cb  = cb;
  if (mi)
    tvh_mutex_unlock(&mi->mi_output_lock);
}

/*
 * Start DAB probe - called when mux scan starts
 */
void
mpegts_dab_probe_start(mpegts_mux_t *mm)
{
  dab_probe_ctx_t *ctx;

  /* Only a transponder (not a sub-mux) of a DVB network that may get new
   * muxes, while it is tuned: the probe opens the whole mux while the scan
   * runs. Its sub-muxes run from it on the sub-mux input. */
  if (mm->mm_type != MM_TYPE_TS ||
      mm->mm_network->mn_autodiscovery == MN_DISCOVERY_DISABLE ||
      !idnode_is_instance(&mm->mm_network->mn_id, &dvb_network_class) ||
      !((dvb_network_t *)mm->mm_network)->ln_dab_probe ||
      mm->mm_active == NULL)
    return;

  /* Already probing? */
  if (mm->mm_dab_probe_ctx)
    return;

  /* Allocate context */
  ctx = calloc(1, sizeof(*ctx));
  if (!ctx)
    return;

  /* Create scanner */
  dab_log_init();
  ctx->scanner = dvbdab_scanner_create();
  if (!ctx->scanner) {
    free(ctx);
    return;
  }
  dvbdab_scanner_set_timeout(ctx->scanner, DAB_PROBE_TIMEOUT_MS);

  ctx->mm = mm;
  ctx->started = mclk();

  /* Store in dedicated field */
  mm->mm_dab_probe_ctx = ctx;

  /* Install secondary callback (the input thread calls it under mi_output_lock) */
  dab_probe_set_cb(mm, ctx, dab_probe_packet_cb);

  tvhdebug(LS_MPEGTS, "mux %s: DAB probe started", mm->mm_nicename);

  /* Trigger PID update to enable fullmux for DAB probe */
  mm->mm_update_pids_flag = 1;
  mpegts_mux_update_pids(mm);
}

/* the scanner has all it can get (it is fed by the input thread) */
static int
dab_probe_scanner_done(dab_probe_ctx_t *ctx)
{
  mpegts_mux_t *mm = ctx->mm;
  mpegts_mux_instance_t *mmi = mm->mm_active;
  mpegts_input_t *mi = mmi ? mmi->mmi_input : NULL;
  int done;

  if (mi)
    tvh_mutex_lock(&mi->mi_output_lock);
  done = dvbdab_scanner_is_done(ctx->scanner);
#ifdef DVBDAB_HAVE_FORMAT_IPV4
  if (done)
    done = dab_probe_mpe_done(ctx);
#endif
  if (mi)
    tvh_mutex_unlock(&mi->mi_output_lock);
  return done;
}

static void
dab_probe_wait_cb(void *aux)
{
  mpegts_mux_t *mm = aux;
  dab_probe_ctx_t *ctx = mm->mm_dab_probe_ctx;

  if (ctx)
    mpegts_network_scan_mux_end(mm, ctx->result, ctx->weight);
}

/* the end of the scan waits for the probe (the tables go on reporting meanwhile) */
int
mpegts_dab_probe_waiting(mpegts_mux_t *mm)
{
  dab_probe_ctx_t *ctx = mm->mm_dab_probe_ctx;

  return ctx != NULL && ctx->waiting;
}

/*
 * The DAB data come slower than the tables of a scan: a scan that ended
 * (not a cancelled one) waits until the scanner is done, for a while at
 * most. Returns 1 if the end of the scan is put off.
 */
int
mpegts_dab_probe_wait(mpegts_mux_t *mm, int result, int weight)
{
  dab_probe_ctx_t *ctx = mm->mm_dab_probe_ctx;

  if (ctx == NULL || dab_probe_scanner_done(ctx) ||
      mclk() - ctx->started >= ms2mono(DAB_PROBE_MAX_MS))
    return 0;
  if (!ctx->waiting) {
    ctx->waiting = 1;
    tvhdebug(LS_MPEGTS, "mux %s: waiting for the DAB probe", mm->mm_nicename);
  }
  ctx->result = result;
  ctx->weight = weight;
  mtimer_arm_rel(&ctx->wait_timer, dab_probe_wait_cb, mm, ms2mono(500));
  return 1;
}

/*
 * Complete DAB probe - called when mux scan ends
 */
int
mpegts_dab_probe_complete(mpegts_mux_t *mm, int evaluate)
{
  dab_probe_ctx_t *ctx = mm->mm_dab_probe_ctx;
  dvbdab_results_t *results;
  int found = 0;

  if (!ctx)
    return 0;

  mtimer_disarm(&ctx->wait_timer);

  /* Remove callback only if still ours - under mi_output_lock, so the input
   * thread is not inside the scanner when it is destroyed below */
  if (mm->mm_secondary_ctx == ctx)
    dab_probe_set_cb(mm, NULL, NULL);
  mm->mm_dab_probe_ctx = NULL;

  /* the whole mux was opened for the probe only */
  mm->mm_update_pids_flag = 1;
  mpegts_mux_update_pids(mm);

  /* Get and process results (not of a cancelled scan) */
  if (ctx->scanner) {
    results = evaluate ? dvbdab_scanner_get_results(ctx->scanner) : NULL;
    if (results) {
      found = dab_probe_process_results(mm, results);
      dvbdab_results_free(results);
    }
    dvbdab_scanner_destroy(ctx->scanner);
  }
#ifdef DVBDAB_HAVE_FORMAT_IPV4
  {
    dvbdab_ensemble_t *e;
    int i, j, n;
    for (i = 0; i < ctx->mpe.n; i++) {
      if (evaluate && ctx->mpe.p[i].user) {
        e = dvbdab_streamer_get_all_ensembles(ctx->mpe.p[i].user, &n);
        tvhdebug(LS_MPEGTS, "mux %s: MPE on PID %d: %" PRIu64 " datagrams, %" PRIu64
                 " CRC errors, %d ensemble(s)", mm->mm_nicename, ctx->mpe.p[i].pid,
                 mpe_counts(ctx->mpe.p[i].mpe)->datagrams,
                 mpe_counts(ctx->mpe.p[i].mpe)->crc_errors, n);
        for (j = 0; j < n; j++) {
          e[j].source_pid = ctx->mpe.p[i].pid;
          e[j].is_etina = e[j].is_tsni = e[j].is_gse = 0;
          if (process_ensemble(mm, (dvb_network_t *)mm->mm_network, (dvb_mux_t *)mm, &e[j]))
            found = 1;
        }
        dvbdab_streamer_free_all_ensembles(e, n);
      }
    }
    mpe_pids_clear(&ctx->mpe, dab_probe_mpe_streamer_free);
  }
#endif

  free(ctx);

  if (found > 0)
    tvhinfo(LS_MPEGTS, "mux %s: DAB probe complete, found=%d", mm->mm_nicename, found);
  return found;
}

#endif /* ENABLE_DVBDAB */
