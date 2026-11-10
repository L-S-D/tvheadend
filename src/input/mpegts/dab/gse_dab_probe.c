/*
 *  GSE-DAB Probe - Detection of DAB within GSE streams
 *
 *  Finds the DAB ensembles of a GSE input stream of a multistream
 *  transponder (MM_TYPE_GSE) and makes a DAB-GSE mux for each one.
 *
 *  The GSE mux runs on the sub-mux input: its stream is the BBFrames of its
 *  input stream, as the Neumo driver embeds them (DVB_BBFRAMES_PID). The
 *  probe reads them while the scan runs (mm_secondary_cb), with a libdvbdab
 *  streamer in discovery mode (DVBDAB_FORMAT_BBF_TS, filter_ip=0).
 *
 *  Copyright (C) 2025
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "tvheadend.h"
#include "input.h"
#include "input/mpegts/mpegts_dvb.h"
#include <dvbdab/dvbdab_c.h>
#include "dab_stream.h"

typedef struct gse_dab_probe_ctx {
  mpegts_mux_t          *mm;
  dvbdab_streamer_t     *streamer;   /* fed by the input thread, mi_output_lock */
  int64_t                start_time;
} gse_dab_probe_ctx_t;

/* Discovery timeout in milliseconds */
#define GSE_DAB_PROBE_TIMEOUT_MS  8000

/*
 * Format IP address to string
 */
static void
gse_format_ip_str(char *buf, size_t len, uint32_t ip)
{
  snprintf(buf, len, "%d.%d.%d.%d",
           (ip >> 24) & 0xFF, (ip >> 16) & 0xFF,
           (ip >> 8) & 0xFF, ip & 0xFF);
}

/*
 * Create the DAB-GSE mux of an ensemble: a sub-mux of the transponder, in
 * the input stream of the GSE mux
 */
static dvb_mux_t *
gse_create_child_mux(mpegts_mux_t *mm, dvb_network_t *ln,
                     const dvb_mux_conf_t *dmc, const dvbdab_ensemble_t *ens,
                     const char *ip_str)
{
  dvb_mux_t *dab_mux, *t;

  dab_mux = dvb_mux_create0(ln, MPEGTS_ONID_NONE, ens->eid, dmc, NULL, NULL);
  if (!dab_mux)
    return NULL;

  dab_mux->mm_type = MM_TYPE_DAB_GSE;
  t = dvb_mux_submux_parent((dvb_mux_t *)mm);
  if (t)
    dvb_mux_submux_link(dab_mux, t);

  dvb_mux_set_dab_ensemble((mpegts_mux_t *)dab_mux, ens->label);
  mpegts_mux_update_nice_name((mpegts_mux_t *)dab_mux);

  tvhinfo(LS_MPEGTS, "mux %s: created DAB-GSE child mux \"%s\" (EID=0x%04X, %s:%d)",
          mm->mm_nicename, ens->label, ens->eid, ip_str, ens->source_port);

  dab_mux->mm_scan_first = gclk();
  dab_mux->mm_scan_last_seen = dab_mux->mm_scan_first;
  idnode_changed(&dab_mux->mm_id);

  mpegts_network_scan_queue_add((mpegts_mux_t *)dab_mux,
                                 SUBSCRIPTION_PRIO_SCAN_INIT,
                                 SUBSCRIPTION_INITSCAN, 10);
  return dab_mux;
}

/*
 * Process a single GSE ensemble
 * Returns 1 if ensemble was processed, 0 otherwise
 */
static int
gse_process_ensemble(mpegts_mux_t *mm, dvb_network_t *ln, dvb_mux_t *outer_dm,
                     const dvbdab_ensemble_t *ens)
{
  dvb_mux_conf_t dmc;
  dvb_mux_t *dab_mux;
  char ip_str[20];

  gse_format_ip_str(ip_str, sizeof(ip_str), ens->source_ip);

  tvhdebug(LS_MPEGTS, "mux %s: GSE-DAB ensemble EID=0x%04X \"%s\" at %s:%d with %d service(s)",
           mm->mm_nicename, ens->eid, ens->label, ip_str, ens->source_port, ens->service_count);

  dmc = outer_dm->lm_tuning;
  dmc.dmc_dab_ip = ens->source_ip;
  dmc.dmc_dab_port = ens->source_port;

  /* Check for existing mux */
  dab_mux = dvb_network_find_mux_dab_gse(ln, &dmc);
  if (dab_mux) {
    tvhdebug(LS_MPEGTS, "mux %s: DAB-GSE mux already exists for %s:%d",
             mm->mm_nicename, ip_str, ens->source_port);
    dvb_mux_set_dab_ensemble((mpegts_mux_t *)dab_mux, ens->label);
    dab_mux->mm_scan_last_seen = gclk();
    idnode_changed(&dab_mux->mm_id);
    return 1;
  }

  return gse_create_child_mux(mm, ln, &dmc, ens, ip_str) != NULL;
}

/*
 * Process the streamer results: a DAB-GSE mux for each ensemble
 */
static int
gse_dab_probe_process_results(mpegts_mux_t *mm, dvbdab_streamer_t *streamer)
{
  dvbdab_ensemble_t *ensembles;
  int count = 0;
  int i;
  int found = 0;

  ensembles = dvbdab_streamer_get_all_ensembles(streamer, &count);
  if (!ensembles || count == 0) {
    tvhdebug(LS_MPEGTS, "mux %s: GSE-DAB probe found no ensembles", mm->mm_nicename);
    return 0;
  }

  tvhdebug(LS_MPEGTS, "mux %s: GSE-DAB probe found %d ensemble(s)", mm->mm_nicename, count);

  for (i = 0; i < count; i++)
    found += gse_process_ensemble(mm, (dvb_network_t *)mm->mm_network,
                                  (dvb_mux_t *)mm, &ensembles[i]);

  dvbdab_streamer_free_all_ensembles(ensembles, count);

  return found;
}

/* the packets of the GSE mux (the input thread, under mi_output_lock) */
static void
gse_dab_probe_packet_cb(void *aux, const uint8_t *pkt, int len)
{
  gse_dab_probe_ctx_t *ctx = aux;

  dvbdab_streamer_feed(ctx->streamer, pkt, len);
}

static void
gse_dab_probe_set_cb(mpegts_mux_t *mm, gse_dab_probe_ctx_t *ctx)
{
  mpegts_input_t *mi = mm->mm_active ? mm->mm_active->mmi_input : NULL;

  if (mi)
    tvh_mutex_lock(&mi->mi_output_lock);
  mm->mm_secondary_ctx = ctx;
  mm->mm_secondary_cb  = ctx ? gse_dab_probe_packet_cb : NULL;
  if (mi)
    tvh_mutex_unlock(&mi->mi_output_lock);
}

/*
 * Start GSE-DAB probe - called when the scan of a GSE mux starts
 */
void
mpegts_gse_dab_probe_start(mpegts_mux_t *mm)
{
  gse_dab_probe_ctx_t *ctx;
  dvbdab_streamer_config_t config;

  if (mm->mm_type != MM_TYPE_GSE || mm->mm_gse_dab_probe_ctx ||
      mm->mm_active == NULL || mm->mm_secondary_cb)
    return;

  ctx = calloc(1, sizeof(*ctx));
  if (!ctx)
    return;

  /* Create streamer in discovery mode (filter_ip=0) with BBF_TS format */
  memset(&config, 0, sizeof(config));
  config.format = DVBDAB_FORMAT_BBF_TS;  /* BBFrames in the packets of the driver */
  config.pid = DVB_BBFRAMES_PID;
  config.filter_ip = 0;    /* Discovery mode: find all ensembles */
  config.filter_port = 0;

  dab_log_init();
  ctx->streamer = dvbdab_streamer_create(&config);
  if (!ctx->streamer) {
    tvherror(LS_MPEGTS, "mux %s: GSE-DAB probe failed to create streamer",
             mm->mm_nicename);
    free(ctx);
    return;
  }

  ctx->mm = mm;
  ctx->start_time = mclk();
  mm->mm_gse_dab_probe_ctx = ctx;
  gse_dab_probe_set_cb(mm, ctx);

  tvhdebug(LS_MPEGTS, "mux %s: GSE-DAB probe started", mm->mm_nicename);
}

/*
 * Complete GSE-DAB probe - called when mux scan ends
 */
int
mpegts_gse_dab_probe_complete(mpegts_mux_t *mm, int evaluate)
{
  gse_dab_probe_ctx_t *ctx = mm->mm_gse_dab_probe_ctx;
  int found = 0;

  if (!ctx)
    return 0;

  mm->mm_gse_dab_probe_ctx = NULL;

  /* the input thread is not inside the streamer after this */
  if (mm->mm_secondary_ctx == ctx)
    gse_dab_probe_set_cb(mm, NULL);

  /* Process results (not of a cancelled scan) */
  if (evaluate)
    found = gse_dab_probe_process_results(mm, ctx->streamer);
  dvbdab_streamer_destroy(ctx->streamer);
  free(ctx);

  tvhdebug(LS_MPEGTS, "mux %s: GSE-DAB probe complete, found %d ensemble(s)",
           mm->mm_nicename, found);

  return found;
}

/*
 * Check if GSE-DAB probe is done
 */
int
mpegts_gse_dab_probe_is_done(mpegts_mux_t *mm)
{
  gse_dab_probe_ctx_t *ctx = mm->mm_gse_dab_probe_ctx;
  mpegts_input_t *mi;
  int done;

  if (!ctx)
    return 1;
  if (mclk() - ctx->start_time >= ms2mono(GSE_DAB_PROBE_TIMEOUT_MS))
    return 1;

  mi = mm->mm_active ? mm->mm_active->mmi_input : NULL;
  if (mi)
    tvh_mutex_lock(&mi->mi_output_lock);
  done = dvbdab_streamer_is_basic_ready(ctx->streamer);
  if (mi)
    tvh_mutex_unlock(&mi->mi_output_lock);
  return done;
}
