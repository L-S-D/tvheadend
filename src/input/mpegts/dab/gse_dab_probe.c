/*
 *  GSE-DAB Probe - Detection of DAB within GSE streams
 *
 *  Finds the DAB ensembles of a GSE input stream of a multistream
 *  transponder (MM_TYPE_GSE) and makes a DAB-GSE mux for each one.
 *
 *  The GSE probe (gse_probe.c) reads the stream while the scan of the mux
 *  runs and takes the PDUs out of GSE; the IPv4 ones come here and go to a
 *  libdvbdab streamer in discovery mode (DVBDAB_FORMAT_IPV4, filter_ip=0);
 *  with a libdvbdab before 1.1.0 the streamer gets the packets of the
 *  BBFrames (DVBDAB_FORMAT_BBF_TS).
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
#include "input/mpegts/gse.h"
#include "gse_dab_probe.h"

struct gse_dab_probe {
  mpegts_mux_t          *mm;
  dvbdab_streamer_t     *streamer;   /* fed by the input thread, mi_output_lock */
};

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
/* a scan for a known ensemble whose last scan did not succeed, or that
 * this probe did not find: its result may be from an earlier tuning */
static void
gse_requeue_ensemble(dvb_mux_t *lm)
{
  mpegts_mux_t *mm = (mpegts_mux_t *)lm;

  if (mm->mm_scan_state != MM_SCAN_STATE_IDLE || !mm->mm_is_enabled(mm))
    return;
  mpegts_network_scan_queue_add(mm, SUBSCRIPTION_PRIO_SCAN_INIT,
                                SUBSCRIPTION_INITSCAN, 10);
}

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

  /* no ensemble id, no label: not a DAB ensemble (other data on IP) */
  if (ens->eid == 0 && ens->label[0] == '\0') {
    tvhdebug(LS_MPEGTS, "mux %s: %s:%d is no DAB ensemble, skipped",
             mm->mm_nicename, ip_str, ens->source_port);
    return 0;
  }

  dmc = outer_dm->lm_tuning;
  dmc.dmc_dab_ip = ens->source_ip;
  dmc.dmc_dab_port = ens->source_port;

  /* Check for existing mux */
  dab_mux = dvb_network_find_mux_dab_gse(ln, &dmc);
  if (dab_mux) {
    tvhdebug(LS_MPEGTS, "mux %s: DAB-GSE mux already exists for %s:%d",
             mm->mm_nicename, ip_str, ens->source_port);
    dvb_mux_set_dab_ensemble((mpegts_mux_t *)dab_mux, ens->label);
    idnode_changed(&dab_mux->mm_id);
    if (dab_mux->mm_scan_result != MM_SCAN_OK &&
        dab_mux->mm_scan_result != MM_SCAN_PARTIAL &&
        dab_mux->mm_scan_result != MM_SCAN_IGNORE)
      gse_requeue_ensemble(dab_mux);
    return 1;
  }

  return gse_create_child_mux(mm, ln, &dmc, ens, ip_str) != NULL;
}

static void
gse_requeue_missing(mpegts_mux_t *mm, const dvbdab_ensemble_t *ens, int count)
{
  const dvb_mux_t *gse = (const dvb_mux_t *)mm;
  mpegts_mux_t *mm2;
  dvb_mux_t *lm;
  int i;

  LIST_FOREACH(mm2, &mm->mm_network->mn_muxes, mm_network_link) {
    if (mm2->mm_type != MM_TYPE_DAB_GSE)
      continue;
    lm = (dvb_mux_t *)mm2;
    if (lm->lm_tuning.dmc_fe_type != gse->lm_tuning.dmc_fe_type ||
        deltaU32(lm->lm_tuning.dmc_fe_freq, gse->lm_tuning.dmc_fe_freq) > 2000 ||
        lm->lm_tuning.u.dmc_fe_qpsk.polarisation != gse->lm_tuning.u.dmc_fe_qpsk.polarisation ||
        lm->lm_tuning.dmc_fe_stream_id != gse->lm_tuning.dmc_fe_stream_id)
      continue;
    for (i = 0; i < count; i++)
      if (ens[i].source_ip == lm->lm_tuning.dmc_dab_ip &&
          ens[i].source_port == lm->lm_tuning.dmc_dab_port)
        break;
    if (i < count)
      continue;
    tvhdebug(LS_MPEGTS, "mux %s: %s not found by the GSE-DAB probe, scanning it",
             mm->mm_nicename, mm2->mm_nicename);
    gse_requeue_ensemble(lm);
  }
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

  /* the known ensembles of this stream the probe did not find get a scan:
   * one that is gone ends as failed, not with an old OK */
  gse_requeue_missing(mm, ensembles, count);

  dvbdab_streamer_free_all_ensembles(ensembles, count);

  return found;
}

gse_dab_probe_t *
gse_dab_probe_create(mpegts_mux_t *mm)
{
  gse_dab_probe_t *dp;
  dvbdab_streamer_config_t config;

  if ((dp = calloc(1, sizeof(*dp))) == NULL)
    return NULL;

  /* Create streamer in discovery mode (filter_ip=0): the IPv4 PDUs of the
   * stream (or, with an older libdvbdab, the packets of its BBFrames) */
  memset(&config, 0, sizeof(config));
#ifdef DVBDAB_HAVE_FORMAT_IPV4
  config.format = DVBDAB_FORMAT_IPV4;
#else
  config.format = DVBDAB_FORMAT_BBF_TS;  /* BBFrames in the packets of the driver */
#endif
  config.pid = DVB_BBFRAMES_PID;
  config.filter_ip = 0;    /* Discovery mode: find all ensembles */
  config.filter_port = 0;

  dab_log_init();
  dp->streamer = dvbdab_streamer_create(&config);
  if (!dp->streamer) {
    tvherror(LS_MPEGTS, "mux %s: GSE-DAB probe failed to create streamer",
             mm->mm_nicename);
    free(dp);
    return NULL;
  }
  dp->mm = mm;
  tvhdebug(LS_MPEGTS, "mux %s: GSE-DAB probe started", mm->mm_nicename);
  return dp;
}

void
gse_dab_probe_pdu(gse_dab_probe_t *dp, const struct gse_pdu *pdu)
{
#ifndef DVBDAB_HAVE_FORMAT_IPV4
  (void)dp; (void)pdu;
#else
  if (dp && pdu->protocol_type == 0x0800)
    dvbdab_streamer_feed(dp->streamer, pdu->data, pdu->len);
#endif
}

void
gse_dab_probe_packets(gse_dab_probe_t *dp, const uint8_t *pkt, int len)
{
#ifdef DVBDAB_HAVE_FORMAT_IPV4
  (void)dp; (void)pkt; (void)len;
#else
  if (dp)
    dvbdab_streamer_feed(dp->streamer, pkt, len);
#endif
}

int
gse_dab_probe_ready(gse_dab_probe_t *dp)
{
  return dp == NULL || dvbdab_streamer_is_basic_ready(dp->streamer);
}

int
gse_dab_probe_finish(gse_dab_probe_t *dp, int evaluate)
{
  int found = 0;

  if (dp == NULL)
    return 0;
  if (evaluate)
    found = gse_dab_probe_process_results(dp->mm, dp->streamer);
  dvbdab_streamer_destroy(dp->streamer);
  tvhdebug(LS_MPEGTS, "mux %s: GSE-DAB probe complete, found %d ensemble(s)",
           dp->mm->mm_nicename, found);
  free(dp);
  return found;
}
