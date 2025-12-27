/*
 *  DAB Probe - Detection of DAB over DVB (EN 301 192)
 *  Copyright (C) 2025
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "tvheadend.h"
#include "input.h"
#include "input/mpegts/mpegts_dvb.h"
#include <dvbdab/dvbdab_c.h>

/*
 * DAB probe context - tracks state during async probe
 */
typedef struct dab_probe_ctx {
  mpegts_mux_t          *mm;           /* Parent mux */
  mpegts_input_t        *mi;           /* Input adapter */
  mpegts_pid_t          *fullmux_pid;  /* FULLMUX subscription */
  dvbdab_scanner_t      *scanner;      /* libdvbdab scanner */
  int                    complete;     /* Set when probe should complete */
  mtimer_t               complete_timer; /* Timer for deferred completion */
} dab_probe_ctx_t;

/* Forward declaration */
static void dab_probe_complete_cb(void *aux);
static int dab_probe_process_results(mpegts_mux_t *mm, dvbdab_results_t *results);

/* Scanner timeout in milliseconds - for ensemble completion when DAB is found */
#define DAB_PROBE_TIMEOUT_MS  20000

/*
 * Deferred completion callback - called from main thread via timer
 */
static void
dab_probe_complete_cb(void *aux)
{
  mpegts_mux_t *mm = aux;
  mpegts_dab_probe_complete(mm);
}

/*
 * Raw packet callback - receives all TS packets during probe
 */
static void
dab_probe_raw_cb(void *opaque, const uint8_t *tsb, int len)
{
  dab_probe_ctx_t *ctx = opaque;

  if (!ctx || !ctx->mm || ctx->complete || !ctx->scanner)
    return;

  /* Feed data to libdvbdab scanner */
  int rc = dvbdab_scanner_feed(ctx->scanner, tsb, len);

  /* Check if scanner is done (timeout or all complete) */
  if (rc || dvbdab_scanner_is_done(ctx->scanner)) {
    ctx->complete = 1;
    mtimer_arm_rel(&ctx->complete_timer, dab_probe_complete_cb, ctx->mm, 0);
  }
}

/*
 * Process scanner results - create child muxes for each discovered ensemble
 * Returns 1 if any DAB content was found, 0 otherwise
 */
static int
dab_probe_process_results(mpegts_mux_t *mm, dvbdab_results_t *results)
{
  int i, j;
  dvb_network_t *ln;
  dvb_mux_t *outer_dm;
  dvb_mux_conf_t dmc;
  dvb_mux_t *dab_mux;
  char ip_str[20];
  int found_dab = 0;

  if (!results)
    return 0;

  /* Get network and outer mux */
  ln = (dvb_network_t *)mm->mm_network;
  outer_dm = (dvb_mux_t *)mm;

  /* Process MPE-based DAB ensembles */
  tvhdebug(LS_MPEGTS, "mux %p: DAB probe found %d ensemble(s), %d ETI-NA stream(s)",
           mm, results->ensemble_count, results->etina_count);

  for (i = 0; i < results->ensemble_count; i++) {
    dvbdab_ensemble_t *ens = &results->ensembles[i];
    int is_etina = ens->is_etina;
    char location[64];

    if (is_etina) {
      snprintf(location, sizeof(location), "ETI-NA PID %d", ens->source_pid);
    } else {
      snprintf(ip_str, sizeof(ip_str), "%d.%d.%d.%d",
               (ens->source_ip >> 24) & 0xFF,
               (ens->source_ip >> 16) & 0xFF,
               (ens->source_ip >> 8) & 0xFF,
               ens->source_ip & 0xFF);
      snprintf(location, sizeof(location), "%s:%d", ip_str, ens->source_port);
    }

    tvhinfo(LS_MPEGTS, "mux %p: %s ensemble EID=0x%04X \"%s\" at %s with %d service(s)",
            mm, is_etina ? "ETI-NA" : "DAB-MPE", ens->eid, ens->label, location, ens->service_count);

    for (j = 0; j < ens->service_count; j++) {
      dvbdab_service_t *svc = &ens->services[j];
      tvhdebug(LS_MPEGTS, "mux %p:   SID=0x%08X SubCh=%d %s \"%s\" %dkbps",
               mm, svc->sid, svc->subchannel_id,
               svc->dabplus ? "DAB+" : "DAB",
               svc->label, svc->bitrate);
    }

    /* Copy outer mux tuning parameters */
    dmc = outer_dm->lm_tuning;
    dmc.dmc_fe_pid = ens->source_pid;

    if (is_etina) {
      /* ETI-NA specific parameters */
      dmc.dmc_dab_eti_padding = ens->etina_padding;
      dmc.dmc_dab_eti_bit_offset = ens->etina_bit_offset;
      dmc.dmc_dab_eti_inverted = ens->etina_inverted;
      dmc.dmc_dab_ip = 0;
      dmc.dmc_dab_port = 0;
      /* Check if ETI-NA mux already exists */
      dab_mux = dvb_network_find_mux_dab_eti(ln, &dmc);
    } else {
      /* DAB-MPE specific parameters */
      dmc.dmc_dab_ip = ens->source_ip;
      dmc.dmc_dab_port = ens->source_port;
      dmc.dmc_dab_eti_padding = 0;
      dmc.dmc_dab_eti_bit_offset = 0;
      dmc.dmc_dab_eti_inverted = 0;
      /* Check if DAB-MPE mux already exists */
      dab_mux = dvb_network_find_mux_dab_mpe(ln, &dmc);
    }

    if (!dab_mux) {
      /* Create new DAB mux - use EID as TSID */
      dab_mux = dvb_mux_create0(ln, MPEGTS_ONID_NONE, ens->eid,
                                &dmc, NULL, NULL);
      if (dab_mux) {
        dab_mux->mm_type = is_etina ? MM_TYPE_DAB_ETI : MM_TYPE_DAB_MPE;

        /* Set provider network name to ensemble label */
        if (ens->label[0]) {
          free(dab_mux->mm_provider_network_name);
          dab_mux->mm_provider_network_name = strdup(ens->label);
        }
        tvhinfo(LS_MPEGTS, "mux %p: created %s child mux \"%s\" (EID=0x%04X) at %s (parent: %s)",
                mm, is_etina ? "ETI-NA" : "DAB-MPE", ens->label, ens->eid, location, mm->mm_nicename);

        /* Create services for each DAB service */
        for (j = 0; j < ens->service_count; j++) {
          dvbdab_service_t *svc = &ens->services[j];
          mpegts_service_t *s;

          /* Use SID as service ID, subchannel as PMT PID placeholder */
          s = mpegts_service_create1(NULL, (mpegts_mux_t *)dab_mux,
                                     svc->sid, svc->subchannel_id, NULL);
          if (s) {
            /* Set service name and provider */
            if (svc->label[0]) {
              free(s->s_dvb_svcname);
              s->s_dvb_svcname = strdup(svc->label);
            }
            if (ens->label[0]) {
              free(s->s_dvb_provider);
              s->s_dvb_provider = strdup(ens->label);
            }
            /* Set service type - radio */
            s->s_dvb_servicetype = 0x02; /* Digital Radio */
            /* DAB services are verified at discovery, no PMT to scan */
            s->s_verified = 1;
            idnode_changed(&s->s_id);
            tvhdebug(LS_MPEGTS, "mux %p:   created service SID=0x%08X \"%s\"",
                     mm, svc->sid, svc->label);
          }
        }

        /* Set scan timestamps for new mux */
        dab_mux->mm_scan_first = gclk();
        dab_mux->mm_scan_last_seen = dab_mux->mm_scan_first;

        idnode_changed(&dab_mux->mm_id);

        if (is_etina) {
          /* ETI-NA: services discovered, mark scan as complete */
          dab_mux->mm_scan_result = MM_SCAN_OK;
          dab_mux->mm_scan_state  = MM_SCAN_STATE_IDLE;
        } else {
          /* DAB-MPE: Queue for scanning */
          mpegts_network_scan_queue_add((mpegts_mux_t *)dab_mux,
                                        SUBSCRIPTION_PRIO_SCAN_INIT,
                                        SUBSCRIPTION_INITSCAN, 10);
        }
        found_dab = 1;  /* Created new DAB mux */
      }
    } else {
      /* Update last seen for existing mux */
      dab_mux->mm_scan_last_seen = gclk();
      idnode_changed(&dab_mux->mm_id);
      tvhdebug(LS_MPEGTS, "mux %p: %s mux already exists for EID=0x%04X at %s",
               mm, is_etina ? "ETI-NA" : "DAB-MPE", ens->eid, location);
      found_dab = 1;  /* Found existing DAB mux */
    }
  }

  /* Process ETI-NA streams */
  for (i = 0; i < results->etina_count; i++) {
    dvbdab_etina_info_t *etina = &results->etina_streams[i];
    tvhinfo(LS_MPEGTS, "mux %p: ETI-NA stream on PID %d (padding=%d, offset=%d, inverted=%d)",
            mm, etina->pid, etina->padding_bytes, etina->sync_bit_offset, etina->inverted);

    /* Copy outer mux tuning parameters and add ETI-NA specific info */
    dmc = outer_dm->lm_tuning;
    dmc.dmc_fe_pid = etina->pid;
    dmc.dmc_dab_eti_padding = etina->padding_bytes;
    dmc.dmc_dab_eti_bit_offset = etina->sync_bit_offset;
    dmc.dmc_dab_eti_inverted = etina->inverted ? 1 : 0;
    /* Clear DAB-MPE fields */
    dmc.dmc_dab_ip = 0;
    dmc.dmc_dab_port = 0;

    /* Check if ETI-NA mux already exists (match by PID) */
    dab_mux = dvb_network_find_mux_dab_eti(ln, &dmc);
    if (!dab_mux) {
      /* Create new ETI-NA mux - use PID as TSID */
      dab_mux = dvb_mux_create0(ln, MPEGTS_ONID_NONE, etina->pid,
                                &dmc, NULL, NULL);
      if (dab_mux) {
        dab_mux->mm_type = MM_TYPE_DAB_ETI;

        /* Set provider network name */
        free(dab_mux->mm_provider_network_name);
        dab_mux->mm_provider_network_name = strdup("ETI-NA");

        tvhinfo(LS_MPEGTS, "mux %p: created ETI-NA child mux on PID %d (parent: %s)",
                mm, etina->pid, mm->mm_nicename);

        /* Set scan timestamps for new mux */
        dab_mux->mm_scan_first = gclk();
        dab_mux->mm_scan_last_seen = dab_mux->mm_scan_first;

        idnode_changed(&dab_mux->mm_id);
        /* ETI-NA service discovery happens during streaming, not scanning */
        /* Mark scan as complete since there's nothing to scan */
        dab_mux->mm_scan_result = MM_SCAN_OK;
        dab_mux->mm_scan_state  = MM_SCAN_STATE_IDLE;
        found_dab = 1;  /* Created new ETI-NA mux */
      }
    } else {
      /* Update last seen for existing mux */
      dab_mux->mm_scan_last_seen = gclk();
      idnode_changed(&dab_mux->mm_id);
      tvhdebug(LS_MPEGTS, "mux %p: ETI-NA mux already exists for PID %d",
               mm, etina->pid);
      found_dab = 1;  /* Found existing ETI-NA mux */
    }
  }

  return found_dab;
}

/*
 * Start DAB probe - opens FULLMUX subscription with callback
 */
void
mpegts_dab_probe_start(mpegts_mux_t *mm)
{
  mpegts_input_t *mi;
  mpegts_mux_instance_t *mmi;
  dab_probe_ctx_t *ctx;

  tvhdebug(LS_MPEGTS, "mux %p: DAB probe starting", mm);

  /* Already probing? */
  if (mm->mm_dab_probe_ctx) {
    tvhdebug(LS_MPEGTS, "mux %p: DAB probe already in progress", mm);
    return;
  }

  /* Get active input */
  mmi = LIST_FIRST(&mm->mm_instances);
  if (!mmi || !mmi->mmi_input) {
    tvherror(LS_MPEGTS, "mux %p: DAB probe - no active input", mm);
    mpegts_dab_probe_complete(mm);
    return;
  }
  mi = mmi->mmi_input;

  /* Allocate context */
  ctx = calloc(1, sizeof(*ctx));
  if (!ctx) {
    tvherror(LS_MPEGTS, "mux %p: DAB probe - failed to allocate context", mm);
    mpegts_dab_probe_complete(mm);
    return;
  }

  /* Create libdvbdab scanner */
  ctx->scanner = dvbdab_scanner_create();
  if (!ctx->scanner) {
    tvherror(LS_MPEGTS, "mux %p: DAB probe - failed to create scanner", mm);
    free(ctx);
    mpegts_dab_probe_complete(mm);
    return;
  }
  dvbdab_scanner_set_timeout(ctx->scanner, DAB_PROBE_TIMEOUT_MS);

  ctx->mm = mm;
  ctx->mi = mi;
  mm->mm_dab_probe_ctx = ctx;
  mm->mm_dab_probe_pending = 1;

  /* Open FULLMUX subscription with callback - requires mi_output_lock */
  tvh_mutex_lock(&mi->mi_output_lock);
  ctx->fullmux_pid = mpegts_input_open_pid_cb(
    mi, mm, MPEGTS_FULLMUX_PID,
    MPS_RAW | MPS_ALL, SUBSCRIPTION_PRIO_SCAN_USER,
    ctx, dab_probe_raw_cb, ctx
  );
  tvh_mutex_unlock(&mi->mi_output_lock);

  if (!ctx->fullmux_pid) {
    tvherror(LS_MPEGTS, "mux %p: DAB probe - failed to open FULLMUX PID", mm);
    dvbdab_scanner_destroy(ctx->scanner);
    free(ctx);
    mm->mm_dab_probe_ctx = NULL;
    mm->mm_dab_probe_pending = 0;
    mpegts_dab_probe_complete(mm);
    return;
  }

  tvhdebug(LS_MPEGTS, "mux %p: DAB probe - FULLMUX subscription opened", mm);
}

/*
 * Complete DAB probe - cleanup and continue scan
 */
void
mpegts_dab_probe_complete(mpegts_mux_t *mm)
{
  dab_probe_ctx_t *ctx = mm->mm_dab_probe_ctx;
  dvbdab_results_t *results = NULL;
  int found_dab = 0;

  if (!ctx) {
    /* No context - just continue scan */
    mm->mm_dab_probe_pending = 0;
    mpegts_network_scan_mux_done_continue(mm);
    return;
  }

  tvhdebug(LS_MPEGTS, "mux %p: DAB probe complete", mm);

  /* Mark complete to stop callback processing */
  ctx->complete = 1;

  /* Disarm completion timer if armed */
  mtimer_disarm(&ctx->complete_timer);

  /* Get scanner results before destroying */
  if (ctx->scanner) {
    results = dvbdab_scanner_get_results(ctx->scanner);
    tvhdebug(LS_MPEGTS, "mux %p: DAB probe results: %d ensembles, %d etina_streams",
             mm, results ? results->ensemble_count : -1,
             results ? results->etina_count : -1);
    found_dab = dab_probe_process_results(mm, results);
    dvbdab_results_free(results);
    dvbdab_scanner_destroy(ctx->scanner);
    ctx->scanner = NULL;
  }

  /* If DAB content was found, override scan result to OK */
  if (found_dab && mm->mm_dab_scan_result != MM_SCAN_OK) {
    tvhinfo(LS_MPEGTS, "mux %p: DAB probe found content, marking scan OK", mm);
    mm->mm_dab_scan_result = MM_SCAN_OK;
  }

  /* Close FULLMUX subscription - requires mi_output_lock */
  if (ctx->fullmux_pid && ctx->mi) {
    tvh_mutex_lock(&ctx->mi->mi_output_lock);
    mpegts_input_close_pid(ctx->mi, mm, MPEGTS_FULLMUX_PID,
                           MPS_RAW | MPS_ALL, ctx);
    tvh_mutex_unlock(&ctx->mi->mi_output_lock);
    ctx->fullmux_pid = NULL;
  }

  /* Free context */
  free(ctx);
  mm->mm_dab_probe_ctx = NULL;
  mm->mm_dab_probe_pending = 0;

  /* Continue scan done processing */
  mpegts_network_scan_mux_done_continue(mm);
}
