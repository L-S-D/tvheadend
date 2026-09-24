/*
 *  Tvheadend - instant channel switching, per mux ring buffer
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "dvbbuffer_private.h"

static mpegts_listener_t dvbbuffer_mux_listener;
static int dvbbuffer_mux_count;     /* library muxes alive */
static int dvbbuffer_shutdown;      /* destroy the context with the last mux */

/* kept warm (prebuffer) */
int
dvbbuffer_mux_prebuffer(mpegts_mux_t *mm)
{
  return dvbbuffer_ctx != NULL && !dvbbuffer_shutdown &&
         dvbbuffer_conf.enabled && mm->mm_prebuffer && mm->mm_is_enabled(mm);
}

/* ring buffer wanted: warm, or held by HLS clients */
int
dvbbuffer_mux_wanted(mpegts_mux_t *mm)
{
  return dvbbuffer_mux_prebuffer(mm) ||
         (dvbbuffer_ctx != NULL && !dvbbuffer_shutdown && mm->mm_dvbbuffer_hold > 0);
}

void
dvbbuffer_mux_ref(dvbbuffer_mux_t *dm)
{
  lock_assert(&global_lock);
  dm->dm_refcount++;
}

void
dvbbuffer_mux_unref(dvbbuffer_mux_t *dm)
{
  lock_assert(&global_lock);
  if (--dm->dm_refcount > 0)
    return;
  tvhdebug(LS_DVBBUFFER, "%s: ring buffer released", dm->dm_name);
  dvbbuf_mux_destroy(dm->dm_lib);
  free(dm->dm_name);
  free(dm);
  if (--dvbbuffer_mux_count == 0 && dvbbuffer_shutdown) {
    dvbbuf_ctx_destroy(dvbbuffer_ctx);
    dvbbuffer_ctx = NULL;
  }
}

/*
 * Create the ring buffer of a running mux and publish it to the input thread
 */
void
dvbbuffer_mux_attach(mpegts_mux_t *mm)
{
  mpegts_mux_instance_t *mmi = mm->mm_active;
  mpegts_input_t *mi;
  dvbbuf_mux_config cfg;
  dvbbuffer_mux_t *dm;

  lock_assert(&global_lock);

  if (mm->mm_dvbbuffer || mmi == NULL || !dvbbuffer_mux_wanted(mm))
    return;
  mi = mmi->mmi_input;

  dm = calloc(1, sizeof(*dm));
  dm->dm_name = strdup(mm->mm_nicename ?: "?");
  dm->dm_refcount = 1;

  memset(&cfg, 0, sizeof(cfg));
  cfg.struct_size      = sizeof(cfg);
  cfg.buffer_ms        = dvbbuffer_conf.buffer_sec * 1000;
  cfg.max_bitrate_kbps = dvbbuffer_conf.max_bitrate_kbps;
  cfg.name             = dm->dm_name;
  if (dvbbuf_mux_create(dvbbuffer_ctx, &cfg, &dm->dm_lib) != DVBBUF_OK) {
    tvherror(LS_DVBBUFFER, "%s: unable to create ring buffer: %s",
             dm->dm_name, dvbbuf_last_error());
    free(dm->dm_name);
    free(dm);
    return;
  }

  dvbbuffer_mux_count++;

  tvh_mutex_lock(&mi->mi_output_lock);
  mm->mm_dvbbuffer = dm;
  tvh_mutex_unlock(&mi->mi_output_lock);

  tvhinfo(LS_DVBBUFFER, "%s: ring buffer started (%u s, %u kbit/s)",
          dm->dm_name, dvbbuffer_conf.buffer_sec, dvbbuffer_conf.max_bitrate_kbps);
}

/*
 * Stop feeding and drop the mux reference (services may still hold one)
 */
void
dvbbuffer_mux_detach(mpegts_mux_t *mm)
{
  mpegts_mux_instance_t *mmi = mm->mm_active;
  dvbbuffer_mux_t *dm = mm->mm_dvbbuffer;
  mpegts_input_t *mi;

  lock_assert(&global_lock);

  if (dm == NULL)
    return;
  if (mmi) {
    mi = mmi->mmi_input;
    tvh_mutex_lock(&mi->mi_output_lock);
    mm->mm_dvbbuffer = NULL;
    tvh_mutex_unlock(&mi->mi_output_lock);
  } else {
    /* mpegts_mux_stop() cleared mm_active and the PIDs under
     * mi_output_lock before the stop event, the input thread
     * does not touch this mux any more */
    mm->mm_dvbbuffer = NULL;
  }
  tvhinfo(LS_DVBBUFFER, "%s: ring buffer stopped", dm->dm_name);
  dvbbuffer_mux_unref(dm);
}

/*
 * H2 - input thread, mi_output_lock held
 */
void
dvbbuffer_mux_input0(mpegts_mux_t *mm, uint64_t pos,
                     const uint8_t *tsb, int len, int cc_restart)
{
  dvbbuffer_mux_t *dm = mm->mm_dvbbuffer;

  if (cc_restart)
    dvbbuf_mux_mark_gap(dm->dm_lib);
  dvbbuf_mux_feed(dm->dm_lib, tsb, len, pos, getfastmonoclock());
}

/*
 * H8 - mux property changed
 */
void
dvbbuffer_mux_prebuffer_notify(void *p, const char *lang)
{
  mpegts_mux_t *mm = p;

  if (dvbbuffer_ctx == NULL)
    return;
  if (dvbbuffer_mux_wanted(mm))
    dvbbuffer_mux_attach(mm);
  else
    dvbbuffer_mux_detach(mm);
  dvbbuffer_warm_reconcile();
}

/*
 * Mux events (global_lock)
 */
static void
dvbbuffer_mux_start_cb(mpegts_mux_t *mm, void *p)
{
  dvbbuffer_mux_attach(mm);
}

static void
dvbbuffer_mux_stop_cb(mpegts_mux_t *mm, void *p, int reason)
{
  dvbbuffer_mux_detach(mm);
}

static void
dvbbuffer_mux_delete_cb(mpegts_mux_t *mm, void *p)
{
  dvbbuffer_mux_detach(mm);
  dvbbuffer_warm_mux_delete(mm);
}

void
dvbbuffer_mux_init(void)
{
  dvbbuffer_mux_listener.ml_mux_start  = dvbbuffer_mux_start_cb;
  dvbbuffer_mux_listener.ml_mux_stop   = dvbbuffer_mux_stop_cb;
  dvbbuffer_mux_listener.ml_mux_delete = dvbbuffer_mux_delete_cb;
  mpegts_add_listener(&dvbbuffer_mux_listener);
}

/*
 * Shutdown (global_lock): stop all ring buffers; services which are still
 * running keep their reference, the library context goes with the last one
 */
void
dvbbuffer_mux_done(void)
{
  mpegts_network_t *mn;
  mpegts_mux_t *mm;

  lock_assert(&global_lock);

  mpegts_rem_listener(&dvbbuffer_mux_listener);
  LIST_FOREACH(mn, &mpegts_network_all, mn_global_link)
    LIST_FOREACH(mm, &mn->mn_muxes, mm_network_link)
      dvbbuffer_mux_detach(mm);
  dvbbuffer_shutdown = 1;
  if (dvbbuffer_mux_count == 0 && dvbbuffer_ctx) {
    dvbbuf_ctx_destroy(dvbbuffer_ctx);
    dvbbuffer_ctx = NULL;
  }
}
