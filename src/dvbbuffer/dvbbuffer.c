/*
 *  Tvheadend - instant channel switching (libdvbbuffer glue)
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "dvbbuffer_private.h"
#include "streaming.h"

dvbbuf_ctx *dvbbuffer_ctx;

/*
 * Route library log messages into tvhlog. Called from library threads
 * without any library lock held.
 */
static void
dvbbuffer_log_cb(void *user, int level, const char *category, const char *msg)
{
  int sev;

  switch (level) {
  case DVBBUF_LOG_ERROR: sev = LOG_ERR;     break;
  case DVBBUF_LOG_WARN:  sev = LOG_WARNING; break;
  case DVBBUF_LOG_INFO:  sev = LOG_INFO;    break;
  case DVBBUF_LOG_DEBUG: sev = LOG_DEBUG;   break;
  default:               sev = LOG_TRACE;   break;
  }
  tvhlog(sev, LS_DVBBUFFER, "%s: %s", category, msg);
}

void
dvbbuffer_init(void)
{
  dvbbuf_ctx_config cfg;

  memset(&cfg, 0, sizeof(cfg));
  cfg.struct_size = sizeof(cfg);
  cfg.log_level   = DVBBUF_LOG_TRACE;   /* tvhlog filters by its own settings */
  cfg.log         = dvbbuffer_log_cb;

  if (dvbbuf_ctx_create(&cfg, &dvbbuffer_ctx) != DVBBUF_OK) {
    tvherror(LS_DVBBUFFER, "unable to initialise libdvbbuffer: %s",
             dvbbuf_last_error());
    dvbbuffer_ctx = NULL;
    return;
  }
  tvhinfo(LS_DVBBUFFER, "libdvbbuffer %s initialised", dvbbuf_version());

  dvbbuffer_conf_init();
  dvbbuffer_mux_init();
  dvbbuffer_warm_init();
  dvbbuffer_hls_init();
}

/*
 * Called before mpegts_done(): inputs and services may still run
 */
void
dvbbuffer_done(void)
{
  if (dvbbuffer_ctx == NULL)
    return;
  /* first: its request threads may wait for global_lock */
  dvbbuffer_hls_done();
  tvh_mutex_lock(&global_lock);
  dvbbuffer_warm_done();
  dvbbuffer_mux_done();
  streaming_start_reserve      = 0;
  streaming_start_reserve_time = 0;
  tvh_mutex_unlock(&global_lock);
}
