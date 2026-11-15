/*
 *  Tvheadend - instant channel switching
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "dvbbuffer_private.h"

uint32_t dvbbuffer_running;
memoryinfo_t dvbbuffer_memoryinfo = { .my_name = "Instant zapping ring buffers" };

void
dvbbuffer_init(void)
{
  tvhdebug(LS_DVBBUFFER, "warm muxes with a ring buffer in front of the input processing");
  dvbbuffer_running = 1;
  memoryinfo_register(&dvbbuffer_memoryinfo);

  dvbbuffer_conf_init();
  dvbbuffer_prefetch_init();
  dvbbuffer_mux_init();
  dvbbuffer_warm_init();
}

/*
 * Called before mpegts_done(): inputs and services may still run
 */
void
dvbbuffer_done(void)
{
  if (!dvbbuffer_running)
    return;
  tvh_mutex_lock(&global_lock);
  dvbbuffer_warm_done();
  dvbbuffer_mux_done();
  dvbbuffer_prefetch_done();
  dvbbuffer_running = 0;
  memoryinfo_unregister(&dvbbuffer_memoryinfo);
  tvh_mutex_unlock(&global_lock);
}
