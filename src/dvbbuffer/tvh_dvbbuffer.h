/*
 *  Tvheadend - instant channel switching (libdvbbuffer glue)
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#ifndef __TVH_DVBBUFFER_H__
#define __TVH_DVBBUFFER_H__

#include "build.h"

#if ENABLE_DVBBUFFER

void dvbbuffer_init(void);
void dvbbuffer_done(void);

#else

static inline void dvbbuffer_init(void) { }
static inline void dvbbuffer_done(void) { }

#endif

#endif /* __TVH_DVBBUFFER_H__ */
