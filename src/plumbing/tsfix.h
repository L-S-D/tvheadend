/**
 *  Timestamp fixup
 *  Copyright (C) 2010 Andreas Öman
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

#ifndef TSFIX_H__
#define TSFIX_H__

#include "tvheadend.h"

streaming_target_t *tsfix_create(streaming_target_t *output);

void tsfix_destroy(streaming_target_t *gh);

/* Without audio and video this long after the start (a scrambled service
 * without a key): take the reference clock from any stream and do not wait
 * for the video size, so the clear streams start; 0 = wait */
void tsfix_early_start(streaming_target_t *tsfix, int ms);


#endif // TSFIX_H__
