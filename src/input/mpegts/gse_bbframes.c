/*
 *  Tvheadend - the GSE PDUs of an input stream in the BBFrame packets
 *
 *  Copyright (C) 2026 L-S-D
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

#include "gse_bbframes.h"

#include <stdlib.h>

gse_bbframes_t *
gse_bbframes_create ( int isi, gse_pdu_cb_t cb, void *opaque )
{
  gse_bbframes_t *gb = calloc(1, sizeof(*gb));

  if (gb == NULL)
    return NULL;
  if ((gb->gse = gse_create()) == NULL) {
    free(gb);
    return NULL;
  }
  bbframe_split_init(&gb->bs, isi);
  gb->cb = cb;
  gb->opaque = opaque;
  return gb;
}

void
gse_bbframes_destroy ( gse_bbframes_t *gb )
{
  if (gb == NULL)
    return;
  gse_destroy(gb->gse);
  free(gb);
}

static void
gse_bbframes_frame ( void *opaque, const uint8_t *frame, size_t len, int isi )
{
  gse_bbframes_t *gb = opaque;

  (void)isi;
  gb->frames++;
  if (len >= BBFRAME_HEADER && bbframe_is_ts(frame)) {
    gb->ts_frames++;
    return;
  }
  gse_bbframe(gb->gse, frame, len, gb->cb, gb->opaque);
}

void
gse_bbframes_feed ( gse_bbframes_t *gb, const uint8_t *tsb, size_t len, int pid )
{
  for ( ; len >= 188; tsb += 188, len -= 188)
    if (tsb[0] == 0x47 && (((tsb[1] & 0x1f) << 8) | tsb[2]) == pid)
      bbframe_split_frames(&gb->bs, tsb, gse_bbframes_frame, gb);
}
