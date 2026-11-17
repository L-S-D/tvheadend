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

#ifndef __TVH_GSE_BBFRAMES_H__
#define __TVH_GSE_BBFRAMES_H__

#include "bbframe.h"
#include "gse.h"

/*
 * The BBFrames of an input stream (isi; -1 all) the Neumo driver passes in
 * the TS packets of a PID, their GSE PDUs to a callback. A stream of 188
 * byte user packets is a TS one, not GSE: its frames only count.
 * Plain computation: no tvheadend state, no locks.
 */

typedef struct gse_bbframes {
  bbframe_split_t bs;
  gse_t          *gse;
  gse_pdu_cb_t    cb;
  void           *opaque;
  uint64_t        frames;
  uint64_t        ts_frames;     /* frames of 188 byte user packets */
} gse_bbframes_t;

gse_bbframes_t *gse_bbframes_create ( int isi, gse_pdu_cb_t cb, void *opaque );
void gse_bbframes_destroy ( gse_bbframes_t *gb );

/* len bytes of TS packets: those of pid */
void gse_bbframes_feed ( gse_bbframes_t *gb, const uint8_t *tsb, size_t len, int pid );

/* most frames carry 188 byte user packets: the stream is a TS one */
static inline int gse_bbframes_is_ts ( const gse_bbframes_t *gb )
{
  return gb->frames && gb->ts_frames * 2 > gb->frames;
}

#endif /* __TVH_GSE_BBFRAMES_H__ */
