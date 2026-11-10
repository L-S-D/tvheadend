/*
 *  Tvheadend - the TS packets of BBFrames (DVB-S2, DVB-T2)
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

#ifndef __TVH_BBFRAME_H__
#define __TVH_BBFRAME_H__

#include <stdint.h>
#include <stddef.h>

/*
 * A BBFrame (EN 302 307-1 5.1, EN 302 755 5.1): a 10 byte header (MATYPE,
 * UPL, DFL, SYNC, SYNCD, CRC-8 xored with the mode) and the data field. In
 * a TS stream the data field carries the user packets: TS packets, in normal
 * mode with the CRC-8 of the packet before in place of the sync byte and the
 * ISSY and DNP fields behind, in high efficiency mode without the sync byte
 * (and with the DNP field behind when null packets are deleted). A packet
 * goes on in the next frame of the stream; SYNCD tells where the first one
 * of a frame starts. No tvheadend dependency: usable offline.
 */

#define BBFRAME_HEADER  10

/* the largest data field (normal FECFRAME, rate 9/10: Kbch 58192 bits) */
#define BBFRAME_DATA_MAX  7274

typedef void (*bbframe_ts_cb_t)(void *opaque, const uint8_t *ts);

/* the TS packets out of the frames of one stream */
typedef struct bbframe_ts {
  uint8_t  bt_up[192];   /* a user packet begun in the frame before */
  size_t   bt_up_len;
  int      bt_synced;
  uint64_t bt_frames;    /* frames of the stream */
  uint64_t bt_errors;    /* frames lost (header, length) */
  uint64_t bt_resyncs;   /* user packets lost */
} bbframe_ts_t;

/* the mode of a good header: 0 normal, 1 high efficiency; -1 a bad header */
int  bbframe_header_mode ( const uint8_t *h );

void bbframe_ts_init ( bbframe_ts_t *bt );

/* the frame (header and data field, len bytes): its TS packets (a TS
 * stream) to cb; 0, or -1 for a frame that is bad or too short */
int  bbframe_frame_ts ( bbframe_ts_t *bt, const uint8_t *frame, size_t len,
                        bbframe_ts_cb_t cb, void *opaque );

#endif /* __TVH_BBFRAME_H__ */
