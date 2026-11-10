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

#include <string.h>
#include "bbframe.h"

static uint8_t
bbframe_crc8 ( const uint8_t *p, int len )
{
  uint8_t c = 0;
  int i;

  while (len-- > 0) {
    c ^= *p++;
    for (i = 0; i < 8; i++)
      c = (c & 0x80) ? (uint8_t)((c << 1) ^ 0xd5) : (uint8_t)(c << 1);
  }
  return c;
}

int
bbframe_header_mode ( const uint8_t *h )
{
  int m = bbframe_crc8(h, 9) ^ h[9];
  return (m == 0 || m == 1) ? m : -1;
}

void
bbframe_ts_init ( bbframe_ts_t *bt )
{
  memset(bt, 0, sizeof(*bt));
}

int
bbframe_frame_ts ( bbframe_ts_t *bt, const uint8_t *frame, size_t len,
                   bbframe_ts_cb_t cb, void *opaque )
{
  const uint8_t *h = frame;
  const uint8_t *d = h + BBFRAME_HEADER;
  size_t dfl, syncd, upl, i = 0, n;
  int mode = len < BBFRAME_HEADER ? -1 : bbframe_header_mode(h);
  uint8_t ts[188];

  if (mode < 0) {
    bt->bt_errors++;
    bt->bt_synced = 0;
    return -1;
  }
  dfl = (((size_t)h[4] << 8) | h[5]) >> 3;
  if (BBFRAME_HEADER + dfl > len) {
    bt->bt_errors++;
    bt->bt_synced = 0;
    return -1;
  }
  bt->bt_frames++;
  if ((h[0] >> 6) != 3)
    return 0;               /* not a TS stream */
  syncd = ((size_t)h[7] << 8) | h[8];
  if (mode == 1)
    upl = 187 + ((h[0] & 0x04) ? 1 : 0);
  else
    upl = (((size_t)h[2] << 8) | h[3]) >> 3;
  if (upl < 187 || upl > sizeof(bt->bt_up)) {
    bt->bt_errors++;
    bt->bt_synced = 0;
    return -1;
  }
  if (syncd != 0xffff)
    syncd >>= 3;

  if (!bt->bt_synced || (syncd != 0xffff && syncd != (upl - bt->bt_up_len) % upl)) {
    if (bt->bt_synced)
      bt->bt_resyncs++;
    bt->bt_synced = 0;
    bt->bt_up_len = 0;
    if (syncd == 0xffff || syncd >= dfl)
      return 0;
    i = syncd;
    bt->bt_synced = 1;
  }

  while (i < dfl) {
    n = upl - bt->bt_up_len;
    if (n > dfl - i)
      n = dfl - i;
    memcpy(bt->bt_up + bt->bt_up_len, d + i, n);
    bt->bt_up_len += n;
    i += n;
    if (bt->bt_up_len < upl)
      break;
    ts[0] = 0x47;
    memcpy(ts + 1, bt->bt_up + (mode == 1 ? 0 : 1), 187);
    cb(opaque, ts);
    bt->bt_up_len = 0;
  }
  return 0;
}
