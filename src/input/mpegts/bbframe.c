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

/* **************************************************************************
 * The BBFrames in the packets of the Neumo driver
 * *************************************************************************/

#define BB_SYNC       0xb8
#define BB_PKT_DATA   180   /* bytes of a packet from byte 8 on */

int
bbframe_packet_header ( const uint8_t *pkt, uint8_t *isi, uint8_t *matype )
{
  if (pkt[5] != 0x80 || pkt[7] < 1 + BBFRAME_HEADER || pkt[7] > BB_PKT_DATA ||
      pkt[8] != BB_SYNC || bbframe_header_mode(pkt + 9) < 0)
    return -1;
  *matype = pkt[9];
  *isi = pkt[10];
  return 0;
}

void
bbframe_split_init ( bbframe_split_t *bs, int isi )
{
  memset(bs, 0, sizeof(*bs));
  bbframe_ts_init(&bs->bs_ts);
  bs->bs_isi = isi;
  bs->bs_cur = -1;
}

int
bbframe_split_take ( bbframe_split_t *bs, const uint8_t *pkt )
{
  uint8_t isi, matype;

  if (pkt[5] != 0x80 || pkt[7] == 0 || pkt[7] > BB_PKT_DATA)
    return 0;
  if (pkt[8] == BB_SYNC)
    bs->bs_cur = bbframe_packet_header(pkt, &isi, &matype) == 0 ? isi : -1;
  return bs->bs_cur >= 0 && bs->bs_cur == bs->bs_isi;
}

/* a frame of the stream that was not complete */
static void
bbframe_split_lost ( bbframe_split_t *bs )
{
  bs->bs_ts.bt_errors++;
  bs->bs_ts.bt_synced = 0;
  bs->bs_len = 0;
}

void
bbframe_split_ts ( bbframe_split_t *bs, const uint8_t *pkt,
                   bbframe_ts_cb_t cb, void *opaque )
{
  size_t len = pkt[7], dfl;
  const uint8_t *src;
  int start = pkt[8] == BB_SYNC;

  if (!bbframe_split_take(bs, pkt)) {
    if (start && bs->bs_len)
      bbframe_split_lost(bs);
    return;
  }

  if (start) {
    if (bs->bs_len)
      bbframe_split_lost(bs);
    src = pkt + 8;
  } else {
    if (bs->bs_len == 0)
      return;                      /* the start of the frame was lost */
    src = pkt + 9;
    len--;
  }
  if (bs->bs_len + len > sizeof(bs->bs_frame)) {
    bbframe_split_lost(bs);
    return;
  }
  memcpy(bs->bs_frame + bs->bs_len, src, len);
  bs->bs_len += len;

  if (bs->bs_len < 1 + BBFRAME_HEADER)
    return;
  dfl = (((size_t)bs->bs_frame[5] << 8) | bs->bs_frame[6]) >> 3;
  if (bs->bs_len < 1 + BBFRAME_HEADER + dfl)
    return;
  bbframe_frame_ts(&bs->bs_ts, bs->bs_frame + 1, bs->bs_len - 1, cb, opaque);
  bs->bs_len = 0;
}
