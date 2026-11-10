/*
 *  Tvheadend - a TS carried in a PID of another TS (T2-MI, data piping)
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

#include <stdlib.h>
#include <string.h>
#include "t2mi.h"

#define TS_SIZE           188
#define T2MI_TYPE_BBFRAME 0x00
#define T2MI_BBF_HEADER   3       /* frame_idx, plp_id, intl_frame_start */

/* CRC-32 of MPEG-2 (EN 300 468 annex B), four bits at a time */
static const uint32_t t2mi_crc_tab[16] = {
  0x00000000, 0x04c11db7, 0x09823b6e, 0x0d4326d9,
  0x130476dc, 0x17c56b6b, 0x1a864db2, 0x1e475005,
  0x2608edb8, 0x22c9f00f, 0x2f8ad6d6, 0x2b4bcb61,
  0x350c9b64, 0x31cd86d3, 0x3c8ea00a, 0x384fbdbd
};

static uint32_t
t2mi_crc32 ( const uint8_t *p, size_t len )
{
  uint32_t c = 0xffffffff;

  while (len-- > 0) {
    c = (c << 4) ^ t2mi_crc_tab[(c >> 28) ^ (*p >> 4)];
    c = (c << 4) ^ t2mi_crc_tab[(c >> 28) ^ (*p & 0x0f)];
    p++;
  }
  return c;
}

const char *
t2mi_format_name ( int format )
{
  switch (format) {
  case T2MI_FORMAT_T2MI:   return "T2-MI";
  case T2MI_FORMAT_PIPING: return "TS piping";
  default:                 return "auto";
  }
}

static void
t2mi_out ( void *opaque, const uint8_t *ts )
{
  t2mi_t *tm = opaque;

  tm->tm_out++;
  tm->tm_cb(tm->tm_opaque, ts);
}

/* **************************************************************************
 * T2-MI
 * *************************************************************************/

/* the packet begun and the user packet begun in it are lost: on at the
 * next start of a packet */
static void
t2mi_t2mi_lost ( t2mi_t *tm )
{
  tm->tm_pkt_len = 0;
  tm->tm_pkt_sync = 0;
  tm->tm_bt.bt_synced = 0;
}

/* a T2-MI packet with a good CRC, pl bytes of payload */
static void
t2mi_t2mi_packet ( t2mi_t *tm, const uint8_t *pkt, size_t pl )
{
  const uint8_t *d = pkt + T2MI_HEADER;
  int plp;

  if (tm->tm_probing) {
    tm->tm_probe_t2mi++;
    return;
  }
  tm->tm_packets++;
  /* the count goes over all packets: one lost, the user packet begun in
   * a BBFrame before may be lost with it */
  if (tm->tm_count >= 0 && pkt[1] != ((tm->tm_count + 1) & 0xff)) {
    tm->tm_count_gaps++;
    tm->tm_bt.bt_synced = 0;
  }
  tm->tm_count = pkt[1];
  if (pkt[0] != T2MI_TYPE_BBFRAME || pl < T2MI_BBF_HEADER + BBFRAME_HEADER) {
    tm->tm_other++;
    return;
  }
  plp = d[1];
  /* a PLP of a TS (TS/GS 11 in MATYPE-1, a good header) */
  if ((tm->tm_plps[plp >> 5] & (1u << (plp & 31))) == 0 &&
      (d[T2MI_BBF_HEADER] >> 6) == 3 && bbframe_header_mode(d + T2MI_BBF_HEADER) >= 0) {
    tm->tm_plps[plp >> 5] |= 1u << (plp & 31);
    tm->tm_plp_new = 1;
  }
  /* auto: the first one */
  if (tm->tm_plp == T2MI_PLP_AUTO && (tm->tm_plps[plp >> 5] & (1u << (plp & 31))))
    tm->tm_plp = plp;
  if (plp == tm->tm_plp)
    bbframe_frame_ts(&tm->tm_bt, d + T2MI_BBF_HEADER, pl - T2MI_BBF_HEADER, t2mi_out, tm);
}

/* the complete packets of the buffer */
static void
t2mi_t2mi_parse ( t2mi_t *tm )
{
  const uint8_t *b = tm->tm_pkt;
  size_t start = 0;
  size_t pl;
  size_t tot;
  uint32_t crc;

  while (tm->tm_pkt_len - start >= T2MI_HEADER) {
    pl = ((((size_t)b[start + 4] << 8) | b[start + 5]) + 7) >> 3;
    tot = T2MI_HEADER + pl + 4;
    if (tm->tm_pkt_len - start < tot)
      break;
    crc = ((uint32_t)b[start + tot - 4] << 24) | ((uint32_t)b[start + tot - 3] << 16) |
          ((uint32_t)b[start + tot - 2] << 8) | b[start + tot - 1];
    if (t2mi_crc32(b + start, tot - 4) != crc) {
      if (!tm->tm_probing)
        tm->tm_crc_errors++;
      t2mi_t2mi_lost(tm);
      return;
    }
    t2mi_t2mi_packet(tm, b + start, pl);
    start += tot;
  }
  if (start > 0) {
    tm->tm_pkt_len -= start;
    memmove(tm->tm_pkt, tm->tm_pkt + start, tm->tm_pkt_len);
  }
}

static void
t2mi_t2mi_append ( t2mi_t *tm, const uint8_t *p, size_t len )
{
  if (tm->tm_pkt_len + len > sizeof(tm->tm_pkt)) {
    t2mi_t2mi_lost(tm);
    return;
  }
  memcpy(tm->tm_pkt + tm->tm_pkt_len, p, len);
  tm->tm_pkt_len += len;
  t2mi_t2mi_parse(tm);
}

/* the payload of a packet of the PID */
static void
t2mi_t2mi_input ( t2mi_t *tm, const uint8_t *p, size_t len, int pusi )
{
  size_t ptr;

  if (!pusi) {
    if (tm->tm_pkt_sync)
      t2mi_t2mi_append(tm, p, len);
    return;
  }
  ptr = p[0];
  p++;
  len--;
  if (ptr > len) {
    t2mi_t2mi_lost(tm);
    return;
  }
  /* the bytes before the pointer end the packet before; what is left of
   * it then (stuffing, or a packet that lost bytes) goes */
  if (tm->tm_pkt_sync)
    t2mi_t2mi_append(tm, p, ptr);
  tm->tm_pkt_len = 0;
  tm->tm_pkt_sync = 1;
  t2mi_t2mi_append(tm, p + ptr, len - ptr);
}

/* **************************************************************************
 * Plain piping
 * *************************************************************************/

static void
t2mi_pipe_input ( t2mi_t *tm, const uint8_t *p, size_t len )
{
  uint8_t *b = tm->tm_pipe;
  size_t i = 0;

  /* less than a packet is kept, or a packet whose successor is not there
   * yet: the payload fits behind it */
  memcpy(b + tm->tm_pipe_len, p, len);
  tm->tm_pipe_len += len;
  for (;;) {
    if (!tm->tm_pipe_sync) {
      /* a sync byte with the next one a packet on */
      while (i + TS_SIZE < tm->tm_pipe_len && (b[i] != 0x47 || b[i + TS_SIZE] != 0x47))
        i++;
      if (i + TS_SIZE >= tm->tm_pipe_len)
        break;
      tm->tm_pipe_sync = 1;
      if (!tm->tm_probing)
        tm->tm_pipe_resyncs++;
    }
    if (tm->tm_pipe_len - i < TS_SIZE)
      break;
    if (b[i] != 0x47) {
      tm->tm_pipe_sync = 0;
      tm->tm_probe_pipe = 0;
      continue;
    }
    if (tm->tm_probing)
      tm->tm_probe_pipe++;
    else
      t2mi_out(tm, b + i);
    i += TS_SIZE;
  }
  tm->tm_pipe_len -= i;
  memmove(b, b + i, tm->tm_pipe_len);
}

/* **************************************************************************
 * The packets of the PID
 * *************************************************************************/

static void
t2mi_reset ( t2mi_t *tm )
{
  tm->tm_cc = -1;
  tm->tm_count = -1;
  tm->tm_pkt_len = 0;
  tm->tm_pkt_sync = 0;
  tm->tm_pipe_len = 0;
  tm->tm_pipe_sync = 0;
  tm->tm_bt.bt_synced = 0;
}

static void
t2mi_input ( t2mi_t *tm, const uint8_t *ts )
{
  const uint8_t *p = ts + 4;
  size_t len = TS_SIZE - 4;
  int cc = ts[3] & 0x0f;

  if ((ts[3] & 0x10) == 0)
    return;                       /* no payload */
  if (tm->tm_cc >= 0 && cc != ((tm->tm_cc + 1) & 0x0f)) {
    if (cc == tm->tm_cc)
      return;                     /* the packet again */
    if (!tm->tm_probing)
      tm->tm_cc_errors++;
    t2mi_t2mi_lost(tm);
    tm->tm_pipe_len = 0;
    tm->tm_pipe_sync = 0;
    tm->tm_probe_pipe = 0;
  }
  tm->tm_cc = cc;
  if (ts[3] & 0x20) {
    if (ts[4] > TS_SIZE - 6)
      return;                     /* the adaptation field takes it all */
    p += 1 + ts[4];
    len -= 1 + ts[4];
  }
  if (tm->tm_format == T2MI_FORMAT_T2MI || tm->tm_probing)
    t2mi_t2mi_input(tm, p, len, ts[1] & 0x40);
  if (tm->tm_format == T2MI_FORMAT_PIPING || tm->tm_probing)
    t2mi_pipe_input(tm, p, len);
}

/* both parsers count until one finds its format, then the packets kept
 * go through that one */
static void
t2mi_probe ( t2mi_t *tm, const uint8_t *ts )
{
  int i;
  int n;

  if (tm->tm_probe == NULL) {
    tm->tm_probe = malloc(T2MI_PROBE * TS_SIZE);
    if (tm->tm_probe == NULL)
      return;
  }
  memcpy(tm->tm_probe + tm->tm_probe_n * TS_SIZE, ts, TS_SIZE);
  tm->tm_probe_n++;
  tm->tm_probing = 1;
  t2mi_input(tm, ts);
  tm->tm_probing = 0;
  if (tm->tm_probe_t2mi >= 2)
    tm->tm_format = T2MI_FORMAT_T2MI;
  else if (tm->tm_probe_pipe >= 5)
    tm->tm_format = T2MI_FORMAT_PIPING;
  else {
    /* nothing found yet: the newer half stays */
    if (tm->tm_probe_n == T2MI_PROBE) {
      tm->tm_probe_n = T2MI_PROBE / 2;
      memmove(tm->tm_probe, tm->tm_probe + tm->tm_probe_n * TS_SIZE,
              tm->tm_probe_n * TS_SIZE);
    }
    return;
  }
  t2mi_reset(tm);
  n = tm->tm_probe_n;
  for (i = 0; i < n; i++)
    t2mi_input(tm, tm->tm_probe + i * TS_SIZE);
  free(tm->tm_probe);
  tm->tm_probe = NULL;
  tm->tm_probe_n = 0;
}

void
t2mi_packet ( t2mi_t *tm, const uint8_t *ts )
{
  tm->tm_in++;
  if (tm->tm_format == T2MI_FORMAT_AUTO)
    t2mi_probe(tm, ts);
  else
    t2mi_input(tm, ts);
}

void
t2mi_init ( t2mi_t *tm, int format, int plp, bbframe_ts_cb_t cb, void *opaque )
{
  memset(tm, 0, sizeof(*tm));
  bbframe_ts_init(&tm->tm_bt);
  tm->tm_format = (format == T2MI_FORMAT_T2MI || format == T2MI_FORMAT_PIPING) ?
                  format : T2MI_FORMAT_AUTO;
  tm->tm_plp = (plp >= 0 && plp < 256) ? plp : T2MI_PLP_AUTO;
  tm->tm_cb = cb;
  tm->tm_opaque = opaque;
  t2mi_reset(tm);
}

void
t2mi_done ( t2mi_t *tm )
{
  free(tm->tm_probe);
  tm->tm_probe = NULL;
}
