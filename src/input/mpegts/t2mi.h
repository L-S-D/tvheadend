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

#ifndef __TVH_T2MI_H__
#define __TVH_T2MI_H__

#include "bbframe.h"

/*
 * The payload of the packets of a PID (EN 301 192 data piping) carries
 *  - T2-MI packets (TS 102 773), the feed of DVB-T2 transmitters: the
 *    BBFrames of the PLPs of a T2 multiplex. A packet starts where the
 *    pointer field of a packet with PUSI points; its CRC-32 covers header
 *    and payload. A BBFrame packet (type 0x00) names its PLP; the TS of one
 *    PLP is passed on.
 *  - or the TS itself, its packets one after the other (plain piping).
 * The format can be detected: two T2-MI packets with a good CRC, or five TS
 * packets in a row. The packets looked at until then are passed on after.
 * No tvheadend dependency: usable offline.
 */

#define T2MI_FORMAT_AUTO    0
#define T2MI_FORMAT_T2MI    1
#define T2MI_FORMAT_PIPING  2

#define T2MI_PLP_AUTO      (-1)   /* the first PLP with a TS */

/* the largest T2-MI packet: header, payload (its length in bits: 16 bit),
 * CRC-32 */
#define T2MI_HEADER       6
#define T2MI_PACKET_MAX   (T2MI_HEADER + 8192 + 4)

/* packets of the PID the format detection keeps */
#define T2MI_PROBE        256

typedef struct t2mi {
  int       tm_format;      /* the format; AUTO: not detected yet */
  int       tm_plp;         /* the PLP passed on; AUTO: none yet */
  bbframe_ts_cb_t tm_cb;
  void     *tm_opaque;
  int       tm_cc;          /* the continuity counter of the PID, -1 */
  int       tm_probing;     /* the parsers count only */

  /* T2-MI */
  uint8_t   tm_pkt[T2MI_PACKET_MAX + 184];
  size_t    tm_pkt_len;
  int       tm_pkt_sync;    /* the start of a packet was seen */
  int       tm_count;       /* the packet_count of the packet before, -1 */
  bbframe_ts_t tm_bt;       /* the TS of the PLP passed on */
  uint32_t  tm_plps[8];     /* the PLPs of a TS seen, a bit each */
  int       tm_plp_new;     /* a PLP seen the first time (the caller clears it) */

  /* plain piping */
  uint8_t   tm_pipe[2 * 188];
  size_t    tm_pipe_len;
  int       tm_pipe_sync;

  /* the format detection */
  uint8_t  *tm_probe;
  int       tm_probe_n;
  int       tm_probe_t2mi;  /* T2-MI packets with a good CRC */
  int       tm_probe_pipe;  /* TS packets in a row */

  /* statistics */
  uint64_t  tm_in;          /* packets of the PID */
  uint64_t  tm_out;         /* TS packets passed on */
  uint64_t  tm_packets;     /* T2-MI packets */
  uint64_t  tm_other;       /* of them not BBFrames (L1, timestamps, ...) */
  uint64_t  tm_crc_errors;  /* T2-MI packets with a bad CRC */
  uint64_t  tm_cc_errors;   /* packets of the PID lost */
  uint64_t  tm_count_gaps;  /* T2-MI packets lost (packet_count) */
  uint64_t  tm_pipe_resyncs;
} t2mi_t;

void t2mi_init ( t2mi_t *tm, int format, int plp, bbframe_ts_cb_t cb, void *opaque );
void t2mi_done ( t2mi_t *tm );

/* a packet of the PID (188 bytes, not scrambled) */
void t2mi_packet ( t2mi_t *tm, const uint8_t *ts );

const char *t2mi_format_name ( int format );

#endif /* __TVH_T2MI_H__ */
