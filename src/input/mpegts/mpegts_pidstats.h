/*
 *  Tvheadend - MPEG-TS: what the PIDs of a mux carry
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

#ifndef __TVH_MPEGTS_PIDSTATS_H__
#define __TVH_MPEGTS_PIDSTATS_H__

#include <stdint.h>

/*
 * The statistics of the PIDs of a TS: packets, scrambled ones, continuity
 * errors, and what the payloads start with - a PES (its stream id, and what
 * its elementary stream starts with) or sections (their table ids, those
 * with a valid CRC). Plain computation, fed TS packets; no tvheadend state.
 */

typedef struct mpegts_pidstats mpegts_pidstats_t;

/* what a PES elementary stream starts with */
typedef enum {
  MPEGTS_PIDSTATS_ES_NONE = 0,
  MPEGTS_PIDSTATS_ES_MPEG2V,     /* start code 0xB3, 0xB8 or 0x00 */
  MPEGTS_PIDSTATS_ES_H264,       /* NAL 9 (access unit delimiter) or 7 */
  MPEGTS_PIDSTATS_ES_HEVC,       /* NAL 35 (access unit delimiter) or 32 */
  MPEGTS_PIDSTATS_ES_MPA,        /* MPEG audio frame sync, layer 1-3 */
  MPEGTS_PIDSTATS_ES_ADTS,       /* AAC, ADTS */
  MPEGTS_PIDSTATS_ES_LATM,       /* AAC, LATM/LOAS */
  MPEGTS_PIDSTATS_ES_AC3,        /* AC-3 / E-AC-3 sync word */
  MPEGTS_PIDSTATS_ES_AC4,        /* AC-4 sync word */
  MPEGTS_PIDSTATS_ES_TELETEXT,   /* data identifier 0x10-0x1F */
  MPEGTS_PIDSTATS_ES_SUBTITLES,  /* data identifier 0x20 */
  MPEGTS_PIDSTATS_ES_OTHER,      /* none of these */
} mpegts_pidstats_es_t;

/* the BBFrames a demodulator passes in the packets of a PID (Neumo: the
 * frames of a generic stream, or of all input streams) */
typedef struct mpegts_pidstats_bb {
  uint64_t frames;               /* frames with a valid BBHEADER */
  uint64_t hem;                  /* of them in high efficiency mode */
  uint64_t dfl_bits;             /* their data fields */
  int      matype;               /* MATYPE-1 of the last one, -1 none */
  uint8_t  isis[32];             /* MATYPE-2 of multistream frames (bits) */
} mpegts_pidstats_bb_t;

typedef struct mpegts_pidstats_pid {
  uint16_t pid;
  uint64_t packets;
  uint64_t scrambled;            /* transport scrambling control set */
  uint64_t cc_errors;
  uint64_t starts;               /* unscrambled payload unit starts */
  int      stream_id;            /* PES: of the first one, else -1 */
  mpegts_pidstats_es_t es;
  int      sections;             /* payload starts that were sections */
  uint8_t  tables[32];           /* table ids of sections with a valid CRC (bits) */
  uint8_t  tables_short[32];     /* of short form sections (no CRC) */
  int      bbframes;             /* the BBFrame PID (see mpegts_pidstats_bbframes()) */
  mpegts_pidstats_bb_t bb;
} mpegts_pidstats_pid_t;

typedef struct mpegts_pidstats_total {
  uint64_t packets;              /* all, the null packets too */
  uint64_t nulls;
  uint64_t tei;                  /* transport error indicator */
  int      pids;                 /* PIDs seen (not counting the null PID) */
  int64_t  duration;             /* us from the first packet to the last */
} mpegts_pidstats_total_t;

mpegts_pidstats_t *mpegts_pidstats_create ( void );
void mpegts_pidstats_destroy ( mpegts_pidstats_t *ps );

/* the PID whose packets carry BBFrames (the Neumo driver: a 0xB8 byte
 * before the BBHEADER of a frame that starts in a packet), -1 none */
void mpegts_pidstats_bbframes ( mpegts_pidstats_t *ps, int pid );

/* each packet of the BBFrame PID also to cb (the frames, what they carry) */
typedef void (*mpegts_pidstats_bb_cb_t)(void *opaque, const uint8_t *pkt);
void mpegts_pidstats_bbsink ( mpegts_pidstats_t *ps, mpegts_pidstats_bb_cb_t cb, void *opaque );
void *mpegts_pidstats_bbsink_opaque ( mpegts_pidstats_t *ps );

/* len bytes of whole TS packets, at now (a clock in microseconds) */
void mpegts_pidstats_feed ( mpegts_pidstats_t *ps, const uint8_t *tsb, int len, int64_t now );

/* the totals, and the top PIDs by packets into out (at most n of them);
 * their number */
int mpegts_pidstats_get ( mpegts_pidstats_t *ps, mpegts_pidstats_total_t *total,
                          mpegts_pidstats_pid_t *out, int n );

#endif /* __TVH_MPEGTS_PIDSTATS_H__ */
