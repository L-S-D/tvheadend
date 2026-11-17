/*
 *  Tvheadend - what the GSE streams of BBFrames carry
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

#ifndef __TVH_GSE_STATS_H__
#define __TVH_GSE_STATS_H__

#include "gse.h"
#include "ip_stats.h"

/*
 * The BBFrames of a transponder by their stream type (TS/GS), and of the
 * GSE ones (continuous, GSE-HEM) what their PDUs are: protocol types,
 * labels, and what the IPv4 and IPv6 ones carry (ip_stats.h).
 * Plain computation: no tvheadend state, no locks.
 */

#define GSE_STATS_PROTOS 16

typedef struct gse_stats_proto {
  uint16_t type;
  uint64_t pdus, bytes;
} gse_stats_proto_t;

typedef struct gse_stats_result {
  uint64_t          frames[4];      /* by TS/GS: packetized, continuous, GSE-HEM, TS */
  uint64_t          frames_upl;     /* generic ones with user packets (UPL not 0): no GSE */
  int               upl;            /* their UPL (bits), of the last one */
  uint8_t           isis[32];       /* input streams of multistream frames (bits) */
  gse_counts_t      gse;            /* of all GSE streams */
  int               nprotos;
  gse_stats_proto_t protos[GSE_STATS_PROTOS];
  uint64_t          protos_other;   /* PDUs of further protocol types */
  uint64_t          labels[3];      /* PDUs with a 6, 3 byte, broadcast label */
  int               distinct_labels;
  int               labels_more;    /* more than counted */
  ip_stats_t        ip;             /* of the IPv4 and IPv6 PDUs */
} gse_stats_result_t;

typedef struct gse_stats gse_stats_t;

gse_stats_t *gse_stats_create ( void );
void gse_stats_destroy ( gse_stats_t *gs );

/* a BBFrame (header and data field, len bytes) of input stream isi */
void gse_stats_frame ( gse_stats_t *gs, const uint8_t *frame, size_t len, int isi );

/* a PDU (the decoder of gse.c gives them) */
void gse_stats_pdu ( gse_stats_t *gs, const gse_pdu_t *pdu );

/* the PDUs that gse_stats_frame() decodes go to cb as well (NULL: not) */
void gse_stats_tap ( gse_stats_t *gs, gse_pdu_cb_t cb, void *opaque );

void gse_stats_get ( gse_stats_t *gs, gse_stats_result_t *out );

#endif /* __TVH_GSE_STATS_H__ */
