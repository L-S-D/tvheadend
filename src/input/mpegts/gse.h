/*
 *  Tvheadend - GSE (Generic Stream Encapsulation, TS 102 606-1)
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

#ifndef __TVH_GSE_H__
#define __TVH_GSE_H__

#include <stddef.h>
#include <stdint.h>

/*
 * The PDUs (IP packets, ...) of a GSE stream, from its BBFrames: GSE packets
 * in the data field of a frame (continuous GSE), or across frames (GSE-HEM,
 * DVB-S2X), with label re-use, put together from their fragments (CRC-32).
 * Plain computation: no tvheadend state, no locks.
 */

#define GSE_BBHEADER 10

typedef struct gse_pdu {
  uint16_t       protocol_type;  /* < 0x0600: an extension header */
  uint8_t        label[6];
  int            label_len;      /* 6, 3, 0 (broadcast) */
  int            fragmented;     /* put together from fragments */
  const uint8_t *data;
  size_t         len;
} gse_pdu_t;

typedef void (*gse_pdu_cb_t) ( void *opaque, const gse_pdu_t *pdu );

typedef struct gse_counts {
  uint64_t frames;               /* BBFrames taken */
  uint64_t hem_frames;           /* of them GSE-HEM */
  uint64_t packets;              /* GSE packets */
  uint64_t pdus;                 /* PDUs given out */
  uint64_t fragmented;           /* of them put together */
  uint64_t crc_errors;           /* PDUs whose CRC-32 was wrong */
  uint64_t length_errors;        /* PDUs far longer than their total length */
  uint64_t length_off;           /* PDUs with a right CRC-32 and another total length */
  uint64_t corrupt;              /* data that was no GSE packet */
  uint64_t hem_errors;           /* GSE-HEM frames whose SYNCD did not fit */
} gse_counts_t;

typedef struct gse gse_t;

gse_t *gse_create ( void );
void   gse_destroy ( gse_t *g );

/* a BBFrame: its 10 header bytes and its data field (len bytes in all);
 * its PDUs go to cb */
void   gse_bbframe ( gse_t *g, const uint8_t *frame, size_t len,
                     gse_pdu_cb_t cb, void *opaque );

const gse_counts_t *gse_counts ( const gse_t *g );

#endif /* __TVH_GSE_H__ */
