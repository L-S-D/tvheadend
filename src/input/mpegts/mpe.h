/*
 *  Tvheadend - MPE (multiprotocol encapsulation, EN 301 192)
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

#ifndef __TVH_MPE_H__
#define __TVH_MPE_H__

#include <stddef.h>
#include <stdint.h>

/*
 * The datagrams (IP packets) of MPE: datagram sections (table ID 0x3E) in
 * the TS packets of a PID, put together across packets, with their CRC-32
 * checked. Plain computation: no tvheadend state, no locks.
 */

typedef struct mpe_datagram {
  uint8_t        mac[6];         /* the receiver's MAC address, MAC_address_1 first */
  uint16_t       ethertype;      /* 0x0800 IPv4, 0x86DD IPv6, else by LLC/SNAP; 0 not known */
  int            llc_snap;
  const uint8_t *data;
  size_t         len;
} mpe_datagram_t;

typedef void (*mpe_datagram_cb_t) ( void *opaque, const mpe_datagram_t *d );

typedef struct mpe_counts {
  uint64_t sections;             /* datagram sections taken */
  uint64_t datagrams;            /* given out */
  uint64_t crc_errors;           /* sections whose CRC-32 was wrong */
  uint64_t checksum;             /* sections with a checksum (not checked) */
  uint64_t scrambled;            /* sections with a scrambled payload */
  uint64_t other;                /* sections of other tables in the PID */
  uint64_t lost;                 /* sections cut by a continuity error */
} mpe_counts_t;

typedef struct mpe mpe_t;

mpe_t *mpe_create ( void );
void   mpe_destroy ( mpe_t *m );

/* len bytes of TS packets: those of pid; the datagrams to cb */
void   mpe_feed ( mpe_t *m, const uint8_t *tsb, size_t len, int pid,
                  mpe_datagram_cb_t cb, void *opaque );

/* a whole section (put together elsewhere): its datagram to cb */
void   mpe_section ( mpe_t *m, const uint8_t *s, size_t len,
                     mpe_datagram_cb_t cb, void *opaque );

const mpe_counts_t *mpe_counts ( const mpe_t *m );

/*
 * The MPE PIDs of a TS: a PID whose section starts with table ID 0x3E at a
 * payload unit start gets a decoder, as many as the TS has; their datagrams
 * to cb with the entry of their PID, whose user pointer is the caller's (its
 * state of the PID, NULL at first).
 */
typedef struct mpe_pid {
  uint16_t  pid;
  mpe_t    *mpe;
  void     *user;
} mpe_pid_t;

typedef void (*mpe_pids_cb_t) ( void *opaque, mpe_pid_t *p, const mpe_datagram_t *d );

typedef struct mpe_pids {
  int        n, size;
  mpe_pid_t *p;                  /* n of them, in the order found */
  uint16_t   idx[8192];          /* of a PID its index in p + 1, 0 none */
} mpe_pids_t;

/* len bytes of TS packets of any PID; s zeroed at first */
void mpe_pids_feed ( mpe_pids_t *s, const uint8_t *tsb, size_t len,
                     mpe_pids_cb_t cb, void *opaque );

/* the decoders go, free_user (if not NULL) gets the user pointer of each
 * PID that has one */
void mpe_pids_clear ( mpe_pids_t *s, void (*free_user)(void *user) );

#endif /* __TVH_MPE_H__ */
