/*
 *  Tvheadend - what IP packets carry
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

#ifndef __TVH_IP_STATS_H__
#define __TVH_IP_STATS_H__

#include <stddef.h>
#include <stdint.h>

/*
 * IPv4 and IPv6 packets (of GSE, MPE): multicast, broadcast and unicast;
 * of multicast UDP the groups and ports and what they carry (MPEG-TS, RTP,
 * MPEG-TS in RTP, DAB EDI); of unicast traffic the packets and bytes only,
 * no addresses: it is somebody's own.
 * Plain computation: no tvheadend state, no locks.
 */

#define IP_STATS_FLOWS 32

typedef enum {
  IP_FLOW_UDP = 0,            /* nothing of the below */
  IP_FLOW_TS,                 /* MPEG-TS packets */
  IP_FLOW_RTP,                /* RTP (its payload type) */
  IP_FLOW_RTP_TS,             /* MPEG-TS in RTP */
  IP_FLOW_EDI,                /* DAB EDI (AF or PF packets) */
} ip_flow_kind_t;

typedef struct ip_flow {
  int      v6;
  uint8_t  addr[16];          /* the multicast group */
  uint16_t port;
  uint64_t packets, bytes;    /* bytes of the UDP payloads */
  ip_flow_kind_t kind;
  int      rtp_pt;            /* -1 not RTP */
} ip_flow_t;

typedef struct ip_count {
  uint64_t packets, bytes;
} ip_count_t;

typedef struct ip_stats {
  ip_count_t v4_multicast, v4_broadcast, v4_unicast_udp, v4_unicast_tcp, v4_unicast_other;
  ip_count_t v6_multicast, v6_unicast_udp, v6_unicast_tcp, v6_unicast_other;
  ip_count_t unicast_protos[256];   /* the other unicast ones by IP protocol
                                       (IPv4 protocol, IPv6 next header) */
  int        nflows;
  ip_flow_t  flows[IP_STATS_FLOWS];
  uint64_t   flows_other;     /* packets of further groups */
} ip_stats_t;

/* an IPv4 or IPv6 packet (n bytes) */
void ip_stats_ipv4 ( ip_stats_t *s, const uint8_t *d, size_t n );
void ip_stats_ipv6 ( ip_stats_t *s, const uint8_t *d, size_t n );

/* the flows by bytes */
void ip_stats_sort ( ip_stats_t *s );

#endif /* __TVH_IP_STATS_H__ */
