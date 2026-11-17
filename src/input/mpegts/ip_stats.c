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

#include "ip_stats.h"

#include <string.h>
#include <stdlib.h>

/* what a UDP payload is */
static ip_flow_kind_t
ip_udp_kind ( const uint8_t *d, size_t n, int *rtp_pt )
{
  size_t h;

  *rtp_pt = -1;
  if (n >= 188 && n % 188 == 0 && d[0] == 0x47 && (n == 188 || d[188] == 0x47))
    return IP_FLOW_TS;
  if (n >= 2 && (d[0] == 'A' || d[0] == 'P') && d[1] == 'F')
    return IP_FLOW_EDI;
  if (n >= 12 && (d[0] >> 6) == 2) {
    *rtp_pt = d[1] & 0x7f;
    h = 12 + 4 * (size_t)(d[0] & 0x0f);
    if ((d[0] & 0x10) && h + 4 <= n)
      h += 4 + 4 * (((size_t)d[h + 2] << 8) | d[h + 3]);
    if (h < n && (n - h) % 188 == 0 && d[h] == 0x47)
      return IP_FLOW_RTP_TS;
    return IP_FLOW_RTP;
  }
  return IP_FLOW_UDP;
}

static void
ip_flow ( ip_stats_t *s, int v6, const uint8_t *addr, uint16_t port,
          const uint8_t *payload, size_t n )
{
  ip_flow_t *f;
  size_t alen = v6 ? 16 : 4;
  int i;

  for (i = 0; i < s->nflows; i++) {
    f = &s->flows[i];
    if (f->v6 == v6 && f->port == port && !memcmp(f->addr, addr, alen))
      break;
  }
  if (i == s->nflows) {
    if (i == IP_STATS_FLOWS) {
      s->flows_other++;
      return;
    }
    s->nflows++;
    f = &s->flows[i];
    memset(f, 0, sizeof(*f));
    f->v6 = v6;
    memcpy(f->addr, addr, alen);
    f->port = port;
    f->kind = ip_udp_kind(payload, n, &f->rtp_pt);
  }
  f = &s->flows[i];
  f->packets++;
  f->bytes += n;
}

static void
ip_add ( ip_count_t *s, size_t bytes )
{
  s->packets++;
  s->bytes += bytes;
}

void
ip_stats_ipv4 ( ip_stats_t *s, const uint8_t *d, size_t n )
{
  size_t ihl, tot;
  const uint8_t *dst;

  if (n < 20 || (d[0] >> 4) != 4 || (ihl = 4 * (size_t)(d[0] & 0x0f)) < 20 || ihl > n)
    return;
  tot = ((size_t)d[2] << 8) | d[3];
  if (tot < ihl || tot > n)
    tot = n;
  dst = d + 16;
  if ((dst[0] & 0xf0) == 0xe0) {
    ip_add(&s->v4_multicast, tot);
    if (d[9] == 17 && tot >= ihl + 8)
      ip_flow(s, 0, dst, (uint16_t)((d[ihl + 2] << 8) | d[ihl + 3]), d + ihl + 8, tot - ihl - 8);
  } else if (dst[0] == 0xff && dst[1] == 0xff && dst[2] == 0xff && dst[3] == 0xff) {
    ip_add(&s->v4_broadcast, tot);
  } else if (d[9] == 17) {
    ip_add(&s->v4_unicast_udp, tot);
  } else if (d[9] == 6) {
    ip_add(&s->v4_unicast_tcp, tot);
  } else {
    ip_add(&s->v4_unicast_other, tot);
    ip_add(&s->unicast_protos[d[9]], tot);
  }
}

void
ip_stats_ipv6 ( ip_stats_t *s, const uint8_t *d, size_t n )
{
  size_t tot;

  if (n < 40 || (d[0] >> 4) != 6)
    return;
  tot = 40 + (((size_t)d[4] << 8) | d[5]);
  if (tot > n)
    tot = n;
  if (d[24] == 0xff) {
    ip_add(&s->v6_multicast, tot);
    if (d[6] == 17 && tot >= 48)
      ip_flow(s, 1, d + 24, (uint16_t)((d[42] << 8) | d[43]), d + 48, tot - 48);
  } else if (d[6] == 17) {
    ip_add(&s->v6_unicast_udp, tot);
  } else if (d[6] == 6) {
    ip_add(&s->v6_unicast_tcp, tot);
  } else {
    ip_add(&s->v6_unicast_other, tot);
    ip_add(&s->unicast_protos[d[6]], tot);
  }
}

static int
ip_cmp_flow ( const void *a, const void *b )
{
  const ip_flow_t *x = a, *y = b;
  return x->bytes == y->bytes ? 0 : x->bytes > y->bytes ? -1 : 1;
}

void
ip_stats_sort ( ip_stats_t *s )
{
  qsort(s->flows, s->nflows, sizeof(s->flows[0]), ip_cmp_flow);
}
