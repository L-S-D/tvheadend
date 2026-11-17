/*
 *  Tvheadend - what GSE streams and IP packets carry, as messages
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

#include "tvheadend.h"
#include "gse_stats_msg.h"

#include <arpa/inet.h>

static void
ip_count_msg ( htsmsg_t *m, const char *name, const ip_count_t *s )
{
  htsmsg_t *e;

  if (s->packets == 0)
    return;
  e = htsmsg_create_map();
  htsmsg_add_s64(e, "packets", s->packets);
  htsmsg_add_s64(e, "bytes", s->bytes);
  htsmsg_add_msg(m, name, e);
}

static const char *ip_flow_kinds[] = {
  [IP_FLOW_UDP] = "udp", [IP_FLOW_TS] = "ts", [IP_FLOW_RTP] = "rtp",
  [IP_FLOW_RTP_TS] = "rtp-ts", [IP_FLOW_EDI] = "edi",
};

typedef struct ip_proto_count {
  int        proto;
  ip_count_t c;
} ip_proto_count_t;

static int
ip_proto_cmp ( const void *a, const void *b )
{
  const ip_proto_count_t *x = a, *y = b;
  return x->c.bytes == y->c.bytes ? 0 : x->c.bytes > y->c.bytes ? -1 : 1;
}

/* the IP protocols of unicast traffic besides UDP and TCP, by bytes (8 at
 * most) */
static void
ip_protos_msg ( htsmsg_t *m, const ip_stats_t *s )
{
  ip_proto_count_t p[256];
  htsmsg_t *l, *e;
  int i, n = 0;

  for (i = 0; i < 256; i++)
    if (s->unicast_protos[i].packets) {
      p[n].proto = i;
      p[n++].c = s->unicast_protos[i];
    }
  if (n == 0)
    return;
  qsort(p, n, sizeof(p[0]), ip_proto_cmp);
  l = htsmsg_create_list();
  for (i = 0; i < n && i < 8; i++) {
    e = htsmsg_create_map();
    htsmsg_add_u32(e, "proto", p[i].proto);
    htsmsg_add_s64(e, "packets", p[i].c.packets);
    htsmsg_add_s64(e, "bytes", p[i].c.bytes);
    htsmsg_add_msg(l, NULL, e);
  }
  htsmsg_add_msg(m, "unicast_protos", l);
}

void
ip_stats_msg ( htsmsg_t *m, const ip_stats_t *s )
{
  htsmsg_t *e, *l;
  char buf[INET6_ADDRSTRLEN];
  int i;

  e = htsmsg_create_map();
  ip_count_msg(e, "v4_multicast", &s->v4_multicast);
  ip_count_msg(e, "v4_broadcast", &s->v4_broadcast);
  ip_count_msg(e, "v4_unicast_udp", &s->v4_unicast_udp);
  ip_count_msg(e, "v4_unicast_tcp", &s->v4_unicast_tcp);
  ip_count_msg(e, "v4_unicast_other", &s->v4_unicast_other);
  ip_count_msg(e, "v6_multicast", &s->v6_multicast);
  ip_count_msg(e, "v6_unicast_udp", &s->v6_unicast_udp);
  ip_count_msg(e, "v6_unicast_tcp", &s->v6_unicast_tcp);
  ip_count_msg(e, "v6_unicast_other", &s->v6_unicast_other);
  ip_protos_msg(e, s);
  htsmsg_add_msg(m, "ip", e);
  l = htsmsg_create_list();
  for (i = 0; i < s->nflows; i++) {
    const ip_flow_t *f = &s->flows[i];
    e = htsmsg_create_map();
    inet_ntop(f->v6 ? AF_INET6 : AF_INET, f->addr, buf, sizeof(buf));
    htsmsg_add_str(e, "addr", buf);
    htsmsg_add_u32(e, "port", f->port);
    htsmsg_add_s64(e, "packets", f->packets);
    htsmsg_add_s64(e, "bytes", f->bytes);
    htsmsg_add_str(e, "kind", ip_flow_kinds[f->kind]);
    if (f->rtp_pt >= 0)
      htsmsg_add_u32(e, "rtp_pt", f->rtp_pt);
    htsmsg_add_msg(l, NULL, e);
  }
  htsmsg_add_msg(m, "flows", l);
  if (s->flows_other)
    htsmsg_add_s64(m, "flows_other", s->flows_other);
}

htsmsg_t *
gse_stats_msg ( const gse_stats_result_t *r )
{
  htsmsg_t *m, *l, *e;
  int i;

  if (r->frames[0] + r->frames[1] + r->frames[2] + r->frames[3] == 0)
    return NULL;
  m = htsmsg_create_map();
  l = htsmsg_create_list();
  for (i = 0; i < 4; i++)
    htsmsg_add_s64(l, NULL, r->frames[i]);
  htsmsg_add_msg(m, "frames", l);
  if (r->frames_upl) {
    htsmsg_add_s64(m, "frames_upl", r->frames_upl);
    htsmsg_add_s32(m, "upl", r->upl);
  }
  l = NULL;
  for (i = 0; i < 256; i++)
    if (r->isis[i >> 3] & (1 << (i & 7))) {
      if (l == NULL)
        l = htsmsg_create_list();
      htsmsg_add_u32(l, NULL, i);
    }
  if (l)
    htsmsg_add_msg(m, "isis", l);
  htsmsg_add_s64(m, "packets", r->gse.packets);
  htsmsg_add_s64(m, "pdus", r->gse.pdus);
  htsmsg_add_s64(m, "fragmented", r->gse.fragmented);
  htsmsg_add_s64(m, "crc_errors", r->gse.crc_errors);
  htsmsg_add_s64(m, "length_errors", r->gse.length_errors);
  htsmsg_add_s64(m, "length_off", r->gse.length_off);
  htsmsg_add_s64(m, "corrupt", r->gse.corrupt);
  htsmsg_add_s64(m, "hem_errors", r->gse.hem_errors);
  l = htsmsg_create_list();
  for (i = 0; i < r->nprotos; i++) {
    e = htsmsg_create_map();
    htsmsg_add_u32(e, "type", r->protos[i].type);
    htsmsg_add_s64(e, "pdus", r->protos[i].pdus);
    htsmsg_add_s64(e, "bytes", r->protos[i].bytes);
    htsmsg_add_msg(l, NULL, e);
  }
  htsmsg_add_msg(m, "protocols", l);
  e = htsmsg_create_map();
  htsmsg_add_s64(e, "six", r->labels[0]);
  htsmsg_add_s64(e, "three", r->labels[1]);
  htsmsg_add_s64(e, "broadcast", r->labels[2]);
  htsmsg_add_s32(e, "distinct", r->distinct_labels);
  if (r->labels_more)
    htsmsg_add_bool(e, "more", 1);
  htsmsg_add_msg(m, "labels", e);
  ip_stats_msg(m, &r->ip);
  return m;
}

/* bytes as a rate over us microseconds, else as an amount */
static void
gse_stats_amount ( char *buf, size_t len, int64_t bytes, int64_t us )
{
  double r;

  if (us > 0) {
    r = (double)bytes * 8 / us;              /* bit per us = Mbit/s */
    if (r >= 0.1)
      snprintf(buf, len, "%.1f Mbit/s", r);
    else
      snprintf(buf, len, "%.0f kbit/s", r * 1000);
  } else {
    snprintf(buf, len, "%.1f MB", (double)bytes / 1e6);
  }
}

/* the name of an IP protocol (IPv4 protocol, IPv6 next header) */
static const char *
gse_stats_ip_proto_name ( int proto )
{
  switch (proto) {
  case 1:   return "ICMP";
  case 2:   return "IGMP";
  case 4:   return "IP in IP";
  case 41:  return "IPv6 in IP";
  case 47:  return "GRE";
  case 50:  return "ESP";
  case 51:  return "AH";
  case 58:  return "ICMPv6";
  case 132: return "SCTP";
  case 253: return "protocol 253 (experimental)";
  case 254: return "protocol 254 (experimental)";
  }
  return NULL;
}

/* the name of a GSE protocol type (an EtherType from 0x0600 on, below an
 * extension header) */
static const char *
gse_stats_type_name ( uint32_t type )
{
  switch (type) {
  case 0x0800: return "IPv4";
  case 0x86dd: return "IPv6";
  case 0x0806: return "ARP";
  case 0x8100: return "VLAN";
  }
  return NULL;
}

/* the parts of total that are 1 % or more: " (name p %, ...)" */
static void
gse_stats_share ( char *buf, size_t len, size_t *n, const char *name, int64_t part,
                  int64_t total, const char **sep )
{
  if (total <= 0 || part * 100 < total)
    return;
  tvh_strlcatf(buf, len, *n, "%s%s %d %%", *sep, name, (int)(part * 100 / total));
  *sep = ", ";
}

static int64_t
gse_stats_ip_bytes ( htsmsg_t *ip, const char *const *names )
{
  htsmsg_t *e;
  int64_t sum = 0;

  for (; *names; names++)
    if ((e = htsmsg_get_map(ip, *names)) != NULL)
      sum += htsmsg_get_s64_or_default(e, "bytes", 0);
  return sum;
}

void
gse_stats_msg_text ( htsmsg_t *m, char *buf, size_t len )
{
  static const char *const multicast[] = { "v4_multicast", "v6_multicast", NULL };
  static const char *const unicast[] = {
    "v4_unicast_udp", "v4_unicast_tcp", "v4_unicast_other",
    "v6_unicast_udp", "v6_unicast_tcp", "v6_unicast_other", NULL
  };
  static const char *const broadcast[] = { "v4_broadcast", NULL };
  htsmsg_t *l, *e, *ip;
  htsmsg_field_t *f;
  int64_t us, frames = 0, pdus, bytes, all = 0, v4 = 0, mc, uc, bc;
  int kinds[ARRAY_SIZE(ip_flow_kinds)] = { 0 }, groups = 0, i;
  size_t n = 0;
  char a[32];
  const char *s;

  buf[0] = '\0';
  us = htsmsg_get_s64_or_default(m, "us", 0);
  if ((l = htsmsg_get_list(m, "frames")) != NULL)
    HTSMSG_FOREACH(f, l)
      if (htsmsg_field_get_s64(f, &bytes) == 0)
        frames += bytes;
  if (frames == 0) {
    snprintf(buf, len, "no BBFrames");
    return;
  }
  if (htsmsg_get_s64_or_default(m, "frames_upl", 0) * 2 > frames &&
      htsmsg_get_s32_or_default(m, "upl", 0) == 188 * 8) {
    snprintf(buf, len, "TS in 188 byte user packets, no GSE");
    return;
  }
  pdus = htsmsg_get_s64_or_default(m, "pdus", 0);
  if (pdus == 0) {
    snprintf(buf, len, "GSE: %" PRId64 " frames, no PDUs", frames);
    return;
  }
  if ((l = htsmsg_get_list(m, "protocols")) != NULL)
    HTSMSG_FOREACH(f, l)
      if ((e = htsmsg_field_get_map(f)) != NULL) {
        bytes = htsmsg_get_s64_or_default(e, "bytes", 0);
        all += bytes;
        if (htsmsg_get_u32_or_default(e, "type", 0) == 0x0800)
          v4 += bytes;
      }
  /* what the stream carries first (a cell of a list cuts the end): the IP
   * traffic, the other protocol types; then the PDUs and the share of IPv4 */
  if ((ip = htsmsg_get_map(m, "ip")) != NULL) {
    mc = gse_stats_ip_bytes(ip, multicast);
    uc = gse_stats_ip_bytes(ip, unicast);
    bc = gse_stats_ip_bytes(ip, broadcast);
    if ((l = htsmsg_get_list(m, "flows")) != NULL)
      HTSMSG_FOREACH(f, l)
        if ((e = htsmsg_field_get_map(f)) != NULL && (s = htsmsg_get_str(e, "kind")) != NULL) {
          groups++;
          for (i = 0; i < (int)ARRAY_SIZE(ip_flow_kinds); i++)
            if (!strcmp(s, ip_flow_kinds[i]))
              kinds[i]++;
        }
    if (mc) {
      gse_stats_amount(a, sizeof(a), mc, us);
      tvh_strlcatf(buf, len, n, "multicast %s, %d group%s%s", a, groups,
                   groups == 1 ? "" : "s",
                   htsmsg_get_s64_or_default(m, "flows_other", 0) ? " and more" : "");
      if (groups) {
        tvh_strlcatf(buf, len, n, " (");
        for (i = 0, s = ""; i < (int)ARRAY_SIZE(ip_flow_kinds); i++)
          if (kinds[i]) {
            tvh_strlcatf(buf, len, n, "%s%d %s", s, kinds[i], ip_flow_kinds[i]);
            s = ", ";
          }
        tvh_strlcatf(buf, len, n, ")");
      }
    }
    if (bc) {
      gse_stats_amount(a, sizeof(a), bc, us);
      tvh_strlcatf(buf, len, n, "%sbroadcast %s", n ? "; " : "", a);
    }
    if (uc) {
      static const char *const udp[] = { "v4_unicast_udp", "v6_unicast_udp", NULL };
      static const char *const tcp[] = { "v4_unicast_tcp", "v6_unicast_tcp", NULL };
      gse_stats_amount(a, sizeof(a), uc, us);
      tvh_strlcatf(buf, len, n, "%sunicast %s", n ? "; " : "", a);
      /* by protocol: UDP, TCP, the others (the list is by bytes) */
      s = " (";
      gse_stats_share(buf, len, &n, "UDP", gse_stats_ip_bytes(ip, udp), uc, &s);
      gse_stats_share(buf, len, &n, "TCP", gse_stats_ip_bytes(ip, tcp), uc, &s);
      if ((l = htsmsg_get_list(ip, "unicast_protos")) != NULL) {
        i = 0;
        HTSMSG_FOREACH(f, l)
          if ((e = htsmsg_field_get_map(f)) != NULL && i++ < 3) {
            int proto = htsmsg_get_u32_or_default(e, "proto", 0);
            const char *name = gse_stats_ip_proto_name(proto);
            char pbuf[24];
            if (name == NULL) {
              snprintf(pbuf, sizeof(pbuf), "protocol %d", proto);
              name = pbuf;
            }
            gse_stats_share(buf, len, &n, name, htsmsg_get_s64_or_default(e, "bytes", 0), uc, &s);
          }
      }
      if (s[0] == ',')
        tvh_strlcatf(buf, len, n, ")");
    }
  }
  /* the other protocol types (the list is by bytes) */
  if (all && v4 < all && (l = htsmsg_get_list(m, "protocols")) != NULL) {
    i = 0;
    HTSMSG_FOREACH(f, l)
      if ((e = htsmsg_field_get_map(f)) != NULL && i < 3) {
        uint32_t type = htsmsg_get_u32_or_default(e, "type", 0);
        const char *name = gse_stats_type_name(type);
        char tbuf[16];
        if (type == 0x0800)
          continue;
        if (name == NULL) {
          snprintf(tbuf, sizeof(tbuf), "type 0x%04X", type);
          name = tbuf;
        }
        s = n ? "; " : "";
        gse_stats_share(buf, len, &n, name, htsmsg_get_s64_or_default(e, "bytes", 0), all, &s);
        i++;
      }
  }
  tvh_strlcatf(buf, len, n, "%sGSE: %" PRId64 " PDUs", n ? "; " : "", pdus);
  if (all)
    tvh_strlcatf(buf, len, n, ", %d %% IPv4", (int)(v4 * 100 / all));
}
