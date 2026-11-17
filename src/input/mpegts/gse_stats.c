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

#include "gse_stats.h"

#include <stdlib.h>
#include <string.h>

#define LABEL_SET 4096           /* distinct labels counted, at most 3/4 of it */

typedef struct label_slot {
  uint8_t used, len;
  uint8_t data[6];
} label_slot_t;

struct gse_stats {
  gse_t             *gse[256];   /* a decoder by input stream */
  gse_stats_result_t r;
  label_slot_t       labels[LABEL_SET];
};

gse_stats_t *
gse_stats_create ( void )
{
  return calloc(1, sizeof(gse_stats_t));
}

void
gse_stats_destroy ( gse_stats_t *gs )
{
  int i;

  if (gs == NULL)
    return;
  for (i = 0; i < 256; i++)
    gse_destroy(gs->gse[i]);
  free(gs);
}

static void
gs_pdu_cb ( void *opaque, const gse_pdu_t *pdu )
{
  gse_stats_pdu(opaque, pdu);
}

void
gse_stats_frame ( gse_stats_t *gs, const uint8_t *frame, size_t len, int isi )
{
  int tsgs;

  if (len < GSE_BBHEADER || isi < 0 || isi > 255)
    return;
  tsgs = (frame[0] >> 6) & 3;
  gs->r.frames[tsgs]++;
  if (!(frame[0] & 0x20))
    gs->r.isis[isi >> 3] |= 1 << (isi & 7);
  /* GSE: continuous (DVB-S2, no user packets) or GSE-HEM (DVB-S2X) */
  if (tsgs == 3)
    return;
  if (tsgs != 2 && (frame[2] | frame[3])) {
    gs->r.frames_upl++;
    gs->r.upl = (frame[2] << 8) | frame[3];
    return;
  }
  if (gs->gse[isi] == NULL && (gs->gse[isi] = gse_create()) == NULL)
    return;
  gse_bbframe(gs->gse[isi], frame, len, gs_pdu_cb, gs);
}

static void
gs_label ( gse_stats_t *gs, const gse_pdu_t *pdu )
{
  uint32_t h = 2166136261u;
  int i;
  label_slot_t *s;

  if (pdu->label_len <= 0) {
    gs->r.labels[2]++;
    return;
  }
  gs->r.labels[pdu->label_len == 6 ? 0 : 1]++;
  for (i = 0; i < pdu->label_len; i++)
    h = (h ^ pdu->label[i]) * 16777619u;
  for (i = 0; i < LABEL_SET; i++) {
    s = &gs->labels[(h + i) & (LABEL_SET - 1)];
    if (!s->used) {
      if (gs->r.distinct_labels >= LABEL_SET * 3 / 4) {
        gs->r.labels_more = 1;
        return;
      }
      s->used = 1;
      s->len = pdu->label_len;
      memcpy(s->data, pdu->label, pdu->label_len);
      gs->r.distinct_labels++;
      return;
    }
    if (s->len == pdu->label_len && !memcmp(s->data, pdu->label, pdu->label_len))
      return;
  }
}

static void
gs_proto ( gse_stats_t *gs, const gse_pdu_t *pdu )
{
  gse_stats_proto_t *p;
  int i;

  for (i = 0; i < gs->r.nprotos; i++)
    if (gs->r.protos[i].type == pdu->protocol_type)
      break;
  if (i == gs->r.nprotos) {
    if (i == GSE_STATS_PROTOS) {
      gs->r.protos_other++;
      return;
    }
    gs->r.nprotos++;
    gs->r.protos[i].type = pdu->protocol_type;
  }
  p = &gs->r.protos[i];
  p->pdus++;
  p->bytes += pdu->len;
}

void
gse_stats_pdu ( gse_stats_t *gs, const gse_pdu_t *pdu )
{
  gs_proto(gs, pdu);
  gs_label(gs, pdu);
  if (pdu->protocol_type == 0x0800)
    ip_stats_ipv4(&gs->r.ip, pdu->data, pdu->len);
  else if (pdu->protocol_type == 0x86dd)
    ip_stats_ipv6(&gs->r.ip, pdu->data, pdu->len);
}

static int
gs_cmp_proto ( const void *a, const void *b )
{
  const gse_stats_proto_t *x = a, *y = b;
  return x->bytes == y->bytes ? 0 : x->bytes > y->bytes ? -1 : 1;
}

void
gse_stats_get ( gse_stats_t *gs, gse_stats_result_t *out )
{
  const gse_counts_t *c;
  int i;

  *out = gs->r;
  memset(&out->gse, 0, sizeof(out->gse));
  for (i = 0; i < 256; i++) {
    if (gs->gse[i] == NULL)
      continue;
    c = gse_counts(gs->gse[i]);
    out->gse.frames += c->frames;
    out->gse.hem_frames += c->hem_frames;
    out->gse.packets += c->packets;
    out->gse.pdus += c->pdus;
    out->gse.fragmented += c->fragmented;
    out->gse.crc_errors += c->crc_errors;
    out->gse.length_errors += c->length_errors;
    out->gse.length_off += c->length_off;
    out->gse.corrupt += c->corrupt;
    out->gse.hem_errors += c->hem_errors;
  }
  ip_stats_sort(&out->ip);
  qsort(out->protos, out->nprotos, sizeof(out->protos[0]), gs_cmp_proto);
}
