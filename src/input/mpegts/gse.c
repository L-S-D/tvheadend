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
 *
 *  The GSE header, the packets of a BBFrame, GSE-HEM and the putting
 *  together of fragments follow dvb-gse (https://github.com/daniestevez/dvb-gse),
 *  by Daniel Estevez, under this license:
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to permit
 *  persons to whom the Software is furnished to do so, subject to the
 *  following conditions:
 *
 *  The above copyright notice and this permission notice shall be included
 *  in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 *  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 *  NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 *  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 *  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 *  USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/*
 * A GSE packet: S(tart), E(nd), LT (label type: 6 bytes, 3 bytes,
 * broadcast, re-use) and the GSE length (12 bits, the bytes after these
 * 2); a fragment ID unless S and E; the total length if S and not E; the
 * protocol type and the label if S; then the data, of a last fragment
 * with the CRC-32 of the PDU (total length, protocol type, label, data).
 * The CRC-32 decides: some encoders put another total length (NRK on 1W:
 * 2 bytes more in each PDU, every CRC-32 right), as dvb-gse's option to
 * skip the check of the length knows.
 * S = E = 0 with LT 0 is padding: the rest of the data field.
 *
 * Continuous GSE: the packets of a BBFrame start at its data field, a
 * label re-use takes the label of the packet before in the frame.
 * GSE-HEM (TS/GS 10, DVB-S2X): packets go on from frame to frame; SYNCD
 * says where the first one that starts in a frame begins (0xFFFF: none).
 */

#include "gse.h"

#include <stdlib.h>
#include <string.h>

#define GSE_LT_6     0
#define GSE_LT_3     1
#define GSE_LT_BC    2
#define GSE_LT_REUSE 3

/* bytes a PDU may have more than its total length says (a wrong total
 * length; more is data that does not belong to it) */
#define GSE_LENGTH_SLACK 64

typedef struct gse_label {
  uint8_t data[6];
  int     len;                   /* -1: none */
} gse_label_t;

typedef struct gse_header {
  int         start, end, lt;
  int         gse_length;        /* bytes after the first two */
  int         frag_id;           /* -1 none */
  int         total_length;      /* -1 none */
  int         protocol_type;     /* -1 none */
  gse_label_t label;             /* of a start packet (re-use: the one before) */
  int         len;               /* bytes of the header in the packet */
} gse_header_t;

typedef struct gse_defrag {
  int         active;
  int         total_length;
  uint16_t    protocol_type;
  gse_label_t label;
  uint32_t    crc;
  size_t      length;            /* counted against the total length */
  uint8_t    *buf;               /* the data of the fragments */
  size_t      have, cap;
} gse_defrag_t;

struct gse {
  uint32_t     crc_table[256];
  gse_defrag_t defrag[256];
  /* GSE-HEM: the end of the frame before (a packet that goes on) */
  uint8_t     *hem;
  size_t       hem_len, hem_cap;
  gse_label_t  hem_label;
  gse_counts_t counts;
};

gse_t *
gse_create ( void )
{
  gse_t *g = calloc(1, sizeof(*g));
  uint32_t c;
  int i, j;

  if (g == NULL)
    return NULL;
  for (i = 0; i < 256; i++) {
    c = (uint32_t)i << 24;
    for (j = 0; j < 8; j++)
      c = (c & 0x80000000) ? (c << 1) ^ 0x04c11db7 : c << 1;
    g->crc_table[i] = c;
  }
  g->hem_label.len = -1;
  return g;
}

void
gse_destroy ( gse_t *g )
{
  int i;

  if (g == NULL)
    return;
  for (i = 0; i < 256; i++)
    free(g->defrag[i].buf);
  free(g->hem);
  free(g);
}

const gse_counts_t *
gse_counts ( const gse_t *g )
{
  return &g->counts;
}

static uint32_t
gse_crc ( const gse_t *g, uint32_t crc, const uint8_t *d, size_t n )
{
  while (n--)
    crc = (crc << 8) ^ g->crc_table[((crc >> 24) ^ *d++) & 0xff];
  return crc;
}

#define GSE_PADDING  -1
#define GSE_SHORT    -2
#define GSE_BAD      -3

/* the header at b (n bytes); 0, or GSE_PADDING, GSE_SHORT (the bytes end
 * in it), GSE_BAD (a label re-use without a label before) */
static int
gse_header ( const uint8_t *b, size_t n, const gse_label_t *reuse, gse_header_t *h )
{
  size_t p = 2, ll;

  if (n < 2)
    return GSE_SHORT;
  h->start = !!(b[0] & 0x80);
  h->end = !!(b[0] & 0x40);
  h->lt = (b[0] >> 4) & 3;
  if (!h->start && !h->end && h->lt == GSE_LT_6)
    return GSE_PADDING;
  h->gse_length = ((b[0] & 0x0f) << 8) | b[1];
  h->frag_id = h->total_length = h->protocol_type = -1;
  h->label.len = -1;
  if (!h->start || !h->end) {
    if (p + 1 > n)
      return GSE_SHORT;
    h->frag_id = b[p++];
  }
  if (h->start && !h->end) {
    if (p + 2 > n)
      return GSE_SHORT;
    h->total_length = (b[p] << 8) | b[p + 1];
    p += 2;
  }
  if (h->start) {
    if (p + 2 > n)
      return GSE_SHORT;
    h->protocol_type = (b[p] << 8) | b[p + 1];
    p += 2;
    if (h->lt == GSE_LT_REUSE) {
      if (reuse == NULL || reuse->len < 0)
        return GSE_BAD;
      h->label = *reuse;
    } else {
      ll = h->lt == GSE_LT_6 ? 6 : h->lt == GSE_LT_3 ? 3 : 0;
      if (p + ll > n)
        return GSE_SHORT;
      memcpy(h->label.data, b + p, ll);
      h->label.len = (int)ll;
      p += ll;
    }
  }
  h->len = (int)p;
  return 0;
}

/* the packet n bytes at b start with: 1 a whole one (*plen its length),
 * GSE_SHORT one that goes on after them, GSE_PADDING, GSE_BAD */
static int
gse_packet_at ( const uint8_t *b, size_t n, const gse_label_t *reuse,
                gse_header_t *h, size_t *plen )
{
  size_t total;
  int r;

  if ((r = gse_header(b, n, reuse, h)) != 0)
    return r;
  total = 2 + (size_t)h->gse_length;
  if (total < (size_t)h->len)
    return GSE_BAD;
  if (total > n)
    return GSE_SHORT;
  *plen = total;
  return 1;
}

/* the PDU of a fragment ID taken: its buffer for at least n more bytes */
static int
gse_defrag_room ( gse_defrag_t *d, size_t n )
{
  uint8_t *b;
  size_t cap;

  if (d->have + n <= d->cap)
    return 0;
  cap = d->cap ? d->cap : 2048;
  while (cap < d->have + n)
    cap *= 2;
  if ((b = realloc(d->buf, cap)) == NULL)
    return -1;
  d->buf = b;
  d->cap = cap;
  return 0;
}

/* a packet (header h, data d of n bytes): its PDU when complete to cb */
static void
gse_take ( gse_t *g, const gse_header_t *h, const uint8_t *d, size_t n,
           gse_pdu_cb_t cb, void *opaque )
{
  gse_defrag_t *f;
  gse_pdu_t pdu;
  uint8_t b[2];
  uint32_t crc;

  g->counts.packets++;
  if (h->start && h->end) {
    pdu.protocol_type = (uint16_t)h->protocol_type;
    memcpy(pdu.label, h->label.data, sizeof(pdu.label));
    pdu.label_len = h->label.len;
    pdu.fragmented = 0;
    pdu.data = d;
    pdu.len = n;
    g->counts.pdus++;
    cb(opaque, &pdu);
    return;
  }
  f = &g->defrag[h->frag_id];
  if (h->start) {
    /* a new PDU (one of the same ID not finished goes) */
    f->active = 1;
    f->total_length = h->total_length;
    f->protocol_type = (uint16_t)h->protocol_type;
    f->label = h->label;
    f->have = 0;
    b[0] = h->total_length >> 8;
    b[1] = h->total_length;
    f->crc = gse_crc(g, 0xffffffff, b, 2);
    b[0] = h->protocol_type >> 8;
    b[1] = h->protocol_type;
    f->crc = gse_crc(g, f->crc, b, 2);
    f->length = 2;
    if (h->lt != GSE_LT_REUSE) {
      f->crc = gse_crc(g, f->crc, h->label.data, h->label.len);
      f->length += h->label.len;
    }
  } else if (!f->active) {
    return;                                     /* its start was lost */
  }
  if (h->end) {
    f->active = 0;
    if (n < 4) {
      g->counts.length_errors++;
      return;
    }
    n -= 4;
  }
  f->crc = gse_crc(g, f->crc, d, n);
  f->length += n;
  if (f->length > (size_t)f->total_length + GSE_LENGTH_SLACK) {
    f->active = 0;
    g->counts.length_errors++;
    return;
  }
  if (gse_defrag_room(f, n)) {
    f->active = 0;
    return;
  }
  memcpy(f->buf + f->have, d, n);
  f->have += n;
  if (!h->end)
    return;
  crc = ((uint32_t)d[n] << 24) | ((uint32_t)d[n + 1] << 16) | ((uint32_t)d[n + 2] << 8) | d[n + 3];
  if (crc != f->crc) {
    g->counts.crc_errors++;
    return;
  }
  if (f->length != (size_t)f->total_length)
    g->counts.length_off++;
  pdu.protocol_type = f->protocol_type;
  memcpy(pdu.label, f->label.data, sizeof(pdu.label));
  pdu.label_len = f->label.len;
  pdu.fragmented = 1;
  pdu.data = f->buf;
  pdu.len = f->have;
  g->counts.pdus++;
  g->counts.fragmented++;
  cb(opaque, &pdu);
}

/* the packets of n bytes from b on (label: re-use, updated); the bytes
 * used up by whole packets */
static size_t
gse_packets ( gse_t *g, const uint8_t *b, size_t n, gse_label_t *label,
              int hem, gse_pdu_cb_t cb, void *opaque )
{
  gse_header_t h;
  size_t p = 0, len = 0;
  int r;

  while (p < n) {
    r = gse_packet_at(b + p, n - p, label, &h, &len);
    if (r != 1) {
      /* GSE-HEM: a packet that goes on in the next frame stays; in a
       * frame of continuous GSE it is as wrong as a bad one */
      if (r == GSE_BAD || (r == GSE_SHORT && !hem))
        g->counts.corrupt++;
      return hem && r == GSE_SHORT ? p : n;
    }
    if (h.label.len >= 0)
      *label = h.label;
    gse_take(g, &h, b + p + h.len, len - h.len, cb, opaque);
    p += len;
  }
  return p;
}

static int
gse_hem_add ( gse_t *g, const uint8_t *b, size_t n )
{
  uint8_t *nb;
  size_t cap;

  if (g->hem_len + n > g->hem_cap) {
    cap = g->hem_cap ? g->hem_cap : 8192;
    while (cap < g->hem_len + n)
      cap *= 2;
    if ((nb = realloc(g->hem, cap)) == NULL)
      return -1;
    g->hem = nb;
    g->hem_cap = cap;
  }
  memcpy(g->hem + g->hem_len, b, n);
  g->hem_len += n;
  return 0;
}

void
gse_bbframe ( gse_t *g, const uint8_t *frame, size_t len,
              gse_pdu_cb_t cb, void *opaque )
{
  const uint8_t *d = frame + GSE_BBHEADER;
  size_t n, syncd, used, plen = 0;
  gse_label_t label = { .len = -1 };
  gse_header_t h;
  int r;

  if (len < GSE_BBHEADER)
    return;
  n = len - GSE_BBHEADER;
  g->counts.frames++;
  if (((frame[0] >> 6) & 3) == 2)
    g->counts.hem_frames++;
  if (n == 0)
    return;                                     /* an empty data field */

  if (((frame[0] >> 6) & 3) != 2) {
    /* continuous GSE: the packets of the frame */
    g->hem_len = 0;
    gse_packets(g, d, n, &label, 0, cb, opaque);
    return;
  }

  /* GSE-HEM */
  syncd = ((size_t)frame[7] << 8) | frame[8];
  if (syncd == 0xffff) {
    /* no packet starts in it: all of it goes on the one before, which may
     * end with it */
    if (g->hem_len == 0)
      return;
    if (gse_hem_add(g, d, n)) {
      g->hem_len = 0;
      return;
    }
    r = gse_packet_at(g->hem, g->hem_len, &g->hem_label, &h, &plen);
    if (r == 1 && plen == g->hem_len) {
      if (h.label.len >= 0)
        g->hem_label = h.label;
      gse_take(g, &h, g->hem + h.len, plen - h.len, cb, opaque);
      g->hem_len = 0;
    } else if (r != GSE_SHORT) {
      g->counts.hem_errors++;
      g->hem_len = 0;
    }
    return;
  }
  if (syncd & 7 || syncd / 8 >= n) {
    g->counts.hem_errors++;
    g->hem_len = 0;
    return;
  }
  syncd /= 8;
  if (g->hem_len) {
    if (syncd == 0) {
      g->counts.hem_errors++;                   /* the one before is lost */
    } else if (gse_hem_add(g, d, syncd) == 0) {
      r = gse_packet_at(g->hem, g->hem_len, &g->hem_label, &h, &plen);
      if (r == 1 && plen == g->hem_len) {
        if (h.label.len >= 0)
          g->hem_label = h.label;
        gse_take(g, &h, g->hem + h.len, plen - h.len, cb, opaque);
      } else {
        g->counts.hem_errors++;
      }
    }
    g->hem_len = 0;
  } else if (syncd) {
    g->counts.hem_errors++;                     /* the start of it was lost */
  }
  used = gse_packets(g, d + syncd, n - syncd, &g->hem_label, 1, cb, opaque);
  if (syncd + used < n && gse_hem_add(g, d + syncd + used, n - syncd - used))
    g->hem_len = 0;
}
