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

/*
 * A datagram section (EN 301 192, 7.1): table ID 0x3E, the section syntax
 * indicator, the section length (12 bits); MAC_address_6 and _5; payload
 * and address scrambling control, the LLC/SNAP flag; section number, last
 * section number; MAC_address_4 to _1; the datagram (with an LLC/SNAP
 * header before it if the flag is set); a CRC-32 (section syntax indicator
 * set) or a checksum. A TS packet with the payload unit start indicator
 * says by its pointer field where the first section that starts in it
 * begins; more may follow it, 0xFF is stuffing.
 */

#include "mpe.h"

#include <stdlib.h>
#include <string.h>

#define MPE_SECTION_MAX 4096
#define MPE_HEADER      12

struct mpe {
  uint32_t     crc_table[256];
  uint8_t      sec[MPE_SECTION_MAX];
  size_t       have;             /* bytes of a section begun */
  int          cc;               /* the last continuity counter, -1 none */
  mpe_counts_t counts;
};

mpe_t *
mpe_create ( void )
{
  mpe_t *m = calloc(1, sizeof(*m));
  uint32_t c;
  int i, j;

  if (m == NULL)
    return NULL;
  for (i = 0; i < 256; i++) {
    c = (uint32_t)i << 24;
    for (j = 0; j < 8; j++)
      c = (c & 0x80000000) ? (c << 1) ^ 0x04c11db7 : c << 1;
    m->crc_table[i] = c;
  }
  m->cc = -1;
  return m;
}

void
mpe_destroy ( mpe_t *m )
{
  free(m);
}

const mpe_counts_t *
mpe_counts ( const mpe_t *m )
{
  return &m->counts;
}

static uint32_t
mpe_crc ( const mpe_t *m, const uint8_t *d, size_t n )
{
  uint32_t crc = 0xffffffff;

  while (n--)
    crc = (crc << 8) ^ m->crc_table[((crc >> 24) ^ *d++) & 0xff];
  return crc;
}

void
mpe_section ( mpe_t *m, const uint8_t *s, size_t len,
              mpe_datagram_cb_t cb, void *opaque )
{
  static const uint8_t snap[6] = { 0xaa, 0xaa, 0x03, 0x00, 0x00, 0x00 };
  mpe_datagram_t d;

  if (len < 3 || s[0] != 0x3e) {
    m->counts.other++;
    return;
  }
  if (len < MPE_HEADER + 4 || len != 3 + (size_t)(((s[1] & 0x0f) << 8) | s[2]))
    return;
  m->counts.sections++;
  if (s[1] & 0x80) {
    if (mpe_crc(m, s, len) != 0) {
      m->counts.crc_errors++;
      return;
    }
  } else {
    m->counts.checksum++;
  }
  if ((s[5] >> 4) & 3) {
    m->counts.scrambled++;
    return;
  }
  d.mac[0] = s[11];
  d.mac[1] = s[10];
  d.mac[2] = s[9];
  d.mac[3] = s[8];
  d.mac[4] = s[4];
  d.mac[5] = s[3];
  d.llc_snap = (s[5] >> 1) & 1;
  d.data = s + MPE_HEADER;
  d.len = len - MPE_HEADER - 4;
  if (d.llc_snap) {
    if (d.len < 8)
      return;
    d.ethertype = memcmp(d.data, snap, 6) == 0 ? (d.data[6] << 8) | d.data[7] : 0;
    d.data += 8;
    d.len -= 8;
  } else {
    d.ethertype = d.len && (d.data[0] >> 4) == 4 ? 0x0800 :
                  d.len && (d.data[0] >> 4) == 6 ? 0x86dd : 0;
  }
  m->counts.datagrams++;
  cb(opaque, &d);
}

/* bytes of a section: its start (have 0) or more of it; the bytes taken */
static size_t
mpe_add ( mpe_t *m, const uint8_t *d, size_t n, mpe_datagram_cb_t cb, void *opaque )
{
  size_t take, len, used = 0;

  if (m->have < 3) {
    take = 3 - m->have < n ? 3 - m->have : n;
    memcpy(m->sec + m->have, d, take);
    m->have += take;
    used += take;
    if (m->have < 3)
      return used;
  }
  len = 3 + (((size_t)(m->sec[1] & 0x0f) << 8) | m->sec[2]);
  if (len > MPE_SECTION_MAX) {
    m->have = 0;
    return n;
  }
  take = len - m->have < n - used ? len - m->have : n - used;
  memcpy(m->sec + m->have, d + used, take);
  m->have += take;
  used += take;
  if (m->have == len) {
    mpe_section(m, m->sec, len, cb, opaque);
    m->have = 0;
  }
  return used;
}

void
mpe_feed ( mpe_t *m, const uint8_t *tsb, size_t len, int pid,
           mpe_datagram_cb_t cb, void *opaque )
{
  const uint8_t *t, *d;
  size_t n, ptr, used;
  int afc, cc, off;

  for (t = tsb; len >= 188; t += 188, len -= 188) {
    if (t[0] != 0x47 || (((t[1] & 0x1f) << 8) | t[2]) != pid || (t[1] & 0x80))
      continue;
    afc = (t[3] >> 4) & 3;
    if (!(afc & 1) || (t[3] & 0xc0))
      continue;                                 /* no payload, scrambled */
    cc = t[3] & 0x0f;
    if (m->cc >= 0 && cc == m->cc)
      continue;                                 /* a duplicate */
    if (m->cc >= 0 && cc != ((m->cc + 1) & 0x0f) && m->have) {
      m->counts.lost++;
      m->have = 0;
    }
    m->cc = cc;
    off = afc == 3 ? 5 + t[4] : 4;
    if (off >= 188)
      continue;
    d = t + off;
    n = 188 - off;
    if (!(t[1] & 0x40)) {
      if (m->have)
        mpe_add(m, d, n, cb, opaque);
      continue;
    }
    /* the end of the section before, then those that start here */
    ptr = d[0];
    d++;
    n--;
    if (ptr > n)
      continue;
    if (m->have) {
      used = mpe_add(m, d, ptr, cb, opaque);
      if (m->have) {                            /* shorter than it said */
        m->counts.lost++;
        m->have = 0;
      }
      (void)used;
    }
    d += ptr;
    n -= ptr;
    while (n > 0 && d[0] != 0xff) {
      used = mpe_add(m, d, n, cb, opaque);
      d += used;
      n -= used;
      if (m->have)
        break;                                  /* goes on in the next packet */
    }
  }
}

typedef struct mpe_pids_ctx {
  mpe_pids_cb_t cb;
  void         *opaque;
  mpe_pids_t   *s;
  int           i;
} mpe_pids_ctx_t;

static void
mpe_pids_datagram ( void *opaque, const mpe_datagram_t *d )
{
  mpe_pids_ctx_t *c = opaque;
  c->cb(c->opaque, &c->s->p[c->i], d);
}

/* a decoder for pid: its index in s->p; -1 without */
static int
mpe_pids_add ( mpe_pids_t *s, int pid )
{
  mpe_pid_t *p;
  int size;

  if (s->n == s->size) {
    size = s->size ? s->size * 2 : 8;
    if ((p = realloc(s->p, size * sizeof(*p))) == NULL)
      return -1;
    s->p = p;
    s->size = size;
  }
  p = &s->p[s->n];
  if ((p->mpe = mpe_create()) == NULL)
    return -1;
  p->pid = pid;
  p->user = NULL;
  s->idx[pid] = ++s->n;
  return s->n - 1;
}

void
mpe_pids_feed ( mpe_pids_t *s, const uint8_t *tsb, size_t len,
                mpe_pids_cb_t cb, void *opaque )
{
  mpe_pids_ctx_t c = { cb, opaque, s, 0 };
  const uint8_t *d;
  int pid, i, off;

  for ( ; len >= 188; tsb += 188, len -= 188) {
    if (tsb[0] != 0x47)
      continue;
    pid = ((tsb[1] & 0x1f) << 8) | tsb[2];
    i = s->idx[pid] - 1;
    if (i < 0) {
      /* a payload unit start, unscrambled, with a datagram section? */
      if (!(tsb[1] & 0x40) || (tsb[1] & 0x80) || (tsb[3] & 0xc0) || !(tsb[3] & 0x10) ||
          pid == 0x1fff)
        continue;
      off = (tsb[3] & 0x20) ? 5 + tsb[4] : 4;
      if (off >= 187)
        continue;
      d = tsb + off;
      if (1 + d[0] >= 188 - off || d[1 + d[0]] != 0x3e)
        continue;
      if ((i = mpe_pids_add(s, pid)) < 0)
        continue;
    }
    c.i = i;
    mpe_feed(s->p[i].mpe, tsb, 188, pid, mpe_pids_datagram, &c);
  }
}

void
mpe_pids_clear ( mpe_pids_t *s, void (*free_user)(void *user) )
{
  int i;

  for (i = 0; i < s->n; i++) {
    if (free_user && s->p[i].user)
      free_user(s->p[i].user);
    mpe_destroy(s->p[i].mpe);
    s->idx[s->p[i].pid] = 0;
  }
  free(s->p);
  s->p = NULL;
  s->n = s->size = 0;
}
