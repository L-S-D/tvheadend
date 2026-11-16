/*
 *  Tvheadend - DVB-S blindscan: carriers in a spectrum
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
 * The spectrum of a satellite position, taken by the demodulator in slices,
 * is merged into one (the levels of the slices matched where they overlap)
 * and smoothed twice (Gauss, sigma 2 and 5 samples); a carrier is a peak at
 * least min_snr_db above the noise floor and as prominent, its edges where
 * the level falls 3 dB below the peak. The results of both smoothings are
 * merged, the wider of two overlapping carriers kept.
 *
 * The arithmetic follows what NumPy/SciPy do for the same steps (percentile,
 * gaussian_filter1d, find_peaks, interp), so results can be compared with an
 * implementation using them.
 */

#include "dvb_blindscan_detect.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>

#define GRID_KHZ 25.0         /* resolution the merging assumes */

void
dvb_blindscan_params_default ( dvb_blindscan_params_t *p )
{
  p->min_bw_khz  = 250;
  p->max_bw_khz  = 45000;
  p->min_snr_db  = 3;
  p->overlap_khz = 20000;
  p->extract_khz = 40000;
}

void
dvb_blindscan_spectrum_free ( dvb_blindscan_spectrum_t *s )
{
  if (s == NULL)
    return;
  free(s->freq_khz);
  free(s->level_db);
  memset(s, 0, sizeof(*s));
}

/* ------------------------------------------------------------------------
 * NumPy's arithmetic
 * ----------------------------------------------------------------------*/

/* np.sum: pairwise summation (blocks of 128, eight partial sums) */
static double
np_sum ( const double *a, size_t n )
{
  double r[8], res;
  size_t i, j;

  if (n < 8) {
    res = 0.;
    for (i = 0; i < n; i++)
      res += a[i];
    return res;
  }
  if (n <= 128) {
    for (j = 0; j < 8; j++)
      r[j] = a[j];
    for (i = 8; i < n - (n % 8); i += 8)
      for (j = 0; j < 8; j++)
        r[j] += a[i + j];
    res = ((r[0] + r[1]) + (r[2] + r[3])) + ((r[4] + r[5]) + (r[6] + r[7]));
    for (; i < n; i++)
      res += a[i];
    return res;
  }
  i = n / 2;
  i -= i % 8;
  return np_sum(a, i) + np_sum(a + i, n - i);
}

static double
np_mean ( const double *a, size_t n )
{
  return n ? np_sum(a, n) / (double)n : NAN;
}

static int
cmp_double ( const void *a, const void *b )
{
  double x = *(const double *)a, y = *(const double *)b;
  return x < y ? -1 : x > y;
}

/* np.percentile(a, q), linear */
static double
np_percentile ( const double *a, size_t n, double q )
{
  double *s, v, t, lo, hi, d;
  size_t i;

  if (n == 0 || (s = malloc(n * sizeof(double))) == NULL)
    return NAN;
  memcpy(s, a, n * sizeof(double));
  qsort(s, n, sizeof(double), cmp_double);
  /* NumPy's virtual index for "linear" (alpha = beta = 1) */
  q /= 100.0;
  v = (double)n * q + (1.0 + q * (1.0 - 1.0 - 1.0)) - 1.0;
  i = (size_t)floor(v);
  t = v - (double)i;
  lo = s[i];
  hi = s[i + 1 < n ? i + 1 : i];
  free(s);
  d = hi - lo;
  return t >= 0.5 ? hi - d * (1 - t) : lo + d * t;
}

/* np.interp(x, xp, fp): xp ascending */
static double
np_interp ( double x, const double *xp, const double *fp, size_t n )
{
  size_t lo = 0, hi = n - 1, mid;

  if (x <= xp[0])
    return fp[0];
  if (x >= xp[n - 1])
    return fp[n - 1];
  while (hi - lo > 1) {
    mid = (lo + hi) / 2;
    if (xp[mid] <= x)
      lo = mid;
    else
      hi = mid;
  }
  return (fp[hi] - fp[lo]) / (xp[hi] - xp[lo]) * (x - xp[lo]) + fp[lo];
}

/* scipy.ndimage.gaussian_filter1d(x, sigma) (truncate 4, mode "reflect") */
static void
gauss1d ( const double *x, size_t n, double sigma, double *out )
{
  int r = (int)(4.0 * sigma + 0.5), k;
  double w[2 * 64 + 1], sum, t;
  long i, a, b;

  if (r > 64)
    r = 64;
  for (k = -r; k <= r; k++)
    w[k + r] = exp(-0.5 / (sigma * sigma) * (double)(k * k));
  sum = np_sum(w, 2 * r + 1);
  for (k = 0; k <= 2 * r; k++)
    w[k] /= sum;
  for (i = 0; i < (long)n; i++) {
    t = x[i] * w[r];
    for (k = r; k >= 1; k--) {
      a = i - k;
      b = i + k;
      /* reflect at the half sample: d c b a | a b c d | d c b a */
      while (a < 0 || a >= (long)n)
        a = a < 0 ? -a - 1 : 2 * (long)n - a - 1;
      while (b < 0 || b >= (long)n)
        b = b < 0 ? -b - 1 : 2 * (long)n - b - 1;
      t += (x[a] + x[b]) * w[r - k];
    }
    out[i] = t;
  }
}

/* ------------------------------------------------------------------------
 * scipy.signal.find_peaks(x, height, prominence, distance)
 * ----------------------------------------------------------------------*/

typedef struct { double v; size_t i; } prio_t;

static int
cmp_prio ( const void *a, const void *b )
{
  const prio_t *x = a, *y = b;
  if (x->v != y->v)
    return x->v < y->v ? -1 : 1;
  return x->i < y->i ? -1 : x->i > y->i;
}

/* the indices of the peaks; their number, or -1 */
static long
find_peaks ( const double *x, size_t n, double height, double prominence,
             size_t distance, size_t **out )
{
  size_t *p, np = 0, i, ahead, k, m;
  char *keep;
  prio_t *pr;
  double lmin, rmin;
  long j;

  *out = NULL;
  if (n < 3)
    return 0;
  if ((p = malloc(n * sizeof(size_t))) == NULL)
    return -1;
  /* local maxima, the middle of a flat one */
  for (i = 1; i < n - 1; i++) {
    if (x[i - 1] < x[i]) {
      ahead = i + 1;
      while (ahead < n - 1 && x[ahead] == x[i])
        ahead++;
      if (x[ahead] < x[i]) {
        p[np++] = (i + ahead - 1) / 2;
        i = ahead;
      }
    }
  }
  /* height */
  for (i = k = 0; i < np; i++)
    if (x[p[i]] >= height)
      p[k++] = p[i];
  np = k;
  /* distance: the highest first, nearer ones away */
  if (distance > 1 && np > 1) {
    keep = malloc(np);
    pr = malloc(np * sizeof(prio_t));
    if (keep == NULL || pr == NULL) {
      free(keep); free(pr); free(p);
      return -1;
    }
    memset(keep, 1, np);
    for (i = 0; i < np; i++) {
      pr[i].v = x[p[i]];
      pr[i].i = i;
    }
    qsort(pr, np, sizeof(prio_t), cmp_prio);
    for (m = np; m-- > 0; ) {
      i = pr[m].i;
      if (!keep[i])
        continue;
      for (j = (long)i - 1; j >= 0 && p[i] - p[j] < distance; j--)
        keep[j] = 0;
      for (k = i + 1; k < np && p[k] - p[i] < distance; k++)
        keep[k] = 0;
    }
    for (i = k = 0; i < np; i++)
      if (keep[i])
        p[k++] = p[i];
    np = k;
    free(keep);
    free(pr);
  }
  /* prominence: above the higher of the lowest points either side, up to
   * a higher sample or the end */
  for (i = k = 0; i < np; i++) {
    lmin = rmin = x[p[i]];
    for (j = (long)p[i]; j >= 0 && x[j] <= x[p[i]]; j--)
      if (x[j] < lmin)
        lmin = x[j];
    for (m = p[i]; m < n && x[m] <= x[p[i]]; m++)
      if (x[m] < rmin)
        rmin = x[m];
    if (x[p[i]] - (lmin > rmin ? lmin : rmin) >= prominence)
      p[k++] = p[i];
  }
  np = k;
  *out = p;
  return (long)np;
}

/* ------------------------------------------------------------------------
 * Detection
 * ----------------------------------------------------------------------*/

static long
detect1 ( const dvb_blindscan_spectrum_t *s, const dvb_blindscan_params_t *p,
          const double *peaks_sm, const double *edge_sm, int sigma,
          dvb_blindscan_carrier_t **out )
{
  dvb_blindscan_carrier_t *c;
  size_t *pk = NULL, i, li, ri;
  long n, k, cnt = 0;
  double lvl, thr;

  *out = NULL;
  n = find_peaks(peaks_sm, s->count, s->noise_db + p->min_snr_db, p->min_snr_db, 10, &pk);
  if (n <= 0)
    return n;
  if ((c = calloc(n, sizeof(*c))) == NULL) {
    free(pk);
    return -1;
  }
  for (k = 0; k < n; k++) {
    lvl = peaks_sm[pk[k]];
    thr = lvl - 3.0;
    if (thr < s->noise_db + 3.0)
      thr = s->noise_db + 3.0;
    li = pk[k];
    for (i = pk[k]; i-- > 0; ) {
      li = i;
      if (edge_sm[i] < thr)
        break;
    }
    ri = pk[k];
    for (i = pk[k] + 1; i < s->count; i++) {
      ri = i;
      if (edge_sm[i] < thr)
        break;
    }
    c[cnt].left_khz  = s->freq_khz[li];
    c[cnt].right_khz = s->freq_khz[ri];
    c[cnt].bw_khz    = c[cnt].right_khz - c[cnt].left_khz;
    c[cnt].freq_khz  = (c[cnt].left_khz + c[cnt].right_khz) / 2.0;
    c[cnt].snr_db    = lvl - s->noise_db;
    c[cnt].peak_db   = lvl;
    c[cnt].sigma     = sigma;
    if (c[cnt].bw_khz < p->min_bw_khz || c[cnt].bw_khz > p->max_bw_khz ||
        c[cnt].snr_db < p->min_snr_db)
      continue;
    cnt++;
  }
  free(pk);
  *out = c;
  return cnt;
}

static int
overlap ( const dvb_blindscan_carrier_t *a, const dvb_blindscan_carrier_t *b )
{
  return !(a->right_khz < b->left_khz || b->right_khz < a->left_khz);
}

static int
cmp_carrier ( const void *a, const void *b )
{
  const dvb_blindscan_carrier_t *x = a, *y = b;
  return x->freq_khz < y->freq_khz ? -1 : x->freq_khz > y->freq_khz;
}

int
dvb_blindscan_detect
  ( const dvb_blindscan_spectrum_t *s, const dvb_blindscan_params_t *p,
    dvb_blindscan_carrier_t **out )
{
  dvb_blindscan_carrier_t *c2 = NULL, *c5 = NULL, *m = NULL, *t;
  double *sm2 = NULL, *sm5 = NULL;
  char *used = NULL;
  long n2, n5, i, j, k = 0, l;
  int r = -1;

  *out = NULL;
  if (s->count == 0)
    return 0;
  sm2 = malloc(s->count * sizeof(double));
  sm5 = malloc(s->count * sizeof(double));
  if (sm2 == NULL || sm5 == NULL)
    goto end;
  gauss1d(s->level_db, s->count, 2, sm2);
  gauss1d(s->level_db, s->count, 5, sm5);
  if ((n2 = detect1(s, p, sm2, sm2, 2, &c2)) < 0 ||
      (n5 = detect1(s, p, sm5, sm2, 5, &c5)) < 0)
    goto end;
  if (n2 + n5 == 0) {
    r = 0;
    goto end;
  }
  m = malloc((n2 + n5) * sizeof(*m));
  used = calloc(n5 + 1, 1);
  if (m == NULL || used == NULL)
    goto end;
  /* each carrier of sigma 2 with the first unused one of sigma 5 it
   * overlaps: the wider; then the unused ones of sigma 5 */
  for (i = 0; i < n2; i++) {
    for (j = 0; j < n5; j++)
      if (!used[j] && overlap(&c2[i], &c5[j]))
        break;
    if (j < n5) {
      m[k++] = c2[i].bw_khz > c5[j].bw_khz ? c2[i] : c5[j];
      used[j] = 1;
    } else {
      m[k++] = c2[i];
    }
  }
  for (j = 0; j < n5; j++)
    if (!used[j])
      m[k++] = c5[j];
  /* by frequency, keeping the order of equal ones */
  for (i = 1; i < k; i++)
    for (l = i; l > 0 && cmp_carrier(&m[l - 1], &m[l]) > 0; l--) {
      dvb_blindscan_carrier_t x = m[l];
      m[l] = m[l - 1];
      m[l - 1] = x;
    }
  t = m;
  m = NULL;
  *out = t;
  r = (int)k;
end:
  free(sm2);
  free(sm5);
  free(c2);
  free(c5);
  free(m);
  free(used);
  return r;
}

/* ------------------------------------------------------------------------
 * Merging the slices
 * ----------------------------------------------------------------------*/

static int
spectrum_alloc ( dvb_blindscan_spectrum_t *s, size_t n )
{
  memset(s, 0, sizeof(*s));
  s->freq_khz = malloc(n * sizeof(double));
  s->level_db = malloc(n * sizeof(double));
  if (s->freq_khz == NULL || s->level_db == NULL) {
    dvb_blindscan_spectrum_free(s);
    return -1;
  }
  s->count = n;
  return 0;
}

static int
merge_single ( const dvb_blindscan_slice_t *a, dvb_blindscan_spectrum_t *out )
{
  if (spectrum_alloc(out, a->count))
    return -1;
  memcpy(out->freq_khz, a->freq_khz, a->count * sizeof(double));
  memcpy(out->level_db, a->level_db, a->count * sizeof(double));
  out->noise_db = np_percentile(out->level_db, out->count, 10);
  return 0;
}

/* two slices of a universal LNB (low, high band): the second one's levels
 * matched to the first one in the overlap (cosine blend), or at the border */
static int
merge_standard ( const dvb_blindscan_slice_t *a, const dvb_blindscan_slice_t *b,
                 dvb_blindscan_spectrum_t *out )
{
  size_t i, k, n, pre, post0, nov;
  double corr, end_a = a->freq_khz[a->count - 1], beg_b = b->freq_khz[0], w1, *tmp;

  if (end_a > beg_b) {
    /* a: [0, pre) before the overlap, [pre, count) in it;
     * b: [0, post0) in it, [post0, count) after it */
    for (pre = 0; pre < a->count && a->freq_khz[pre] < beg_b; pre++);
    for (post0 = 0; post0 < b->count && b->freq_khz[post0] <= end_a; post0++);
    nov = a->count - pre;
    corr = np_mean(a->level_db + pre, nov) - np_mean(b->level_db, post0);
    if ((tmp = malloc(b->count * sizeof(double))) == NULL)
      return -1;
    for (i = 0; i < b->count; i++)
      tmp[i] = b->level_db[i] + corr;
    n = pre + nov + (b->count - post0);
    if (spectrum_alloc(out, n)) {
      free(tmp);
      return -1;
    }
    for (i = k = 0; i < pre; i++, k++) {
      out->freq_khz[k] = a->freq_khz[i];
      out->level_db[k] = a->level_db[i];
    }
    for (i = 0; i < nov; i++, k++) {
      w1 = 0.5 * (1 + cos(M_PI * (double)i / (double)nov));
      out->freq_khz[k] = a->freq_khz[pre + i];
      out->level_db[k] = a->level_db[pre + i] * w1 +
                         np_interp(a->freq_khz[pre + i], b->freq_khz, tmp, b->count) * (1 - w1);
    }
    for (i = post0; i < b->count; i++, k++) {
      out->freq_khz[k] = b->freq_khz[i];
      out->level_db[k] = tmp[i];
    }
    free(tmp);
  } else {
    /* the mean of the last and first 100 samples */
    size_t na = a->count < 100 ? a->count : 100, nb = b->count < 100 ? b->count : 100;
    corr = np_mean(a->level_db + a->count - na, na) - np_mean(b->level_db, nb);
    if (spectrum_alloc(out, a->count + b->count))
      return -1;
    memcpy(out->freq_khz, a->freq_khz, a->count * sizeof(double));
    memcpy(out->level_db, a->level_db, a->count * sizeof(double));
    for (i = 0; i < b->count; i++) {
      out->freq_khz[a->count + i] = b->freq_khz[i];
      out->level_db[a->count + i] = b->level_db[i] + corr;
    }
  }
  out->noise_db = np_percentile(out->level_db, out->count, 10);
  return 0;
}

/* narrow slices through a Unicable user band: each one's levels matched to
 * the one before in their overlap, the middles used, overlaps blended. The
 * grid is the spacing of the samples (25 kHz as requested; another
 * resolution of the driver must not squeeze a slice into a part of its
 * range), and a cell no slice reaches gets the level of its neighbours
 * (else it would be 0 dB: a false carrier at each gap) */
static int
merge_unicable ( const dvb_blindscan_slice_t *sl, size_t cnt,
                 const dvb_blindscan_params_t *p, dvb_blindscan_spectrum_t *out )
{
  double ov_khz = p->overlap_khz, ex_khz = p->extract_khz;
  double *corr = NULL, *lsum = NULL, *wsum = NULL, *buf = NULL;
  double width, step, os, oe, mp, mc, fstart, fend, gap, grid = GRID_KHZ;
  size_t i, j, k, c, hp, *e0 = NULL, *e1 = NULL, npts, off, len, ew;
  char *lov = NULL, *rov = NULL;
  int r = -1;

  if (sl[0].count > 1) {
    grid = (sl[0].freq_khz[sl[0].count - 1] - sl[0].freq_khz[0]) / (double)(sl[0].count - 1);
    grid = round(grid);
    if (grid < 1)
      grid = GRID_KHZ;
  }
  if (cnt >= 2) {
    step = sl[1].center_khz - sl[0].center_khz;
    width = sl[0].freq_khz[sl[0].count - 1] - sl[0].freq_khz[0];
    if (width - step < 1000) {
      ex_khz = width / 1000.0 * 1000.0;
      ov_khz = 0;
    }
  }
  corr = calloc(cnt, sizeof(double));
  e0 = calloc(cnt, sizeof(size_t));
  e1 = calloc(cnt, sizeof(size_t));
  lov = calloc(cnt, 1);
  rov = calloc(cnt, 1);
  if (corr == NULL || e0 == NULL || e1 == NULL || lov == NULL || rov == NULL)
    goto end;
  /* level corrections from the overlaps of whole slices */
  for (i = 1; i < cnt; i++) {
    const dvb_blindscan_slice_t *a = &sl[i - 1], *b = &sl[i];
    os = a->freq_khz[0] > b->freq_khz[0] ? a->freq_khz[0] : b->freq_khz[0];
    oe = a->freq_khz[a->count - 1] < b->freq_khz[b->count - 1] ?
         a->freq_khz[a->count - 1] : b->freq_khz[b->count - 1];
    if ((buf = realloc(buf, (a->count + b->count) * sizeof(double))) == NULL)
      goto end;
    for (j = k = 0; j < a->count; j++)
      if (a->freq_khz[j] >= os && a->freq_khz[j] <= oe)
        buf[k++] = a->level_db[j];
    for (j = c = 0; j < b->count; j++)
      if (b->freq_khz[j] >= os && b->freq_khz[j] <= oe)
        buf[k + c++] = b->level_db[j];
    if (k && c) {
      mp = np_mean(buf, k);
      mc = np_mean(buf + k, c);
      corr[i] = corr[i - 1] + (mp - mc);
    } else {
      corr[i] = corr[i - 1];
    }
  }
  /* the middle of each slice */
  for (i = 0; i < cnt; i++) {
    c = sl[i].count / 2;
    hp = (size_t)(ex_khz / 2 / grid);
    e0[i] = c > hp ? c - hp : 0;
    e1[i] = c + hp < sl[i].count ? c + hp : sl[i].count;
    if (e1[i] <= e0[i])
      goto end;
  }
  /* blend on the grid */
  fstart = sl[0].freq_khz[e0[0]];
  fend = sl[cnt - 1].freq_khz[e1[cnt - 1] - 1];
  npts = (size_t)((fend - fstart) / grid) + 1;
  lsum = calloc(npts, sizeof(double));
  wsum = calloc(npts, sizeof(double));
  if (lsum == NULL || wsum == NULL || spectrum_alloc(out, npts))
    goto end;
  for (i = 0; i + 1 < cnt; i++) {
    gap = sl[i + 1].freq_khz[e0[i + 1]] - sl[i].freq_khz[e1[i] - 1];
    if (gap < 0)
      rov[i] = lov[i + 1] = 1;
  }
  ew = (size_t)(ov_khz / grid / 2);
  if ((buf = realloc(buf, npts * sizeof(double))) == NULL)
    goto end;
  for (i = 0; i < cnt; i++) {
    off = (size_t)((sl[i].freq_khz[e0[i]] - fstart) / grid);
    len = e1[i] - e0[i];
    if (off + len > npts)
      len = off < npts ? npts - off : 0;
    for (j = 0; j < len; j++)
      buf[j] = 1.0;
    if (ew > 0) {
      if (lov[i])
        for (j = 0; j < ew && j < len; j++)
          buf[j] = 0.5 * (1 - cos(M_PI * (double)j / (double)ew));
      if (rov[i])
        for (j = 0; j < ew && j < len; j++)
          buf[len - 1 - j] = 0.5 * (1 - cos(M_PI * (double)j / (double)ew));
    }
    for (j = 0; j < len; j++) {
      lsum[off + j] += (sl[i].level_db[e0[i] + j] + corr[i]) * buf[j];
      wsum[off + j] += buf[j];
    }
  }
  /* np.linspace(fstart, fend, npts) */
  for (j = 0; j < npts; j++) {
    out->freq_khz[j] = npts > 1 ? (double)j * ((fend - fstart) / (double)(npts - 1)) + fstart : fstart;
    out->level_db[j] = lsum[j] / (wsum[j] > 1e-10 ? wsum[j] : 1e-10);
  }
  if (npts > 1)
    out->freq_khz[npts - 1] = fend;
  /* cells without a sample: between their neighbours */
  for (j = 0; j < npts; j = k) {
    for (; j < npts && wsum[j] > 0; j++);
    for (k = j; k < npts && wsum[k] <= 0; k++);
    if (j == npts)
      break;
    for (c = j; c < k; c++) {
      if (j == 0)
        out->level_db[c] = k < npts ? out->level_db[k] : 0;
      else if (k == npts)
        out->level_db[c] = out->level_db[j - 1];
      else
        out->level_db[c] = out->level_db[j - 1] + (out->level_db[k] - out->level_db[j - 1]) *
                           (double)(c - j + 1) / (double)(k - j + 1);
    }
  }
  out->noise_db = np_percentile(out->level_db, out->count, 10);
  r = 0;
end:
  if (r)
    dvb_blindscan_spectrum_free(out);
  free(corr); free(e0); free(e1); free(lov); free(rov);
  free(lsum); free(wsum); free(buf);
  return r;
}

static int
cmp_slice ( const void *a, const void *b )
{
  const dvb_blindscan_slice_t *x = *(dvb_blindscan_slice_t * const *)a;
  const dvb_blindscan_slice_t *y = *(dvb_blindscan_slice_t * const *)b;
  if (x->center_khz != y->center_khz)
    return x->center_khz < y->center_khz ? -1 : 1;
  return x < y ? -1 : x > y;
}

int
dvb_blindscan_merge
  ( const dvb_blindscan_slice_t *slices, size_t count, dvb_blindscan_mode_t mode,
    const dvb_blindscan_params_t *p, dvb_blindscan_spectrum_t *out )
{
  const dvb_blindscan_slice_t **o;
  dvb_blindscan_slice_t *s;
  size_t i;
  int r;

  memset(out, 0, sizeof(*out));
  if (count == 0)
    return -1;
  for (i = 0; i < count; i++)
    if (slices[i].count == 0)
      return -1;
  if (mode == DVB_BLINDSCAN_AUTO)
    mode = count == 1 ? DVB_BLINDSCAN_SINGLE :
           count == 2 ? DVB_BLINDSCAN_STANDARD : DVB_BLINDSCAN_UNICABLE;
  if (mode == DVB_BLINDSCAN_SINGLE)
    return merge_single(&slices[0], out);
  /* by their frequency */
  o = malloc(count * sizeof(*o));
  s = malloc(count * sizeof(*s));
  if (o == NULL || s == NULL) {
    free(o); free(s);
    return -1;
  }
  for (i = 0; i < count; i++)
    o[i] = &slices[i];
  qsort(o, count, sizeof(*o), cmp_slice);
  for (i = 0; i < count; i++)
    s[i] = *o[i];
  if (mode == DVB_BLINDSCAN_STANDARD && count >= 2)
    r = merge_standard(&s[0], &s[1], out);
  else
    r = merge_unicable(s, count, p, out);
  free(o);
  free(s);
  return r;
}
