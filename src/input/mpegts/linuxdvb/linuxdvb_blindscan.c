/*
 *  Tvheadend - Linux DVB blindscan (Neumo driver)
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
 * A blindscan of a DVB-S network takes a Neumo frontend whose satconf has
 * the network and reserves it - for a path with DiSEqC switch or plain LNB
 * the RF input alone, since voltage, tone and switch change with band and
 * polarisation; through Unicable each tuner has its own user band. It takes
 * the spectrum of each polarisation in slices: the low and the high band,
 * or narrow overlapping slices through the user band. The slices are merged
 * and the carriers found (dvb_blindscan_detect.c), the result is kept with
 * the network ("blindscan" next to its configuration).
 *
 * The thread does not take the global lock: nothing else uses the reserved
 * frontend and its satconf. Its end - giving the frontend back, keeping the
 * result - is a tasklet with the global lock.
 */

#include "tvheadend.h"
#include "linuxdvb_private.h"
#include "notify.h"
#include "settings.h"
#include "input/mpegts/dvb_blindscan_detect.h"
#include "input/mpegts/mpegts_network_scan.h"

#include <sys/ioctl.h>
#include <poll.h>
#include <linux/dvb/frontend.h>

#if ENABLE_LINUXDVB_NEUMO

#define BS_RESOLUTION 25          /* kHz between spectrum points */
#define BS_FFT_SIZE   512
#define BS_UNI_STEP   20000       /* kHz between the middles of Unicable slices */
#define BS_UNI_WIDTH  45000       /* kHz of a Unicable slice */
#define BS_BAND_OVERLAP 50000     /* kHz each band is taken into the other */
#define BS_POINTS     65536       /* asked for; the buffers have room for 4 times */
#define BS_MAX_POINTS (BS_POINTS * 4)
#define BS_CANDIDATES 512
#define BS_TIMEOUT_MS 60000       /* for the spectrum of one slice */

typedef enum {
  LB_RUNNING = 1,
  LB_DONE,
  LB_FAILED,
  LB_CANCELLED
} lb_state_t;

static const char *lb_state_str[] = { "", "running", "done", "failed", "cancelled" };

typedef struct linuxdvb_blindscan {
  LIST_ENTRY(linuxdvb_blindscan) lb_link;
  uint32_t                lb_id;               /* for the end's tasklet */
  char                    lb_network[UUID_HEX_SIZE];
  char                    lb_name[256];        /* of the frontend */
  linuxdvb_frontend_t    *lb_lfe;              /* NULL once given back */
  linuxdvb_satconf_ele_t *lb_lse;
  int                     lb_unicable;
  char                    lb_pols[3];
  uint32_t                lb_start, lb_end;    /* kHz */
  int                     lb_lock_snr;         /* -1, or lock the new carriers this far above the noise (0.01 dB) */
  pthread_t               lb_thread;
  int                     lb_joined;
  int                     lb_cancel;           /* atomic */

  /* written by the thread */
  tvh_mutex_t             lb_lock;
  lb_state_t              lb_state;
  int                     lb_slice, lb_slices;
  char                    lb_pol;
  char                    lb_error[128];
  htsmsg_t               *lb_result;
  time_t                  lb_started, lb_finished;
} linuxdvb_blindscan_t;

static LIST_HEAD(, linuxdvb_blindscan) linuxdvb_blindscans; /* global_lock */
static uint32_t linuxdvb_blindscan_id;

/* ------------------------------------------------------------------------
 * Thread
 * ----------------------------------------------------------------------*/

static void
lb_notify ( linuxdvb_blindscan_t *lb )
{
  htsmsg_t *m = htsmsg_create_map();

  tvh_mutex_lock(&lb->lb_lock);
  htsmsg_add_str(m, "uuid", lb->lb_network);
  htsmsg_add_str(m, "state", lb_state_str[lb->lb_state]);
  htsmsg_add_s32(m, "slice", lb->lb_slice);
  htsmsg_add_s32(m, "slices", lb->lb_slices);
  if (lb->lb_pol)
    htsmsg_add_str(m, "pol", (char []){ lb->lb_pol, 0 });
  if (lb->lb_error[0])
    htsmsg_add_str(m, "error", lb->lb_error);
  tvh_mutex_unlock(&lb->lb_lock);
  notify_by_msg("blindscan", m, 0, 0);
}

static void
lb_fail ( linuxdvb_blindscan_t *lb, const char *fmt, ... )
{
  va_list ap;

  tvh_mutex_lock(&lb->lb_lock);
  va_start(ap, fmt);
  vsnprintf(lb->lb_error, sizeof(lb->lb_error), fmt, ap);
  va_end(ap);
  tvh_mutex_unlock(&lb->lb_lock);
  tvhwarn(LS_LINUXDVB, "%s - blindscan: %s", lb->lb_name, lb->lb_error);
}

/* the spectrum from ifl to ifh (kHz at the demodulator): its points and
 * levels (0.001 dB); the number of points, or -1 */
static long
lb_acquire ( linuxdvb_blindscan_t *lb, int fd, uint32_t ifl, uint32_t ifh,
             uint32_t *freq, int32_t *level )
{
  struct dtv_property p[7];
  struct dtv_properties ps;
  struct dvb_frontend_event ev;
  struct pollfd pfd;
  struct spectral_peak_t *cand;
  int64_t until = getfastmonoclock() + ms2mono(BS_TIMEOUT_MS);
  int n = 0, done = 0, r;

  memset(p, 0, sizeof(p));
  p[0].cmd = DTV_CLEAR;
  ps.num = 1;
  ps.props = p;
  if (ioctl(fd, FE_SET_PROPERTY, &ps)) {
    lb_fail(lb, "DTV_CLEAR failed (%s)", strerror(errno));
    return -1;
  }
  while (ioctl(fd, FE_GET_EVENT, &ev) >= 0);
  memset(p, 0, sizeof(p));
  p[n].cmd = DTV_DELIVERY_SYSTEM;      p[n++].u.data = SYS_DVBS2;
  p[n].cmd = DTV_SCAN_START_FREQUENCY; p[n++].u.data = ifl;
  p[n].cmd = DTV_SCAN_END_FREQUENCY;   p[n++].u.data = ifh;
  p[n].cmd = DTV_SCAN_RESOLUTION;      p[n++].u.data = BS_RESOLUTION;
  p[n].cmd = DTV_SCAN_FFT_SIZE;        p[n++].u.data = BS_FFT_SIZE;
  p[n].cmd = DTV_SYMBOL_RATE;          p[n++].u.data = 2000000;
  p[n].cmd = DTV_SPECTRUM;             p[n++].u.data = SPECTRUM_METHOD_FFT;
  ps.num = n;
  ps.props = p;
  if (ioctl(fd, FE_SET_PROPERTY, &ps)) {
    lb_fail(lb, "spectrum request failed (%s)", strerror(errno));
    return -1;
  }
  /* the driver reports the end of the acquisition as an event with SYNC */
  while (!done) {
    if (atomic_get(&lb->lb_cancel))
      return -1;
    if (getfastmonoclock() > until) {
      lb_fail(lb, "no spectrum from %u to %u kHz", ifl, ifh);
      return -1;
    }
    pfd.fd = fd;
    pfd.events = POLLIN | POLLPRI;
    r = poll(&pfd, 1, 500);
    if (r <= 0)
      continue;
    while (ioctl(fd, FE_GET_EVENT, &ev) >= 0)
      if (ev.status & FE_HAS_SYNC)
        done = 1;
  }
  if ((cand = calloc(BS_CANDIDATES, sizeof(*cand))) == NULL)
    return -1;
  memset(p, 0, sizeof(p));
  p[0].cmd = DTV_SPECTRUM;
  p[0].u.spectrum.freq = freq;
  p[0].u.spectrum.rf_level = level;
  p[0].u.spectrum.num_freq = BS_POINTS;
  p[0].u.spectrum.candidates = cand;
  p[0].u.spectrum.num_candidates = BS_CANDIDATES;
  ps.num = 1;
  ps.props = p;
  r = ioctl(fd, FE_GET_PROPERTY, &ps);
  free(cand);
  if (r) {
    lb_fail(lb, "spectrum read failed (%s)", strerror(errno));
    return -1;
  }
  return MIN(p[0].u.spectrum.num_freq, BS_MAX_POINTS);
}

/* the direction of the LNB's conversion around f: a C band LNB turns it
 * round (two frequencies in the band of f) */
static int
lb_dir ( linuxdvb_satconf_ele_t *lse, uint32_t f, int vertical )
{
  int b0, b1;
  uint32_t i0 = linuxdvb_satconf_blindscan_if(lse, f, vertical, &b0);
  uint32_t i1 = linuxdvb_satconf_blindscan_if(lse, f + 1000, vertical, &b1);

  if (b0 != b1) {
    i1 = i0;
    i0 = linuxdvb_satconf_blindscan_if(lse, f - 1000, vertical, NULL);
  }
  return i1 >= i0 ? 1 : -1;
}

/* one slice from lo to hi: the RF path for ref (in the band of the slice,
 * or the middle of a Unicable slice), the spectrum, its points as
 * frequencies of the transponders; 0 or -1 */
static int
lb_slice ( linuxdvb_blindscan_t *lb, int vertical, uint32_t ref,
           uint32_t lo, uint32_t hi, dvb_blindscan_slice_t *s,
           uint32_t *fbuf, int32_t *lbuf )
{
  linuxdvb_frontend_t *lfe = lb->lb_lfe;
  uint32_t ifreq, ifl, ifh;
  int64_t a, b;
  double *f, *l;
  long n, i, k;
  int dir;

  if (linuxdvb_satconf_blindscan_path(lb->lb_lse, lfe, ref, vertical, &ifreq)) {
    lb_fail(lb, "RF path for %u kHz %c failed", ref, vertical ? 'V' : 'H');
    return -1;
  }
  /* at the demodulator: ref is at ifreq (the LNB's frequency, or the
   * Unicable user band), the slice around it */
  dir = lb_dir(lb->lb_lse, ref, vertical);
  a = (int64_t)ifreq + dir * ((int64_t)lo - ref);
  b = (int64_t)ifreq + dir * ((int64_t)hi - ref);
  if (MIN(a, b) <= 0) {
    lb_fail(lb, "no frequency at the tuner for %u to %u kHz", lo, hi);
    return -1;
  }
  ifl = MIN(a, b);
  ifh = MAX(a, b);
  n = lb_acquire(lb, lfe->lfe_fe_fd, ifl, ifh, fbuf, lbuf);
  if (n <= 0) {
    if (n == 0)
      lb_fail(lb, "empty spectrum from %u to %u kHz", ifl, ifh);
    return -1;
  }
  f = malloc(n * sizeof(double));
  l = malloc(n * sizeof(double));
  if (f == NULL || l == NULL) {
    free(f);
    free(l);
    return -1;
  }
  /* ascending in the frequencies of the transponders */
  for (i = 0; i < n; i++) {
    k = dir > 0 ? i : n - 1 - i;
    f[i] = (double)ref + (double)dir * ((double)fbuf[k] - (double)ifreq);
    l[i] = (double)lbuf[k] / 1000.0;
  }
  s->freq_khz = f;
  s->level_db = l;
  s->count = n;
  s->center_khz = lb->lb_unicable ? (double)ref : (f[0] + f[n - 1]) / 2;
  return 0;
}

typedef struct lb_band {
  uint32_t lo, hi, ref;
} lb_band_t;

/* the bands of the range, where the LNB's band changes: each taken a bit
 * into the other one, the merge matches their levels there */
static int
lb_bands ( linuxdvb_blindscan_t *lb, int vertical, lb_band_t *bands )
{
  uint32_t lo = lb->lb_start, hi = lb->lb_end, m;
  int b0, b1, bm;

  linuxdvb_satconf_blindscan_if(lb->lb_lse, lo, vertical, &b0);
  linuxdvb_satconf_blindscan_if(lb->lb_lse, hi, vertical, &b1);
  if (b0 == b1) {
    bands[0].lo = lo;
    bands[0].hi = hi;
    bands[0].ref = lo + (hi - lo) / 2;
    return 1;
  }
  /* the last frequency of the first band, the first of the other */
  while (hi - lo > 1) {
    m = lo + (hi - lo) / 2;
    linuxdvb_satconf_blindscan_if(lb->lb_lse, m, vertical, &bm);
    if (bm == b0)
      lo = m;
    else
      hi = m;
  }
  bands[0].lo = lb->lb_start;
  bands[0].hi = MIN(lb->lb_end, lo + BS_BAND_OVERLAP);
  bands[0].ref = lb->lb_start + (lo - lb->lb_start) / 2;
  bands[1].lo = MAX(lb->lb_start, hi - BS_BAND_OVERLAP);
  bands[1].hi = lb->lb_end;
  bands[1].ref = hi + (lb->lb_end - hi) / 2;
  return 2;
}

/* the number of Unicable slices of the range */
static int
lb_uni_slices ( linuxdvb_blindscan_t *lb )
{
  return (lb->lb_end - lb->lb_start + BS_UNI_STEP - 1) / BS_UNI_STEP;
}

static void
lb_free_slices ( dvb_blindscan_slice_t *s, size_t n )
{
  size_t i;

  for (i = 0; i < n; i++) {
    free((double *)s[i].freq_khz);
    free((double *)s[i].level_db);
  }
}

/* the spectrum and carriers of a polarisation, for the result */
static htsmsg_t *
lb_result_pol ( dvb_blindscan_spectrum_t *sp, dvb_blindscan_carrier_t *c, int nc )
{
  htsmsg_t *m = htsmsg_create_map(), *l = htsmsg_create_list(), *e;
  uint32_t *f = malloc(sp->count * sizeof(uint32_t));
  int16_t *lv = malloc(sp->count * sizeof(int16_t));
  double v;
  size_t i;
  int k;

  if (f && lv) {
    for (i = 0; i < sp->count; i++) {
      f[i] = (uint32_t)(sp->freq_khz[i] + 0.5);
      v = sp->level_db[i] * 100.0;
      lv[i] = (int16_t)(v < -32000 ? -32000 : v > 32000 ? 32000 : v);
    }
    htsmsg_add_bin(m, "freq", f, sp->count * sizeof(uint32_t));
    htsmsg_add_bin(m, "level", lv, sp->count * sizeof(int16_t));
  }
  free(f);
  free(lv);
  htsmsg_add_s64(m, "noise", (int64_t)(sp->noise_db * 100));
  /* the noise floor along the band (smooth: a point every 2 MHz) */
  if (sp->floor_count) {
    htsmsg_t *ff = htsmsg_create_list(), *fv = htsmsg_create_list();
    for (i = 0; i < sp->floor_count; i++) {
      v = sp->floor_db[i] * 100.0;
      htsmsg_add_u32(ff, NULL, (uint32_t)(sp->floor_freq_khz[i] + 0.5));
      htsmsg_add_s32(fv, NULL, (int32_t)(v < 0 ? v - 0.5 : v + 0.5));
    }
    htsmsg_add_msg(m, "floor_freq", ff);
    htsmsg_add_msg(m, "floor", fv);
  }
  for (k = 0; k < nc; k++) {
    e = htsmsg_create_map();
    htsmsg_add_u32(e, "freq", (uint32_t)(c[k].freq_khz + 0.5));
    htsmsg_add_u32(e, "bw", (uint32_t)(c[k].bw_khz + 0.5));
    htsmsg_add_u32(e, "sr", (uint32_t)(dvb_blindscan_symbol_rate(&c[k]) + 0.5));
    htsmsg_add_s64(e, "snr", (int64_t)(c[k].snr_db * 100));
    htsmsg_add_s64(e, "peak", (int64_t)(c[k].peak_db * 100));
    htsmsg_add_msg(l, NULL, e);
  }
  htsmsg_add_msg(m, "carriers", l);
  return m;
}

static void lb_end_cb ( void *opaque, int disarmed );
static int lb_lock_new ( struct linuxdvb_blindscan *lb );

static void *
lb_thread ( void *aux )
{
  linuxdvb_blindscan_t *lb = aux;
  dvb_blindscan_params_t prm;
  dvb_blindscan_slice_t *sl = NULL;
  dvb_blindscan_spectrum_t sp;
  dvb_blindscan_carrier_t *car = NULL;
  htsmsg_t *res = htsmsg_create_map(), *pols = htsmsg_create_map();
  uint32_t *fbuf = malloc(BS_MAX_POINTS * sizeof(uint32_t));
  int32_t *lbuf = malloc(BS_MAX_POINTS * sizeof(int32_t));
  lb_band_t bands[2];
  uint32_t c;
  size_t ns = 0, cap;
  int p, i, nb, nc, failed, total = 0, ok = 0;
  char pol;

  dvb_blindscan_params_default(&prm);
  if (fbuf == NULL || lbuf == NULL)
    goto end;

  /* the number of slices, for the progress */
  for (p = 0; lb->lb_pols[p]; p++)
    if (lb->lb_unicable)
      total += lb_uni_slices(lb);
    else
      total += lb_bands(lb, lb->lb_pols[p] == 'V', bands);
  tvh_mutex_lock(&lb->lb_lock);
  lb->lb_slices = total;
  tvh_mutex_unlock(&lb->lb_lock);

  for (p = 0; (pol = lb->lb_pols[p]) != 0; p++) {
    ns = 0;
    cap = lb->lb_unicable ? (lb->lb_end - lb->lb_start) / BS_UNI_STEP + 2 : 2;
    if ((sl = calloc(cap, sizeof(*sl))) == NULL)
      goto end;
    if (lb->lb_unicable) {
      nb = lb_uni_slices(lb);
      for (i = failed = 0; i < nb && ns < cap; i++) {
        if (atomic_get(&lb->lb_cancel))
          goto end;
        tvh_mutex_lock(&lb->lb_lock);
        lb->lb_pol = pol;
        lb->lb_slice++;
        tvh_mutex_unlock(&lb->lb_lock);
        lb_notify(lb);
        /* the middles a step apart, the last one a half step inside the
         * end: a user band reaches the transponders around it only */
        c = MIN(lb->lb_start + BS_UNI_STEP / 2 + (uint32_t)i * BS_UNI_STEP,
                lb->lb_end - BS_UNI_STEP / 2);
        if (lb_slice(lb, pol == 'V', c, c - BS_UNI_WIDTH / 2, c + BS_UNI_WIDTH / 2, &sl[ns], fbuf, lbuf)) {
          /* a slice that failed leaves a gap, the merge fills it */
          if (atomic_get(&lb->lb_cancel) || ++failed > nb / 10)
            goto end;
          continue;
        }
        ns++;
      }
      if (failed) {
        tvh_mutex_lock(&lb->lb_lock);
        lb->lb_error[0] = '\0';
        tvh_mutex_unlock(&lb->lb_lock);
        tvhwarn(LS_LINUXDVB, "%s - blindscan %c: %d of %d slices failed", lb->lb_name, pol, failed, nb);
      }
    } else {
      nb = lb_bands(lb, pol == 'V', bands);
      for (i = 0; i < nb; i++) {
        if (atomic_get(&lb->lb_cancel))
          goto end;
        tvh_mutex_lock(&lb->lb_lock);
        lb->lb_pol = pol;
        lb->lb_slice++;
        tvh_mutex_unlock(&lb->lb_lock);
        lb_notify(lb);
        if (lb_slice(lb, pol == 'V', bands[i].ref, bands[i].lo, bands[i].hi, &sl[ns], fbuf, lbuf))
          goto end;
        ns++;
      }
    }
    if (dvb_blindscan_merge(sl, ns, lb->lb_unicable ? DVB_BLINDSCAN_UNICABLE : DVB_BLINDSCAN_AUTO,
                            &prm, &sp)) {
      lb_fail(lb, "merging the spectrum failed");
      goto end;
    }
    nc = dvb_blindscan_detect(&sp, &prm, &car);
    if (nc >= 0) {
      htsmsg_t *pm = lb_result_pol(&sp, car, nc);
      double flo = sp.noise_db, fhi = sp.noise_db;
      size_t q;
      for (q = 0; q < sp.floor_count; q++) {
        if (q == 0 || sp.floor_db[q] < flo) flo = sp.floor_db[q];
        if (q == 0 || sp.floor_db[q] > fhi) fhi = sp.floor_db[q];
      }
      htsmsg_add_s64(pm, "time", gclk());
      htsmsg_add_msg(pols, (char []){ pol, 0 }, pm);
      tvhinfo(LS_LINUXDVB, "%s - blindscan %c: %d carrier(s), noise floor %.1f to %.1f dB, %zu points",
              lb->lb_name, pol, nc, flo, fhi, sp.count);
    }
    free(car);
    car = NULL;
    dvb_blindscan_spectrum_free(&sp);
    lb_free_slices(sl, ns);
    free(sl);
    sl = NULL;
  }
  ok = 1;

end:
  if (sl) {
    lb_free_slices(sl, ns);
    free(sl);
  }
  free(fbuf);
  free(lbuf);
  htsmsg_add_s64(res, "time", gclk());
  htsmsg_add_str(res, "frontend", lb->lb_name);
  htsmsg_add_msg(res, "pols", pols);
  tvh_mutex_lock(&lb->lb_lock);
  lb->lb_state = ok ? LB_DONE : atomic_get(&lb->lb_cancel) ? LB_CANCELLED : LB_FAILED;
  lb->lb_finished = gclk();
  if (ok)
    lb->lb_result = res;
  else
    htsmsg_destroy(res);
  tvh_mutex_unlock(&lb->lb_lock);
  /* the end with the global lock: by its id, the blindscan may be gone */
  tasklet_arm_alloc(lb_end_cb, (void *)(intptr_t)lb->lb_id);
  return NULL;
}

/* ------------------------------------------------------------------------
 * Start, end (global_lock)
 * ----------------------------------------------------------------------*/

static linuxdvb_blindscan_t *
lb_find_id ( uint32_t id )
{
  linuxdvb_blindscan_t *lb;

  LIST_FOREACH(lb, &linuxdvb_blindscans, lb_link)
    if (lb->lb_id == id)
      return lb;
  return NULL;
}

static linuxdvb_blindscan_t *
lb_find ( const char *uuid )
{
  linuxdvb_blindscan_t *lb;

  LIST_FOREACH(lb, &linuxdvb_blindscans, lb_link)
    if (!strcmp(lb->lb_network, uuid))
      return lb;
  return NULL;
}

/* the frontend back to tuning, the thread joined */
static void
lb_release ( linuxdvb_blindscan_t *lb )
{
  linuxdvb_frontend_t *lfe = lb->lb_lfe;

  if (!lb->lb_joined) {
    pthread_join(lb->lb_thread, NULL);
    lb->lb_joined = 1;
  }
  if (lfe) {
    lfe->lfe_blindscan = NULL;
    lfe->lfe_blindscan_rf = 0;
    linuxdvb_frontend_blindscan_close(lfe, !lb->lb_unicable);
    lb->lb_lfe = NULL;
  }
}

static void
lb_destroy ( linuxdvb_blindscan_t *lb )
{
  atomic_set(&lb->lb_cancel, 1);
  lb_release(lb);
  LIST_REMOVE(lb, lb_link);
  htsmsg_destroy(lb->lb_result);
  tvh_mutex_destroy(&lb->lb_lock);
  free(lb);
}

/* the spectrum of a result kept with the network: hts_settings_save()
 * writes JSON, binary fields go as base64 text (to_text), and back */
static void
lb_result_convert ( htsmsg_t *res, int to_text )
{
  static const char *names[] = { "freq", "level" };
  htsmsg_t *pols, *p;
  htsmsg_field_t *f;
  const void *bin;
  const char *s;
  size_t len, i;
  char *txt;
  uint8_t *buf;
  int n;

  if ((pols = htsmsg_get_map(res, "pols")) == NULL)
    return;
  HTSMSG_FOREACH(f, pols) {
    if ((p = htsmsg_field_get_map(f)) == NULL)
      continue;
    for (i = 0; i < ARRAY_SIZE(names); i++) {
      if (to_text && !htsmsg_get_bin(p, names[i], &bin, &len)) {
        if ((txt = malloc(BASE64_SIZE(len))) == NULL)
          continue;
        base64_encode(txt, BASE64_SIZE(len), bin, len);
        htsmsg_delete_field(p, names[i]);
        htsmsg_add_str(p, names[i], txt);
        free(txt);
      } else if (!to_text && (s = htsmsg_get_str(p, names[i])) != NULL) {
        len = strlen(s) / 4 * 3 + 3;
        if ((buf = malloc(len)) == NULL)
          continue;
        n = base64_decode(buf, s, len);
        htsmsg_delete_field(p, names[i]);
        if (n > 0)
          htsmsg_add_bin(p, names[i], buf, n);
        free(buf);
      }
    }
  }
}

static void
lb_end_cb ( void *opaque, int disarmed )
{
  linuxdvb_blindscan_t *lb;

  if (disarmed)
    return;
  tvh_mutex_lock(&global_lock);
  if ((lb = lb_find_id((intptr_t)opaque)) != NULL) {
    lb_release(lb);
    tvh_mutex_lock(&lb->lb_lock);
    if (lb->lb_result) {
      htsmsg_t *save, *old, *pols, *oldpols, *p;
      htsmsg_field_t *f;
      /* a polarisation this run did not take stays as it was */
      if ((old = hts_settings_load("input/dvb/networks/%s/blindscan", lb->lb_network)) != NULL) {
        lb_result_convert(old, 0);
        if ((oldpols = htsmsg_get_map(old, "pols")) != NULL &&
            (pols = htsmsg_get_map(lb->lb_result, "pols")) != NULL)
          HTSMSG_FOREACH(f, oldpols)
            if ((p = htsmsg_field_get_map(f)) != NULL && !htsmsg_get_map(pols, htsmsg_field_name(f)))
              htsmsg_add_msg(pols, htsmsg_field_name(f), htsmsg_copy(p));
        htsmsg_destroy(old);
      }
      save = htsmsg_copy(lb->lb_result);
      lb_result_convert(save, 1);
      hts_settings_save(save, "input/dvb/networks/%s/blindscan", lb->lb_network);
      htsmsg_destroy(save);
    }
    tvh_mutex_unlock(&lb->lb_lock);
    tvhinfo(LS_LINUXDVB, "%s - blindscan %s", lb->lb_name, lb_state_str[lb->lb_state]);
    lb_notify(lb);
    if (lb->lb_state == LB_DONE && lb->lb_lock_snr >= 0)
      tvhinfo(LS_LINUXDVB, "%s - blindscan: %d new carrier(s) to lock", lb->lb_name, lb_lock_new(lb));
  }
  tvh_mutex_unlock(&global_lock);
}

/* lfe and lfe2 share the RF input, or the demod */
static int
lb_rf_shared ( linuxdvb_frontend_t *lfe, linuxdvb_frontend_t *lfe2 )
{
  return linuxdvb_frontend_same_rf_input(lfe, lfe2) ||
         !strcmp(lfe->lfe_fe_path, lfe2->lfe_fe_path);
}

/* in use: tuned, or its device kept by a tuning */
static int
lb_busy ( linuxdvb_frontend_t *lfe )
{
  return lfe->lfe_refcount > 0 || LIST_FIRST(&lfe->mi_mux_active) != NULL ||
         lfe->lfe_blindscan != NULL || lfe->lfe_in_setup;
}

/* a free Neumo DVB-S frontend with the network in its satconf: for a
 * DiSEqC/LNB path one whose RF input no other tuner uses */
static linuxdvb_frontend_t *
lb_frontend ( mpegts_network_t *mn, linuxdvb_satconf_ele_t **lsep, const char **err )
{
  tvh_hardware_t *th, *th2;
  linuxdvb_adapter_t *la, *la2;
  linuxdvb_frontend_t *lfe, *lfe2;
  linuxdvb_satconf_ele_t *lse;
  int found = 0, shared;

  LIST_FOREACH(th, &tvh_hardware, th_link) {
    if (!idnode_is_instance(&th->th_id, &linuxdvb_adapter_class)) continue;
    la = (linuxdvb_adapter_t*)th;
    LIST_FOREACH(lfe, &la->la_frontends, lfe_link) {
      if (lfe->lfe_type != DVB_TYPE_S || !lfe->lfe_neumo_supported || !lfe->mi_enabled ||
          lfe->lfe_satconf == NULL)
        continue;
      TAILQ_FOREACH(lse, &lfe->lfe_satconf->ls_elements, lse_link)
        if (lse->lse_enabled && idnode_set_exists(lse->lse_networks, &mn->mn_id))
          break;
      if (lse == NULL || lse->lse_rotor)
        continue;
      found = 1;
      if (lb_busy(lfe))
        continue;
      shared = 0;
      if (lse->lse_en50494 == NULL) {
        LIST_FOREACH(th2, &tvh_hardware, th_link) {
          if (!idnode_is_instance(&th2->th_id, &linuxdvb_adapter_class)) continue;
          la2 = (linuxdvb_adapter_t*)th2;
          LIST_FOREACH(lfe2, &la2->la_frontends, lfe_link)
            if (lfe2 != lfe && lb_rf_shared(lfe, lfe2) && lb_busy(lfe2))
              shared = 1;
        }
      }
      if (shared)
        continue;
      *lsep = lse;
      return lfe;
    }
  }
  *err = found ? "all tuners for this network are busy (or their RF input is)"
               : "no Neumo DVB-S tuner has this network";
  return NULL;
}

int
linuxdvb_blindscan_start
  ( mpegts_network_t *mn, const char *pols, uint32_t start, uint32_t end, int lock_snr,
    const char **err )
{
  linuxdvb_blindscan_t *lb;
  linuxdvb_frontend_t *lfe;
  linuxdvb_satconf_ele_t *lse = NULL;
  char uuid[UUID_HEX_SIZE];
  int i;

  lock_assert(&global_lock);
  *err = NULL;
  idnode_uuid_as_str(&mn->mn_id, uuid);
  if ((lb = lb_find(uuid)) != NULL) {
    if (lb->lb_lfe != NULL) {
      *err = "a blindscan of this network runs";
      return EBUSY;
    }
    lb_destroy(lb);
  }
  if ((lfe = lb_frontend(mn, &lse, err)) == NULL)
    return EBUSY;
  if (linuxdvb_frontend_blindscan_open(lfe)) {
    *err = "the tuner's device does not open";
    return EIO;
  }
  lb = calloc(1, sizeof(*lb));
  lb->lb_id = ++linuxdvb_blindscan_id;
  strlcpy(lb->lb_network, uuid, sizeof(lb->lb_network));
  lfe->mi_display_name((mpegts_input_t *)lfe, lb->lb_name, sizeof(lb->lb_name));
  lb->lb_lfe = lfe;
  lb->lb_lse = lse;
  lb->lb_unicable = lse->lse_en50494 != NULL;
  for (i = 0; pols && *pols && i < 2; pols++)
    if ((*pols == 'H' || *pols == 'V') && !strchr(lb->lb_pols, *pols))
      lb->lb_pols[i++] = *pols;
  if (i == 0)
    strcpy(lb->lb_pols, "HV");
  lb->lb_lock_snr = lock_snr;
  lb->lb_start = start ?: 10700000;
  lb->lb_end = end > lb->lb_start ? end : 12750000;
  tvh_mutex_init(&lb->lb_lock, NULL);
  lb->lb_state = LB_RUNNING;
  lb->lb_started = gclk();
  LIST_INSERT_HEAD(&linuxdvb_blindscans, lb, lb_link);
  /* reserved: no tuning, the device stays open; a DiSEqC path keeps the RF
   * input, idle tuners holding it give it up */
  lfe->lfe_blindscan = lb;
  lfe->lfe_blindscan_rf = !lb->lb_unicable;
  if (!lb->lb_unicable)
    linuxdvb_frontend_rf_input_release(lfe, 1);
  tvhinfo(LS_LINUXDVB, "%s - blindscan of %s (%s) from %u to %u kHz%s",
          lb->lb_name, mn->mn_network_name ?: uuid, lb->lb_pols, lb->lb_start, lb->lb_end,
          lb->lb_unicable ? " through Unicable" : "");
  tvh_thread_create(&lb->lb_thread, NULL, lb_thread, lb, "blindscan");
  lb_notify(lb);
  return 0;
}

void
linuxdvb_blindscan_stop ( mpegts_network_t *mn )
{
  linuxdvb_blindscan_t *lb;
  char uuid[UUID_HEX_SIZE];

  lock_assert(&global_lock);
  if ((lb = lb_find(idnode_uuid_as_str(&mn->mn_id, uuid))) != NULL)
    atomic_set(&lb->lb_cancel, 1);
}

void
linuxdvb_blindscan_frontend_gone ( linuxdvb_frontend_t *lfe )
{
  linuxdvb_blindscan_t *lb = lfe->lfe_blindscan;

  lock_assert(&global_lock);
  if (lb)
    lb_destroy(lb);
}

void
linuxdvb_blindscan_done ( void )
{
  linuxdvb_blindscan_t *lb;

  tvh_mutex_lock(&global_lock);
  while ((lb = LIST_FIRST(&linuxdvb_blindscans)) != NULL)
    lb_destroy(lb);
  tvh_mutex_unlock(&global_lock);
}

/* ------------------------------------------------------------------------
 * A tune at a frequency (global_lock)
 * ----------------------------------------------------------------------*/

/* the mux of the network at freq: the transponder (not one of its
 * sub-muxes) whose band freq is in (its symbol rate, rolloff 0.35), 1 MHz
 * at least - two carriers of one satellite and polarisation do not overlap */
static dvb_mux_t *
lb_mux_at ( mpegts_network_t *mn, uint32_t freq, int pol )
{
  mpegts_mux_t *mm;
  dvb_mux_t *lm, *best = NULL;
  uint32_t d, tol, bestd = UINT32_MAX;

  LIST_FOREACH(mm, &mn->mn_muxes, mm_network_link) {
    if (!idnode_is_instance(&mm->mm_id, &dvb_mux_class) || mpegts_mux_is_submux(mm))
      continue;
    lm = (dvb_mux_t *)mm;
    if (lm->lm_tuning.dmc_fe_type != DVB_TYPE_S ||
        lm->lm_tuning.u.dmc_fe_qpsk.polarisation != pol)
      continue;
    d = deltaU32(lm->lm_tuning.dmc_fe_freq, freq);
    tol = MAX(1000, (uint32_t)((uint64_t)lm->lm_tuning.u.dmc_fe_qpsk.symbol_rate * 27 / 40000));
    if (d <= tol && d < bestd) {
      best = lm;
      bestd = d;
    }
  }
  return best;
}

int
linuxdvb_blindscan_tune
  ( mpegts_network_t *mn, uint32_t freq, const char *pols, uint32_t sr,
    uint32_t range, const char **err, char *muxuuid )
{
  dvb_mux_t *lm;
  mpegts_mux_t *mm;
  htsmsg_t *conf, *m;
  char ubuf[UUID_HEX_SIZE];
  int pol, flags = DVB_MUX_BLINDSCAN_TUNE;

  lock_assert(&global_lock);
  *err = NULL;
  if (!idnode_is_instance(&mn->mn_id, &dvb_network_dvbs_class)) {
    *err = "not a DVB-S network";
    return EINVAL;
  }
  if (pols == NULL || (pols[0] != 'H' && pols[0] != 'V') || pols[1] ||
      freq < 1000000 || freq > 30000000 || sr < 100000 || sr > 100000000) {
    *err = "frequency (kHz), polarisation (H or V) and symbol rate (symbols/s) needed";
    return EINVAL;
  }
  pol = pols[0] == 'V' ? DVB_POLARISATION_VERTICAL : DVB_POLARISATION_HORIZONTAL;
  if (range == 0)
    range = MIN(20000, MAX(4000, sr / 1000 * 2));
  if ((lm = lb_mux_at(mn, freq, pol)) != NULL) {
    mm = (mpegts_mux_t *)lm;
    if (mm->mm_enabled != MM_ENABLE) {
      *err = mm->mm_enabled == MM_IGNORE ? "the mux there is ignored" : "the mux there is disabled";
      return EINVAL;
    }
    /* searched around the frequency asked for */
    tvhinfo(LS_LINUXDVB, "blindscan: tune %s blind around %u kHz", mm->mm_nicename, freq);
  } else {
    conf = htsmsg_create_map();
    htsmsg_add_str(conf, "delsys", "DVB-S2");
    htsmsg_add_u32(conf, "frequency", freq);
    htsmsg_add_u32(conf, "symbolrate", sr);
    htsmsg_add_str(conf, "polarisation", pols);
    htsmsg_add_str(conf, "modulation", "AUTO");
    htsmsg_add_str(conf, "fec", "AUTO");
    mm = mn->mn_mux_create2(mn, conf);
    htsmsg_destroy(conf);
    if (mm == NULL) {
      *err = "the mux cannot be created";
      return EIO;
    }
    lm = (dvb_mux_t *)mm;
    flags |= DVB_MUX_BLINDSCAN_NEW;
    tvhinfo(LS_LINUXDVB, "blindscan: new mux %s, tuned blind", mm->mm_nicename);
  }
  lm->lm_blindscan_tune = flags;
  lm->lm_blindscan_range = range;
  mpegts_network_scan_queue_add(mm, SUBSCRIPTION_PRIO_SCAN_USER, SUBSCRIPTION_USERSCAN, 0);
  idnode_uuid_as_str(&mm->mm_id, muxuuid);
  m = htsmsg_create_map();
  htsmsg_add_str(m, "uuid", idnode_uuid_as_str(&mn->mn_id, ubuf));
  htsmsg_add_str(m, "mux", muxuuid);
  htsmsg_add_str(m, "state", "tuning");
  htsmsg_add_u32(m, "freq", freq);
  htsmsg_add_str(m, "pol", pols);
  htsmsg_add_u32(m, "sr", sr);
  notify_by_msg("blindscan", m, 0, 0);
  return 0;
}

/* after a blindscan: a tune at each carrier it found (in the polarisations
 * it took, not those kept from earlier runs) where the network has no
 * transponder, at least lock_snr above the noise floor; how many */
static int
lb_lock_new ( linuxdvb_blindscan_t *lb )
{
  mpegts_network_t *mn = mpegts_network_find(lb->lb_network);
  htsmsg_t *pols, *carriers, *c;
  htsmsg_field_t *f, *g;
  const char *pol, *err;
  char muxuuid[UUID_HEX_SIZE];
  uint32_t freq, sr;
  int64_t snr;
  int n = 0;

  if (mn == NULL || lb->lb_result == NULL || (pols = htsmsg_get_map(lb->lb_result, "pols")) == NULL)
    return 0;
  HTSMSG_FOREACH(f, pols) {
    pol = htsmsg_field_name(f);
    if (!pol[0] || pol[1] || !strchr(lb->lb_pols, pol[0]) ||
        (c = htsmsg_field_get_map(f)) == NULL || (carriers = htsmsg_get_list(c, "carriers")) == NULL)
      continue;
    HTSMSG_FOREACH(g, carriers) {
      if ((c = htsmsg_field_get_map(g)) == NULL ||
          htsmsg_get_u32(c, "freq", &freq) || htsmsg_get_u32(c, "sr", &sr) ||
          htsmsg_get_s64(c, "snr", &snr) || snr < lb->lb_lock_snr)
        continue;
      if (lb_mux_at(mn, freq, pol[0] == 'V' ? DVB_POLARISATION_VERTICAL
                                            : DVB_POLARISATION_HORIZONTAL))
        continue;
      if (linuxdvb_blindscan_tune(mn, freq, pol, sr, 0, &err, muxuuid) == 0)
        n++;
    }
  }
  return n;
}

/* ------------------------------------------------------------------------
 * Result (global_lock)
 * ----------------------------------------------------------------------*/

/* a spectrum for display: at most points points, the highest level of each
 * group (a narrow carrier stays visible) */
static htsmsg_t *
lb_display ( htsmsg_t *pol, int points )
{
  htsmsg_t *m = htsmsg_create_map(), *fl = htsmsg_create_list(), *ll = htsmsg_create_list(), *c;
  const void *fb, *lbb;
  size_t fsz, lsz, n, i, j, step;
  const uint32_t *f;
  const int16_t *l;
  int16_t mx;
  int64_t noise;

  if (!htsmsg_get_bin(pol, "freq", &fb, &fsz) && !htsmsg_get_bin(pol, "level", &lbb, &lsz)) {
    f = fb;
    l = lbb;
    n = MIN(fsz / sizeof(uint32_t), lsz / sizeof(int16_t));
    step = points > 0 && n > (size_t)points ? (n + points - 1) / points : 1;
    for (i = 0; i < n; i += step) {
      mx = l[i];
      for (j = i + 1; j < i + step && j < n; j++)
        if (l[j] > mx)
          mx = l[j];
      htsmsg_add_u32(fl, NULL, f[i]);
      htsmsg_add_s32(ll, NULL, mx);
    }
  }
  htsmsg_add_msg(m, "freq", fl);
  htsmsg_add_msg(m, "level", ll);
  if (!htsmsg_get_s64(pol, "noise", &noise))
    htsmsg_add_s64(m, "noise", noise);
  if (!htsmsg_get_s64(pol, "time", &noise))
    htsmsg_add_s64(m, "time", noise);
  if ((c = htsmsg_get_list(pol, "floor_freq")) != NULL)
    htsmsg_add_msg(m, "floor_freq", htsmsg_copy(c));
  if ((c = htsmsg_get_list(pol, "floor")) != NULL)
    htsmsg_add_msg(m, "floor", htsmsg_copy(c));
  if ((c = htsmsg_get_list(pol, "carriers")) != NULL)
    htsmsg_add_msg(m, "carriers", htsmsg_copy(c));
  return m;
}

htsmsg_t *
linuxdvb_blindscan_result ( mpegts_network_t *mn, int points )
{
  linuxdvb_blindscan_t *lb;
  htsmsg_t *r = htsmsg_create_map(), *res = NULL, *pols, *out = htsmsg_create_map(), *p;
  htsmsg_field_t *f;
  char uuid[UUID_HEX_SIZE];
  int64_t t;
  const char *s;

  lock_assert(&global_lock);
  idnode_uuid_as_str(&mn->mn_id, uuid);
  if ((lb = lb_find(uuid)) != NULL) {
    tvh_mutex_lock(&lb->lb_lock);
    htsmsg_add_str(r, "state", lb_state_str[lb->lb_state]);
    htsmsg_add_s32(r, "slice", lb->lb_slice);
    htsmsg_add_s32(r, "slices", lb->lb_slices);
    if (lb->lb_pol)
      htsmsg_add_str(r, "pol", (char []){ lb->lb_pol, 0 });
    if (lb->lb_error[0])
      htsmsg_add_str(r, "error", lb->lb_error);
    if (lb->lb_result)
      res = htsmsg_copy(lb->lb_result);
    tvh_mutex_unlock(&lb->lb_lock);
  }
  if (res == NULL && (res = hts_settings_load("input/dvb/networks/%s/blindscan", uuid)) != NULL)
    lb_result_convert(res, 0);
  if (res) {
    if (!htsmsg_get_s64(res, "time", &t))
      htsmsg_add_s64(r, "time", t);
    if ((s = htsmsg_get_str(res, "frontend")) != NULL)
      htsmsg_add_str(r, "frontend", s);
    if ((pols = htsmsg_get_map(res, "pols")) != NULL)
      HTSMSG_FOREACH(f, pols)
        if ((p = htsmsg_field_get_map(f)) != NULL)
          htsmsg_add_msg(out, htsmsg_field_name(f), lb_display(p, points));
    htsmsg_destroy(res);
  }
  htsmsg_add_msg(r, "pols", out);
  return r;
}

#endif /* ENABLE_LINUXDVB_NEUMO */
