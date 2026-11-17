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

#ifndef __TVH_DVB_BLINDSCAN_DETECT_H__
#define __TVH_DVB_BLINDSCAN_DETECT_H__

#include <stddef.h>

/*
 * A spectrum comes in slices (frequencies of the transponders, in kHz, and
 * levels in dB): one for the whole band, two for the low and high band of a
 * universal LNB, or many narrow overlapping ones through a Unicable user band.
 * dvb_blindscan_merge() makes one spectrum of them with its noise floor,
 * dvb_blindscan_detect() finds the carriers above that floor. Plain
 * computation: no tvheadend state, no locks.
 */

typedef struct dvb_blindscan_slice {
  double        center_khz;   /* the frequency the slice was taken around */
  size_t        count;
  const double *freq_khz;     /* ascending */
  const double *level_db;
} dvb_blindscan_slice_t;

typedef struct dvb_blindscan_spectrum {
  size_t  count;
  double *freq_khz;
  double *level_db;
  double  noise_db;           /* the 10th percentile of the levels */
  /* the noise floor along the band, floor_count points: the levels without
   * the carriers. The levels of the slices of a Unicable LNB, which levels
   * each user band on its own, are matched one to the next and drift; a
   * cable dampens the upper frequencies more; the polarisations differ:
   * levels above the floor compare along the band and between them */
  size_t  floor_count;
  double *floor_freq_khz;
  double *floor_db;
} dvb_blindscan_spectrum_t;

typedef struct dvb_blindscan_params {
  double min_bw_khz;          /* narrower peaks are not carriers (250) */
  double max_bw_khz;          /* nor wider ones (45000) */
  double min_snr_db;          /* above the noise floor and as prominence (3) */
  double overlap_khz;         /* Unicable: blended overlap of slices (20000) */
  double extract_khz;         /* Unicable: used middle of a slice (40000) */
  double floor_block_khz;     /* noise floor: the 10th percentile of blocks (2000;
                                 0: noise_db for the whole band) */
  double floor_window_khz;    /* their lower envelope and its mean over this,
                                 wider than the widest carrier (120000) */
  int    edge_top;            /* edges 3 dB below the top's middle level,
                                 between the samples (1); or 3 dB below the
                                 peak, at the sample past it (0) */
} dvb_blindscan_params_t;

typedef struct dvb_blindscan_carrier {
  double freq_khz;            /* middle between the edges */
  double bw_khz;              /* between the edges, 3 dB down */
  double left_khz, right_khz;
  double snr_db;              /* peak above the noise floor there */
  double peak_db;
  int    sigma;               /* smoothing that found it (2 or 5) */
} dvb_blindscan_carrier_t;

typedef enum {
  DVB_BLINDSCAN_AUTO = 0,     /* by the number of slices: 1, 2, more */
  DVB_BLINDSCAN_SINGLE,
  DVB_BLINDSCAN_STANDARD,     /* low and high band of a universal LNB */
  DVB_BLINDSCAN_UNICABLE,     /* narrow slices: levels matched in the overlaps */
} dvb_blindscan_mode_t;

void dvb_blindscan_params_default ( dvb_blindscan_params_t *p );

/* 0, or -1 (no slices, no memory); free with dvb_blindscan_spectrum_free() */
int dvb_blindscan_merge
  ( const dvb_blindscan_slice_t *slices, size_t count, dvb_blindscan_mode_t mode,
    const dvb_blindscan_params_t *p, dvb_blindscan_spectrum_t *out );

void dvb_blindscan_spectrum_free ( dvb_blindscan_spectrum_t *s );

/* the noise floor at a frequency (noise_db without a floor) */
double dvb_blindscan_floor_at ( const dvb_blindscan_spectrum_t *s, double freq_khz );

/* the carriers by frequency (*out: malloc'ed, NULL if none); their number,
 * or -1 (no memory) */
int dvb_blindscan_detect
  ( const dvb_blindscan_spectrum_t *s, const dvb_blindscan_params_t *p,
    dvb_blindscan_carrier_t **out );

/* symbol rate (symbols/s) of a carrier of this width: half the power of a
 * root raised cosine carrier is at its symbol rate apart, whatever the
 * rolloff (the occupied width is the symbol rate times 1 + rolloff) */
static inline double dvb_blindscan_symbol_rate ( const dvb_blindscan_carrier_t *c )
{
  return c->bw_khz * 1000.0;
}

#endif /* __TVH_DVB_BLINDSCAN_DETECT_H__ */
