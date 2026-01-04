/*
 *  TVHeadend - Linux DVB Blindscan
 *
 *  Copyright (C) 2024
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
#include "linuxdvb_private.h"
#include "linuxdvb_blindscan.h"
#include "notify.h"
#include "htsmsg.h"
#include "htsmsg_json.h"

#include <sys/ioctl.h>
#include <sys/epoll.h>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <linux/dvb/frontend.h>
#include <linux/dvb/dmx.h>

/* ============================================================================
 * Module-level data
 * ============================================================================ */

static LIST_HEAD(, blindscan_session) blindscan_sessions;
static tvh_mutex_t blindscan_mutex;

/* Logging subsystem */
#define LS_BLINDSCAN LS_LINUXDVB

/* Forward declarations for conversion helpers */
static int blindscan_convert_modulation(int linux_mod);
static int blindscan_convert_fec(int linux_fec);
static int blindscan_convert_rolloff(int linux_rolloff);
static int blindscan_convert_pilot(int linux_pilot);

/* ============================================================================
 * Neumo driver ioctl extensions (not in standard DVB headers)
 * ============================================================================ */

/* FE_SET_RF_INPUT - Set RF input for multi-input cards */
#ifndef FE_SET_RF_INPUT

enum fe_reservation_mode {
  FE_RESERVATION_MODE_MASTER_OR_SLAVE = 0,  /* Driver decides if caller can control SEC */
  FE_RESERVATION_MODE_MASTER = 1,            /* Caller needs to control voltage/tone/switches */
  FE_RESERVATION_MODE_SLAVE = 2              /* Caller does not want to control SEC */
};

struct fe_rf_input_control {
  int owner;              /* Process ID of the owner (use 0xffffffff for legacy) */
  int config_id;          /* Configuration ID */
  int16_t rf_in;          /* RF input number to select */
  int8_t unicable_mode;   /* 1 = allow slaves to send DiSEqC (for Unicable/shared RF) */
  int8_t mode;            /* Reservation mode (fe_reservation_mode) */
};

#define FE_SET_RF_INPUT _IOW('o', 85, struct fe_rf_input_control)
#endif

/* ============================================================================
 * Neumo driver spectral_peak_t structure (matches driver definition)
 * ============================================================================ */

struct neumo_spectral_peak {
  int32_t freq;
  int32_t symbol_rate;
  int32_t snr;
  int32_t level;
};

/* ============================================================================
 * Internal helper functions
 * ============================================================================ */

/**
 * Allocate spectrum data container
 */
static blindscan_spectrum_data_t *
blindscan_spectrum_alloc(size_t initial_size, int band, char pol)
{
  blindscan_spectrum_data_t *sd = calloc(1, sizeof(*sd));
  if (!sd) return NULL;

  sd->points = malloc(initial_size * sizeof(blindscan_spectrum_point_t));
  if (!sd->points) {
    free(sd);
    return NULL;
  }

  sd->alloc_points = initial_size;
  sd->num_points = 0;
  sd->band = band;
  sd->polarisation = pol;

  return sd;
}

/**
 * Free spectrum data container
 */
static void
blindscan_spectrum_free(blindscan_spectrum_data_t *sd)
{
  if (!sd) return;
  free(sd->points);
  free(sd);
}

/**
 * Add point to spectrum data
 */
static int
blindscan_spectrum_add_point(blindscan_spectrum_data_t *sd,
                             uint32_t freq, int32_t level)
{
  if (sd->num_points >= sd->alloc_points) {
    size_t new_alloc = sd->alloc_points * 2;
    blindscan_spectrum_point_t *new_points;
    new_points = realloc(sd->points, new_alloc * sizeof(*new_points));
    if (!new_points) return -1;
    sd->points = new_points;
    sd->alloc_points = new_alloc;
  }

  sd->points[sd->num_points].frequency = freq;
  sd->points[sd->num_points].level = level;
  sd->num_points++;

  return 0;
}

/**
 * Free a peak
 */
static void
blindscan_peak_free(blindscan_peak_t *peak)
{
  if (!peak) return;
  /* Note: bp_mux is owned by the network, not freed here */
  free(peak);
}

/**
 * Free all peaks in a session
 */
static void
blindscan_peaks_free(blindscan_session_t *bs)
{
  blindscan_peak_t *peak, *next;

  for (peak = LIST_FIRST(&bs->bs_peaks); peak; peak = next) {
    next = LIST_NEXT(peak, bp_link);
    LIST_REMOVE(peak, bp_link);
    blindscan_peak_free(peak);
  }
  bs->bs_peak_count = 0;
}

/**
 * Detect peaks from spectrum data using sliding window algorithm
 *
 * Algorithm:
 * 1. Calculate noise floor from lowest 10% of samples
 * 2. Find local maxima using sliding window
 * 3. Filter peaks above noise floor + threshold
 * 4. Estimate symbol rate from peak width
 *
 * @param sd         Spectrum data
 * @param out_peaks  Output array for detected peaks
 * @param max_peaks  Maximum number of peaks
 * @param threshold_db  Threshold above noise floor in 0.01 dB units (e.g., 300 = 3dB)
 * @return Number of peaks detected
 */
static int
blindscan_detect_peaks(blindscan_spectrum_data_t *sd,
                       blindscan_spectral_peak_t *out_peaks,
                       int max_peaks, int32_t threshold_db)
{
  if (!sd || !sd->points || sd->num_points < 100 || !out_peaks)
    return 0;

  size_t n = sd->num_points;

  /* Step 1: Find min/max levels for threshold calculation */
  int32_t min_level = sd->points[0].level;
  int32_t max_level = sd->points[0].level;
  for (size_t i = 1; i < n; i++) {
    if (sd->points[i].level < min_level) min_level = sd->points[i].level;
    if (sd->points[i].level > max_level) max_level = sd->points[i].level;
  }

  int32_t peak_threshold = min_level + threshold_db;

  tvhdebug(LS_BLINDSCAN, "Peak detection: min=%.1f max=%.1f thresh=%.1f dB",
           min_level / 100.0, max_level / 100.0, peak_threshold / 100.0);

  /* Step 2: Find all local maxima above threshold using small window */
  typedef struct { size_t idx; int32_t level; } candidate_t;
  candidate_t *candidates = malloc(512 * sizeof(candidate_t));
  if (!candidates) return 0;
  int num_cand = 0;

  size_t window = 20;  /* Small window to find all candidate peaks */
  size_t half_win = window / 2;

  for (size_t i = half_win; i < n - half_win && num_cand < 512; i++) {
    int32_t lvl = sd->points[i].level;
    if (lvl < peak_threshold)
      continue;

    /* Check if local maximum */
    int is_max = 1;
    for (size_t j = i - half_win; j <= i + half_win && is_max; j++) {
      if (j != i && sd->points[j].level > lvl)
        is_max = 0;
    }
    if (is_max) {
      candidates[num_cand].idx = i;
      candidates[num_cand].level = lvl;
      num_cand++;
      i += half_win;  /* Skip ahead */
    }
  }

  tvhdebug(LS_BLINDSCAN, "Found %d initial candidates above threshold", num_cand);

  if (num_cand == 0) {
    free(candidates);
    return 0;
  }

  /* Step 3: Valley-based merging - merge candidates if no significant valley between them */
  /* Valley must drop at least 4dB (400 units) below the weaker peak to be considered real */
  const int32_t min_valley_depth = 400;  /* 4 dB in 0.01 dB units */

  candidate_t *merged = malloc(512 * sizeof(candidate_t));
  if (!merged) {
    free(candidates);
    return 0;
  }

  merged[0] = candidates[0];
  int num_merged = 1;

  for (int i = 1; i < num_cand; i++) {
    candidate_t *prev = &merged[num_merged - 1];
    candidate_t *curr = &candidates[i];

    /* Find valley (minimum) between prev and curr */
    int32_t valley_level = (prev->level < curr->level) ? prev->level : curr->level;
    for (size_t j = prev->idx + 1; j < curr->idx; j++) {
      if (sd->points[j].level < valley_level)
        valley_level = sd->points[j].level;
    }

    /* Valley depth = weaker peak level - valley level */
    int32_t weaker_peak = (prev->level < curr->level) ? prev->level : curr->level;
    int32_t valley_depth = weaker_peak - valley_level;

    if (valley_depth >= min_valley_depth) {
      /* Significant valley - keep both as separate peaks */
      merged[num_merged++] = *curr;
    } else {
      /* No significant valley - merge (keep the stronger one) */
      if (curr->level > prev->level) {
        *prev = *curr;
      }
    }
  }

  tvhdebug(LS_BLINDSCAN, "After valley merge: %d -> %d peaks", num_cand, num_merged);
  free(candidates);

  /* Step 4: Calculate symbol rates from bandwidth (-6dB points) and output */
  int num_peaks = 0;
  for (int i = 0; i < num_merged && num_peaks < max_peaks; i++) {
    size_t peak_idx = merged[i].idx;
    int32_t peak_level = merged[i].level;

    /* Find -6dB points for bandwidth estimate */
    int32_t edge_level = peak_level - 600;  /* -6 dB */
    size_t left_idx = peak_idx, right_idx = peak_idx;

    for (size_t j = peak_idx; j > 0; j--) {
      if (sd->points[j].level < edge_level) {
        left_idx = j;
        break;
      }
      left_idx = j;
    }
    for (size_t j = peak_idx; j < n; j++) {
      if (sd->points[j].level < edge_level) {
        right_idx = j;
        break;
      }
      right_idx = j;
    }

    uint32_t bandwidth_khz = sd->points[right_idx].frequency - sd->points[left_idx].frequency;
    uint32_t center_freq = (sd->points[left_idx].frequency + sd->points[right_idx].frequency) / 2;

    /* Symbol rate ≈ bandwidth * 0.8 (rolloff) */
    uint32_t symbol_rate = bandwidth_khz * 800;
    if (symbol_rate < 1000000) symbol_rate = 2000000;
    if (symbol_rate > 45000000) symbol_rate = 45000000;

    out_peaks[num_peaks].frequency = center_freq;
    out_peaks[num_peaks].symbol_rate = symbol_rate;
    out_peaks[num_peaks].level = peak_level;
    out_peaks[num_peaks].snr = peak_level - min_level;
    num_peaks++;

    tvhdebug(LS_BLINDSCAN, "Peak %d: %u kHz, BW=%u kHz, SR=%u",
             num_peaks, center_freq, bandwidth_khz, symbol_rate);
  }

  free(merged);

  tvhinfo(LS_BLINDSCAN, "Peak detection found %d peaks (threshold %.1f dB, valley=4dB)",
          num_peaks, threshold_db / 100.0);

  return num_peaks;
}

/**
 * Acquire spectrum using Neumo driver DTV_SPECTRUM
 *
 * @param fd        Frontend file descriptor
 * @param bs        Blindscan session
 * @param band      Band (0=low, 1=high)
 * @param pol_is_v  True if V polarisation
 * @param out_peaks Output array for driver-detected peaks
 * @param max_peaks Maximum number of peaks to store
 * @return Spectrum data on success, NULL on failure
 */
static blindscan_spectrum_data_t *
blindscan_acquire_spectrum_neumo(int fd, blindscan_session_t *bs,
                                 int band, int pol_is_v,
                                 blindscan_spectral_peak_t *out_peaks,
                                 int *num_peaks, int max_peaks)
{
  struct dtv_property props[8];
  struct dtv_properties cmdseq;
  int i = 0;

  /* Calculate driver frequencies */
  uint32_t start_freq = bs->bs_start_freq;
  uint32_t end_freq = bs->bs_end_freq;

  /* Clip to band boundaries */
  if (band == 0) {
    if (end_freq > BLINDSCAN_LNB_SLOF)
      end_freq = BLINDSCAN_LNB_SLOF;
  } else {
    if (start_freq < BLINDSCAN_LNB_SLOF)
      start_freq = BLINDSCAN_LNB_SLOF;
  }

  int32_t start_driver = blindscan_driver_freq(start_freq, band);
  int32_t end_driver = blindscan_driver_freq(end_freq, band);

  /* Ensure start < end (for C-band or inverted LNB) */
  if (start_driver > end_driver) {
    int32_t tmp = start_driver;
    start_driver = end_driver;
    end_driver = tmp;
  }

  tvhdebug(LS_BLINDSCAN, "Spectrum acquire: driver freq %d-%d kHz (transponder %u-%u kHz)",
           start_driver, end_driver, start_freq, end_freq);

  /* Clear frontend */
  memset(props, 0, sizeof(props));
  props[i].cmd = DTV_CLEAR;
  cmdseq.num = 1;
  cmdseq.props = props;
  if (ioctl(fd, FE_SET_PROPERTY, &cmdseq) < 0) {
    tvherror(LS_BLINDSCAN, "DTV_CLEAR failed: %s", strerror(errno));
    return NULL;
  }

  /* Configure spectrum scan */
  i = 0;
  memset(props, 0, sizeof(props));

  props[i].cmd = DTV_DELIVERY_SYSTEM;
  props[i].u.data = SYS_DVBS2;
  i++;

  props[i].cmd = DTV_SCAN_START_FREQUENCY;
  props[i].u.data = start_driver;
  i++;

  props[i].cmd = DTV_SCAN_END_FREQUENCY;
  props[i].u.data = end_driver;
  i++;

  props[i].cmd = DTV_SCAN_RESOLUTION;
  props[i].u.data = bs->bs_spectral_resolution;  /* 0 = driver default */
  i++;

  props[i].cmd = DTV_SCAN_FFT_SIZE;
  props[i].u.data = bs->bs_fft_size > 0 ? bs->bs_fft_size : 512;
  i++;

  props[i].cmd = DTV_SPECTRUM;
  props[i].u.data = SPECTRUM_METHOD_FFT;
  i++;

  cmdseq.num = i;
  cmdseq.props = props;

  if (ioctl(fd, FE_SET_PROPERTY, &cmdseq) < 0) {
    tvherror(LS_BLINDSCAN, "FE_SET_PROPERTY spectrum failed: %s", strerror(errno));
    return NULL;
  }

  /* Wait for acquisition using epoll */
  int efd = epoll_create1(0);
  if (efd < 0) {
    tvherror(LS_BLINDSCAN, "epoll_create1 failed: %s", strerror(errno));
    return NULL;
  }

  struct epoll_event ep;
  ep.data.fd = fd;
  ep.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLET;
  epoll_ctl(efd, EPOLL_CTL_ADD, fd, &ep);

  struct epoll_event events[1];
  int found = 0;
  int timeout_ms = 60000;  /* 60 seconds */

  for (int count = 0; count < 10 && !found; count++) {
    if (bs->bs_should_stop) {
      close(efd);
      return NULL;
    }

    int s = epoll_wait(efd, events, 1, timeout_ms);
    if (s < 0) {
      if (errno == EINTR) continue;
      tvherror(LS_BLINDSCAN, "epoll_wait failed: %s", strerror(errno));
      break;
    }
    if (s == 0) {
      tvherror(LS_BLINDSCAN, "Spectrum acquisition timeout");
      break;
    }

    struct dvb_frontend_event event;
    if (ioctl(fd, FE_GET_EVENT, &event) < 0) {
      tvherror(LS_BLINDSCAN, "FE_GET_EVENT failed: %s", strerror(errno));
      continue;
    }

    if (event.status & FE_HAS_SYNC) {
      found = 1;
      tvhdebug(LS_BLINDSCAN, "Spectrum acquisition complete, status=%d", event.status);
    }
  }

  close(efd);

  if (!found) {
    tvherror(LS_BLINDSCAN, "Failed to acquire spectrum");
    return NULL;
  }

  /* Allocate buffers for spectrum data */
  size_t max_freq = 65536 * 4;
  uint32_t *freq_buffer = malloc(max_freq * sizeof(uint32_t));
  int32_t *level_buffer = malloc(max_freq * sizeof(int32_t));
  struct neumo_spectral_peak *candidates_buffer = malloc(512 * sizeof(struct neumo_spectral_peak));

  if (!freq_buffer || !level_buffer || !candidates_buffer) {
    free(freq_buffer);
    free(level_buffer);
    free(candidates_buffer);
    return NULL;
  }

  /* Read spectrum data using special dtv_fe_spectrum structure */
  struct dtv_property p_spectrum;
  memset(&p_spectrum, 0, sizeof(p_spectrum));
  p_spectrum.cmd = DTV_SPECTRUM;

  /* The u.spectrum field maps to dtv_fe_spectrum structure:
   * struct dtv_fe_spectrum {
   *   __u32 *freq;
   *   __s32 *rf_level;
   *   struct spectral_peak_t *candidates;
   *   __u32 num_freq;
   *   __u32 num_candidates;
   *   __u32 scale;
   *   __u8 spectrum_method;
   * };
   * We access this via buffer.data8 to set the pointers.
   */

  /* Neumo driver expects spectrum data in buffer */
  /* This is a workaround for the union layout */
  struct {
    uint32_t *freq;
    int32_t *rf_level;
    struct neumo_spectral_peak *candidates;
    uint32_t num_freq;
    uint32_t num_candidates;
    uint32_t scale;
    uint8_t spectrum_method;
  } __attribute__((packed)) spectrum_req;

  spectrum_req.freq = freq_buffer;
  spectrum_req.rf_level = level_buffer;
  spectrum_req.candidates = candidates_buffer;
  spectrum_req.num_freq = max_freq;
  spectrum_req.num_candidates = 512;
  spectrum_req.scale = 0;
  spectrum_req.spectrum_method = 0;

  /* Copy to property buffer */
  memcpy(p_spectrum.u.buffer.data, &spectrum_req, sizeof(spectrum_req));
  p_spectrum.u.buffer.len = sizeof(spectrum_req);

  struct dtv_properties props_get = { .num = 1, .props = &p_spectrum };

  if (ioctl(fd, FE_GET_PROPERTY, &props_get) < 0) {
    tvherror(LS_BLINDSCAN, "FE_GET_PROPERTY spectrum failed: %s", strerror(errno));
    free(freq_buffer);
    free(level_buffer);
    free(candidates_buffer);
    return NULL;
  }

  /* Read back results */
  memcpy(&spectrum_req, p_spectrum.u.buffer.data, sizeof(spectrum_req));

  uint32_t num_freq = spectrum_req.num_freq;
  uint32_t num_candidates = spectrum_req.num_candidates;

  tvhinfo(LS_BLINDSCAN, "Got %u spectrum points, %u candidates", num_freq, num_candidates);

  /* Debug: show sample of level values */
  if (num_freq > 0) {
    int32_t min_level = level_buffer[0], max_level = level_buffer[0];
    for (uint32_t j = 1; j < num_freq; j++) {
      if (level_buffer[j] < min_level) min_level = level_buffer[j];
      if (level_buffer[j] > max_level) max_level = level_buffer[j];
    }
    tvhdebug(LS_BLINDSCAN, "Spectrum levels: min=%.2f dB, max=%.2f dB (first=%d, mid=%d, last=%d)",
             min_level / 100.0, max_level / 100.0,
             level_buffer[0], level_buffer[num_freq/2], level_buffer[num_freq-1]);
  }

  /* Create spectrum data */
  blindscan_spectrum_data_t *sd = blindscan_spectrum_alloc(num_freq, band, pol_is_v ? 'V' : 'H');
  if (!sd) {
    free(freq_buffer);
    free(level_buffer);
    free(candidates_buffer);
    return NULL;
  }

  /* Convert to our format
   * Note: Driver returns levels in 0.001 dB (millidB), we store in 0.01 dB */
  for (uint32_t j = 0; j < num_freq; j++) {
    uint32_t tp_freq = blindscan_transponder_freq(freq_buffer[j], band);
    blindscan_spectrum_add_point(sd, tp_freq, level_buffer[j] / 10);
  }

  /* Copy peaks */
  if (num_peaks && out_peaks) {
    *num_peaks = 0;
    for (uint32_t j = 0; j < num_candidates && j < (uint32_t)max_peaks; j++) {
      out_peaks[j].frequency = blindscan_transponder_freq(candidates_buffer[j].freq, band);
      out_peaks[j].symbol_rate = candidates_buffer[j].symbol_rate;
      out_peaks[j].snr = candidates_buffer[j].snr / 10;  /* 0.001 dB -> 0.01 dB */
      out_peaks[j].level = candidates_buffer[j].level / 10;  /* 0.001 dB -> 0.01 dB */
      (*num_peaks)++;
    }
  }

  free(freq_buffer);
  free(level_buffer);
  free(candidates_buffer);

  return sd;
}

/**
 * Send unicable command using the existing TVHeadend EN50494/EN50607 tune function
 */
static int
blindscan_send_unicable_command(linuxdvb_satconf_ele_t *lse,
                                uint32_t center_freq, int pol_is_v, int band)
{
  linuxdvb_diseqc_t *ld = lse->lse_en50494;
  if (!ld || !ld->ld_tune)
    return -1;

  /*
   * Convert transponder frequency (kHz) to IF frequency (kHz)
   * center_freq is in kHz, LNB LO frequencies are in kHz
   * Universal LNB: low band LO = 9750 MHz, high band LO = 10600 MHz
   */
  uint32_t lnb_lo = (band == 0) ? 9750000 : 10600000;  /* kHz */
  uint32_t if_freq = (center_freq > lnb_lo) ? (center_freq - lnb_lo) : (lnb_lo - center_freq);

  tvhtrace(LS_BLINDSCAN, "Unicable command: tp_freq=%u kHz, band=%d, lnb_lo=%u kHz, if_freq=%u kHz",
           center_freq, band, lnb_lo, if_freq);

  /* Call the existing unicable tune function
   * Parameters: ld, lm (NULL ok), lsp, sc, vol, pol, band, freq (kHz) */
  return ld->ld_tune(ld, NULL, lse->lse_parent, lse,
                     0,  /* vol: 0=13V for unicable */
                     pol_is_v ? 0 : 1,  /* pol: 0=V, 1=H */
                     band,
                     if_freq);  /* IF frequency in kHz */
}

/**
 * Acquire a single spectrum slice for unicable at the fixed SCR frequency
 *
 * @param fd          Frontend file descriptor
 * @param bs          Blindscan session
 * @param center_freq Center transponder frequency for this slice (kHz)
 * @param scr_freq    SCR output frequency (kHz)
 * @param step_size   Width of each slice (kHz)
 * @param pol_is_v    Polarisation
 * @param out_peaks   Output array for driver-detected peaks (converted to transponder freq)
 * @param num_peaks   Output: number of peaks found
 * @param max_peaks   Max peaks to store
 * @return Spectrum data for this slice, or NULL on failure
 */
static blindscan_spectrum_data_t *
blindscan_acquire_unicable_slice(int fd, blindscan_session_t *bs,
                                 uint32_t center_freq, uint32_t scr_freq,
                                 uint32_t step_size, int pol_is_v,
                                 blindscan_spectral_peak_t *out_peaks,
                                 int *num_peaks, int max_peaks)
{
  if (num_peaks) *num_peaks = 0;
  /* Scan range around SCR frequency - use smaller range for unicable slices */
  int32_t half_step = step_size / 2;
  int32_t start_if = scr_freq - half_step;
  int32_t end_if = scr_freq + half_step;

  tvhdebug(LS_BLINDSCAN, "Unicable slice: center=%u kHz, SCR=%u kHz, scan IF=%d-%d kHz",
           center_freq, scr_freq, start_if, end_if);

  /* Clear frontend state first (important!) */
  struct dtv_property p_clear;
  memset(&p_clear, 0, sizeof(p_clear));
  p_clear.cmd = DTV_CLEAR;
  struct dtv_properties props_clear = { .num = 1, .props = &p_clear };
  if (ioctl(fd, FE_SET_PROPERTY, &props_clear) < 0) {
    tvherror(LS_BLINDSCAN, "DTV_CLEAR failed: %s", strerror(errno));
    return NULL;
  }

  /* Configure scan properties */
  struct dtv_property props[8];
  memset(props, 0, sizeof(props));
  int pi = 0;

  props[pi].cmd = DTV_DELIVERY_SYSTEM;
  props[pi].u.data = SYS_DVBS2;
  pi++;

  props[pi].cmd = DTV_SCAN_START_FREQUENCY;
  props[pi].u.data = start_if;
  pi++;

  props[pi].cmd = DTV_SCAN_END_FREQUENCY;
  props[pi].u.data = end_if;
  pi++;

  props[pi].cmd = DTV_SCAN_RESOLUTION;
  props[pi].u.data = bs->bs_spectral_resolution ? bs->bs_spectral_resolution : 100;  /* 100 kHz default */
  pi++;

  props[pi].cmd = DTV_SCAN_FFT_SIZE;
  props[pi].u.data = bs->bs_fft_size ? bs->bs_fft_size : 512;
  pi++;

  props[pi].cmd = DTV_SPECTRUM;
  props[pi].u.data = SPECTRUM_METHOD_FFT;
  pi++;

  struct dtv_properties dtv_props = { .num = pi, .props = props };

  if (ioctl(fd, FE_SET_PROPERTY, &dtv_props) < 0) {
    tvherror(LS_BLINDSCAN, "FE_SET_PROPERTY for unicable slice failed: %s", strerror(errno));
    return NULL;
  }

  /* Wait for acquisition using epoll */
  int efd = epoll_create1(0);
  if (efd < 0) {
    tvherror(LS_BLINDSCAN, "epoll_create1 failed: %s", strerror(errno));
    return NULL;
  }

  struct epoll_event ep;
  ep.data.fd = fd;
  ep.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLET;
  epoll_ctl(efd, EPOLL_CTL_ADD, fd, &ep);

  struct epoll_event events[1];
  int found = 0;
  int timeout_ms = 10000;

  for (int count = 0; count < 5 && !found; count++) {
    int s = epoll_wait(efd, events, 1, timeout_ms);
    if (s < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (s == 0) break;  /* Timeout */

    struct dvb_frontend_event event;
    if (ioctl(fd, FE_GET_EVENT, &event) >= 0) {
      if (event.status & FE_HAS_SYNC) {
        found = 1;
      }
    }
  }

  close(efd);

  if (!found) {
    tvhdebug(LS_BLINDSCAN, "Unicable slice: no sync");
    return NULL;
  }

  /* Allocate buffers and read results (same structure as neumo function) */
  size_t max_freq = 65536;
  uint32_t *freq_buffer = malloc(max_freq * sizeof(uint32_t));
  int32_t *level_buffer = malloc(max_freq * sizeof(int32_t));
  struct neumo_spectral_peak *candidates_buffer = malloc(512 * sizeof(struct neumo_spectral_peak));

  if (!freq_buffer || !level_buffer || !candidates_buffer) {
    free(freq_buffer);
    free(level_buffer);
    free(candidates_buffer);
    return NULL;
  }

  /* Build spectrum request structure for GET - must match dtv_fe_spectrum layout */
  struct {
    uint32_t *freq;
    int32_t *rf_level;
    struct neumo_spectral_peak *candidates;
    uint32_t num_freq;
    uint32_t num_candidates;
    uint32_t scale;
    uint8_t spectrum_method;
  } __attribute__((packed)) spectrum_req;

  spectrum_req.freq = freq_buffer;
  spectrum_req.rf_level = level_buffer;
  spectrum_req.candidates = candidates_buffer;
  spectrum_req.num_freq = max_freq;
  spectrum_req.num_candidates = 512;
  spectrum_req.scale = 0;
  spectrum_req.spectrum_method = 0;

  struct dtv_property p_get;
  memset(&p_get, 0, sizeof(p_get));
  p_get.cmd = DTV_SPECTRUM;
  memcpy(p_get.u.buffer.data, &spectrum_req, sizeof(spectrum_req));
  p_get.u.buffer.len = sizeof(spectrum_req);

  struct dtv_properties get_props = { .num = 1, .props = &p_get };
  if (ioctl(fd, FE_GET_PROPERTY, &get_props) < 0) {
    tvherror(LS_BLINDSCAN, "FE_GET_PROPERTY spectrum failed: %s", strerror(errno));
    free(freq_buffer);
    free(level_buffer);
    free(candidates_buffer);
    return NULL;
  }

  memcpy(&spectrum_req, p_get.u.buffer.data, sizeof(spectrum_req));
  uint32_t num_points = spectrum_req.num_freq;

  /* Show level and frequency range */
  if (num_points > 0) {
    int32_t min_lvl = level_buffer[0], max_lvl = level_buffer[0];
    for (uint32_t j = 1; j < num_points; j++) {
      if (level_buffer[j] < min_lvl) min_lvl = level_buffer[j];
      if (level_buffer[j] > max_lvl) max_lvl = level_buffer[j];
    }
    tvhtrace(LS_BLINDSCAN, "Unicable slice: %u points, levels: %d to %d, IF freq: %u-%u kHz, center=%u, scr=%u",
             num_points, min_lvl, max_lvl, freq_buffer[0], freq_buffer[num_points-1], center_freq, scr_freq);
  } else {
    tvhtrace(LS_BLINDSCAN, "Unicable slice: 0 points returned");
  }

  if (num_points == 0) {
    free(freq_buffer);
    free(level_buffer);
    free(candidates_buffer);
    return NULL;
  }

  /* Create spectrum data - convert IF frequencies back to transponder frequencies */
  int band = blindscan_band_for_freq(center_freq);
  blindscan_spectrum_data_t *sd = blindscan_spectrum_alloc(num_points, band, pol_is_v ? 'V' : 'H');
  if (!sd) {
    free(freq_buffer);
    free(level_buffer);
    free(candidates_buffer);
    return NULL;
  }

  /* Debug: show first few raw frequencies from driver */
  if (num_points >= 3) {
    tvhtrace(LS_BLINDSCAN, "Driver raw freq: first=%u mid=%u last=%u (scr=%u, center=%u)",
             freq_buffer[0], freq_buffer[num_points/2], freq_buffer[num_points-1],
             scr_freq, center_freq);
  }

  /* Convert: IF offset from SCR → transponder frequency offset from center
   * Note: Driver returns levels in 0.001 dB (millidB), we store in 0.01 dB */
  for (uint32_t j = 0; j < num_points; j++) {
    int32_t if_offset = (int32_t)freq_buffer[j] - (int32_t)scr_freq;
    uint32_t tp_freq = center_freq + if_offset;
    blindscan_spectrum_add_point(sd, tp_freq, level_buffer[j] / 10);
  }

  /* Convert driver candidates to transponder frequencies */
  uint32_t num_cand = spectrum_req.num_candidates;
  if (out_peaks && num_peaks && num_cand > 0) {
    tvhdebug(LS_BLINDSCAN, "Unicable slice: %u driver candidates", num_cand);
    for (uint32_t j = 0; j < num_cand && (int)j < max_peaks; j++) {
      int32_t if_freq = candidates_buffer[j].freq;
      int32_t tp_freq = center_freq + (if_freq - (int32_t)scr_freq);
      tvhdebug(LS_BLINDSCAN, "Candidate %u: driver_freq=%d, scr=%u, center=%u -> tp_freq=%d",
               j, if_freq, scr_freq, center_freq, tp_freq);
      out_peaks[*num_peaks].frequency = tp_freq;
      out_peaks[*num_peaks].symbol_rate = candidates_buffer[j].symbol_rate;
      out_peaks[*num_peaks].snr = candidates_buffer[j].snr / 10;  /* 0.001 dB -> 0.01 dB */
      out_peaks[*num_peaks].level = candidates_buffer[j].level / 10;  /* 0.001 dB -> 0.01 dB */
      (*num_peaks)++;
    }
  }

  free(freq_buffer);
  free(level_buffer);
  free(candidates_buffer);

  return sd;
}

/**
 * Configure LNB for spectrum acquisition using standard TVH diseqc chain
 */
static int
blindscan_configure_lnb(int fd, linuxdvb_satconf_ele_t *lse,
                        int band, int pol_is_v,
                        blindscan_session_t *bs)
{
  linuxdvb_satconf_t *ls = lse->lse_parent;
  int pol = pol_is_v ? 1 : 0;
  int vol = pol;  /* voltage: 0=18V (H), 1=13V (V) */
  int i, r;

  /* Debug: log what diseqc devices are configured */
  tvhdebug(LS_BLINDSCAN, "Diseqc config: switch=%p, rotor=%p, ls_switch_rotor=%d",
           lse->lse_switch, lse->lse_rotor, ls->ls_switch_rotor);

  if (lse->lse_switch) {
    tvhdebug(LS_BLINDSCAN, "Switch found: type=%s",
             ((linuxdvb_diseqc_t*)lse->lse_switch)->ld_type);
  }

  /* Build diseqc device chain - same order as standard tune path */
  linuxdvb_diseqc_t *lds[] = {
    ls->ls_switch_rotor ? (linuxdvb_diseqc_t*)lse->lse_switch :
                          (linuxdvb_diseqc_t*)lse->lse_rotor,
    ls->ls_switch_rotor ? (linuxdvb_diseqc_t*)lse->lse_rotor  :
                          (linuxdvb_diseqc_t*)lse->lse_switch,
    NULL,  /* Skip en50494 for spectrum - handled separately */
    NULL   /* Skip LNB tune - we set voltage/tone directly */
  };

  /* Turn off tone before sending DiSEqC commands */
  if (ioctl(fd, FE_SET_TONE, SEC_TONE_OFF) < 0)
    tvhwarn(LS_BLINDSCAN, "FE_SET_TONE OFF failed: %s", strerror(errno));
  usleep(15000);

  /* Set initial voltage for DiSEqC */
  if (linuxdvb_diseqc_set_volt(ls, vol) < 0) {
    tvherror(LS_BLINDSCAN, "Failed to set initial voltage");
    return -1;
  }

  /* Force full diseqc sequence for blindscan by clearing cached state */
  tvhdebug(LS_BLINDSCAN, "Clearing diseqc cache: last_switch=%p, last_pol=%d, last_band=%d",
           ls->ls_last_switch, ls->ls_last_switch_pol, ls->ls_last_switch_band);
  ls->ls_last_switch = NULL;
  ls->ls_last_switch_pol = 0;
  ls->ls_last_switch_band = 0;
  ls->ls_last_toneburst = 0;

  /* Call each diseqc device's tune method (switch, rotor) */
  for (i = 0; i < ARRAY_SIZE(lds); i++) {
    if (!lds[i]) continue;

    tvhdebug(LS_BLINDSCAN, "Calling diseqc tune for %s (pol=%d, band=%d, vol=%d)",
             lds[i]->ld_type, pol, band, vol);

    /* Call the device's tune method - uses linuxdvb_satconf_fe_fd internally */
    r = lds[i]->ld_tune(lds[i], NULL, ls, lse, vol, pol, band, 0);

    if (r < 0) {
      tvherror(LS_BLINDSCAN, "DiSEqC device %s tune failed", lds[i]->ld_type);
      return -1;
    }

    tvhdebug(LS_BLINDSCAN, "DiSEqC device %s tune returned %d", lds[i]->ld_type, r);

    /* Wait if device requested delay */
    if (r > 0) {
      tvhdebug(LS_BLINDSCAN, "DiSEqC device %s: waiting %d seconds", lds[i]->ld_type, r);
      usleep(r * 1000000);
    }
  }

  /* Set final voltage */
  fe_sec_voltage_t volt_val = pol_is_v ? SEC_VOLTAGE_13 : SEC_VOLTAGE_18;
  if (ioctl(fd, FE_SET_VOLTAGE, volt_val) < 0) {
    tvherror(LS_BLINDSCAN, "FE_SET_VOLTAGE failed: %s", strerror(errno));
    return -1;
  }
  usleep(15000);

  /* Set tone for band selection */
  fe_sec_tone_mode_t tone = (band == 1) ? SEC_TONE_ON : SEC_TONE_OFF;
  if (ioctl(fd, FE_SET_TONE, tone) < 0) {
    tvherror(LS_BLINDSCAN, "FE_SET_TONE failed: %s", strerror(errno));
    return -1;
  }
  usleep(20000);

  tvhdebug(LS_BLINDSCAN, "LNB configured: pol=%c, band=%s",
           pol_is_v ? 'V' : 'H', band ? "high" : "low");

  return 0;
}

/**
 * Calculate mux half-bandwidth based on symbol rate and rolloff
 * Returns half-bandwidth in kHz
 */
static uint32_t
blindscan_mux_half_bandwidth(dvb_mux_t *dm)
{
  uint32_t sr = dm->lm_tuning.u.dmc_fe_qpsk.symbol_rate;  /* sym/s */

  /* Determine rolloff factor (default 0.35 for DVB-S, varies for DVB-S2) */
  int rolloff_percent = 35;  /* Default 0.35 */
  switch (dm->lm_tuning.dmc_fe_rolloff) {
    case DVB_ROLLOFF_20: rolloff_percent = 20; break;
    case DVB_ROLLOFF_25: rolloff_percent = 25; break;
    case DVB_ROLLOFF_35: rolloff_percent = 35; break;
    case DVB_ROLLOFF_15: rolloff_percent = 15; break;
    case DVB_ROLLOFF_10: rolloff_percent = 10; break;
    case DVB_ROLLOFF_5:  rolloff_percent = 5;  break;
    default: rolloff_percent = 35; break;
  }

  /* Bandwidth = SR × (1 + rolloff), half-bandwidth = SR × (1 + rolloff) / 2
   * Result in kHz: sr is in sym/s, divide by 1000 to get ksym/s = kHz equivalent */
  uint32_t half_bw = (sr / 1000) * (100 + rolloff_percent) / 200;

  return half_bw;
}

/**
 * Check if a peak falls within an existing mux's bandwidth
 * Returns the matching mux, or NULL if no match
 *
 * A peak is considered to match/overlap if it falls within the bandwidth
 * of an existing verified mux. This filters false positives from sidelobes.
 */
static dvb_mux_t *
blindscan_peak_find_overlapping_mux(blindscan_peak_t *peak, mpegts_network_t *mn)
{
  mpegts_mux_t *mm;
  dvb_mux_t *dm;

  LIST_FOREACH(mm, &mn->mn_muxes, mm_network_link) {
    dm = (dvb_mux_t *)mm;

    /* Check polarisation first */
    if (dm->lm_tuning.u.dmc_fe_qpsk.polarisation != peak->bp_polarisation)
      continue;

    /* Get mux center frequency and half-bandwidth
     * Note: dmc_fe_freq is already in kHz for DVB-S */
    uint32_t mux_freq = dm->lm_tuning.dmc_fe_freq;
    uint32_t half_bw = blindscan_mux_half_bandwidth(dm);
    uint32_t mux_low = mux_freq - half_bw;
    uint32_t mux_high = mux_freq + half_bw;

    /* Check if peak falls within mux bandwidth */
    if (peak->bp_frequency >= mux_low && peak->bp_frequency <= mux_high) {
      tvhdebug(LS_BLINDSCAN, "Peak %u kHz falls within mux %u kHz ±%u kHz [%u-%u]",
               peak->bp_frequency, mux_freq, half_bw, mux_low, mux_high);
      return dm;
    }
  }

  return NULL;
}


/**
 * Main blindscan worker thread
 */
static void *
blindscan_worker(void *arg)
{
  blindscan_session_t *bs = arg;
  linuxdvb_frontend_t *lfe = bs->bs_frontend;
  linuxdvb_satconf_ele_t *lse = bs->bs_satconf_ele;
  int fd;

  bs->bs_start_time = getmonoclock();

  tvhinfo(LS_BLINDSCAN, "Starting blindscan: %s freq=%u-%u kHz on %s",
          bs->bs_uuid_hex, bs->bs_start_freq, bs->bs_end_freq, lfe->lfe_fe_path);

  /* Use frontend's existing fd */
  fd = lfe->lfe_fe_fd;
  if (fd <= 0) {
    tvherror(LS_BLINDSCAN, "Frontend not open (fd=%d)", fd);
    bs->bs_state = BLINDSCAN_STATE_ERROR;
    free(bs->bs_status_msg);
    bs->bs_status_msg = strdup("Frontend not available");
    goto done;
  }

  /* Check if driver supports Neumo extensions */
  if (!lfe->lfe_neumo_supported) {
    tvhwarn(LS_BLINDSCAN, "Driver does not support Neumo spectrum extensions, using sweep mode");
    /* TODO: Implement sweep fallback */
    bs->bs_state = BLINDSCAN_STATE_ERROR;
    free(bs->bs_status_msg);
    bs->bs_status_msg = strdup("Driver does not support spectrum acquisition");
    goto done;
  }

  /* RF input is set by satconf tune path - blindscan uses same satconf */

  /* Determine polarisations to scan */
  int pol_list[2] = {0, 0};  /* 0=H, 1=V */
  int pol_count = 0;

  if (bs->bs_polarisation == -1 || bs->bs_polarisation == DVB_POLARISATION_HORIZONTAL) {
    pol_list[pol_count++] = 0;  /* H */
  }
  if (bs->bs_polarisation == -1 || bs->bs_polarisation == DVB_POLARISATION_VERTICAL) {
    pol_list[pol_count++] = 1;  /* V */
  }

  /* Determine bands */
  int band_list[2] = {-1, -1};
  int band_count = 0;

  if (bs->bs_start_freq < BLINDSCAN_LNB_SLOF)
    band_list[band_count++] = 0;  /* Low band */
  if (bs->bs_end_freq > BLINDSCAN_LNB_SLOF)
    band_list[band_count++] = 1;  /* High band */

  int total_scans = pol_count * band_count;
  int current_scan = 0;

  /* Array for driver peaks */
  blindscan_spectral_peak_t driver_peaks[512];
  int num_driver_peaks;

  /* Scan each polarisation and band */
  for (int pi = 0; pi < pol_count && !bs->bs_should_stop; pi++) {
    int pol_is_v = pol_list[pi];
    char pol_char = pol_is_v ? 'V' : 'H';

    for (int bi = 0; bi < band_count && !bs->bs_should_stop; bi++) {
      int band = band_list[bi];

      current_scan++;
      bs->bs_progress = (current_scan * 50) / total_scans;  /* 0-50% for spectrum */

      tvhinfo(LS_BLINDSCAN, "Acquiring spectrum: %c pol, %s band",
              pol_char, band == 0 ? "low" : "high");

      free(bs->bs_status_msg);
      bs->bs_status_msg = NULL;
      if (asprintf(&bs->bs_status_msg, "Acquiring %c %s band spectrum",
                   pol_char, band == 0 ? "low" : "high") < 0)
        bs->bs_status_msg = NULL;

      bs->bs_state = BLINDSCAN_STATE_ACQUIRING;

      blindscan_spectrum_data_t *sd = NULL;
      num_driver_peaks = 0;

      /* Check if this is a unicable setup */
      if (lse && lse->lse_en50494) {
        /* Unicable: use slice-based acquisition */
        linuxdvb_en50494_t *uc = (linuxdvb_en50494_t *)lse->lse_en50494;
        uint32_t scr_freq = uc->le_frequency * 1000;  /* MHz to kHz */

        tvhinfo(LS_BLINDSCAN, "Unicable mode: SCR=%d, freq=%u kHz, pos=%d",
                uc->le_id, scr_freq, uc->le_position);

        /* Calculate frequency range for this band */
        uint32_t band_start = bs->bs_start_freq;
        uint32_t band_end = bs->bs_end_freq;
        if (band == 0) {
          /* Low band: up to SLOF */
          if (band_end > BLINDSCAN_LNB_SLOF)
            band_end = BLINDSCAN_LNB_SLOF;
        } else {
          /* High band: from SLOF */
          if (band_start < BLINDSCAN_LNB_SLOF)
            band_start = BLINDSCAN_LNB_SLOF;
        }

        /* Step through frequency range */
        uint32_t step_size = 50000;  /* 50 MHz steps */
        uint32_t range = band_end - band_start;
        int total_steps = (range + step_size - 1) / step_size;

        /* Allocate combined spectrum */
        sd = blindscan_spectrum_alloc(total_steps * 2000, band, pol_is_v ? 'V' : 'H');
        if (!sd) {
          tvherror(LS_BLINDSCAN, "Failed to allocate spectrum buffer");
          continue;
        }

        for (int step = 0; step < total_steps && !bs->bs_should_stop; step++) {
          uint32_t center_freq = band_start + (step * step_size) + (step_size / 2);
          if (center_freq > band_end)
            center_freq = band_end - (step_size / 2);

          /* Update progress */
          bs->bs_progress = ((current_scan - 1) * 50 / total_scans) +
                            ((step + 1) * 50 / total_steps / total_scans);

          free(bs->bs_status_msg);
          bs->bs_status_msg = NULL;
          if (asprintf(&bs->bs_status_msg, "%c %s: slice %d/%d (%.0f MHz)",
                       pol_char, band == 0 ? "low" : "high",
                       step + 1, total_steps, center_freq / 1000.0) < 0)
            bs->bs_status_msg = NULL;

          /* Set satconf's frontend to blindscan's frontend (like normal tuning does) */
          lse->lse_parent->ls_frontend = (mpegts_input_t *)lfe;

          /* Send unicable command to tune LNB using existing TVH unicable function */
          if (blindscan_send_unicable_command(lse, center_freq, pol_is_v, band) < 0) {
            tvhwarn(LS_BLINDSCAN, "Failed unicable command for %u kHz", center_freq);
            continue;
          }

          /* Acquire slice at SCR frequency - also collect driver candidates */
          int slice_peaks = 0;
          blindscan_spectrum_data_t *slice = blindscan_acquire_unicable_slice(
            fd, bs, center_freq, scr_freq, step_size, pol_is_v,
            &driver_peaks[num_driver_peaks], &slice_peaks, 512 - num_driver_peaks);

          if (slice && slice->num_points > 0) {
            /* Append slice points to combined spectrum */
            for (size_t i = 0; i < slice->num_points; i++) {
              blindscan_spectrum_add_point(sd, slice->points[i].frequency,
                                           slice->points[i].level);
            }
            blindscan_spectrum_free(slice);
          }

          if (slice_peaks > 0) {
            tvhdebug(LS_BLINDSCAN, "Slice %d/%d: %d driver candidates",
                     step + 1, total_steps, slice_peaks);
            num_driver_peaks += slice_peaks;
          }
        }

        tvhinfo(LS_BLINDSCAN, "Unicable acquisition complete: %zu points, %d driver candidates",
                sd->num_points, num_driver_peaks);
      } else {
        /* Standard LNB: configure and do direct sweep */
        /* Set satconf's frontend so diseqc functions can get the fd */
        lse->lse_parent->ls_frontend = (mpegts_input_t *)lfe;

        if (blindscan_configure_lnb(fd, lse, band, pol_is_v, bs) < 0) {
          tvherror(LS_BLINDSCAN, "Failed to configure LNB");
          continue;
        }

        /* Acquire spectrum */
        sd = blindscan_acquire_spectrum_neumo(
          fd, bs, band, pol_is_v, driver_peaks, &num_driver_peaks, 512);
      }

      if (!sd) {
        tvherror(LS_BLINDSCAN, "Failed to acquire spectrum");
        continue;
      }

      /* Store spectrum */
      if (pol_is_v) {
        if (band == 0) {
          blindscan_spectrum_free(bs->bs_spectrum_v_low);
          bs->bs_spectrum_v_low = sd;
        } else {
          blindscan_spectrum_free(bs->bs_spectrum_v_high);
          bs->bs_spectrum_v_high = sd;
        }
      } else {
        if (band == 0) {
          blindscan_spectrum_free(bs->bs_spectrum_h_low);
          bs->bs_spectrum_h_low = sd;
        } else {
          blindscan_spectrum_free(bs->bs_spectrum_h_high);
          bs->bs_spectrum_h_high = sd;
        }
      }

      tvhinfo(LS_BLINDSCAN, "Acquired %zu spectrum points, %d driver candidates",
              sd->num_points, num_driver_peaks);

      /* Deduplicate driver candidates (unicable may have overlapping slices) */
      if (num_driver_peaks > 1) {
        int deduped = 0;
        blindscan_spectral_peak_t deduped_peaks[512];
        const uint32_t dedupe_thresh = 2000;  /* 2 MHz in kHz */

        for (int i = 0; i < num_driver_peaks; i++) {
          int is_dup = 0;
          for (int j = 0; j < deduped; j++) {
            uint32_t diff = (driver_peaks[i].frequency > deduped_peaks[j].frequency) ?
                            (driver_peaks[i].frequency - deduped_peaks[j].frequency) :
                            (deduped_peaks[j].frequency - driver_peaks[i].frequency);
            if (diff < dedupe_thresh) {
              /* Keep the one with higher level */
              if (driver_peaks[i].level > deduped_peaks[j].level)
                deduped_peaks[j] = driver_peaks[i];
              is_dup = 1;
              break;
            }
          }
          if (!is_dup && deduped < 512)
            deduped_peaks[deduped++] = driver_peaks[i];
        }

        if (deduped < num_driver_peaks) {
          tvhinfo(LS_BLINDSCAN, "Deduplicated: %d -> %d candidates", num_driver_peaks, deduped);
          memcpy(driver_peaks, deduped_peaks, deduped * sizeof(blindscan_spectral_peak_t));
          num_driver_peaks = deduped;
        }
      }

      /* Peak detection based on bs_peak_detect setting:
       * 0 = Auto: driver first, fallback to algorithm if no peaks
       * 1 = Driver only: use only driver-detected peaks
       * 2 = Algorithm only: always use our peak detection algorithm
       */
      if (bs->bs_peak_detect == 2) {
        /* Algorithm only mode - ignore driver peaks, use our detection */
        if (sd->num_points > 100) {
          tvhinfo(LS_BLINDSCAN, "Peak detection: algorithm mode");
          num_driver_peaks = blindscan_detect_peaks(sd, driver_peaks, 512, 1000);
          tvhinfo(LS_BLINDSCAN, "Algorithm found %d candidates", num_driver_peaks);
        }
      } else if (bs->bs_peak_detect == 0 && num_driver_peaks == 0 && sd->num_points > 100) {
        /* Auto mode - driver returned nothing, fall back to algorithm */
        tvhinfo(LS_BLINDSCAN, "Driver returned no candidates, running peak detection algorithm");
        num_driver_peaks = blindscan_detect_peaks(sd, driver_peaks, 512, 1000);
        if (num_driver_peaks > 0)
          tvhinfo(LS_BLINDSCAN, "Peak detection found %d candidates", num_driver_peaks);
      }
      /* Driver only mode (1): just use driver peaks as-is */

      /* Log detected peaks */
      for (int i = 0; i < num_driver_peaks; i++) {
        tvhinfo(LS_BLINDSCAN, "Peak %d: freq=%u kHz, SR=%u, level=%.1f dB",
                i + 1, driver_peaks[i].frequency, driver_peaks[i].symbol_rate,
                driver_peaks[i].level / 100.0);
      }

      /* Add peaks to session */
      for (int i = 0; i < num_driver_peaks; i++) {
        blindscan_peak_t *peak = calloc(1, sizeof(*peak));
        if (!peak) continue;

        peak->bp_frequency = driver_peaks[i].frequency;
        peak->bp_symbol_rate = driver_peaks[i].symbol_rate;
        peak->bp_snr = driver_peaks[i].snr;
        peak->bp_level = driver_peaks[i].level;
        peak->bp_polarisation = pol_is_v ? DVB_POLARISATION_VERTICAL : DVB_POLARISATION_HORIZONTAL;
        peak->bp_status = BLINDSCAN_PEAK_PENDING;
        peak->bp_stream_id = -1;
        peak->bp_verified_freq = 0;
        peak->bp_verified_sr = 0;

        /* Check if peak falls within an existing mux's bandwidth */
        dvb_mux_t *overlap_mux = blindscan_peak_find_overlapping_mux(peak, bs->bs_network);
        if (overlap_mux) {
          peak->bp_status = BLINDSCAN_PEAK_SKIPPED;
          /* Store verified mux info (dmc_fe_freq already in kHz for DVB-S) */
          peak->bp_verified_freq = overlap_mux->lm_tuning.dmc_fe_freq;
          peak->bp_verified_sr = overlap_mux->lm_tuning.u.dmc_fe_qpsk.symbol_rate;
          tvhdebug(LS_BLINDSCAN, "Peak %u kHz within existing mux %u kHz SR %u",
                   peak->bp_frequency, peak->bp_verified_freq, peak->bp_verified_sr);
        }

        LIST_INSERT_HEAD(&bs->bs_peaks, peak, bp_link);
        bs->bs_peak_count++;
      }
    }
  }

  if (bs->bs_should_stop) {
    bs->bs_state = BLINDSCAN_STATE_CANCELLED;
    goto done;
  }

  /* TODO: Blind tune each peak to verify and get exact parameters */
  /* For now, just mark them as complete */

  bs->bs_state = BLINDSCAN_STATE_COMPLETE;
  bs->bs_progress = 100;

done:
  bs->bs_duration_ms = (getmonoclock() - bs->bs_start_time) / 1000;
  bs->bs_thread_running = 0;

  tvhinfo(LS_BLINDSCAN, "Blindscan complete: %s, %d peaks found, duration=%"PRId64"ms",
          bs->bs_uuid_hex, bs->bs_peak_count, bs->bs_duration_ms);

  /* Send notification */
  htsmsg_t *m = htsmsg_create_map();
  htsmsg_add_str(m, "uuid", bs->bs_uuid_hex);
  htsmsg_add_str(m, "state", bs->bs_state == BLINDSCAN_STATE_COMPLETE ? "complete" :
                            (bs->bs_state == BLINDSCAN_STATE_CANCELLED ? "cancelled" : "error"));
  htsmsg_add_u32(m, "peaks", bs->bs_peak_count);
  htsmsg_add_s64(m, "duration", bs->bs_duration_ms);
  notify_by_msg("blindscan", m, 0, 0);

  return NULL;
}

/* ============================================================================
 * Public API Implementation
 * ============================================================================ */

void
linuxdvb_blindscan_init(void)
{
  LIST_INIT(&blindscan_sessions);
  tvh_mutex_init(&blindscan_mutex, NULL);
  tvhinfo(LS_BLINDSCAN, "Blindscan subsystem initialized");
}

void
linuxdvb_blindscan_done(void)
{
  blindscan_session_t *bs;

  tvh_mutex_lock(&blindscan_mutex);

  /* Cancel all running sessions */
  LIST_FOREACH(bs, &blindscan_sessions, bs_link) {
    bs->bs_should_stop = 1;
  }

  /* Wait for threads to finish */
  LIST_FOREACH(bs, &blindscan_sessions, bs_link) {
    if (bs->bs_thread_running)
      pthread_join(bs->bs_thread, NULL);
  }

  /* Free all sessions */
  while ((bs = LIST_FIRST(&blindscan_sessions)) != NULL) {
    LIST_REMOVE(bs, bs_link);
    blindscan_peaks_free(bs);
    blindscan_spectrum_free(bs->bs_spectrum_h_low);
    blindscan_spectrum_free(bs->bs_spectrum_h_high);
    blindscan_spectrum_free(bs->bs_spectrum_v_low);
    blindscan_spectrum_free(bs->bs_spectrum_v_high);
    free(bs->bs_status_msg);
    tvh_mutex_destroy(&bs->bs_lock);
    free(bs);
  }

  tvh_mutex_unlock(&blindscan_mutex);
  tvh_mutex_destroy(&blindscan_mutex);

  tvhinfo(LS_BLINDSCAN, "Blindscan subsystem shutdown");
}

blindscan_session_t *
linuxdvb_blindscan_find(const char *uuid)
{
  blindscan_session_t *bs;

  tvh_mutex_lock(&blindscan_mutex);
  LIST_FOREACH(bs, &blindscan_sessions, bs_link) {
    if (strcmp(bs->bs_uuid_hex, uuid) == 0) {
      tvh_mutex_unlock(&blindscan_mutex);
      return bs;
    }
  }
  tvh_mutex_unlock(&blindscan_mutex);
  return NULL;
}

const char *
linuxdvb_blindscan_start(linuxdvb_frontend_t *frontend,
                         linuxdvb_satconf_ele_t *satconf,
                         mpegts_network_t *network,
                         uint32_t start_freq,
                         uint32_t end_freq,
                         char pol,
                         htsmsg_t *opts)
{
  blindscan_session_t *bs;

  if (!frontend || !network) return NULL;

  /* Check frontend has Neumo support */
  if (!frontend->lfe_neumo_supported) {
    tvhwarn(LS_BLINDSCAN, "Frontend %s does not support Neumo extensions",
            frontend->lfe_name);
    /* Continue anyway - will fail later with better error message */
  }

  /* Create session */
  bs = calloc(1, sizeof(*bs));
  if (!bs) return NULL;

  /* Generate UUID */
  uuid_random(bs->bs_uuid.bin, UUID_BIN_SIZE);
  uuid_get_hex(&bs->bs_uuid, bs->bs_uuid_hex);

  bs->bs_frontend = frontend;
  bs->bs_satconf_ele = satconf;
  bs->bs_network = network;
  bs->bs_start_freq = start_freq;
  bs->bs_end_freq = end_freq;

  /* Parse polarisation */
  if (pol == 'H' || pol == 'h')
    bs->bs_polarisation = DVB_POLARISATION_HORIZONTAL;
  else if (pol == 'V' || pol == 'v')
    bs->bs_polarisation = DVB_POLARISATION_VERTICAL;
  else
    bs->bs_polarisation = -1;  /* Both */

  /* Default options */
  bs->bs_fft_size = 512;
  bs->bs_spectral_resolution = 0;  /* Driver default */
  bs->bs_peak_detect = 0;  /* Auto: driver first, fallback to algorithm */
  bs->bs_diseqc_committed = -1;
  bs->bs_diseqc_uncommitted = -1;
  bs->bs_unicable_scr = -1;

  /* Parse options */
  if (opts) {
    bs->bs_fft_size = htsmsg_get_u32_or_default(opts, "fft_size", 512);
    bs->bs_spectral_resolution = htsmsg_get_u32_or_default(opts, "resolution", 0);
    bs->bs_peak_detect = htsmsg_get_s32_or_default(opts, "peak_detect", 0);
    bs->bs_diseqc_committed = htsmsg_get_s32_or_default(opts, "diseqc_committed", -1);
    bs->bs_diseqc_uncommitted = htsmsg_get_s32_or_default(opts, "diseqc_uncommitted", -1);
  }

  bs->bs_state = BLINDSCAN_STATE_ACQUIRING;  /* Set before thread starts to avoid race */
  LIST_INIT(&bs->bs_peaks);
  tvh_mutex_init(&bs->bs_lock, NULL);

  /* Add to session list */
  tvh_mutex_lock(&blindscan_mutex);
  LIST_INSERT_HEAD(&blindscan_sessions, bs, bs_link);
  tvh_mutex_unlock(&blindscan_mutex);

  /* Start worker thread */
  bs->bs_thread_running = 1;
  tvh_thread_create(&bs->bs_thread, NULL, blindscan_worker, bs, "blindscan");

  tvhinfo(LS_BLINDSCAN, "Started blindscan session %s", bs->bs_uuid_hex);

  return bs->bs_uuid_hex;
}

void
linuxdvb_blindscan_cancel(const char *uuid)
{
  blindscan_session_t *bs = linuxdvb_blindscan_find(uuid);
  if (!bs) return;

  tvhinfo(LS_BLINDSCAN, "Cancelling blindscan session %s", uuid);
  bs->bs_should_stop = 1;
}

void
linuxdvb_blindscan_release(const char *uuid)
{
  blindscan_session_t *bs;

  tvh_mutex_lock(&blindscan_mutex);
  bs = NULL;
  blindscan_session_t *tmp;
  LIST_FOREACH(tmp, &blindscan_sessions, bs_link) {
    if (strcmp(tmp->bs_uuid_hex, uuid) == 0) {
      bs = tmp;
      break;
    }
  }

  if (!bs) {
    tvh_mutex_unlock(&blindscan_mutex);
    return;
  }

  tvhinfo(LS_BLINDSCAN, "Releasing blindscan session %s", uuid);

  /* Stop thread if running */
  bs->bs_should_stop = 1;
  if (bs->bs_thread_running) {
    tvh_mutex_unlock(&blindscan_mutex);
    pthread_join(bs->bs_thread, NULL);
    tvh_mutex_lock(&blindscan_mutex);
  }

  /* Remove from list and clean up */
  LIST_REMOVE(bs, bs_link);

  blindscan_peaks_free(bs);
  blindscan_spectrum_free(bs->bs_spectrum_h_low);
  blindscan_spectrum_free(bs->bs_spectrum_h_high);
  blindscan_spectrum_free(bs->bs_spectrum_v_low);
  blindscan_spectrum_free(bs->bs_spectrum_v_high);
  free(bs->bs_status_msg);
  tvh_mutex_destroy(&bs->bs_lock);
  free(bs);

  tvh_mutex_unlock(&blindscan_mutex);
}

htsmsg_t *
linuxdvb_blindscan_status(const char *uuid)
{
  blindscan_session_t *bs = linuxdvb_blindscan_find(uuid);
  if (!bs) return NULL;

  htsmsg_t *m = htsmsg_create_map();

  const char *state_str;
  switch (bs->bs_state) {
    case BLINDSCAN_STATE_IDLE:       state_str = "idle"; break;
    case BLINDSCAN_STATE_ACQUIRING:  state_str = "acquiring"; break;
    case BLINDSCAN_STATE_SCANNING:   state_str = "scanning"; break;
    case BLINDSCAN_STATE_COMPLETE:   state_str = "complete"; break;
    case BLINDSCAN_STATE_CANCELLED:  state_str = "cancelled"; break;
    case BLINDSCAN_STATE_ERROR:      state_str = "error"; break;
    default:                         state_str = "unknown"; break;
  }

  htsmsg_add_str(m, "uuid", bs->bs_uuid_hex);
  htsmsg_add_str(m, "state", state_str);
  htsmsg_add_u32(m, "progress", bs->bs_progress);
  if (bs->bs_status_msg)
    htsmsg_add_str(m, "message", bs->bs_status_msg);
  htsmsg_add_u32(m, "peak_count", bs->bs_peak_count);
  htsmsg_add_u32(m, "current_peak", bs->bs_current_peak);
  htsmsg_add_u32(m, "muxes_created", bs->bs_muxes_created);
  htsmsg_add_u32(m, "muxes_locked", bs->bs_muxes_locked);
  htsmsg_add_s64(m, "duration_ms", bs->bs_duration_ms);

  return m;
}

htsmsg_t *
linuxdvb_blindscan_spectrum(const char *uuid, char pol, int band)
{
  blindscan_session_t *bs = linuxdvb_blindscan_find(uuid);
  if (!bs) return NULL;

  blindscan_spectrum_data_t *sd = NULL;

  if (pol == 'H' || pol == 'h') {
    sd = (band == 0) ? bs->bs_spectrum_h_low : bs->bs_spectrum_h_high;
  } else {
    sd = (band == 0) ? bs->bs_spectrum_v_low : bs->bs_spectrum_v_high;
  }

  if (!sd || sd->num_points == 0) return NULL;

  htsmsg_t *m = htsmsg_create_map();
  htsmsg_t *points = htsmsg_create_list();

  for (size_t i = 0; i < sd->num_points; i++) {
    htsmsg_t *pt = htsmsg_create_map();
    htsmsg_add_u32(pt, "f", sd->points[i].frequency);
    htsmsg_add_s32(pt, "l", sd->points[i].level);
    htsmsg_add_msg(points, NULL, pt);
  }

  htsmsg_add_msg(m, "points", points);
  htsmsg_add_u32(m, "count", sd->num_points);
  htsmsg_add_str(m, "pol", pol == 'H' || pol == 'h' ? "H" : "V");
  htsmsg_add_u32(m, "band", band);

  return m;
}

htsmsg_t *
linuxdvb_blindscan_peaks(const char *uuid)
{
  blindscan_session_t *bs = linuxdvb_blindscan_find(uuid);
  if (!bs) return NULL;

  htsmsg_t *m = htsmsg_create_map();
  htsmsg_t *peaks = htsmsg_create_list();
  blindscan_peak_t *peak;
  dvb_network_t *ln = (dvb_network_t *)bs->bs_network;

  LIST_FOREACH(peak, &bs->bs_peaks, bp_link) {
    htsmsg_t *p = htsmsg_create_map();
    htsmsg_add_u32(p, "frequency", peak->bp_frequency);
    htsmsg_add_u32(p, "symbol_rate", peak->bp_symbol_rate);
    htsmsg_add_s32(p, "level", peak->bp_level);
    htsmsg_add_s32(p, "snr", peak->bp_snr);
    htsmsg_add_str(p, "polarisation",
                   peak->bp_polarisation == DVB_POLARISATION_VERTICAL ? "V" : "H");

    /* Check if peak matches an existing mux */
    int existing = 0;
    int existing_failed = 0;
    if (ln && peak->bp_status == BLINDSCAN_PEAK_PENDING) {
      mpegts_mux_t *mm;
      int found_ok = 0, found_fail = 0;
      LIST_FOREACH(mm, &((mpegts_network_t *)ln)->mn_muxes, mm_network_link) {
        dvb_mux_t *dm = (dvb_mux_t *)mm;
        /* Max freq offset = SR/2000 kHz (based on symbol rate bandwidth) */
        uint32_t mux_sr = dm->lm_tuning.u.dmc_fe_qpsk.symbol_rate;
        uint32_t max_offset = mux_sr > 0 ? mux_sr / 2000 : 3000;
        if (max_offset < 1000) max_offset = 1000;  /* Minimum 1 MHz */

        if (abs((int)dm->lm_tuning.dmc_fe_freq - (int)peak->bp_frequency) < (int)max_offset &&
            dm->lm_tuning.u.dmc_fe_qpsk.polarisation == peak->bp_polarisation) {
          /* Check scan result - prioritize OK over failed */
          if (mm->mm_scan_result == MM_SCAN_OK)
            found_ok = 1;
          else
            found_fail = 1;
        }
      }
      /* Only mark as existing if at least one mux scanned OK */
      if (found_ok) {
        existing = 1;
        peak->bp_status = BLINDSCAN_PEAK_SKIPPED;
      } else if (found_fail) {
        existing_failed = 1;
      }
    }

    const char *status_str;
    switch (peak->bp_status) {
      case BLINDSCAN_PEAK_PENDING:
        status_str = existing_failed ? "retry" : "pending";
        break;
      case BLINDSCAN_PEAK_SCANNING: status_str = "scanning"; break;
      case BLINDSCAN_PEAK_LOCKED:   status_str = "locked"; break;
      case BLINDSCAN_PEAK_FAILED:   status_str = "failed"; break;
      case BLINDSCAN_PEAK_SKIPPED:  status_str = "existing"; break;
      default:                      status_str = "unknown"; break;
    }
    htsmsg_add_str(p, "status", status_str);
    if (existing)
      htsmsg_add_bool(p, "existing", 1);
    if (existing_failed)
      htsmsg_add_bool(p, "has_failed_mux", 1);

    /* Include verified mux info if available */
    if (peak->bp_verified_freq > 0) {
      htsmsg_add_u32(p, "verified_freq", peak->bp_verified_freq);
      htsmsg_add_u32(p, "verified_sr", peak->bp_verified_sr);
    }

    if (peak->bp_status == BLINDSCAN_PEAK_LOCKED) {
      htsmsg_add_u32(p, "actual_freq", peak->bp_actual_freq);
      htsmsg_add_u32(p, "actual_sr", peak->bp_actual_sr);
      htsmsg_add_u32(p, "tsid", peak->bp_tsid);
      htsmsg_add_u32(p, "onid", peak->bp_onid);
      htsmsg_add_u32(p, "services", peak->bp_service_count);

      /* Add prescan parameters */
      htsmsg_add_str(p, "delsys", peak->bp_delsys == SYS_DVBS2 ? "DVB-S2" : "DVB-S");

      /* Modulation - convert Linux DVB value to string */
      const char *mod_str = "AUTO";
      switch (peak->bp_modulation) {
        case QPSK:    mod_str = "QPSK"; break;
        case PSK_8:   mod_str = "8PSK"; break;
        case APSK_16: mod_str = "16APSK"; break;
        case APSK_32: mod_str = "32APSK"; break;
#ifdef APSK_64
        case APSK_64: mod_str = "64APSK"; break;
#endif
      }
      htsmsg_add_str(p, "modulation", mod_str);

      /* FEC - convert Linux DVB value to string */
      const char *fec_str = "AUTO";
      switch (peak->bp_fec) {
        case FEC_1_2: fec_str = "1/2"; break;
        case FEC_2_3: fec_str = "2/3"; break;
        case FEC_3_4: fec_str = "3/4"; break;
        case FEC_4_5: fec_str = "4/5"; break;
        case FEC_5_6: fec_str = "5/6"; break;
        case FEC_6_7: fec_str = "6/7"; break;
        case FEC_7_8: fec_str = "7/8"; break;
        case FEC_8_9: fec_str = "8/9"; break;
        case FEC_3_5: fec_str = "3/5"; break;
#ifdef FEC_9_10
        case FEC_9_10: fec_str = "9/10"; break;
#endif
      }
      htsmsg_add_str(p, "fec", fec_str);

      /* Rolloff */
      const char *rolloff_str = "AUTO";
      switch (peak->bp_rolloff) {
        case ROLLOFF_35: rolloff_str = "35"; break;
        case ROLLOFF_25: rolloff_str = "25"; break;
        case ROLLOFF_20: rolloff_str = "20"; break;
#ifdef ROLLOFF_15
        case ROLLOFF_15: rolloff_str = "15"; break;
#endif
#ifdef ROLLOFF_10
        case ROLLOFF_10: rolloff_str = "10"; break;
#endif
#ifdef ROLLOFF_5
        case ROLLOFF_5: rolloff_str = "5"; break;
#endif
      }
      htsmsg_add_str(p, "rolloff", rolloff_str);

      /* Pilot */
      const char *pilot_str = "AUTO";
      switch (peak->bp_pilot) {
        case PILOT_ON:  pilot_str = "ON"; break;
        case PILOT_OFF: pilot_str = "OFF"; break;
      }
      htsmsg_add_str(p, "pilot", pilot_str);

      /* Stream ID (ISI) */
      htsmsg_add_s32(p, "stream_id", peak->bp_stream_id);

      /* PLS */
      const char *pls_mode_str = "ROOT";
      if (peak->bp_pls_mode == 1) pls_mode_str = "GOLD";
      else if (peak->bp_pls_mode == 2) pls_mode_str = "COMBO";
      htsmsg_add_str(p, "pls_mode", pls_mode_str);
      htsmsg_add_s32(p, "pls_code", peak->bp_pls_code);
    }

    htsmsg_add_msg(peaks, NULL, p);
  }

  htsmsg_add_msg(m, "peaks", peaks);
  htsmsg_add_u32(m, "count", bs->bs_peak_count);

  return m;
}

int
linuxdvb_blindscan_create_muxes(const char *uuid)
{
  blindscan_session_t *bs = linuxdvb_blindscan_find(uuid);
  blindscan_peak_t *peak;
  dvb_network_t *ln;
  dvb_mux_t *dm;
  dvb_mux_conf_t dmc;
  int created = 0;

  if (!bs || !bs->bs_network) return 0;

  ln = (dvb_network_t *)bs->bs_network;

  tvhinfo(LS_BLINDSCAN, "Creating muxes from %d detected peaks", bs->bs_peak_count);

  LIST_FOREACH(peak, &bs->bs_peaks, bp_link) {
    /* Skip peaks that match existing muxes or failed */
    if (peak->bp_status == BLINDSCAN_PEAK_SKIPPED ||
        peak->bp_status == BLINDSCAN_PEAK_FAILED)
      continue;

    /* Initialize mux config - use actual values from prescan if available */
    memset(&dmc, 0, sizeof(dmc));
    dmc.dmc_fe_type = DVB_TYPE_S;
    dmc.dmc_fe_freq = peak->bp_frequency;  /* Already in kHz */
    dmc.u.dmc_fe_qpsk.polarisation = peak->bp_polarisation;
    dmc.u.dmc_fe_qpsk.symbol_rate = peak->bp_symbol_rate;

    /* Use actual values from prescan if peak was locked */
    if (peak->bp_status == BLINDSCAN_PEAK_LOCKED && peak->bp_delsys != 0) {
      dmc.dmc_fe_delsys = (peak->bp_delsys == SYS_DVBS2) ? DVB_SYS_DVBS2 : DVB_SYS_DVBS;
      dmc.dmc_fe_modulation = blindscan_convert_modulation(peak->bp_modulation);
      dmc.u.dmc_fe_qpsk.fec_inner = blindscan_convert_fec(peak->bp_fec);
      dmc.dmc_fe_rolloff = blindscan_convert_rolloff(peak->bp_rolloff);
      dmc.dmc_fe_pilot = blindscan_convert_pilot(peak->bp_pilot);
      dmc.dmc_fe_stream_id = peak->bp_stream_id;
      dmc.dmc_fe_pls_mode = peak->bp_pls_mode;  /* Already 0=ROOT, 1=GOLD, 2=COMBO */
      dmc.dmc_fe_pls_code = peak->bp_pls_code;
    } else {
      /* Defaults if no prescan data */
      dmc.dmc_fe_delsys = DVB_SYS_DVBS2;
      dmc.dmc_fe_modulation = DVB_MOD_AUTO;
      dmc.u.dmc_fe_qpsk.fec_inner = DVB_FEC_AUTO;
      dmc.dmc_fe_rolloff = DVB_ROLLOFF_AUTO;
      dmc.dmc_fe_pilot = DVB_PILOT_AUTO;
      dmc.dmc_fe_stream_id = DVB_NO_STREAM_ID_FILTER;
      dmc.dmc_fe_pls_mode = DVB_PLS_ROOT;
      dmc.dmc_fe_pls_code = 1;
    }

    /* Check if mux already exists */
    dm = dvb_network_find_mux(ln, &dmc, MPEGTS_ONID_NONE, MPEGTS_TSID_NONE, 0, 1);
    if (dm) {
      peak->bp_status = BLINDSCAN_PEAK_SKIPPED;
      continue;
    }

    /* Create mux */
    dm = dvb_mux_create0(ln, MPEGTS_ONID_NONE, MPEGTS_TSID_NONE, &dmc, NULL, NULL);
    if (!dm) {
      tvhwarn(LS_BLINDSCAN, "Failed to create mux for peak %u kHz SR %u",
              peak->bp_frequency, peak->bp_symbol_rate);
      peak->bp_status = BLINDSCAN_PEAK_FAILED;
      continue;
    }

    if (peak->bp_status == BLINDSCAN_PEAK_LOCKED && peak->bp_delsys != 0) {
      tvhinfo(LS_BLINDSCAN, "Created mux: %u kHz, SR %u, %s, mod=%d, fec=%d, rolloff=%d, ISI=%d",
              peak->bp_frequency, peak->bp_symbol_rate,
              peak->bp_delsys == SYS_DVBS2 ? "DVB-S2" : "DVB-S",
              peak->bp_modulation, peak->bp_fec, peak->bp_rolloff, peak->bp_stream_id);
    } else {
      tvhinfo(LS_BLINDSCAN, "Created mux: %u kHz, SR %u, pol %c (no prescan data)",
              peak->bp_frequency, peak->bp_symbol_rate,
              peak->bp_polarisation == DVB_POLARISATION_VERTICAL ? 'V' : 'H');
    }

    /* Queue for scanning */
    mpegts_network_scan_queue_add((mpegts_mux_t *)dm, SUBSCRIPTION_PRIO_SCAN_USER,
                                  SUBSCRIPTION_USERSCAN, 10);

    peak->bp_status = BLINDSCAN_PEAK_SCANNING;
    created++;
  }

  bs->bs_muxes_created = created;
  tvhinfo(LS_BLINDSCAN, "Created %d muxes from peaks", created);

  return created;
}

/**
 * Helper to convert modulation string to enum
 */
static int
blindscan_parse_modulation(const char *mod)
{
  if (!mod) return DVB_MOD_AUTO;
  if (!strcmp(mod, "QPSK")) return DVB_MOD_QPSK;
  if (!strcmp(mod, "8PSK")) return DVB_MOD_PSK_8;
  if (!strcmp(mod, "16APSK")) return DVB_MOD_APSK_16;
  if (!strcmp(mod, "32APSK")) return DVB_MOD_APSK_32;
  if (!strcmp(mod, "64APSK")) return DVB_MOD_APSK_64;
  if (!strcmp(mod, "128APSK")) return DVB_MOD_APSK_128;
  if (!strcmp(mod, "256APSK")) return DVB_MOD_APSK_256;
  return DVB_MOD_AUTO;
}

/**
 * Helper to convert FEC string to enum
 */
static int
blindscan_parse_fec(const char *fec)
{
  if (!fec) return DVB_FEC_AUTO;
  if (!strcmp(fec, "1/2")) return DVB_FEC_1_2;
  if (!strcmp(fec, "2/3")) return DVB_FEC_2_3;
  if (!strcmp(fec, "3/4")) return DVB_FEC_3_4;
  if (!strcmp(fec, "4/5")) return DVB_FEC_4_5;
  if (!strcmp(fec, "5/6")) return DVB_FEC_5_6;
  if (!strcmp(fec, "6/7")) return DVB_FEC_6_7;
  if (!strcmp(fec, "7/8")) return DVB_FEC_7_8;
  if (!strcmp(fec, "8/9")) return DVB_FEC_8_9;
  if (!strcmp(fec, "9/10")) return DVB_FEC_9_10;
  if (!strcmp(fec, "3/5")) return DVB_FEC_3_5;
  return DVB_FEC_AUTO;
}

/**
 * Helper to convert PLS mode string to enum
 */
static int
blindscan_parse_pls_mode(const char *mode)
{
  if (!mode) return DVB_PLS_ROOT;
  if (!strcmp(mode, "GOLD")) return DVB_PLS_GOLD;
  if (!strcmp(mode, "COMBO")) return DVB_PLS_COMBO;
  return DVB_PLS_ROOT;
}

/**
 * Helper to parse rolloff string
 */
static int
blindscan_parse_rolloff(const char *rolloff)
{
  if (!rolloff) return DVB_ROLLOFF_AUTO;
  if (!strcmp(rolloff, "35")) return DVB_ROLLOFF_35;
  if (!strcmp(rolloff, "25")) return DVB_ROLLOFF_25;
  if (!strcmp(rolloff, "20")) return DVB_ROLLOFF_20;
  if (!strcmp(rolloff, "15")) return DVB_ROLLOFF_15;
  if (!strcmp(rolloff, "10")) return DVB_ROLLOFF_10;
  if (!strcmp(rolloff, "5")) return DVB_ROLLOFF_5;
  return DVB_ROLLOFF_AUTO;
}

/**
 * Helper to parse pilot string
 */
static int
blindscan_parse_pilot(const char *pilot)
{
  if (!pilot) return DVB_PILOT_AUTO;
  if (!strcmp(pilot, "ON")) return DVB_PILOT_ON;
  if (!strcmp(pilot, "OFF")) return DVB_PILOT_OFF;
  return DVB_PILOT_AUTO;
}

/**
 * Helper to convert Linux DVB modulation to TVH enum
 */
static int
blindscan_convert_modulation(int linux_mod)
{
  switch (linux_mod) {
    case QPSK:     return DVB_MOD_QPSK;
    case PSK_8:    return DVB_MOD_PSK_8;
    case APSK_16:  return DVB_MOD_APSK_16;
    case APSK_32:  return DVB_MOD_APSK_32;
#ifdef APSK_64
    case APSK_64:  return DVB_MOD_APSK_64;
#endif
#ifdef APSK_128
    case APSK_128: return DVB_MOD_APSK_128;
#endif
#ifdef APSK_256
    case APSK_256: return DVB_MOD_APSK_256;
#endif
    default:       return DVB_MOD_AUTO;
  }
}

/**
 * Helper to convert Linux DVB FEC to TVH enum
 */
static int
blindscan_convert_fec(int linux_fec)
{
  switch (linux_fec) {
    case FEC_1_2:  return DVB_FEC_1_2;
    case FEC_2_3:  return DVB_FEC_2_3;
    case FEC_3_4:  return DVB_FEC_3_4;
    case FEC_4_5:  return DVB_FEC_4_5;
    case FEC_5_6:  return DVB_FEC_5_6;
    case FEC_6_7:  return DVB_FEC_6_7;
    case FEC_7_8:  return DVB_FEC_7_8;
    case FEC_8_9:  return DVB_FEC_8_9;
    case FEC_3_5:  return DVB_FEC_3_5;
#ifdef FEC_9_10
    case FEC_9_10: return DVB_FEC_9_10;
#endif
    default:       return DVB_FEC_AUTO;
  }
}

/**
 * Helper to convert Linux DVB rolloff to TVH enum
 */
static int
blindscan_convert_rolloff(int linux_rolloff)
{
  switch (linux_rolloff) {
    case ROLLOFF_35:  return DVB_ROLLOFF_35;
    case ROLLOFF_25:  return DVB_ROLLOFF_25;
    case ROLLOFF_20:  return DVB_ROLLOFF_20;
#ifdef ROLLOFF_15
    case ROLLOFF_15:  return DVB_ROLLOFF_15;
#endif
#ifdef ROLLOFF_10
    case ROLLOFF_10:  return DVB_ROLLOFF_10;
#endif
#ifdef ROLLOFF_5
    case ROLLOFF_5:   return DVB_ROLLOFF_5;
#endif
    default:          return DVB_ROLLOFF_AUTO;
  }
}

/**
 * Helper to convert Linux DVB pilot to TVH enum
 */
static int
blindscan_convert_pilot(int linux_pilot)
{
  switch (linux_pilot) {
    case PILOT_ON:  return DVB_PILOT_ON;
    case PILOT_OFF: return DVB_PILOT_OFF;
    default:        return DVB_PILOT_AUTO;
  }
}

int
linuxdvb_blindscan_create_muxes_selected(const char *uuid, htsmsg_t *selected_peaks)
{
  blindscan_session_t *bs = linuxdvb_blindscan_find(uuid);
  dvb_network_t *ln;
  dvb_mux_t *dm;
  dvb_mux_conf_t dmc;
  int created = 0;
  htsmsg_t *item;
  htsmsg_field_t *f;

  if (!bs || !bs->bs_network) return 0;
  if (!selected_peaks) return 0;

  ln = (dvb_network_t *)bs->bs_network;

  tvhinfo(LS_BLINDSCAN, "Creating muxes from selected entries");

  /* Iterate through selected peaks from UI */
  HTSMSG_FOREACH(f, selected_peaks) {
    if (!(item = htsmsg_field_get_map(f)))
      continue;

    /* Read parameters from UI selection */
    uint32_t freq = htsmsg_get_u32_or_default(item, "frequency", 0);
    const char *pol_str = htsmsg_get_str(item, "polarisation");
    uint32_t symbol_rate = htsmsg_get_u32_or_default(item, "symbol_rate", 0);
    const char *mod_str = htsmsg_get_str(item, "modulation");
    const char *fec_str = htsmsg_get_str(item, "fec");
    const char *delsys_str = htsmsg_get_str(item, "delsys");
    const char *rolloff_str = htsmsg_get_str(item, "rolloff");
    const char *pilot_str = htsmsg_get_str(item, "pilot");
    int32_t stream_id = htsmsg_get_s32_or_default(item, "stream_id", -1);
    const char *pls_mode_str = htsmsg_get_str(item, "pls_mode");
    int32_t pls_code = htsmsg_get_s32_or_default(item, "pls_code", 0);
    int is_gse = htsmsg_get_bool_or_default(item, "is_gse", 0);

    if (freq == 0) continue;

    /* Determine polarisation */
    int polarisation = DVB_POLARISATION_HORIZONTAL;
    if (pol_str && pol_str[0] == 'V')
      polarisation = DVB_POLARISATION_VERTICAL;

    /* Initialize mux config */
    memset(&dmc, 0, sizeof(dmc));
    dmc.dmc_fe_type = DVB_TYPE_S;

    /* Set delivery system - DVB-S or DVB-S2 */
    if (delsys_str && strstr(delsys_str, "S2"))
      dmc.dmc_fe_delsys = DVB_SYS_DVBS2;
    else if (delsys_str && !strcmp(delsys_str, "DVB-S"))
      dmc.dmc_fe_delsys = DVB_SYS_DVBS;
    else
      dmc.dmc_fe_delsys = DVB_SYS_DVBS2;  /* Default to S2 */

    dmc.dmc_fe_modulation = blindscan_parse_modulation(mod_str);
    dmc.dmc_fe_rolloff = blindscan_parse_rolloff(rolloff_str);
    dmc.dmc_fe_pilot = blindscan_parse_pilot(pilot_str);
    dmc.dmc_fe_freq = freq;

    /* Stream ID (ISI) - for multistream */
    if (stream_id >= 0)
      dmc.dmc_fe_stream_id = stream_id;
    else
      dmc.dmc_fe_stream_id = DVB_NO_STREAM_ID_FILTER;

    /* PLS (Physical Layer Scrambling) */
    dmc.dmc_fe_pls_mode = blindscan_parse_pls_mode(pls_mode_str);
    dmc.dmc_fe_pls_code = (pls_code >= 0 && pls_code < 262144) ? pls_code : 0;

    dmc.u.dmc_fe_qpsk.polarisation = polarisation;
    dmc.u.dmc_fe_qpsk.symbol_rate = symbol_rate > 0 ? symbol_rate : 27500000;
    dmc.u.dmc_fe_qpsk.fec_inner = blindscan_parse_fec(fec_str);

    /* Check if mux already exists with same parameters */
    dm = dvb_network_find_mux(ln, &dmc, MPEGTS_ONID_NONE, MPEGTS_TSID_NONE, 0, 1);
    if (dm) {
      tvhdebug(LS_BLINDSCAN, "Mux already exists: %u kHz pol=%c SR=%u ISI=%d",
               freq, pol_str ? pol_str[0] : '?', symbol_rate, stream_id);
      continue;
    }

    /* Create mux */
    dm = dvb_mux_create0(ln, MPEGTS_ONID_NONE, MPEGTS_TSID_NONE, &dmc, NULL, NULL);
    if (!dm) {
      tvhwarn(LS_BLINDSCAN, "Failed to create mux: %u kHz pol=%c SR=%u",
              freq, pol_str ? pol_str[0] : '?', symbol_rate);
      continue;
    }

    /* Set stream type based on matype detection */
    if (is_gse) {
      ((mpegts_mux_t *)dm)->mm_type = MM_TYPE_GSE;
    }

    tvhinfo(LS_BLINDSCAN, "Created mux: %u kHz, %s %s, ISI=%d, PLS=%s:%d, %s",
            freq, delsys_str ? delsys_str : "DVB-S2",
            mod_str ? mod_str : "AUTO", stream_id,
            pls_mode_str ? pls_mode_str : "ROOT", pls_code,
            is_gse ? "GSE" : "TS");

    /* Queue for scanning */
    mpegts_network_scan_queue_add((mpegts_mux_t *)dm, SUBSCRIPTION_PRIO_SCAN_USER,
                                  SUBSCRIPTION_USERSCAN, 10);

    created++;
  }

  bs->bs_muxes_created += created;
  tvhinfo(LS_BLINDSCAN, "Created %d muxes", created);

  return created;
}

htsmsg_t *
linuxdvb_blindscan_prescan(const char *uuid, uint32_t frequency, char pol)
{
  blindscan_session_t *bs = linuxdvb_blindscan_find(uuid);
  if (!bs || !bs->bs_frontend) return NULL;

  linuxdvb_frontend_t *lfe = bs->bs_frontend;
  linuxdvb_satconf_ele_t *lse = bs->bs_satconf_ele;
  htsmsg_t *result = htsmsg_create_map();
  int fd = -1;
  int locked = 0;

  /* Find matching peak to get estimated symbol rate */
  uint32_t est_symbol_rate = 22000000;  /* Default 22 Msym/s */
  blindscan_peak_t *peak;
  LIST_FOREACH(peak, &bs->bs_peaks, bp_link) {
    if (abs((int)peak->bp_frequency - (int)frequency) < 2000 &&
        ((pol == 'H' || pol == 'h') ?
         peak->bp_polarisation == DVB_POLARISATION_HORIZONTAL :
         peak->bp_polarisation == DVB_POLARISATION_VERTICAL)) {
      est_symbol_rate = peak->bp_symbol_rate > 0 ? peak->bp_symbol_rate : 22000000;
      break;
    }
  }

  tvhinfo(LS_BLINDSCAN, "Prescan: freq=%u kHz, pol=%c, est_sr=%u",
          frequency, pol, est_symbol_rate);

  /* Use frontend's existing fd */
  fd = lfe->lfe_fe_fd;
  if (fd <= 0) {
    tvherror(LS_BLINDSCAN, "Frontend not open for prescan (fd=%d)", fd);
    htsmsg_add_bool(result, "locked", 0);
    htsmsg_add_str(result, "error", "Frontend not available");
    return result;
  }

  /* Determine band and polarisation */
  int band = blindscan_band_for_freq(frequency);
  int pol_is_v = (pol == 'V' || pol == 'v');
  uint32_t lof = band ? BLINDSCAN_LNB_LOF_HIGH : BLINDSCAN_LNB_LOF_LOW;
  int32_t driver_freq;

  /* Check for unicable setup */
  if (lse && lse->lse_en50494) {
    linuxdvb_en50494_t *uc = (linuxdvb_en50494_t *)lse->lse_en50494;
    uint32_t lnb_if = frequency - lof;  /* LNB IF frequency in kHz */
    uint32_t scr_freq = uc->le_frequency * 1000;  /* SCR IF freq: MHz -> kHz */
    int scr_id = uc->le_id;
    int pos = uc->le_position;

    tvhinfo(LS_BLINDSCAN, "Prescan unicable: freq=%u, lnb_if=%u, scr=%d, scr_freq=%u, pos=%d",
            frequency, lnb_if, scr_id, scr_freq, pos);

    /* Send unicable ODU command - pass transponder frequency (kHz) */
    lse->lse_parent->ls_frontend = (mpegts_input_t *)lfe;
    if (blindscan_send_unicable_command(lse, frequency, pol_is_v, band) < 0) {
      tvherror(LS_BLINDSCAN, "Failed to send unicable command");
      htsmsg_add_bool(result, "locked", 0);
      htsmsg_add_str(result, "error", "Unicable command failed");
      return result;
    }

    /* For unicable, tune to SCR IF frequency */
    driver_freq = scr_freq;
  } else {
    /* Standard LNB: Configure voltage/tone */
    if (lse) {
      lse->lse_parent->ls_frontend = (mpegts_input_t *)lfe;
      if (blindscan_configure_lnb(fd, lse, band, pol_is_v, bs) < 0) {
        tvherror(LS_BLINDSCAN, "Failed to configure LNB");
        htsmsg_add_bool(result, "locked", 0);
        htsmsg_add_str(result, "error", "Failed to configure LNB");
        return result;
      }
    }
    /* For standard LNB, tune to LNB IF frequency */
    driver_freq = frequency - lof;
  }

  /* Set up blind tune with Neumo driver extensions */
  /* Order matters! Match lsdvb/neumo-tune exactly:
   * 1. DTV_ALGORITHM
   * 2. DTV_DELIVERY_SYSTEM
   * 3. DTV_SEARCH_RANGE (must come BEFORE frequency)
   * 4. DTV_SYMBOL_RATE
   * 5. DTV_FREQUENCY (must come AFTER search_range)
   * 6. DTV_STREAM_ID
   * 7. DTV_PLS_SEARCH_LIST
   */
  struct dtv_property props[16];
  struct dtv_properties cmdseq = { .num = 0, .props = props };
  int n = 0;

  /* Clear */
  props[n].cmd = DTV_CLEAR;
  props[n++].u.data = 0;

  cmdseq.num = n;
  if (ioctl(fd, FE_SET_PROPERTY, &cmdseq) < 0) {
    tvhwarn(LS_BLINDSCAN, "DTV_CLEAR failed: %s", strerror(errno));
  }

  n = 0;

  /* Algorithm = BLIND for auto-detection */
  props[n].cmd = DTV_ALGORITHM;
  props[n++].u.data = ALGORITHM_BLIND;

  /* Delivery system = AUTO */
  props[n].cmd = DTV_DELIVERY_SYSTEM;
  props[n++].u.data = SYS_AUTO;

  /* Search range - half SR for large transponders, 8 MHz floor for small ones */
  uint32_t search_range = (est_symbol_rate > 8000000) ? est_symbol_rate / 2 : 8000000;
  props[n].cmd = DTV_SEARCH_RANGE;
  props[n++].u.data = search_range;

  /* Symbol rate (estimated, in symbols/sec) */
  if (est_symbol_rate > 0) {
    props[n].cmd = DTV_SYMBOL_RATE;
    props[n++].u.data = est_symbol_rate;
  }

  /* Frequency (in kHz for stid135 driver) - must come after search_range */
  props[n].cmd = DTV_FREQUENCY;
  props[n++].u.data = driver_freq;

  /* Stream ID = -1 for auto */
  props[n].cmd = DTV_STREAM_ID;
  props[n++].u.data = -1;

  /* PLS search list - common codes to try */
  static uint32_t pls_codes[] = {
    (0 << 26) | (0 << 8),       /* ROOT code 0 */
    (0 << 26) | (1 << 8),       /* ROOT code 1 */
    (0 << 26) | (8 << 8),       /* ROOT code 8 */
    (0 << 26) | (16416 << 8),   /* ROOT code 16416 */
    (1 << 26) | (0 << 8),       /* GOLD code 0 */
    (1 << 26) | (8192 << 8),    /* GOLD code 8192 */
  };
  props[n].cmd = DTV_PLS_SEARCH_LIST;
  props[n].u.pls_search_codes.num_codes = sizeof(pls_codes) / sizeof(pls_codes[0]);
  props[n].u.pls_search_codes.codes = pls_codes;
  n++;

  /* Trigger the tune */
  props[n].cmd = DTV_TUNE;
  props[n++].u.data = 0;

  cmdseq.num = n;

  tvhdebug(LS_BLINDSCAN, "Blind tune: freq=%d kHz, SR=%u, search_range=%u",
           driver_freq, est_symbol_rate, search_range);

  if (ioctl(fd, FE_SET_PROPERTY, &cmdseq) < 0) {
    tvherror(LS_BLINDSCAN, "FE_SET_PROPERTY failed: %s", strerror(errno));
    htsmsg_add_bool(result, "locked", 0);
    htsmsg_add_str(result, "error", "Tune failed");
    return result;
  }

  /* Wait for lock with epoll (up to 12 seconds for blind tune) */
  int efd = epoll_create1(0);
  if (efd >= 0) {
    struct epoll_event ev = { .events = EPOLLIN | EPOLLPRI, .data.fd = fd };
    epoll_ctl(efd, EPOLL_CTL_ADD, fd, &ev);

    struct epoll_event events[1];
    int timeout_ms = 12000;
    int64_t start = getmonoclock();

    while (!locked && (getmonoclock() - start) < (timeout_ms * 1000LL)) {
      int remaining = timeout_ms - (int)((getmonoclock() - start) / 1000);
      if (remaining <= 0) break;

      int nfds = epoll_wait(efd, events, 1, remaining);
      if (nfds > 0) {
        struct dvb_frontend_event fe_ev;
        if (ioctl(fd, FE_GET_EVENT, &fe_ev) == 0) {
          /* Require full lock with sync, not just carrier lock */
          if ((fe_ev.status & (FE_HAS_LOCK | FE_HAS_SYNC)) == (FE_HAS_LOCK | FE_HAS_SYNC)) {
            locked = 1;
            tvhinfo(LS_BLINDSCAN, "Prescan locked with sync in %lld ms",
                    (long long)(getmonoclock() - start) / 1000);
          } else if (fe_ev.status & FE_HAS_LOCK) {
            tvhdebug(LS_BLINDSCAN, "Prescan carrier lock but no sync (status=0x%x)",
                     fe_ev.status);
          }
        }
      }
    }

    close(efd);
  }

  htsmsg_add_bool(result, "locked", locked);

  if (locked) {
    /* Read back actual parameters */
    struct dtv_property get_props[10];
    struct dtv_properties get_cmdseq = { .num = 0, .props = get_props };
    int gi = 0;

    get_props[gi++].cmd = DTV_FREQUENCY;      /* 0 */
    get_props[gi++].cmd = DTV_SYMBOL_RATE;    /* 1 */
    get_props[gi++].cmd = DTV_MODULATION;     /* 2 */
    get_props[gi++].cmd = DTV_INNER_FEC;      /* 3 */
    get_props[gi++].cmd = DTV_DELIVERY_SYSTEM;/* 4 */
    get_props[gi++].cmd = DTV_STREAM_ID;      /* 5 */
    get_props[gi++].cmd = DTV_ROLLOFF;        /* 6 */
    get_props[gi++].cmd = DTV_PILOT;          /* 7 */
#ifdef DTV_MATYPE
    get_props[gi++].cmd = DTV_MATYPE;         /* 8 */
#endif
    get_cmdseq.num = gi;

    if (ioctl(fd, FE_GET_PROPERTY, &get_cmdseq) == 0) {
      uint32_t actual_freq;
      /* For unicable, driver returns SCR IF freq, not LNB IF - use original peak freq */
      if (lse && lse->lse_en50494) {
        actual_freq = frequency;  /* Use the peak frequency we were tuning to */
      } else {
        actual_freq = get_props[0].u.data + lof;
      }
      uint32_t actual_sr = get_props[1].u.data;
      uint32_t actual_mod = get_props[2].u.data;
      uint32_t actual_fec = get_props[3].u.data;
      uint32_t actual_delsys = get_props[4].u.data;
      int32_t stream_id = get_props[5].u.data;
      uint32_t actual_rolloff = get_props[6].u.data;
      uint32_t actual_pilot = get_props[7].u.data;

      /* Convert driver ISI encoding: 256 = ISI 0, 511 = no ISI filter (-1) */
      if (stream_id == 511)
        stream_id = -1;
      else if (stream_id >= 256 && stream_id < 511)
        stream_id = stream_id - 256;

#ifdef DTV_MATYPE
      uint32_t matype = get_props[8].u.data;
      int pls_mode = (matype >> 26) & 0x3;
      int pls_code = (matype >> 8) & 0x3FFFF;
      /* Extract actual matype byte (low 8 bits) to check TS/GS mode
       * DVB-S2 MATYPE bits 7-6: 11=TS, 10=GS continuous, 01=GS packetized, 00=reserved
       * BUT: Non-multistream transponders (DVB-S, single-stream DVB-S2) return matype=0
       * which would be misinterpreted as GSE. Only trust MATYPE for multistream. */
      uint8_t matype_byte = matype & 0xFF;
      int ts_gs = (matype_byte >> 6) & 0x3;
      /* Only consider it GSE if:
       * 1. It's a multistream transponder (stream_id >= 0), AND
       * 2. ts_gs indicates non-TS mode (not 11/3) */
      int is_gse = (stream_id >= 0 && matype != 0 && ts_gs != 3);

      tvhdebug(LS_BLINDSCAN, "MATYPE raw=0x%08x, byte=0x%02x, ts_gs=%d, stream_id=%d -> %s",
               matype, matype_byte, ts_gs, stream_id, is_gse ? "GSE" : "TS");
#else
      int pls_mode = 0;
      int pls_code = 0;
      int is_gse = 0;
#endif

      htsmsg_add_u32(result, "frequency", actual_freq);
      htsmsg_add_u32(result, "symbol_rate", actual_sr);

      /* Convert modulation enum to string */
      const char *mod_str = "AUTO";
      switch (actual_mod) {
        case QPSK: mod_str = "QPSK"; break;
        case PSK_8: mod_str = "8PSK"; break;
        case APSK_16: mod_str = "16APSK"; break;
        case APSK_32: mod_str = "32APSK"; break;
        default: break;
      }
      htsmsg_add_str(result, "modulation", mod_str);

      /* Convert FEC enum to string */
      const char *fec_str = "AUTO";
      switch (actual_fec) {
        case FEC_1_2: fec_str = "1/2"; break;
        case FEC_2_3: fec_str = "2/3"; break;
        case FEC_3_4: fec_str = "3/4"; break;
        case FEC_4_5: fec_str = "4/5"; break;
        case FEC_5_6: fec_str = "5/6"; break;
        case FEC_6_7: fec_str = "6/7"; break;
        case FEC_7_8: fec_str = "7/8"; break;
        case FEC_8_9: fec_str = "8/9"; break;
        case FEC_9_10: fec_str = "9/10"; break;
        case FEC_3_5: fec_str = "3/5"; break;
        default: break;
      }
      htsmsg_add_str(result, "fec", fec_str);

      /* Rolloff */
      const char *rolloff_str = "AUTO";
      switch (actual_rolloff) {
        case ROLLOFF_35: rolloff_str = "35"; break;
        case ROLLOFF_25: rolloff_str = "25"; break;
        case ROLLOFF_20: rolloff_str = "20"; break;
#ifdef ROLLOFF_15
        case ROLLOFF_15: rolloff_str = "15"; break;
#endif
#ifdef ROLLOFF_10
        case ROLLOFF_10: rolloff_str = "10"; break;
#endif
#ifdef ROLLOFF_5
        case ROLLOFF_5: rolloff_str = "5"; break;
#endif
        default: break;
      }
      htsmsg_add_str(result, "rolloff", rolloff_str);

      /* Pilot */
      const char *pilot_str = "AUTO";
      switch (actual_pilot) {
        case PILOT_ON: pilot_str = "ON"; break;
        case PILOT_OFF: pilot_str = "OFF"; break;
        default: break;
      }
      htsmsg_add_str(result, "pilot", pilot_str);

      /* Delivery system */
      htsmsg_add_str(result, "delsys",
                     actual_delsys == SYS_DVBS2 ? "DVB-S2" : "DVB-S");

      /* Stream ID (ISI) */
      htsmsg_add_s32(result, "stream_id", stream_id);

      /* PLS */
      const char *pls_mode_str = "";
      switch (pls_mode) {
        case 0: pls_mode_str = "ROOT"; break;
        case 1: pls_mode_str = "GOLD"; break;
        case 2: pls_mode_str = "COMBO"; break;
      }
      htsmsg_add_str(result, "pls_mode", pls_mode_str);
      htsmsg_add_s32(result, "pls_code", pls_code);
      htsmsg_add_bool(result, "is_gse", is_gse);

      tvhinfo(LS_BLINDSCAN, "Prescan result: %u kHz, %u sym/s, %s %s, ISI=%d, PLS=%s:%d, %s",
              actual_freq, actual_sr, mod_str, fec_str, stream_id, pls_mode_str, pls_code,
              is_gse ? "GSE" : "TS");

      /* Read ISI list to detect multistream */
#ifdef DTV_ISI_LIST
      struct dtv_property isi_prop;
      isi_prop.cmd = DTV_ISI_LIST;
      struct dtv_properties isi_cmdseq = { .num = 1, .props = &isi_prop };

      if (ioctl(fd, FE_GET_PROPERTY, &isi_cmdseq) == 0 && isi_prop.u.buffer.len > 0) {
        htsmsg_t *isi_list = htsmsg_create_list();
        int isi_count = 0;
        const uint8_t *bitset = isi_prop.u.buffer.data;
        size_t bitset_len = isi_prop.u.buffer.len;
        if (bitset_len > 32) bitset_len = 32;  /* Max 256 bits */

        for (size_t byte_idx = 0; byte_idx < bitset_len; byte_idx++) {
          uint8_t byte_val = bitset[byte_idx];
          for (int bit = 0; bit < 8; bit++) {
            if (byte_val & (1 << bit)) {
              int isi = byte_idx * 8 + bit;
              htsmsg_add_s64(isi_list, NULL, isi);
              isi_count++;
            }
          }
        }

        if (isi_count > 0) {
          htsmsg_add_msg(result, "isi_list", isi_list);
          htsmsg_add_bool(result, "multistream", isi_count > 1);
          tvhinfo(LS_BLINDSCAN, "Detected %d ISI stream(s)", isi_count);
        } else {
          htsmsg_destroy(isi_list);
        }
      }
#endif

      /* Update the peak with detected parameters */
      if (peak) {
        peak->bp_frequency = actual_freq;
        peak->bp_symbol_rate = actual_sr;
        peak->bp_modulation = actual_mod;
        peak->bp_fec = actual_fec;
        peak->bp_delsys = actual_delsys;
        peak->bp_stream_id = stream_id;
        peak->bp_pls_mode = pls_mode;
        peak->bp_pls_code = pls_code;
        peak->bp_rolloff = actual_rolloff;
        peak->bp_pilot = actual_pilot;
        peak->bp_status = BLINDSCAN_PEAK_LOCKED;
      }
    }
  } else {
    /* Update peak status to failed */
    if (peak) {
      peak->bp_status = BLINDSCAN_PEAK_FAILED;
    }
  }

  /* Clear frontend state (don't close - keep for more prescans) */
  struct dtv_property clear_props[1] = {{ .cmd = DTV_CLEAR, .u.data = 0 }};
  struct dtv_properties clear_cmdseq = { .num = 1, .props = clear_props };
  ioctl(fd, FE_SET_PROPERTY, &clear_cmdseq);

  return result;
}
