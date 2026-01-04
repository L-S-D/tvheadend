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
 *
 *  Blindscan support for DVB-S/S2 satellites using Neumo DVB driver
 *  extensions for spectrum acquisition and peak detection.
 *
 *  Supports:
 *  - Neumo driver spectrum acquisition (DTV_SPECTRUM with FFT method)
 *  - Unicable spectrum via frequency stepping (EN50494/EN50607)
 *  - Sweep fallback for generic DVB-S/S2 drivers
 *  - Automatic mux creation from detected transponders
 */

#ifndef __TVH_LINUXDVB_BLINDSCAN_H__
#define __TVH_LINUXDVB_BLINDSCAN_H__

#include "htsmsg.h"
#include "idnode.h"
#include "queue.h"

/* Forward declarations */
typedef struct linuxdvb_frontend  linuxdvb_frontend_t;
typedef struct linuxdvb_satconf_ele linuxdvb_satconf_ele_t;
typedef struct mpegts_network     mpegts_network_t;
typedef struct dvb_mux            dvb_mux_t;

/* ============================================================================
 * Neumo DVB Driver Extensions
 * These are not in standard Linux DVB headers
 * ============================================================================ */

#ifndef DTV_ALGORITHM
#define DTV_ALGORITHM            74
#endif

#ifndef DTV_SEARCH_RANGE
#define DTV_SEARCH_RANGE         75
#endif

#ifndef DTV_SCAN_START_FREQUENCY
#define DTV_SCAN_START_FREQUENCY 79
#endif

#ifndef DTV_SCAN_END_FREQUENCY
#define DTV_SCAN_END_FREQUENCY   80
#endif

#ifndef DTV_SCAN_RESOLUTION
#define DTV_SCAN_RESOLUTION      81
#endif

#ifndef DTV_SCAN_FFT_SIZE
#define DTV_SCAN_FFT_SIZE        82
#endif

#ifndef DTV_SPECTRUM
#define DTV_SPECTRUM             84
#endif

/* Spectrum acquisition method - uses enum from linux/dvb/frontend.h:
 * SPECTRUM_METHOD_SWEEP = 0
 * SPECTRUM_METHOD_FFT = 1
 */

/* ============================================================================
 * LNB Constants (Universal LNB)
 * ============================================================================ */

#define BLINDSCAN_LNB_SLOF        11700000   /* kHz - switch frequency */
#define BLINDSCAN_LNB_LOF_LOW      9750000   /* kHz - low band LOF */
#define BLINDSCAN_LNB_LOF_HIGH    10600000   /* kHz - high band LOF */

/* ============================================================================
 * Data Structures
 * ============================================================================ */

/* Spectrum point (single frequency/level sample) */
typedef struct blindscan_spectrum_point {
  uint32_t frequency;      /* kHz (transponder frequency) */
  int32_t  level;          /* 0.01 dB units */
} blindscan_spectrum_point_t;

/* Spectral peak (candidate from driver) */
typedef struct blindscan_spectral_peak {
  uint32_t frequency;      /* Center frequency kHz (transponder) */
  uint32_t symbol_rate;    /* Estimated symbol rate in symbols/sec */
  int32_t  snr;            /* SNR in 0.001 dB units */
  int32_t  level;          /* Signal level in 0.01 dB units */
} blindscan_spectral_peak_t;

/* Peak status during blind tuning */
typedef enum {
  BLINDSCAN_PEAK_PENDING = 0,   /* Not yet attempted */
  BLINDSCAN_PEAK_SCANNING,      /* Currently being tuned */
  BLINDSCAN_PEAK_LOCKED,        /* Successfully locked */
  BLINDSCAN_PEAK_FAILED,        /* Failed to lock */
  BLINDSCAN_PEAK_SKIPPED        /* Skipped (matches existing mux) */
} blindscan_peak_status_t;

/* Detected peak/transponder */
typedef struct blindscan_peak {
  LIST_ENTRY(blindscan_peak) bp_link;

  uint32_t bp_frequency;        /* Center frequency kHz */
  uint32_t bp_symbol_rate;      /* Estimated symbol rate symbols/s */
  int      bp_polarisation;     /* Polarisation (DVB_POLARISATION_*) */
  int32_t  bp_level;            /* Signal level */
  int32_t  bp_snr;              /* SNR */

  blindscan_peak_status_t bp_status;

  /* Lock result (if locked) */
  uint32_t bp_actual_freq;      /* Actual locked frequency */
  uint32_t bp_actual_sr;        /* Actual symbol rate */
  int      bp_delsys;           /* Delivery system (DVB-S/S2) */
  int      bp_modulation;       /* Modulation */
  int      bp_fec;              /* FEC */
  int      bp_stream_id;        /* ISI (-1 if single stream) */
  int      bp_pls_mode;         /* PLS mode (0=ROOT, 1=GOLD, 2=COMBO) */
  int      bp_pls_code;         /* PLS code */
  int      bp_rolloff;          /* Rolloff */
  int      bp_pilot;            /* Pilot */

  /* SI data (from quick SDT read) */
  uint16_t bp_tsid;             /* Transport stream ID */
  uint16_t bp_onid;             /* Original network ID */
  uint16_t bp_service_count;    /* Number of services */

  /* Created mux reference */
  dvb_mux_t *bp_mux;            /* Mux created from this peak */

  /* Verified mux info (when peak matches existing mux) */
  uint32_t bp_verified_freq;    /* Verified frequency from DB (kHz) */
  uint32_t bp_verified_sr;      /* Verified symbol rate from DB */
} blindscan_peak_t;

LIST_HEAD(blindscan_peak_list, blindscan_peak);

/* Spectrum data container */
typedef struct blindscan_spectrum_data {
  blindscan_spectrum_point_t *points;
  size_t                      num_points;
  size_t                      alloc_points;
  int                         band;         /* 0=low, 1=high */
  char                        polarisation; /* 'H' or 'V' */
} blindscan_spectrum_data_t;

/* Session state */
typedef enum {
  BLINDSCAN_STATE_IDLE = 0,
  BLINDSCAN_STATE_ACQUIRING,    /* Acquiring spectrum */
  BLINDSCAN_STATE_SCANNING,     /* Scanning peaks */
  BLINDSCAN_STATE_COMPLETE,     /* Finished */
  BLINDSCAN_STATE_CANCELLED,    /* User cancelled */
  BLINDSCAN_STATE_ERROR         /* Error occurred */
} blindscan_state_t;

/* Blindscan session */
typedef struct blindscan_session {
  LIST_ENTRY(blindscan_session) bs_link;
  tvh_uuid_t             bs_uuid;
  char                   bs_uuid_hex[UUID_HEX_SIZE];

  /* Frontend binding */
  linuxdvb_frontend_t   *bs_frontend;
  linuxdvb_satconf_ele_t *bs_satconf_ele;
  mpegts_network_t      *bs_network;

  /* Scan parameters */
  uint32_t               bs_start_freq;    /* kHz */
  uint32_t               bs_end_freq;      /* kHz */
  int                    bs_polarisation;  /* DVB_POLARISATION_* or -1 for both */
  int                    bs_spectral_resolution; /* kHz, 0=driver default */
  int                    bs_fft_size;      /* FFT size (512 default) */
  int                    bs_search_range;  /* kHz for blind tune search */
  int                    bs_peak_detect;   /* 0=auto, 1=driver only, 2=algorithm only */

  /* DiSEqC settings */
  int                    bs_diseqc_committed;   /* 0-3, -1=none */
  int                    bs_diseqc_uncommitted; /* 0-15, -1=none */
  int                    bs_uncommitted_first;

  /* Unicable settings (if applicable) */
  int                    bs_unicable_scr;       /* SCR channel (-1 if not unicable) */
  uint32_t               bs_unicable_freq;      /* SCR frequency kHz */
  int                    bs_unicable_position;  /* Satellite position */
  int                    bs_unicable_pin;       /* PIN (-1 = no PIN) */

  /* State */
  blindscan_state_t      bs_state;
  int                    bs_progress;           /* 0-100% */
  char                  *bs_status_msg;

  /* Results */
  blindscan_spectrum_data_t *bs_spectrum_h_low;  /* H pol, low band */
  blindscan_spectrum_data_t *bs_spectrum_h_high; /* H pol, high band */
  blindscan_spectrum_data_t *bs_spectrum_v_low;  /* V pol, low band */
  blindscan_spectrum_data_t *bs_spectrum_v_high; /* V pol, high band */

  struct blindscan_peak_list bs_peaks;
  int                    bs_peak_count;
  int                    bs_current_peak;

  int                    bs_muxes_created;
  int                    bs_muxes_locked;

  /* Timing */
  int64_t                bs_start_time;         /* Monoclock */
  int64_t                bs_duration_ms;

  /* Thread control */
  pthread_t              bs_thread;
  int                    bs_thread_running;
  int                    bs_should_stop;
  tvh_mutex_t            bs_lock;

} blindscan_session_t;

/* ============================================================================
 * API Functions
 * ============================================================================ */

/**
 * Initialize blindscan subsystem
 */
void linuxdvb_blindscan_init(void);

/**
 * Cleanup blindscan subsystem
 */
void linuxdvb_blindscan_done(void);

/**
 * Start a blindscan session
 *
 * @param frontend   Frontend to use for scanning
 * @param satconf    Satellite configuration element
 * @param network    Target network for mux creation
 * @param start_freq Start frequency in kHz (transponder freq)
 * @param end_freq   End frequency in kHz (transponder freq)
 * @param pol        Polarisation: 'H', 'V', or 'B' (both)
 * @param opts       Additional options (htsmsg with fft_size, resolution, etc.)
 * @return Session UUID on success, NULL on error
 */
const char *linuxdvb_blindscan_start(
  linuxdvb_frontend_t *frontend,
  linuxdvb_satconf_ele_t *satconf,
  mpegts_network_t *network,
  uint32_t start_freq,
  uint32_t end_freq,
  char pol,
  htsmsg_t *opts
);

/**
 * Cancel a running blindscan session
 *
 * @param uuid  Session UUID
 */
void linuxdvb_blindscan_cancel(const char *uuid);

/**
 * Release a blindscan session and free resources
 * Closes the frontend fd and removes the session
 *
 * @param uuid  Session UUID
 */
void linuxdvb_blindscan_release(const char *uuid);

/**
 * Get session status
 *
 * @param uuid  Session UUID
 * @return htsmsg with status, progress, message, etc.
 */
htsmsg_t *linuxdvb_blindscan_status(const char *uuid);

/**
 * Get spectrum data from session
 *
 * @param uuid  Session UUID
 * @param pol   Polarisation ('H' or 'V')
 * @param band  Band (0=low, 1=high)
 * @return htsmsg with spectrum points
 */
htsmsg_t *linuxdvb_blindscan_spectrum(const char *uuid, char pol, int band);

/**
 * Get detected peaks from session
 *
 * @param uuid  Session UUID
 * @return htsmsg with peaks list
 */
htsmsg_t *linuxdvb_blindscan_peaks(const char *uuid);

/**
 * Create muxes from detected peaks
 * Creates muxes with MM_TYPE_PEAK for all locked peaks
 *
 * @param uuid  Session UUID
 * @return Number of muxes created
 */
int linuxdvb_blindscan_create_muxes(const char *uuid);

/**
 * Create muxes from selected peaks only
 * Creates muxes with MM_TYPE_PEAK for selected peaks
 *
 * @param uuid            Session UUID
 * @param selected_peaks  htsmsg list with {frequency, polarisation} for each peak
 * @return Number of muxes created
 */
int linuxdvb_blindscan_create_muxes_selected(const char *uuid, htsmsg_t *selected_peaks);

/**
 * Find session by UUID
 *
 * @param uuid  Session UUID
 * @return Session pointer or NULL
 */
blindscan_session_t *linuxdvb_blindscan_find(const char *uuid);

/**
 * Prescan a peak to detect tuning parameters using Neumo blind tune
 * Uses DTV_ALGORITHM=BLIND with search_range for parameter detection
 *
 * @param uuid       Session UUID
 * @param frequency  Peak frequency in kHz
 * @param pol        Polarisation ('H' or 'V')
 * @return htsmsg with locked status and detected parameters
 */
htsmsg_t *linuxdvb_blindscan_prescan(const char *uuid, uint32_t frequency, char pol);

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

/**
 * Calculate frequency tolerance for mux matching based on symbol rate
 * Lower SR = tighter tolerance, higher SR = wider tolerance
 *
 * @param symbol_rate  Symbol rate in symbols/sec
 * @return Tolerance in Hz
 */
static inline uint32_t
blindscan_freq_tolerance_for_sr(uint32_t symbol_rate)
{
  if (symbol_rate < 5000000)    /* < 5 Msym/s */
    return 1000000;             /* ±1 MHz */
  if (symbol_rate < 30000000)   /* < 30 Msym/s */
    return 5000000;             /* ±5 MHz */
  return 10000000;              /* ±10 MHz */
}

/**
 * Get band (low/high) for frequency
 *
 * @param frequency  Transponder frequency in kHz
 * @return 0 for low band, 1 for high band
 */
static inline int
blindscan_band_for_freq(uint32_t frequency)
{
  return (frequency >= BLINDSCAN_LNB_SLOF) ? 1 : 0;
}

/**
 * Convert transponder frequency to driver IF frequency
 *
 * @param frequency  Transponder frequency in kHz
 * @param band       Band (0=low, 1=high)
 * @return Driver frequency in kHz
 */
static inline int32_t
blindscan_driver_freq(uint32_t frequency, int band)
{
  if (band == 0)
    return frequency - BLINDSCAN_LNB_LOF_LOW;
  else
    return frequency - BLINDSCAN_LNB_LOF_HIGH;
}

/**
 * Convert driver IF frequency to transponder frequency
 *
 * @param driver_freq  Driver frequency in kHz
 * @param band         Band (0=low, 1=high)
 * @return Transponder frequency in kHz
 */
static inline uint32_t
blindscan_transponder_freq(int32_t driver_freq, int band)
{
  if (band == 0)
    return driver_freq + BLINDSCAN_LNB_LOF_LOW;
  else
    return driver_freq + BLINDSCAN_LNB_LOF_HIGH;
}

#endif /* __TVH_LINUXDVB_BLINDSCAN_H__ */
