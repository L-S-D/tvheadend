/*
 *  API - Blindscan
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
#include "access.h"
#include "api.h"
#include "input.h"
#include "htsmsg_json.h"

#if ENABLE_LINUXDVB
#include "input/mpegts/linuxdvb/linuxdvb_private.h"
#include "input/mpegts/linuxdvb/linuxdvb_blindscan.h"
#endif

/**
 * Start a blindscan session
 *
 * Parameters:
 *   frontend_uuid  - UUID of the frontend to use
 *   network_uuid   - UUID of the network for mux creation
 *   satconf_uuid   - UUID of the satconf element (optional)
 *   start_freq     - Start frequency in kHz
 *   end_freq       - End frequency in kHz
 *   polarisation   - 'H', 'V', or 'B' (both)
 *   fft_size       - FFT size (optional, default 512)
 *   resolution     - Spectral resolution in kHz (optional, 0=driver default)
 */
static int
api_blindscan_start
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
#if ENABLE_LINUXDVB
  const char *frontend_uuid, *network_uuid, *satconf_uuid;
  const char *pol_str;
  uint32_t start_freq, end_freq;
  linuxdvb_frontend_t *lfe = NULL;
  mpegts_network_t *mn = NULL;
  linuxdvb_satconf_ele_t *lse = NULL;
  char pol = 'B';
  const char *session_uuid;

  /* Get required parameters */
  frontend_uuid = htsmsg_get_str(args, "frontend_uuid");
  network_uuid = htsmsg_get_str(args, "network_uuid");

  if (!frontend_uuid || !network_uuid) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing frontend_uuid or network_uuid");
    return 0;
  }

  if (htsmsg_get_u32(args, "start_freq", &start_freq) ||
      htsmsg_get_u32(args, "end_freq", &end_freq)) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing start_freq or end_freq");
    return 0;
  }

  /* Get optional parameters */
  satconf_uuid = htsmsg_get_str(args, "satconf_uuid");
  pol_str = htsmsg_get_str(args, "polarisation");
  if (pol_str && strlen(pol_str) > 0)
    pol = pol_str[0];

  tvh_mutex_lock(&global_lock);

  /* Find frontend */
  idnode_t *in = idnode_find(frontend_uuid, NULL, NULL);
  if (in && idnode_is_instance(in, &linuxdvb_frontend_dvbs_class)) {
    lfe = (linuxdvb_frontend_t *)in;
  }

  if (!lfe) {
    tvh_mutex_unlock(&global_lock);
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Frontend not found or not DVB-S/S2");
    return 0;
  }

  /* Find network */
  in = idnode_find(network_uuid, NULL, NULL);
  if (in)
    mn = (mpegts_network_t *)in;

  if (!mn) {
    tvh_mutex_unlock(&global_lock);
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Network not found");
    return 0;
  }

  /* Find satconf (optional) */
  if (satconf_uuid) {
    in = idnode_find(satconf_uuid, NULL, NULL);
    if (in)
      lse = (linuxdvb_satconf_ele_t *)in;

    if (lse) {
      tvhinfo(LS_LINUXDVB, "Blindscan using satconf: %s (uuid=%s), switch=%p",
              lse->lse_name ?: "unnamed", satconf_uuid, lse->lse_switch);
    } else {
      tvhwarn(LS_LINUXDVB, "Blindscan satconf not found: %s", satconf_uuid);
    }
  }

  /* Build options */
  htsmsg_t *opts = htsmsg_create_map();
  uint32_t fft_size = htsmsg_get_u32_or_default(args, "fft_size", 512);
  uint32_t resolution = htsmsg_get_u32_or_default(args, "resolution", 0);
  int32_t peak_detect = htsmsg_get_s32_or_default(args, "peak_detect", 0);
  htsmsg_add_u32(opts, "fft_size", fft_size);
  htsmsg_add_u32(opts, "resolution", resolution);
  htsmsg_add_s32(opts, "peak_detect", peak_detect);

  /* Start blindscan */
  session_uuid = linuxdvb_blindscan_start(lfe, lse, mn, start_freq, end_freq, pol, opts);

  htsmsg_destroy(opts);

  tvh_mutex_unlock(&global_lock);

  *resp = htsmsg_create_map();
  if (session_uuid) {
    htsmsg_add_str(*resp, "uuid", session_uuid);
    htsmsg_add_str(*resp, "status", "started");
  } else {
    htsmsg_add_str(*resp, "error", "Failed to start blindscan");
  }

  return 0;
#else
  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "LinuxDVB not enabled");
  return 0;
#endif
}

/**
 * Cancel a blindscan session
 */
static int
api_blindscan_cancel
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
#if ENABLE_LINUXDVB
  const char *uuid = htsmsg_get_str(args, "uuid");

  if (!uuid) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing uuid");
    return 0;
  }

  linuxdvb_blindscan_cancel(uuid);

  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "status", "cancelled");
  return 0;
#else
  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "LinuxDVB not enabled");
  return 0;
#endif
}

/**
 * Release a blindscan session (close window)
 */
static int
api_blindscan_release
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
#if ENABLE_LINUXDVB
  const char *uuid = htsmsg_get_str(args, "uuid");

  if (!uuid) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing uuid");
    return 0;
  }

  linuxdvb_blindscan_release(uuid);

  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "status", "released");
  return 0;
#else
  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "LinuxDVB not enabled");
  return 0;
#endif
}

/**
 * Get blindscan session status
 */
static int
api_blindscan_status
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
#if ENABLE_LINUXDVB
  const char *uuid = htsmsg_get_str(args, "uuid");

  if (!uuid) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing uuid");
    return 0;
  }

  *resp = linuxdvb_blindscan_status(uuid);
  if (!*resp) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Session not found");
  }
  return 0;
#else
  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "LinuxDVB not enabled");
  return 0;
#endif
}

/**
 * Get spectrum data from a blindscan session
 */
static int
api_blindscan_spectrum
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
#if ENABLE_LINUXDVB
  const char *uuid = htsmsg_get_str(args, "uuid");
  const char *pol_str = htsmsg_get_str(args, "polarisation");
  uint32_t band;

  if (!uuid || !pol_str) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing uuid or polarisation");
    return 0;
  }

  if (htsmsg_get_u32(args, "band", &band))
    band = 0;

  char pol = pol_str[0];

  tvhdebug(LS_WEBUI, "Spectrum request: uuid=%s pol=%c band=%u", uuid, pol, band);

  *resp = linuxdvb_blindscan_spectrum(uuid, pol, band);
  if (!*resp) {
    tvhdebug(LS_WEBUI, "Spectrum request returned NULL");
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "No spectrum data available");
  } else {
    uint32_t count = htsmsg_get_u32_or_default(*resp, "count", 0);
    tvhdebug(LS_WEBUI, "Spectrum request returned %u points", count);
  }
  return 0;
#else
  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "LinuxDVB not enabled");
  return 0;
#endif
}

/**
 * Get detected peaks from a blindscan session
 */
static int
api_blindscan_peaks
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
#if ENABLE_LINUXDVB
  const char *uuid = htsmsg_get_str(args, "uuid");

  if (!uuid) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing uuid");
    return 0;
  }

  *resp = linuxdvb_blindscan_peaks(uuid);
  if (!*resp) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Session not found");
  }
  return 0;
#else
  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "LinuxDVB not enabled");
  return 0;
#endif
}

/**
 * Create muxes from detected peaks
 */
static int
api_blindscan_create_muxes
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
#if ENABLE_LINUXDVB
  const char *uuid = htsmsg_get_str(args, "uuid");
  const char *peaks_json = htsmsg_get_str(args, "peaks");

  if (!uuid) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing uuid");
    return 0;
  }

  /* Parse selected peaks if provided */
  htsmsg_t *selected_peaks = NULL;
  if (peaks_json && peaks_json[0]) {
    selected_peaks = htsmsg_json_deserialize(peaks_json);
  }

  tvh_mutex_lock(&global_lock);
  int count = linuxdvb_blindscan_create_muxes_selected(uuid, selected_peaks);
  tvh_mutex_unlock(&global_lock);

  if (selected_peaks)
    htsmsg_destroy(selected_peaks);

  *resp = htsmsg_create_map();
  htsmsg_add_u32(*resp, "created", count);
  return 0;
#else
  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "LinuxDVB not enabled");
  return 0;
#endif
}

/**
 * Prescan a peak to detect tuning parameters using Neumo blind tune
 */
static int
api_blindscan_prescan
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
#if ENABLE_LINUXDVB
  const char *uuid = htsmsg_get_str(args, "uuid");
  const char *pol_str = htsmsg_get_str(args, "polarisation");
  uint32_t frequency;

  if (!uuid || !pol_str) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing uuid or polarisation");
    return 0;
  }

  if (htsmsg_get_u32(args, "frequency", &frequency)) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing frequency");
    return 0;
  }

  char pol = pol_str[0];

  tvhdebug(LS_WEBUI, "Prescan request: uuid=%s freq=%u pol=%c", uuid, frequency, pol);

  *resp = linuxdvb_blindscan_prescan(uuid, frequency, pol);
  if (!*resp) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Prescan failed");
  }
  return 0;
#else
  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "LinuxDVB not enabled");
  return 0;
#endif
}

/**
 * List satconf entries for a network
 * Returns frontend info + satconf info with LNB type and unicable details
 */
static int
api_blindscan_list_satconfs
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
#if ENABLE_LINUXDVB
  const char *network_uuid = htsmsg_get_str(args, "network_uuid");
  mpegts_network_t *mn = NULL;
  htsmsg_t *list;
  linuxdvb_frontend_t *lfe;
  linuxdvb_satconf_t *ls;
  linuxdvb_satconf_ele_t *lse;
  char buf[512];

  if (!network_uuid) {
    *resp = htsmsg_create_map();
    htsmsg_add_str(*resp, "error", "Missing network_uuid");
    return 0;
  }

  tvh_mutex_lock(&global_lock);

  idnode_t *in = idnode_find(network_uuid, NULL, NULL);
  if (in)
    mn = (mpegts_network_t *)in;

  list = htsmsg_create_list();

  if (mn) {
    /* Iterate all DVB-S frontends */
    extern const idclass_t linuxdvb_frontend_dvbs_class;
    idnode_set_t *is = idnode_find_all(&linuxdvb_frontend_dvbs_class, NULL);
    if (is) {
      for (size_t i = 0; i < is->is_count; i++) {
        lfe = (linuxdvb_frontend_t *)is->is_array[i];
        if (!lfe->lfe_satconf) continue;

        /* Skip disabled frontends */
        mpegts_input_t *mi = (mpegts_input_t *)lfe;
        if (mi->mi_is_enabled && !mi->mi_is_enabled(mi, NULL, 0, 0))
          continue;

        ls = lfe->lfe_satconf;

        /* Check each satconf element */
        TAILQ_FOREACH(lse, &ls->ls_elements, lse_link) {
          /* Skip disabled satconf elements */
          if (!lse->lse_enabled)
            continue;

          /* Check if this satconf is linked to the network */
          int found = 0;
          if (lse->lse_networks) {
            idnode_set_t *nets = lse->lse_networks;
            for (size_t j = 0; j < nets->is_count; j++) {
              if ((mpegts_network_t *)nets->is_array[j] == mn) {
                found = 1;
                break;
              }
            }
          }

          if (found) {
            htsmsg_t *entry = htsmsg_create_map();

            /* Frontend UUID and name */
            htsmsg_add_str(entry, "frontend_uuid", idnode_uuid_as_str(&lfe->ti_id, buf));
            idnode_get_title(&lfe->ti_id, NULL, buf, sizeof(buf));
            htsmsg_add_str(entry, "frontend_name", buf);

            /* Satconf element UUID and name */
            htsmsg_add_str(entry, "satconf_uuid", idnode_uuid_as_str(&lse->lse_id, buf));
            htsmsg_add_str(entry, "satconf_name", lse->lse_name ?: "Unknown");

            /* LNB type */
            if (lse->lse_lnb) {
              htsmsg_add_str(entry, "lnb_type", lse->lse_lnb->ld_type ?: "Unknown");
            }

            /* Unicable info */
            if (lse->lse_en50494) {
              htsmsg_add_bool(entry, "unicable", 1);
              htsmsg_add_str(entry, "unicable_type", lse->lse_en50494->ld_type ?: "");
              /* Get SCR info from the en50494 object */
              linuxdvb_en50494_t *uc = (linuxdvb_en50494_t *)lse->lse_en50494;
              htsmsg_add_u32(entry, "scr", uc->le_id);
              htsmsg_add_u32(entry, "scr_freq", uc->le_frequency);
            } else {
              htsmsg_add_bool(entry, "unicable", 0);
            }

            /* Build display name */
            if (lse->lse_en50494) {
              linuxdvb_en50494_t *uc = (linuxdvb_en50494_t *)lse->lse_en50494;
              snprintf(buf, sizeof(buf), "%s - %s (SCR %d @ %d MHz)",
                       lse->lse_name ?: "Satconf",
                       lse->lse_en50494->ld_type ?: "Unicable",
                       uc->le_id,
                       uc->le_frequency / 1000);
            } else {
              snprintf(buf, sizeof(buf), "%s - %s",
                       lse->lse_name ?: "Satconf",
                       lse->lse_lnb ? lse->lse_lnb->ld_type : "LNB");
            }
            htsmsg_add_str(entry, "display_name", buf);

            htsmsg_add_msg(list, NULL, entry);
          }
        }
      }
      idnode_set_free(is);
    }
  }

  tvh_mutex_unlock(&global_lock);

  *resp = htsmsg_create_map();
  htsmsg_add_msg(*resp, "entries", list);
  return 0;
#else
  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "LinuxDVB not enabled");
  return 0;
#endif
}

/**
 * Blindscan API handler
 */
static int
api_blindscan_handler
  ( access_t *perm, void *opaque, const char *op, htsmsg_t *args, htsmsg_t **resp )
{
  if (!strcmp(op, "start"))
    return api_blindscan_start(perm, opaque, op, args, resp);
  if (!strcmp(op, "cancel"))
    return api_blindscan_cancel(perm, opaque, op, args, resp);
  if (!strcmp(op, "release"))
    return api_blindscan_release(perm, opaque, op, args, resp);
  if (!strcmp(op, "status"))
    return api_blindscan_status(perm, opaque, op, args, resp);
  if (!strcmp(op, "spectrum"))
    return api_blindscan_spectrum(perm, opaque, op, args, resp);
  if (!strcmp(op, "peaks"))
    return api_blindscan_peaks(perm, opaque, op, args, resp);
  if (!strcmp(op, "create_muxes"))
    return api_blindscan_create_muxes(perm, opaque, op, args, resp);
  if (!strcmp(op, "prescan"))
    return api_blindscan_prescan(perm, opaque, op, args, resp);
  if (!strcmp(op, "list_satconfs"))
    return api_blindscan_list_satconfs(perm, opaque, op, args, resp);

  *resp = htsmsg_create_map();
  htsmsg_add_str(*resp, "error", "Unknown operation");
  return 0;
}

static api_hook_t api_blindscan_hooks[] = {
  { "blindscan", ACCESS_ADMIN, api_blindscan_handler, NULL },
  { NULL, 0, NULL, NULL }
};

void
api_blindscan_init(void)
{
  api_register_all(api_blindscan_hooks);
}
