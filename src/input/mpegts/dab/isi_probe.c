/*
 *  ISI Probe - Input Stream Identifier scanning for DVB-S2 Multi-Stream
 *
 *  Automatically discovers all ISIs (Input Stream Identifiers) on a DVB-S2
 *  transponder and creates muxes for each one. Only works with drivers that
 *  support the DTV_ISI_LIST and DTV_MATYPE_LIST ioctls (e.g., Neumo stid135).
 *
 *  For each discovered ISI, the MATYPE is checked to determine if it's a
 *  GSE (Generic Stream Encapsulation) or standard TS stream:
 *    - MATYPE bits 7-6 = 11: Transport Stream (MM_TYPE_TS)
 *    - MATYPE bits 7-6 = 00/01: GSE stream (MM_TYPE_GSE)
 *
 *  Copyright (C) 2025
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "tvheadend.h"
#include "input.h"
#include "input/mpegts/mpegts_dvb.h"
#include "input/mpegts/linuxdvb/linuxdvb_private.h"
#include "isi_probe.h"
#include "input/mpegts/bbframe.h"
#include <linux/dvb/frontend.h>
#include <sys/ioctl.h>

/* Neumo driver ioctls - not in standard kernel headers */
#ifndef DTV_ISI_LIST
#define DTV_ISI_LIST 76
#endif

#ifndef DTV_MATYPE_LIST
#define DTV_MATYPE_LIST 90
#endif

/*
 * ISI probe context - the tuner the scan runs on (only compared, never
 * dereferenced unless it is still the mux's active input)
 */
typedef struct isi_probe_ctx {
  const mpegts_input_t *mi;
  /* a multistream transponder: the streams of the BBFrame headers (the
   * input thread writes them, under mi_output_lock) */
  int                   tap;
  uint8_t               seen[32];
  uint8_t               matype[256];
} isi_probe_ctx_t;

/* the frame headers in the packets of the transponder */
static void
isi_probe_packet_cb(void *aux, const uint8_t *pkt, int len)
{
  isi_probe_ctx_t *ctx = aux;
  uint8_t isi, matype;

  for ( ; len >= 188; pkt += 188, len -= 188)
    if ((((pkt[1] & 0x1f) << 8) | pkt[2]) == DVB_BBFRAMES_PID &&
        bbframe_packet_header(pkt, &isi, &matype) == 0) {
      ctx->seen[isi >> 3] |= 1 << (isi & 7);
      ctx->matype[isi] = matype;
    }
}

static void
isi_probe_set_cb(mpegts_mux_t *mm, isi_probe_ctx_t *ctx)
{
  mpegts_input_t *mi = mm->mm_active ? mm->mm_active->mmi_input : NULL;

  if (mi)
    tvh_mutex_lock(&mi->mi_output_lock);
  mm->mm_secondary_ctx = ctx;
  mm->mm_secondary_cb  = ctx ? isi_probe_packet_cb : NULL;
  if (mi)
    tvh_mutex_unlock(&mi->mi_output_lock);
}

/*
 * Parse ISI bitfield from driver buffer
 * Returns count of ISIs found
 */
static int
isi_parse_bitfield(const uint8_t *bitset, size_t bitset_len, uint8_t *isi_list)
{
  int count = 0;
  size_t byte_idx;
  int bit;

  for (byte_idx = 0; byte_idx < bitset_len && byte_idx < 32; byte_idx++) {
    uint8_t byte_val = bitset[byte_idx];
    for (bit = 0; bit < 8; bit++) {
      if (byte_val & (1 << bit)) {
        isi_list[count++] = byte_idx * 8 + bit;
      }
    }
  }
  return count;
}

/*
 * Log found ISI list
 */
static void
isi_log_list(const uint8_t *isi_list, int count)
{
  char isi_str[512] = "";
  int pos = 0;
  int j;

  if (count == 0) {
    tvhdebug(LS_MPEGTS, "ISI probe: found 0 ISI(s) from driver");
    return;
  }

  for (j = 0; j < count && pos < 500; j++) {
    pos += snprintf(isi_str + pos, sizeof(isi_str) - pos, "%d ", isi_list[j]);
  }
  tvhdebug(LS_MPEGTS, "ISI probe: found %d ISI(s) from driver: %s", count, isi_str);
}

/*
 * Read MATYPE list from driver
 */
static void
isi_read_matype_list(int fe_fd, uint8_t *matype_list)
{
  struct dtv_property prop_matype;
  struct dtv_properties cmdseq;
  uint16_t matype_buffer[256];
  int i;

  /* Neumo uses dtv_matype_list struct */
  struct {
    uint32_t num_entries;
    uint16_t *matypes;
  } matype_req;

  memset(&prop_matype, 0, sizeof(prop_matype));
  memset(matype_buffer, 0, sizeof(matype_buffer));
  prop_matype.cmd = DTV_MATYPE_LIST;

  matype_req.num_entries = 256;
  matype_req.matypes = matype_buffer;
  memcpy(&prop_matype.u, &matype_req, sizeof(matype_req));

  cmdseq.num = 1;
  cmdseq.props = &prop_matype;

  if (ioctl(fe_fd, FE_GET_PROPERTY, &cmdseq) != 0)
    return;

  /* Parse MATYPE entries: format is (matype << 8) | isi */
  for (i = 0; i < 256 && matype_buffer[i] != 0; i++) {
    uint16_t entry = matype_buffer[i];
    uint8_t isi = entry & 0xFF;
    uint8_t matype = (entry >> 8) & 0xFF;
    matype_list[isi] = matype;
    tvhtrace(LS_MPEGTS, "ISI probe: ISI %d MATYPE 0x%02X", isi, matype);
  }
}

/*
 * Read ISI list from Neumo driver using DTV_ISI_LIST ioctl
 * Returns number of ISIs found, populates isi_list array
 * isi_list must be able to hold 256 entries
 */
static int
isi_read_driver_list(int fe_fd, uint8_t *isi_list, uint8_t *matype_list)
{
  struct dtv_property prop_isi;
  struct dtv_properties cmdseq;
  const uint8_t *bitset;
  size_t bitset_len;
  int count;

  if (fe_fd < 0)
    return 0;

  /* Read ISI bitfield */
  memset(&prop_isi, 0, sizeof(prop_isi));
  prop_isi.cmd = DTV_ISI_LIST;

  cmdseq.num = 1;
  cmdseq.props = &prop_isi;

  if (ioctl(fe_fd, FE_GET_PROPERTY, &cmdseq) != 0) {
    tvhtrace(LS_MPEGTS, "ISI probe: DTV_ISI_LIST ioctl failed: %s", strerror(errno));
    return 0;
  }

  bitset = prop_isi.u.buffer.data;
  bitset_len = prop_isi.u.buffer.len;

  if (bitset_len == 0) {
    tvhtrace(LS_MPEGTS, "ISI probe: empty ISI bitset");
    return 0;
  }

  tvhtrace(LS_MPEGTS, "ISI probe: ISI bitset len=%zu", bitset_len);

  count = isi_parse_bitfield(bitset, bitset_len, isi_list);
  isi_log_list(isi_list, count);

  /* Read MATYPE list if we found ISIs */
  if (count > 0 && matype_list)
    isi_read_matype_list(fe_fd, matype_list);

  return count;
}

/*
 * Check if MATYPE indicates GSE mode
 * MATYPE-1 bits 7-6 (EN 302 307): 00=generic packetized, 01=GSE,
 * 10=generic continuous, 11=transport stream. A MATYPE of 0 is an ISI the
 * driver gave no MATYPE for (a real one always has other bits set): not
 * known, so a TS.
 */
static int
isi_is_gse_matype(uint8_t matype)
{
  int ts_gs = (matype >> 6) & 0x3;
  return matype != 0 && (ts_gs == 0 || ts_gs == 1);  /* GSE if not TS mode */
}

/*
 * Find mux by exact match of frequency, polarisation, and stream_id
 * Returns NULL if not found
 */
static dvb_mux_t *
isi_find_mux_exact(dvb_network_t *ln, uint32_t freq, int pol, int stream_id)
{
  mpegts_mux_t *mm;
  dvb_mux_t *dm;

  LIST_FOREACH(mm, &ln->mn_muxes, mm_network_link) {
    dm = (dvb_mux_t *)mm;
    if (dm->lm_tuning.dmc_fe_freq == freq &&
        dm->lm_tuning.u.dmc_fe_qpsk.polarisation == pol &&
        dm->lm_tuning.dmc_fe_stream_id == stream_id) {
      return dm;
    }
  }
  return NULL;
}

/*
 * Create the muxes of the input streams: the scanned transponder is a
 * multistream transponder then, they are its sub-muxes (TS: MM_TYPE_ISI,
 * else GSE). A transponder with one TS stream has nothing to split.
 */
static int
isi_probe_create_muxes(mpegts_mux_t *mm, const uint8_t *isi_list, int isi_count,
                       const uint8_t *matype_list)
{
  dvb_network_t *ln = (dvb_network_t *)mm->mm_network;
  dvb_mux_t *outer_dm = (dvb_mux_t *)mm;
  dvb_mux_t *t, *child_mux;
  dvb_mux_conf_t dmc;
  int i, gse = 0;
  int created = 0;
  uint32_t freq = outer_dm->lm_tuning.dmc_fe_freq;
  int pol = outer_dm->lm_tuning.u.dmc_fe_qpsk.polarisation;

  for (i = 0; i < isi_count; i++)
    gse += isi_is_gse_matype(matype_list[isi_list[i]]);
  if (gse == 0 && isi_count < 2)
    return 0;

  /* the mux of the transponder (the scanned one, if it has no stream set) */
  t = dvb_mux_multistream_transponder(ln, outer_dm);
  if (t == NULL)
    return 0;

  for (i = 0; i < isi_count; i++) {
    uint8_t isi = isi_list[i];
    uint8_t matype = matype_list[isi];
    int is_gse = isi_is_gse_matype(matype);

    tvhdebug(LS_MPEGTS, "ISI probe: ISI %d, MATYPE=0x%02X, %s",
             isi, matype, is_gse ? "GSE" : "TS");

    /* an existing mux of the stream (from before, or the scanned one) */
    child_mux = isi_find_mux_exact(ln, freq, pol, isi);
    if (child_mux) {
      if (((mpegts_mux_t *)child_mux)->mm_type == MM_TYPE_TS)
        ((mpegts_mux_t *)child_mux)->mm_type = MM_TYPE_ISI;
      dvb_mux_submux_link(child_mux, t);
      mpegts_mux_update_nice_name((mpegts_mux_t *)child_mux);
      child_mux->mm_scan_last_seen = gclk();
      idnode_changed(&child_mux->mm_id);
      continue;
    }

    /* the tuning of the transponder, with the stream */
    dmc = t->lm_tuning;
    dmc.dmc_fe_stream_id = isi;
    child_mux = dvb_mux_create0(ln, MPEGTS_ONID_NONE, MPEGTS_TSID_NONE, &dmc, NULL, NULL);
    if (!child_mux)
      continue;
    /* a GSE stream may become DAB-GSE when its scan finds DAB */
    ((mpegts_mux_t *)child_mux)->mm_type = is_gse ? MM_TYPE_GSE : MM_TYPE_ISI;
    dvb_mux_submux_link(child_mux, t);
    mpegts_mux_update_nice_name((mpegts_mux_t *)child_mux);

    tvhinfo(LS_MPEGTS, "ISI probe: created %s mux for ISI %d (MATYPE=0x%02X)",
            is_gse ? "GSE" : "TS", isi, matype);

    child_mux->mm_scan_first = gclk();
    child_mux->mm_scan_last_seen = child_mux->mm_scan_first;
    idnode_changed(&child_mux->mm_id);

    /* Queue for scanning */
    mpegts_network_scan_queue_add((mpegts_mux_t *)child_mux,
                                   SUBSCRIPTION_PRIO_SCAN_INIT,
                                   SUBSCRIPTION_INITSCAN, 10);
    created++;
  }

  return created;
}

/*
 * Start ISI probe on a mux
 * Remembers the tuner the scan runs on for mpegts_isi_probe_complete()
 */
void
mpegts_isi_probe_start(mpegts_mux_t *mm)
{
  isi_probe_ctx_t *ctx;
  const dvb_mux_t *dm;
  const linuxdvb_frontend_t *lfe;
  mpegts_mux_instance_t *mmi;

  /* Already probing? */
  if (mm->mm_isi_probe_ctx)
    return;

  /* Only a transponder (not a sub-mux) of a network that may get new muxes,
   * as the DAB probe */
  if (mm->mm_type != MM_TYPE_TS ||
      mm->mm_network->mn_autodiscovery == MN_DISCOVERY_DISABLE)
    return;

  /* The tuner the scan runs on (mm_instances holds one instance per input
   * of the network, its first entry is not the active one) */
  mmi = mm->mm_active;
  if (!mmi || !mmi->mmi_input ||
      !idnode_is_instance(&mmi->mmi_input->ti_id, &linuxdvb_frontend_dvbs_class))
    return;
  lfe = (linuxdvb_frontend_t *)mmi->mmi_input;

  /* The ISI list comes from the Neumo driver (DTV_ISI_LIST) */
#if ENABLE_LINUXDVB_NEUMO
  if (!lfe->lfe_neumo_supported)
    return;
#else
  return;
#endif

  /* Only for DVB-S2 */
  dm = (dvb_mux_t *)mm;
  if (dm->lm_tuning.dmc_fe_delsys != DVB_SYS_DVBS2)
    return;

  /* Allocate context with frontend FD */
  ctx = calloc(1, sizeof(*ctx));
  if (!ctx)
    return;

  ctx->mi = mmi->mmi_input;
  mm->mm_isi_probe_ctx = ctx;

  /* a multistream transponder in BBFrame mode: the headers tell the streams
   * too (the driver list may be empty), from the PID of the BBFrames */
  if (lfe->lfe_bbframes && mm->mm_secondary_cb == NULL) {
    ctx->tap = 1;
    isi_probe_set_cb(mm, ctx);
    mm->mm_update_pids_flag = 1;
    mpegts_mux_update_pids(mm);
  }

  tvhdebug(LS_MPEGTS, "mux %s: ISI probe started on %s", mm->mm_nicename, lfe->lfe_fe_path);
}

/*
 * Complete ISI probe - read ISI list from driver and create muxes
 */
int
mpegts_isi_probe_complete(mpegts_mux_t *mm, int evaluate)
{
  isi_probe_ctx_t *ctx = mm->mm_isi_probe_ctx;
  const linuxdvb_frontend_t *lfe = NULL;
  uint8_t isi_list[256];
  uint8_t matype_list[256];
  int isi_count = 0;
  int found = 0;

  if (!ctx)
    return 0;

  mm->mm_isi_probe_ctx = NULL;
  if (ctx->tap) {
    if (mm->mm_secondary_ctx == ctx)
      isi_probe_set_cb(mm, NULL);
    mm->mm_update_pids_flag = 1;
    mpegts_mux_update_pids(mm);
  }

  /* Read ISI list from the driver of the tuner the scan ran on - only if it
   * still has the mux (its fd now, not a number saved at the start) */
  memset(isi_list, 0, sizeof(isi_list));
  memset(matype_list, 0, sizeof(matype_list));

  if (!evaluate) {
    tvhdebug(LS_MPEGTS, "mux %s: ISI probe skipped (scan cancelled)", mm->mm_nicename);
  } else {
    if (mm->mm_active && mm->mm_active->mmi_input == ctx->mi)
      lfe = (const linuxdvb_frontend_t *)ctx->mi;
    if (lfe && lfe->lfe_fe_fd >= 0)
      isi_count = isi_read_driver_list(lfe->lfe_fe_fd, isi_list, matype_list);
    else
      tvhdebug(LS_MPEGTS, "mux %s: ISI probe skipped (tuner no longer active)", mm->mm_nicename);
  }

  /* the streams of the frame headers the driver did not list */
  if (evaluate && ctx->tap) {
    int i, j;
    for (i = 0; i < 256; i++) {
      if (!(ctx->seen[i >> 3] & (1 << (i & 7))))
        continue;
      for (j = 0; j < isi_count && isi_list[j] != i; j++)
        ;
      if (j == isi_count)
        isi_list[isi_count++] = i;
      if (matype_list[i] == 0)
        matype_list[i] = ctx->matype[i];
    }
  }

  /* Create muxes for GSE streams */
  if (isi_count > 0) {
    found = isi_probe_create_muxes(mm, isi_list, isi_count, matype_list);
  }

  free(ctx);

  /* a multistream transponder has no tables of its own: its streams are
   * the result of its scan */
  if (((dvb_mux_t *)mm)->lm_multistream && isi_count > 0 && found == 0)
    found = isi_count;

  /* every scan of a DVB-S2 mux ends here: the log gets the ones that made muxes */
  if (found > 0)
    tvhinfo(LS_MPEGTS, "mux %s: ISI probe complete, created %d mux(es) from %d ISI(s)",
            mm->mm_nicename, found, isi_count);
  else
    tvhdebug(LS_MPEGTS, "mux %s: ISI probe complete, created %d mux(es) from %d ISI(s)",
             mm->mm_nicename, found, isi_count);

  return found;
}

/*
 * Check if ISI probe is done
 */
int
mpegts_isi_probe_is_done(mpegts_mux_t *mm __attribute__((unused)))
{
  /* ISI probe is instant (ioctl-based), always done */
  return 1;
}
