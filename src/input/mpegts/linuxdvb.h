/*
 *  Tvheadend - Linux DVB input system
 *
 *  Copyright (C) 2013 Adam Sutton
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

#ifndef __TVH_LINUX_DVB_H__
#define __TVH_LINUX_DVB_H__

void linuxdvb_init ( int mask );

void linuxdvb_done ( void );

idnode_set_t *linuxdvb_root ( void );

#if ENABLE_LINUXDVB_NEUMO
struct mpegts_network;

/* blindscan of a DVB-S network (global_lock): the spectrum of the
 * polarisations ("H", "V", "HV") from start to end (kHz, 0 = 10700000 and
 * 12750000) on a free Neumo tuner; the carriers in it. lock_snr >= 0: then
 * a tune (linuxdvb_blindscan_tune) at each carrier at least this far above
 * the noise (0.01 dB) where the network has no transponder. Errors: EBUSY
 * (*err says why), EIO */
int linuxdvb_blindscan_start
  ( struct mpegts_network *mn, const char *pols, uint32_t start, uint32_t end,
    int lock_snr, const char **err );
void linuxdvb_blindscan_stop ( struct mpegts_network *mn );
/* a blind tune at freq (kHz) with the polarisation ("H", "V") and symbol
 * rate, within range (kHz, the full width; 0: from the symbol rate): the
 * mux there, else a new one; scanned, a new one deleted when nothing was
 * found. The mux in muxuuid (UUID_HEX_SIZE) */
int linuxdvb_blindscan_tune
  ( struct mpegts_network *mn, uint32_t freq, const char *pol, uint32_t sr,
    uint32_t range, const char **err, char *muxuuid );
/* state and the last result, spectra reduced to points points (0 = all) */
htsmsg_t *linuxdvb_blindscan_result ( struct mpegts_network *mn, int points );
void linuxdvb_blindscan_done ( void );
#endif

#endif /* __TVH_LINUX_DVB_H__ */

/******************************************************************************
 * Editor Configuration
 *
 * vim:sts=2:ts=2:sw=2:et
 *****************************************************************************/
