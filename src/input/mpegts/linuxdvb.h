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
 * 12750000) on a free Neumo tuner; the carriers in it. Errors: EBUSY (*err
 * says why), EIO */
int linuxdvb_blindscan_start
  ( struct mpegts_network *mn, const char *pols, uint32_t start, uint32_t end,
    const char **err );
void linuxdvb_blindscan_stop ( struct mpegts_network *mn );
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
