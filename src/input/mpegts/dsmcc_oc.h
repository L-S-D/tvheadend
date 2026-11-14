/*
 *  Tvheadend - DSM-CC object carousel
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

#ifndef __TVH_DSMCC_OC_H__
#define __TVH_DSMCC_OC_H__

#include <stddef.h>
#include <stdint.h>

/*
 * The files of a DSM-CC object carousel (ISO/IEC 13818-6, ETSI TR 101 202)
 * from the sections of one PID: DSI (service gateway), DII (module list),
 * DDB (module data), zlib compressed modules (compressed_module_descriptor),
 * BIOP objects (srg, dir, fil). Until the root directory is known only small
 * modules are collected (probe), so that a caller can look at the names of
 * the root directory and give up on carousels it does not want.
 */

typedef struct dsmcc_oc dsmcc_oc_t;

typedef struct dsmcc_oc_ops {
  /* the names in the root directory; return 0 to read the whole carousel,
   * -1 to stop (all further sections are ignored) */
  int  (*root) (void *opaque, const char *const *names, int count);
  /* a file is complete: path from the root ("dir/file"), its data */
  void (*file) (void *opaque, const char *path, const uint8_t *data, size_t len);
} dsmcc_oc_ops_t;

/* probe_limit: largest module collected before the root directory is known */
dsmcc_oc_t *dsmcc_oc_create ( const dsmcc_oc_ops_t *ops, void *opaque,
                              size_t probe_limit );
void dsmcc_oc_destroy ( dsmcc_oc_t *oc );

/* one whole section (table_id 0x3B or 0x3C, with header, without CRC) */
void dsmcc_oc_section ( dsmcc_oc_t *oc, const uint8_t *sec, size_t len );

/* the root is wanted and all modules of the current module list are read
 * and their files delivered */
int dsmcc_oc_complete ( dsmcc_oc_t *oc );

/* state for the log: modules listed / read, blocks still missing in the
 * modules being collected */
void dsmcc_oc_stats ( dsmcc_oc_t *oc, int *modules, int *read, int *missing );

#endif /* __TVH_DSMCC_OC_H__ */
