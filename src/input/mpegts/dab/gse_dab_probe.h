/*
 *  GSE-DAB Probe - Detection of DAB within GSE streams
 *
 *  The DAB ensembles of a GSE input stream (MM_TYPE_GSE): the GSE probe
 *  (gse_probe.c) gives the PDUs of the stream (or, for a libdvbdab before
 *  1.1.0, the packets of its BBFrames) while the scan of the mux runs; at
 *  its end each ensemble found gets a DAB-GSE mux (MM_TYPE_DAB_GSE).
 *  Without libdvbdab there is none.
 *
 *  Copyright (C) 2025
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#ifndef __GSE_DAB_PROBE_H__
#define __GSE_DAB_PROBE_H__

#include <stdint.h>

struct mpegts_mux;
struct gse_pdu;

typedef struct gse_dab_probe gse_dab_probe_t;

#if ENABLE_DVBDAB

/* the probe of the GSE mux mm; NULL: none */
gse_dab_probe_t *gse_dab_probe_create ( struct mpegts_mux *mm );

/* a PDU of the stream / the packets of its BBFrames (the input thread,
 * under mi_output_lock) */
void gse_dab_probe_pdu ( gse_dab_probe_t *dp, const struct gse_pdu *pdu );
void gse_dab_probe_packets ( gse_dab_probe_t *dp, const uint8_t *pkt, int len );

/* the ensembles have their services (under mi_output_lock) */
int gse_dab_probe_ready ( gse_dab_probe_t *dp );

/* the end (global_lock, no more PDUs): a DAB-GSE mux for each ensemble
 * found when evaluate; the ensembles found; dp goes */
int gse_dab_probe_finish ( gse_dab_probe_t *dp, int evaluate );

#else

static inline gse_dab_probe_t *gse_dab_probe_create ( struct mpegts_mux *mm ) { return NULL; }
static inline void gse_dab_probe_pdu ( gse_dab_probe_t *dp, const struct gse_pdu *pdu ) { }
static inline void gse_dab_probe_packets ( gse_dab_probe_t *dp, const uint8_t *pkt, int len ) { }
static inline int  gse_dab_probe_ready ( gse_dab_probe_t *dp ) { return 1; }
static inline int  gse_dab_probe_finish ( gse_dab_probe_t *dp, int evaluate ) { return 0; }

#endif

#endif /* __GSE_DAB_PROBE_H__ */
