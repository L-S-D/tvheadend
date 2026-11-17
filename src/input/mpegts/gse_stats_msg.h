/*
 *  Tvheadend - what GSE streams and IP packets carry, as messages
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

#ifndef __TVH_GSE_STATS_MSG_H__
#define __TVH_GSE_STATS_MSG_H__

#include "htsmsg.h"
#include "gse_stats.h"

/* what IP packets carried: "ip" (multicast, broadcast, unicast packets and
 * bytes) and "flows" (the multicast groups: addr, port, packets, bytes,
 * kind, rtp_pt), "flows_other" into m */
void ip_stats_msg ( htsmsg_t *m, const ip_stats_t *s );

/* what the GSE streams carried: frames by stream type, input streams,
 * GSE packets and PDUs with their errors, protocol types, labels, and
 * ip_stats_msg() of the IP PDUs; NULL without frames */
htsmsg_t *gse_stats_msg ( const gse_stats_result_t *r );

/* one line of what a message of gse_stats_msg() says, with rates when it
 * has the microseconds ("us") it covers */
void gse_stats_msg_text ( htsmsg_t *m, char *buf, size_t len );

#endif /* __TVH_GSE_STATS_MSG_H__ */
