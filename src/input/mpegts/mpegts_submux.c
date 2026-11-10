/*
 *  Tvheadend - MPEGTS sub-mux input
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

/*
 * A sub-mux (a DAB ensemble in a PID of a transponder, an input stream of a
 * multistream transponder in its BBFrames) is not tuned itself.
 * The sub-mux input starts it with a raw subscription on its transponder, the
 * parent, for the PID the sub-mux is carried in. The stream of the sub-mux
 * is made of that PID on a thread of the sub-mux and goes into the normal
 * input path of the sub-mux. So the transponder, its services and all its
 * sub-muxes share one tuner, whatever input delivers the transponder.
 *
 * The input is virtual and never busy; the real limit is the subscription
 * on the parent, which a tuner serves like any other.
 */

#include "tvheadend.h"
#include "input.h"
#include "subscriptions.h"
#include "streaming.h"
#include "profile.h"
#include "input/mpegts/mpegts_dvb.h"
#include "input/mpegts/bbframe.h"

/* bytes the queue of a sub-mux takes before it drops (the parent's tuner
 * does not wait for the sub-mux thread); an input stream gets all the
 * BBFrames of its transponder: some seconds of a whole transponder */
#define SUBMUX_QUEUE_SIZE       (4 * 1024 * 1024)
#define SUBMUX_QUEUE_SIZE_BBF  (32 * 1024 * 1024)

typedef struct mpegts_submux {
  mpegts_mux_instance_t *sm_mmi;
  th_subscription_t     *sm_sub;    /* raw subscription on the parent */
  profile_chain_t        sm_prch;
  streaming_queue_t      sm_sq;
  pthread_t              sm_thread;
  int                    sm_run;    /* atomic */
  uint16_t               sm_pid;    /* the parent's PID the sub-mux is in */
  sbuf_t                 sm_in;     /* packets of that PID */
  bbframe_split_t       *sm_bbf;    /* an input stream: out of the BBFrames */
  sbuf_t                 sm_out;    /* its TS */
  int                    sm_cc;     /* the continuity counter of the PID */
  uint64_t               sm_packets;
  uint64_t               sm_cc_gaps;  /* packets lost before the sub-mux thread */
} mpegts_submux_t;

extern const idclass_t mpegts_input_class;
extern const idclass_t dvb_mux_class;

const idclass_t mpegts_submux_input_class = {
  .ic_super      = &mpegts_input_class,
  .ic_class      = "mpegts_submux_input",
  .ic_caption    = N_("Sub-mux input"),
  .ic_properties = (const property_t[]){
    {}
  }
};

static mpegts_input_t *submux_input;

/* the transponder a sub-mux is carried in */
static mpegts_mux_t *
submux_parent ( mpegts_mux_t *mm )
{
  if (!mpegts_mux_is_submux(mm) || !idnode_is_instance(&mm->mm_id, &dvb_mux_class))
    return NULL;
  return (mpegts_mux_t *)dvb_mux_submux_parent((dvb_mux_t *)mm);
}

/* **************************************************************************
 * The stream of a sub-mux
 * *************************************************************************/

static void
submux_ts_out ( void *opaque, const uint8_t *ts )
{
  mpegts_submux_t *sm = opaque;

  sbuf_append(&sm->sm_out, ts, 188);
}

/* a packet of the PID of the parent: its continuity tells packets lost on
 * the way (tuner, queue) */
static int
submux_packet ( mpegts_submux_t *sm, const uint8_t *data )
{
  int cc;

  if (data[0] != 0x47 || (((data[1] & 0x1f) << 8) | data[2]) != sm->sm_pid)
    return 0;
  sm->sm_packets++;
  if (data[3] & 0x10) {
    cc = data[3] & 0x0f;
    if (sm->sm_cc >= 0 && cc != ((sm->sm_cc + 1) & 0x0f) && cc != sm->sm_cc)
      sm->sm_cc_gaps++;
    sm->sm_cc = cc;
  }
  return 1;
}

/* an input stream (TS) out of the BBFrames of the transponder */
static void
submux_input_stream ( mpegts_submux_t *sm, const uint8_t *data, size_t len )
{
  for ( ; len >= 188; data += 188, len -= 188)
    if (submux_packet(sm, data))
      bbframe_split_ts(sm->sm_bbf, data, submux_ts_out, sm);
  if (sm->sm_out.sb_ptr > 0)
    mpegts_input_recv_packets(sm->sm_mmi, &sm->sm_out, MPEGTS_DATA_DECAPSULATED, NULL);
}

static void
submux_process ( mpegts_submux_t *sm, streaming_message_t *msg )
{
  mpegts_mux_instance_t *mmi = sm->sm_mmi;
  char ubuf[UUID_HEX_SIZE];
  const uint8_t *data;
  size_t len;

  switch (msg->sm_type) {
  case SMT_MPEGTS:
    data = pktbuf_ptr(msg->sm_data);
    len = pktbuf_len(msg->sm_data);
    if (sm->sm_bbf && mmi->mmi_mux->mm_type == MM_TYPE_ISI) {
      submux_input_stream(sm, data, len);
      break;
    }
    /* the packets of the PID; of a GSE stream: those of its frames, as
     * they come (BBFrames in the packets of the driver, for libdvbdab) */
    for ( ; len >= 188; data += 188, len -= 188)
      if (submux_packet(sm, data) &&
          (sm->sm_bbf == NULL || bbframe_split_take(sm->sm_bbf, data)))
        sbuf_append(&sm->sm_in, data, 188);
    if (sm->sm_in.sb_ptr == 0)
      break;
    if (mmi->mmi_decap)
      mpegts_mux_decap_feed(mmi, &sm->sm_in);
    else
      mpegts_input_recv_packets(mmi, &sm->sm_in, MPEGTS_DATA_DECAPSULATED, NULL);
    break;
  case SMT_START:
    tvhdebug(LS_MPEGTS, "%s - transponder delivers", mmi->mmi_mux->mm_nicename);
    break;
  case SMT_STOP:
    tvhdebug(LS_MPEGTS, "%s - transponder stopped (%s)", mmi->mmi_mux->mm_nicename,
             streaming_code2txt(msg->sm_code));
    break;
  case SMT_NOSTART:
  case SMT_NOSTART_WARN:
    /* the transponder cannot be tuned: the sub-mux fails now, not after its
     * grace time (takes the global lock itself, a no-op once stopped) */
    tvhwarn(LS_MPEGTS, "%s - transponder cannot start (%s)",
            mmi->mmi_mux->mm_nicename, streaming_code2txt(msg->sm_code));
    mpegts_mux_tuning_error(idnode_uuid_as_str(&mmi->mmi_mux->mm_id, ubuf), mmi);
    break;
  default:
    break;
  }
}

/* drains the queue of the subscription on the parent: the tuner thread of
 * the parent only queues */
static void *
submux_thread ( void *aux )
{
  mpegts_submux_t *sm = aux;
  streaming_queue_t *sq = &sm->sm_sq;
  streaming_message_t *msg;

  while (atomic_get(&sm->sm_run)) {
    tvh_mutex_lock(&sq->sq_mutex);
    msg = TAILQ_FIRST(&sq->sq_queue);
    if (msg == NULL) {
      tvh_cond_timedwait(&sq->sq_cond, &sq->sq_mutex, mclk() + sec2mono(1));
      tvh_mutex_unlock(&sq->sq_mutex);
      continue;
    }
    streaming_queue_remove(sq, msg);
    tvh_mutex_unlock(&sq->sq_mutex);
    submux_process(sm, msg);
    streaming_msg_free(msg);
  }
  return NULL;
}

static void
submux_free ( mpegts_submux_t *sm )
{
  lock_assert(&global_lock);

  if (sm->sm_sub) {
    subscription_unsubscribe(sm->sm_sub, UNSUBSCRIBE_FINAL);
    sm->sm_sub = NULL;
  }
  profile_chain_close(&sm->sm_prch);
  if (atomic_get(&sm->sm_run)) {
    atomic_set(&sm->sm_run, 0);
    tvh_cond_signal(&sm->sm_sq.sq_cond, 0);
    pthread_join(sm->sm_thread, NULL);
  }
  streaming_queue_deinit(&sm->sm_sq);
  sbuf_free(&sm->sm_in);
  sbuf_free(&sm->sm_out);
  if (sm->sm_packets)
    tvhdebug(LS_MPEGTS, "%s - %"PRIu64" packets of PID 0x%04x, %"PRIu64" CC gaps",
             sm->sm_mmi->mmi_mux->mm_nicename, sm->sm_packets, sm->sm_pid, sm->sm_cc_gaps);
  if (sm->sm_bbf && sm->sm_bbf->bs_ts.bt_frames)
    tvhdebug(LS_MPEGTS, "%s - %"PRIu64" BBFrames, %"PRIu64" lost, %"PRIu64" resyncs",
             sm->sm_mmi->mmi_mux->mm_nicename, sm->sm_bbf->bs_ts.bt_frames,
             sm->sm_bbf->bs_ts.bt_errors, sm->sm_bbf->bs_ts.bt_resyncs);
  free(sm->sm_bbf);
  free(sm);
}

/* the parent is arbitrated like the most important client of the sub-mux
 * (a client being opened is not linked yet: its weight comes as argument) */
static void
submux_reweight ( mpegts_mux_instance_t *mmi, int opening_weight )
{
  mpegts_submux_t *sm = mmi->mmi_submux;
  int w;

  lock_assert(&global_lock);
  if (sm == NULL || sm->sm_sub == NULL)
    return;
  tvh_mutex_lock(&mmi->mmi_input->mi_output_lock);
  w = mpegts_mux_instance_weight(mmi);
  tvh_mutex_unlock(&mmi->mmi_input->mi_output_lock);
  w = MAX(w, opening_weight);
  if (w > 0 && sm->sm_sub->ths_weight != w)
    subscription_set_weight(sm->sm_sub, w);
}

/* **************************************************************************
 * Input
 * *************************************************************************/

/* the other parents that started sub-muxes still wait for a tuner (their
 * subscription starts later), counting those at least as important as
 * 'weight'; sub-muxes of one parent share its tuner, so it counts once */
static int
submux_input_pending_parents
  ( mpegts_input_t *mi, mpegts_mux_t *mm, mpegts_mux_t *parent, int weight )
{
  mpegts_mux_instance_t *mmi;
  mpegts_submux_t *sm;
  mpegts_mux_t *p;
  mpegts_mux_t *seen[16];
  int i;
  int n = 0;

  LIST_FOREACH(mmi, &mi->mi_mux_active, mmi_active_link) {
    sm = mmi->mmi_submux;
    if (mmi->mmi_mux == mm || sm == NULL || sm->sm_sub == NULL ||
        (int)sm->sm_sub->ths_weight < weight)
      continue;
    p = submux_parent(mmi->mmi_mux);
    if (p == NULL || p == parent || p->mm_active)
      continue;
    for (i = 0; i < n && seen[i] != p; i++)
      ;
    if (i == n && n < (int)ARRAY_SIZE(seen))
      seen[n++] = p;
  }
  return n;
}

static int
submux_input_is_enabled
  ( mpegts_input_t *mi, mpegts_mux_t *mm, int flags, int weight )
{
  mpegts_mux_instance_t *mmi;
  mpegts_input_t *pi;
  mpegts_mux_t *parent;
  int r;
  int any = 0;
  int nfree = 0;

  r = mpegts_input_is_enabled(mi, mm, flags, weight);
  if (r != MI_IS_ENABLED_OK || mm == NULL)
    return r;
  parent = submux_parent(mm);
  if (parent == NULL)
    return MI_IS_ENABLED_NEVER;

  /* a tuned parent is shared; a probe (weight <= 0) is always fine */
  if (parent->mm_active || weight <= 0)
    return MI_IS_ENABLED_OK;

  /* else a real input must be able to take the parent now: if none is, the
   * scheduler tries again later, instead of starting a subscription that
   * takes a tuner from another transponder of the same weight */
  parent->mm_create_instances(parent);
  LIST_FOREACH(mmi, &parent->mm_instances, mmi_mux_link) {
    pi = mmi->mmi_input;
    r = pi->mi_is_enabled(pi, parent, flags, weight);
    if (r == MI_IS_ENABLED_NEVER)
      continue;
    any = 1;
    if (r == MI_IS_ENABLED_OK && pi->mi_get_weight(pi, parent, flags, weight) < weight)
      nfree++;
  }
  if (!any)   /* the subscription reports it; BBFrames need an input for them */
    return mpegts_mux_in_bbframes(mm) ? MI_IS_ENABLED_NEVER : MI_IS_ENABLED_OK;
  if (nfree > submux_input_pending_parents(mi, mm, parent, weight))
    return MI_IS_ENABLED_OK;
  return MI_IS_ENABLED_RETRY;
}

static int
submux_input_get_weight ( mpegts_input_t *mi, mpegts_mux_t *mm, int flags, int weight )
{
  return 0;   /* never busy */
}

static int
submux_input_warm_mux ( mpegts_input_t *mi, mpegts_mux_instance_t *mmi )
{
  return 0;   /* other sub-muxes keep running */
}

static int
submux_input_get_grace ( mpegts_input_t *mi, mpegts_mux_t *mm )
{
  mpegts_mux_t *parent = submux_parent(mm);
  mpegts_mux_instance_t *pmmi = parent ? parent->mm_active : NULL;

  /* the parent is tuned first */
  return (pmmi ? mpegts_input_grace(pmmi->mmi_input, parent) : 10) + 5;
}

static void
submux_input_display_name ( mpegts_input_t *mi, char *buf, size_t len )
{
  snprintf(buf, len, "Sub-mux");
}

static int
submux_input_start_mux ( mpegts_input_t *mi, mpegts_mux_instance_t *mmi, int weight )
{
  mpegts_mux_t *mm = mmi->mmi_mux;
  mpegts_mux_t *parent;
  mpegts_submux_t *sm;
  mpegts_service_t *ms;
  mpegts_apids_t pids;
  uint16_t pid;
  int r;
  int error = 0;

  lock_assert(&global_lock);

  if (mmi->mmi_submux)
    return 0;
  parent = submux_parent(mm);
  pid = dvb_mux_stream_pid(mm);
  if (parent == NULL || pid == 0) {
    tvherror(LS_MPEGTS, "%s - %s", mm->mm_nicename,
             parent ? "no PID of the transponder" : "transponder not found");
    return SM_CODE_TUNING_FAILED;
  }

  sm = calloc(1, sizeof(*sm));
  sm->sm_mmi = mmi;
  sm->sm_pid = pid;
  sm->sm_cc = -1;
  sbuf_init(&sm->sm_in);
  sbuf_init(&sm->sm_out);
  if (mpegts_mux_in_bbframes(mm)) {
    sm->sm_bbf = malloc(sizeof(*sm->sm_bbf));
    bbframe_split_init(sm->sm_bbf, ((dvb_mux_t *)mm)->lm_tuning.dmc_fe_stream_id);
  }
  streaming_queue_init(&sm->sm_sq, 0, sm->sm_bbf ? SUBMUX_QUEUE_SIZE_BBF : SUBMUX_QUEUE_SIZE);
  atomic_set(&sm->sm_run, 1);
  if (tvh_thread_create(&sm->sm_thread, NULL, submux_thread, sm, "submux")) {
    atomic_set(&sm->sm_run, 0);
    submux_free(sm);
    return SM_CODE_TUNING_FAILED;
  }
  mmi->mmi_submux = sm;

  /* the subscription on the parent starts now (ONESHOT: as a new one it
   * would wait for the next run of the scheduler, up to 2 seconds), then it
   * is one like the others: it moves to another tuner when it loses its own */
  profile_chain_init(&sm->sm_prch, NULL, parent, 0);
  sm->sm_prch.prch_st = &sm->sm_sq.sq_st;
  sm->sm_prch.prch_flags = SUBSCRIPTION_MPEGTS;
  sm->sm_sub = subscription_create_from_mux(&sm->sm_prch, NULL, weight > 0 ? weight : 10,
                                            "sub-mux",
                                            SUBSCRIPTION_MPEGTS | SUBSCRIPTION_STREAMING |
                                            SUBSCRIPTION_ONESHOT,
                                            NULL, NULL, mm->mm_nicename, &error);
  r = sm->sm_sub == NULL;
  if (!r) {
    sm->sm_sub->ths_flags &= ~SUBSCRIPTION_ONESHOT;
    ms = (mpegts_service_t *)sm->sm_sub->ths_service;
    mpegts_pid_init(&pids);
    mpegts_pid_add(&pids, pid, MPS_WEIGHT_RAW);
    r = ms->s_update_pids(ms, &pids);
    mpegts_pid_done(&pids);
  }
  if (r) {
    tvhdebug(LS_MPEGTS, "%s - cannot subscribe to the transponder %s (%s)",
             mm->mm_nicename, parent->mm_nicename,
             streaming_code2txt(error ?: SM_CODE_TUNING_FAILED));
    mmi->mmi_submux = NULL;
    submux_free(sm);
    return error ?: SM_CODE_TUNING_FAILED;
  }
  tvhdebug(LS_MPEGTS, "%s - from PID 0x%04x of %s", mm->mm_nicename, pid,
           parent->mm_nicename);

  /* the tables of the sub-mux open their PIDs before the stream comes */
  mm->mm_active = mmi;
  psi_tables_install(mi, mm, ((dvb_mux_t *)mm)->lm_tuning.dmc_fe_delsys);
  return 0;
}

static void
submux_input_stop_mux ( mpegts_input_t *mi, mpegts_mux_instance_t *mmi )
{
  mpegts_submux_t *sm = mmi->mmi_submux;

  if (sm == NULL)
    return;
  mmi->mmi_submux = NULL;
  submux_free(sm);
}

static void
submux_input_open_service
  ( mpegts_input_t *mi, mpegts_service_t *s, int flags, int first, int weight )
{
  mpegts_input_open_service(mi, s, flags, first, weight);
  if (s->s_dvb_mux->mm_active)
    submux_reweight(s->s_dvb_mux->mm_active, weight);
}

static void
submux_input_close_service ( mpegts_input_t *mi, mpegts_service_t *s )
{
  mpegts_mux_instance_t *mmi = s->s_dvb_mux->mm_active;

  mpegts_input_close_service(mi, s);
  if (mmi)
    submux_reweight(mmi, 0);
}

/* **************************************************************************
 * Init
 * *************************************************************************/

void
mpegts_submux_create_instance ( mpegts_mux_t *mm )
{
  mpegts_input_t *mi = submux_input;

  if (mi && mpegts_mux_is_submux(mm) &&
      mi->mi_is_enabled(mi, mm, 0, -1) != MI_IS_ENABLED_NEVER)
    mi->mi_create_mux_instance(mi, mm);
}

void
mpegts_submux_init ( void )
{
  mpegts_input_t *mi;

  mi = mpegts_input_create0(calloc(1, sizeof(mpegts_input_t)),
                            &mpegts_submux_input_class, NULL, NULL);
  if (mi == NULL)
    return;
  mi->mi_enabled       = 1;
  mi->mi_mux_types     = 1u << MM_TYPE_ISI;
#if ENABLE_DVBDAB
  mi->mi_mux_types    |= (1u << MM_TYPE_DAB_MPE) | (1u << MM_TYPE_DAB_ETI) |
                         (1u << MM_TYPE_DAB_TSNI) |
                         (1u << MM_TYPE_GSE) | (1u << MM_TYPE_DAB_GSE);
#endif
  mi->mi_is_enabled    = submux_input_is_enabled;
  mi->mi_get_weight    = submux_input_get_weight;
  mi->mi_warm_mux      = submux_input_warm_mux;
  mi->mi_get_grace     = submux_input_get_grace;
  mi->mi_display_name  = submux_input_display_name;
  mi->mi_start_mux     = submux_input_start_mux;
  mi->mi_stop_mux      = submux_input_stop_mux;
  mi->mi_open_service  = submux_input_open_service;
  mi->mi_close_service = submux_input_close_service;
  submux_input = mi;
}

void
mpegts_submux_done ( void )
{
  mpegts_input_t *mi = submux_input;

  if (mi == NULL)
    return;
  submux_input = NULL;
  mpegts_input_stop_all(mi);
  mpegts_input_delete(mi, 0);
}
