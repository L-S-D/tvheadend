/*
 *  Tvheadend - instant channel switching, warm mux manager
 *
 *  Every mux marked "prebuffer" gets a low-weight full-mux subscription so
 *  it stays tuned and its ring buffer filled. Real viewers (higher weight)
 *  take the tuner when they need it; tvh's subscription scheduler brings
 *  the warm mux back as soon as a tuner is free again.
 *
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "dvbbuffer_private.h"
#include "subscriptions.h"
#include "streaming.h"
#include "profile.h"

typedef struct dvbbuffer_warm {
  LIST_ENTRY(dvbbuffer_warm) dw_link;
  mpegts_mux_t       *dw_mux;
  profile_chain_t     dw_prch;
  streaming_target_t  dw_input;
  th_subscription_t  *dw_sub;
  int                 dw_mark;
} dvbbuffer_warm_t;

static LIST_HEAD(, dvbbuffer_warm) dvbbuffer_warm_all;
static mtimer_t dvbbuffer_warm_timer;

/*
 * Subscription output: the data itself is taken in mpegts_input_process()
 */
static void
dvbbuffer_warm_input(void *opaque, streaming_message_t *sm)
{
  dvbbuffer_warm_t *dw = opaque;

  switch (sm->sm_type) {
  case SMT_STOP:
  case SMT_NOSTART:
    tvhdebug(LS_DVBBUFFER, "%s: warm subscription %s (%s)",
             dw->dw_mux->mm_nicename,
             sm->sm_type == SMT_STOP ? "stopped" : "not started",
             streaming_code2txt(sm->sm_code));
    break;
  default:
    break;
  }
  streaming_msg_free(sm);
}

static htsmsg_t *
dvbbuffer_warm_input_info(void *opaque, htsmsg_t *list)
{
  htsmsg_add_str(list, NULL, "dvbbuffer warm mux input");
  return list;
}

static streaming_ops_t dvbbuffer_warm_input_ops = {
  .st_cb   = dvbbuffer_warm_input,
  .st_info = dvbbuffer_warm_input_info
};

static void
dvbbuffer_warm_destroy(dvbbuffer_warm_t *dw)
{
  tvhinfo(LS_DVBBUFFER, "%s: warm mux released", dw->dw_mux->mm_nicename);
  if (dw->dw_sub)
    subscription_unsubscribe(dw->dw_sub, UNSUBSCRIBE_FINAL);
  LIST_REMOVE(dw, dw_link);
  free(dw);
}

static void
dvbbuffer_warm_create(mpegts_mux_t *mm)
{
  dvbbuffer_warm_t *dw;
  mpegts_service_t *ms;
  mpegts_apids_t pids;

  dw = calloc(1, sizeof(*dw));
  dw->dw_mux = mm;
  dw->dw_mark = 1;
  streaming_target_init(&dw->dw_input, &dvbbuffer_warm_input_ops, dw, 0);
  dw->dw_prch.prch_id = mm;
  dw->dw_prch.prch_st = &dw->dw_input;
  LIST_INSERT_HEAD(&dvbbuffer_warm_all, dw, dw_link);

  dw->dw_sub = subscription_create_from_mux(&dw->dw_prch, NULL,
                                            dvbbuffer_conf.warm_weight,
                                            "dvbbuffer", SUBSCRIPTION_MINIMAL,
                                            NULL, NULL, NULL, NULL);
  if (dw->dw_sub == NULL) {
    tvherror(LS_DVBBUFFER, "%s: unable to create warm subscription",
             mm->mm_nicename);
    LIST_REMOVE(dw, dw_link);
    free(dw);
    return;
  }

  /* the whole transport stream */
  ms = (mpegts_service_t *)dw->dw_sub->ths_service;
  mpegts_pid_init(&pids);
  pids.all = 1;
  ms->s_update_pids(ms, &pids);
  mpegts_pid_done(&pids);

  tvhinfo(LS_DVBBUFFER, "%s: warm mux (weight %u)",
          mm->mm_nicename, dvbbuffer_conf.warm_weight);
}

/*
 * Bring the warm subscriptions in line with the mux flags and the
 * configuration (global_lock)
 */
static void
dvbbuffer_warm_reconcile_cb(void *aux)
{
  mpegts_network_t *mn;
  mpegts_mux_t *mm;
  dvbbuffer_warm_t *dw, *dw_next;
  uint32_t count = 0;

  lock_assert(&global_lock);

  LIST_FOREACH(dw, &dvbbuffer_warm_all, dw_link)
    dw->dw_mark = 0;

  /* keep existing ones first (up to warm_max) */
  LIST_FOREACH(dw, &dvbbuffer_warm_all, dw_link)
    if (dvbbuffer_mux_wanted(dw->dw_mux) && count < dvbbuffer_conf.warm_max) {
      dw->dw_mark = 1;
      count++;
    }

  for (dw = LIST_FIRST(&dvbbuffer_warm_all); dw; dw = dw_next) {
    dw_next = LIST_NEXT(dw, dw_link);
    if (!dw->dw_mark)
      dvbbuffer_warm_destroy(dw);
  }

  LIST_FOREACH(mn, &mpegts_network_all, mn_global_link)
    LIST_FOREACH(mm, &mn->mn_muxes, mm_network_link) {
      if (!dvbbuffer_mux_wanted(mm))
        continue;
      LIST_FOREACH(dw, &dvbbuffer_warm_all, dw_link)
        if (dw->dw_mux == mm)
          break;
      if (dw)
        continue;
      if (count >= dvbbuffer_conf.warm_max) {
        tvhwarn(LS_DVBBUFFER, "%s: not kept warm, limit of %u warm muxes reached",
                mm->mm_nicename, dvbbuffer_conf.warm_max);
        continue;
      }
      dvbbuffer_warm_create(mm);
      count++;
    }
}

void
dvbbuffer_warm_reconcile(void)
{
  mtimer_arm_rel(&dvbbuffer_warm_timer, dvbbuffer_warm_reconcile_cb, NULL, 0);
}

void
dvbbuffer_warm_mux_delete(mpegts_mux_t *mm)
{
  dvbbuffer_warm_t *dw;

  LIST_FOREACH(dw, &dvbbuffer_warm_all, dw_link)
    if (dw->dw_mux == mm) {
      dvbbuffer_warm_destroy(dw);
      break;
    }
}

void
dvbbuffer_warm_init(void)
{
  /* give the inputs some time to settle after startup */
  mtimer_arm_rel(&dvbbuffer_warm_timer, dvbbuffer_warm_reconcile_cb, NULL,
                 sec2mono(5));
}

void
dvbbuffer_warm_done(void)
{
  dvbbuffer_warm_t *dw;

  mtimer_disarm(&dvbbuffer_warm_timer);
  while ((dw = LIST_FIRST(&dvbbuffer_warm_all)) != NULL)
    dvbbuffer_warm_destroy(dw);
}
