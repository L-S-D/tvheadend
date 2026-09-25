/*
 *  Tvheadend - instant channel switching, warm mux manager
 *
 *  Every mux marked "prebuffer" gets a low-weight full-mux subscription so
 *  it stays tuned and its ring buffer filled. Real viewers (higher weight)
 *  take the tuner when they need it; tvh's subscription scheduler brings
 *  the warm mux back as soon as a tuner is free again.
 *
 *  LRU: the muxes last used by viewers (service starts, HLS clients) are
 *  kept warm as well, up to lru_max, with a lower weight (lru_weight). A
 *  used mux gets its warm subscription and ring buffer while it is being
 *  watched, so it stays tuned with a full backlog when the viewer leaves.
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
  int                 dw_lru;       /* kept warm as recently used */
} dvbbuffer_warm_t;

/* mux use by viewers, for the LRU (global_lock) */
typedef struct dvbbuffer_used {
  LIST_ENTRY(dvbbuffer_used) du_link;
  mpegts_mux_t       *du_mux;
  int                 du_users;     /* services / HLS clients now */
  int64_t             du_last;      /* mclk() of the last start or stop */
  int                 du_sel;       /* among the lru_max kept warm */
} dvbbuffer_used_t;

static LIST_HEAD(, dvbbuffer_warm) dvbbuffer_warm_all;
static LIST_HEAD(, dvbbuffer_used) dvbbuffer_used_all;
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

static inline uint32_t
dvbbuffer_warm_weight(int lru)
{
  return lru ? dvbbuffer_conf.lru_weight : dvbbuffer_conf.warm_weight;
}

static void
dvbbuffer_warm_destroy(dvbbuffer_warm_t *dw)
{
  mpegts_mux_t *mm = dw->dw_mux;

  tvhinfo(LS_DVBBUFFER, "%s: %s mux released", mm->mm_nicename,
          dw->dw_lru ? "recently used" : "warm");
  LIST_REMOVE(dw, dw_link);
  if (dw->dw_sub)
    subscription_unsubscribe(dw->dw_sub, UNSUBSCRIBE_FINAL);
  free(dw);
  /* still tuned for others (viewers) - the ring buffer may go */
  if (!dvbbuffer_mux_wanted(mm))
    dvbbuffer_mux_detach(mm);
}

static void
dvbbuffer_warm_create(mpegts_mux_t *mm, int lru)
{
  dvbbuffer_warm_t *dw;
  mpegts_service_t *ms;
  mpegts_apids_t pids;

  dw = calloc(1, sizeof(*dw));
  dw->dw_mux = mm;
  dw->dw_mark = 1;
  dw->dw_lru = lru;
  streaming_target_init(&dw->dw_input, &dvbbuffer_warm_input_ops, dw, 0);
  dw->dw_prch.prch_id = mm;
  dw->dw_prch.prch_st = &dw->dw_input;
  LIST_INSERT_HEAD(&dvbbuffer_warm_all, dw, dw_link);

  dw->dw_sub = subscription_create_from_mux(&dw->dw_prch, NULL,
                                            dvbbuffer_warm_weight(lru),
                                            lru ? "dvbbuffer LRU" : "dvbbuffer",
                                            SUBSCRIPTION_MINIMAL,
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

  tvhinfo(LS_DVBBUFFER, "%s: %s mux (weight %u)", mm->mm_nicename,
          lru ? "recently used" : "warm", dvbbuffer_warm_weight(lru));
  /* a running mux (in use) gets its ring buffer now */
  dvbbuffer_mux_attach(mm);
}

static dvbbuffer_warm_t *
dvbbuffer_warm_find(mpegts_mux_t *mm)
{
  dvbbuffer_warm_t *dw;

  LIST_FOREACH(dw, &dvbbuffer_warm_all, dw_link)
    if (dw->dw_mux == mm)
      break;
  return dw;
}

static void
dvbbuffer_warm_set_lru(dvbbuffer_warm_t *dw, int lru)
{
  if (dw->dw_lru == lru)
    return;
  dw->dw_lru = lru;
  if (dw->dw_sub)
    subscription_change_weight(dw->dw_sub, dvbbuffer_warm_weight(lru));
  tvhdebug(LS_DVBBUFFER, "%s: now kept warm as %s mux (weight %u)",
           dw->dw_mux->mm_nicename, lru ? "recently used" : "prebuffer",
           dvbbuffer_warm_weight(lru));
}

static int
dvbbuffer_used_cmp(const void *a, const void *b)
{
  const dvbbuffer_used_t *x = *(dvbbuffer_used_t * const *)a;
  const dvbbuffer_used_t *y = *(dvbbuffer_used_t * const *)b;

  if ((x->du_users > 0) != (y->du_users > 0))
    return x->du_users > 0 ? -1 : 1;           /* in use first */
  if (x->du_last != y->du_last)
    return x->du_last > y->du_last ? -1 : 1;   /* then the latest */
  return 0;
}

/*
 * Choose the recently used muxes kept warm; forget the others
 */
static void
dvbbuffer_used_select(void)
{
  dvbbuffer_used_t *du, *du_next, **v;
  uint32_t n = 0, i, sel;

  LIST_FOREACH(du, &dvbbuffer_used_all, du_link)
    n++;
  if (n == 0)
    return;
  v = alloca(n * sizeof(*v));
  n = 0;
  LIST_FOREACH(du, &dvbbuffer_used_all, du_link) {
    du->du_sel = 0;
    v[n++] = du;
  }
  qsort(v, n, sizeof(*v), dvbbuffer_used_cmp);
  /* prebuffer muxes are warm anyway: they do not take LRU places */
  for (i = 0, sel = 0; i < n && sel < dvbbuffer_conf.lru_max; i++)
    if (dvbbuffer_ctx && dvbbuffer_conf.enabled &&
        !dvbbuffer_mux_prebuffer(v[i]->du_mux) && v[i]->du_mux->mm_is_enabled(v[i]->du_mux)) {
      v[i]->du_sel = 1;
      sel++;
    }
  for (du = LIST_FIRST(&dvbbuffer_used_all); du; du = du_next) {
    du_next = LIST_NEXT(du, du_link);
    if (!du->du_sel && du->du_users <= 0) {
      LIST_REMOVE(du, du_link);
      free(du);
    }
  }
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

  dvbbuffer_used_t *du;

  lock_assert(&global_lock);

  dvbbuffer_used_select();

  LIST_FOREACH(dw, &dvbbuffer_warm_all, dw_link)
    dw->dw_mark = 0;

  /* keep existing ones first (up to warm_max) */
  LIST_FOREACH(dw, &dvbbuffer_warm_all, dw_link)
    if (dvbbuffer_mux_prebuffer(dw->dw_mux) && count < dvbbuffer_conf.warm_max) {
      dw->dw_mark = 1;
      dvbbuffer_warm_set_lru(dw, 0);
      count++;
    }
  LIST_FOREACH(du, &dvbbuffer_used_all, du_link)
    if (du->du_sel && (dw = dvbbuffer_warm_find(du->du_mux)) != NULL && !dw->dw_mark) {
      dw->dw_mark = 1;
      dvbbuffer_warm_set_lru(dw, 1);
    }

  for (dw = LIST_FIRST(&dvbbuffer_warm_all); dw; dw = dw_next) {
    dw_next = LIST_NEXT(dw, dw_link);
    if (!dw->dw_mark)
      dvbbuffer_warm_destroy(dw);
  }

  LIST_FOREACH(mn, &mpegts_network_all, mn_global_link)
    LIST_FOREACH(mm, &mn->mn_muxes, mm_network_link) {
      if (!dvbbuffer_mux_prebuffer(mm))
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
      dvbbuffer_warm_create(mm, 0);
      count++;
    }

  LIST_FOREACH(du, &dvbbuffer_used_all, du_link)
    if (du->du_sel && dvbbuffer_warm_find(du->du_mux) == NULL)
      dvbbuffer_warm_create(du->du_mux, 1);
}

void
dvbbuffer_warm_reconcile(void)
{
  mtimer_arm_rel(&dvbbuffer_warm_timer, dvbbuffer_warm_reconcile_cb, NULL, 0);
}

/*
 * A viewer starts (+1) or stops (-1) using a mux (global_lock)
 */
void
dvbbuffer_warm_used(mpegts_mux_t *mm, int delta)
{
  dvbbuffer_used_t *du;

  lock_assert(&global_lock);
  LIST_FOREACH(du, &dvbbuffer_used_all, du_link)
    if (du->du_mux == mm)
      break;
  if (du == NULL) {
    if (delta < 0)
      return;
    du = calloc(1, sizeof(*du));
    du->du_mux = mm;
    LIST_INSERT_HEAD(&dvbbuffer_used_all, du, du_link);
  }
  du->du_users += delta;
  if (du->du_users < 0)
    du->du_users = 0;
  du->du_last = mclk();
  if (dvbbuffer_conf.lru_max)
    dvbbuffer_warm_reconcile();
}

/* kept warm as a recently used mux */
int
dvbbuffer_warm_lru(mpegts_mux_t *mm)
{
  dvbbuffer_warm_t *dw = dvbbuffer_warm_find(mm);
  return dw != NULL && dw->dw_lru;
}

void
dvbbuffer_warm_mux_delete(mpegts_mux_t *mm)
{
  dvbbuffer_warm_t *dw;
  dvbbuffer_used_t *du;

  LIST_FOREACH(du, &dvbbuffer_used_all, du_link)
    if (du->du_mux == mm) {
      LIST_REMOVE(du, du_link);
      free(du);
      break;
    }

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

  dvbbuffer_used_t *du;

  mtimer_disarm(&dvbbuffer_warm_timer);
  while ((dw = LIST_FIRST(&dvbbuffer_warm_all)) != NULL)
    dvbbuffer_warm_destroy(dw);
  while ((du = LIST_FIRST(&dvbbuffer_used_all)) != NULL) {
    LIST_REMOVE(du, du_link);
    free(du);
  }
}
