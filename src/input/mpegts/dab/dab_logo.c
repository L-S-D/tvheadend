/*
 *  DAB station logos as channel icons
 *
 *  An ensemble with a programme guide can send the logos of its stations
 *  with it; libdvbdab hands them over while the sub-mux of the ensemble
 *  runs. The largest one of a service is kept as a file in the
 *  configuration directory and is the icon of its channels, as the
 *  setting "DAB station logos" says.
 *
 *  Copyright (C) 2026 L-S-D
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 */

#include "tvheadend.h"

#if ENABLE_DVBDAB

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include "config.h"
#include "settings.h"
#include "channels.h"
#include "input.h"
#include "dab_stream.h"
#include <dvbdab/dvbdab_c.h>

#define DAB_LOGO_DIR      "dablogos"
#define DAB_LOGO_MAX_SIZE (1024 * 1024)

/*
 * A name this file made (<uuid of the service>-<width>x<height>-<crc>.<png|jpg>):
 * the one of a service comes from its configuration file, and must not lead
 * out of the directory of the logos
 */
static int
dab_logo_name_ok(const char *name)
{
  size_t i, n;

  if (name == NULL)
    return 0;
  n = strlen(name);
  if (n < UUID_HEX_SIZE + 4 || n > UUID_HEX_SIZE + 40)
    return 0;
  for (i = 0; i < n; i++) {
    char c = name[i];
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || c == '-' || c == '.'))
      return 0;
  }
  return strstr(name, "..") == NULL;
}

static const char *
dab_logo_url(const char *name, char *buf, size_t len)
{
  static const char scheme[] = "file://";
  const size_t n = sizeof(scheme) - 1;

  if (len <= n)
    return NULL;
  memcpy(buf, scheme, n);
  if (hts_settings_buildpath(buf + n, len - n, DAB_LOGO_DIR "/%s", name))
    return NULL;
  return buf;
}

/*
 * The logo of a service as the icon of its channels; NULL without one, or
 * when the logos are not to be used
 */
const char *
dab_logo_service_icon(mpegts_service_t *ms)
{
  if (config.dab_logo == DAB_LOGO_OFF || !dab_logo_name_ok(ms->s_dab_logo))
    return NULL;
  return dab_logo_url(ms->s_dab_logo, prop_sbuf, PROP_SBUF_LEN);
}

/* a service is deleted with its configuration: its logo file goes too */
void
dab_logo_service_delete(mpegts_service_t *ms)
{
  if (dab_logo_name_ok(ms->s_dab_logo))
    hts_settings_remove(DAB_LOGO_DIR "/%s", ms->s_dab_logo);
}

#ifdef DVBDAB_HAVE_LOGOS

/* a logo file is named <uuid of the service>-<width>x<height>-<crc>.<png|jpg> */
static int
dab_logo_parse_name(const char *name, int *width, int *height, uint32_t *crc)
{
  unsigned int c;

  if (!dab_logo_name_ok(name))
    return 0;
  if (sscanf(name + UUID_HEX_SIZE - 1, "-%dx%d-%8x.", width, height, &c) != 3)
    return 0;
  *crc = c;
  return 1;
}

/* a logo on its way from the thread that feeds the streamer to the services */
typedef struct dab_logo_job {
  tvh_uuid_t mux;
  uint16_t   sid;
  int        width;
  int        height;
  int        jpeg;
  size_t     len;
  uint8_t    data[0];
} dab_logo_job_t;

/*
 * A channel has no icon: none is named, or the one that is named is a local
 * file that is not there (tvheadend fills in the name a picon or a channel
 * icon of the channel would have, whether there is such a file or not)
 */
static int
dab_logo_icon_unset(const char *icon)
{
  char path[PATH_MAX];
  const char *dir;
  struct stat st;

  if (tvh_str_default(icon, NULL) == NULL)
    return 1;
  if (strncmp(icon, "picon://", 8) == 0) {
    dir = tvh_str_default(config.picon_path, NULL);
    if (dir == NULL)
      return 1;
    if (strncmp(dir, "file://", 7) == 0)
      dir += 7;
    else if (strstr(dir, "://"))
      return 0;                 /* picons from a server */
    if (strlen(dir) + strlen(icon + 8) + 2 > sizeof(path))
      return 0;
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, icon + 8);
  } else if (strncmp(icon, "file://", 7) == 0) {
    strlcpy(path, icon + 7, sizeof(path));
    http_deescape(path);
  } else if (icon[0] == '/') {
    strlcpy(path, icon, sizeof(path));
  } else {
    return 0;                   /* from a server */
  }
  return stat(path, &st) != 0 || !S_ISREG(st.st_mode);
}

static int
dab_logo_write(const char *name, const uint8_t *data, size_t len)
{
  int fd = hts_settings_open_file(HTS_SETTINGS_OPEN_WRITE | HTS_SETTINGS_OPEN_DIRECT,
                                  DAB_LOGO_DIR "/%s", name);
  int r;

  if (fd < 0)
    return -1;
  r = tvh_write(fd, data, len);
  close(fd);
  if (r)
    hts_settings_remove(DAB_LOGO_DIR "/%s", name);
  return r ? -1 : 0;
}

/*
 * A logo of a service of the sub-mux: kept when the service has none or a
 * smaller one, or another image of that size; then the icon of the channels
 * of the service as the setting says
 */
static void
dab_logo_apply(const dab_logo_job_t *job)
{
  mpegts_mux_t *mm;
  mpegts_service_t *s;
  idnode_list_mapping_t *ilm;
  channel_t *ch;
  char name[UUID_HEX_SIZE + 48], ubuf[UUID_HEX_SIZE];
  char url[PATH_MAX + 8], old_url[PATH_MAX + 8];
  const char *cur, *old = NULL;
  int mode = config.dab_logo;
  int w = 0, h = 0, have;
  uint32_t crc, hcrc = 0;
  tvh_uuid_t uuid = job->mux;

  lock_assert(&global_lock);

  if (mode == DAB_LOGO_OFF || (mm = mpegts_mux_find0(&uuid)) == NULL)
    return;
  LIST_FOREACH(s, &mm->mm_services, s_dvb_mux_link)
    if (service_id16(s) == job->sid)
      break;
  if (s == NULL)
    return;

  crc = tvh_crc32(job->data, job->len, 0xffffffff);
  have = dab_logo_parse_name(s->s_dab_logo, &w, &h, &hcrc);
  if (have) {
    if ((int64_t)job->width * job->height < (int64_t)w * h)
      return;                   /* a smaller one than the service has */
    old = dab_logo_url(s->s_dab_logo, old_url, sizeof(old_url));
  }

  snprintf(name, sizeof(name), "%s-%dx%d-%08x.%s", idnode_uuid_as_str(&s->s_id, ubuf),
           job->width, job->height, crc, job->jpeg ? "jpg" : "png");
  if (dab_logo_url(name, url, sizeof(url)) == NULL)
    return;

  if (!have || strcmp(s->s_dab_logo, name) ||
      !hts_settings_exists(DAB_LOGO_DIR "/%s", name)) {
    if (dab_logo_write(name, job->data, job->len)) {
      tvherror(LS_MPEGTS, "%s: the logo file %s could not be written", s->s_nicename, name);
      return;
    }
    if (s->s_dab_logo == NULL || strcmp(s->s_dab_logo, name)) {
      if (have)
        hts_settings_remove(DAB_LOGO_DIR "/%s", s->s_dab_logo);
      free(s->s_dab_logo);
      s->s_dab_logo = strdup(name);
      idnode_changed(&s->s_id);
      tvhinfo(LS_MPEGTS, "%s: logo %dx%d from the ensemble", s->s_nicename,
              job->width, job->height);
    }
  }

  /* always, or for a channel that has no icon or the logo this one replaces */
  LIST_FOREACH(ilm, &s->s_channels, ilm_in1_link) {
    ch = (channel_t *)ilm->ilm_in2;
    cur = ch->ch_icon;
    if (mode != DAB_LOGO_ALWAYS && !dab_logo_icon_unset(cur) &&
        (old == NULL || strcmp(cur, old)))
      continue;
    if (channel_set_icon(ch, url))
      idnode_changed(&ch->ch_id);
  }
}

static void
dab_logo_task(void *opaque, int disarmed)
{
  dab_logo_job_t *job = opaque;

  if (!disarmed) {
    tvh_mutex_lock(&global_lock);
    if (tvheadend_is_running())
      dab_logo_apply(job);
    tvh_mutex_unlock(&global_lock);
  }
  free(job);
}

/*
 * libdvbdab has a logo of a service (in the thread that feeds the
 * streamer): on to a tasklet, which takes the global lock
 */
static void
dab_logo_cb(void *opaque, const dvbdab_logo_t *logo)
{
  const tvh_uuid_t *mux = opaque;
  dab_logo_job_t *job;
  int jpeg;

  if (config.dab_logo == DAB_LOGO_OFF)
    return;
  if (logo->data == NULL || logo->len == 0 || logo->len > DAB_LOGO_MAX_SIZE ||
      logo->width <= 0 || logo->height <= 0 || logo->sid > 0xffff ||
      logo->mime_type == NULL)
    return;
  jpeg = strcmp(logo->mime_type, "image/jpeg") == 0;
  if (!jpeg && strcmp(logo->mime_type, "image/png"))
    return;

  job = malloc(sizeof(*job) + logo->len);
  if (job == NULL)
    return;
  job->mux = *mux;
  job->sid = logo->sid;
  job->width = logo->width;
  job->height = logo->height;
  job->jpeg = jpeg;
  job->len = logo->len;
  memcpy(job->data, logo->data, logo->len);
  if (tasklet_arm_alloc(dab_logo_task, job) == NULL)
    free(job);
}

#endif /* DVBDAB_HAVE_LOGOS */

/*
 * The logos of the services of the sub-mux `mux`, from this streamer, as
 * the icons of their channels. `mux` stays valid as long as the streamer.
 */
void
dab_logo_attach(struct dvbdab_streamer *streamer, const tvh_uuid_t *mux)
{
#ifdef DVBDAB_HAVE_LOGOS
  dvbdab_streamer_set_logo_callback(streamer, dab_logo_cb, (void *)mux);
#endif
}

#endif /* ENABLE_DVBDAB */
