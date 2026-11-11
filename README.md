> [!NOTE]
> **This is the L-S-D fork of Tvheadend.** It follows upstream `master` and adds work that waits
> upstream as a pull request or is not meant for upstream. Below this note is the upstream README.

### What is different

- **Instant zapping:** muxes marked *Prebuffer (instant zapping)*, and optionally the last watched
  ones, stay tuned with a ring buffer of their whole transport stream. A channel on such a warm mux
  starts at the last keyframe from the buffer, so the picture comes up in about half a second.
  Viewers start close to live; an ECM prefetch for camd35 (OSCam) has the keys ready.
  Settings: Configuration → Stream → Instant Zapping, also the warm muxes and the ECM PIDs to
  prefetch for each of them.
- **More stream details over HTSP:** component tags, subtitle page and type, initial teletext page,
  audio type and receiver mix, the DVB ids (ONID, TSID, SID) of the channel, and a queue that keeps
  at least 1.5 s of the stream, and on request the internal channel tags. [TVF](https://github.com/L-S-D/tvfrontend),
  a client for LG webOS TVs, uses them.
- **DAB radio from DVB** with [libdvbdab](https://github.com/L-S-D/libdvbdab): ETI-NA, MPE, TS NI
  and DAB over GSE.
- **Neumo DVB driver** (for example TBS 6909X): multistream (ISI), GSE, blind tuning, Unicable
  groups, shared RF inputs, external polarizer control. The input streams of a multistream
  transponder are sub-muxes that share its tuner; each mux shows the MATYPE and MODCOD the
  demodulator reports (CCM or ACM/VCM).
- **T2-MI** and TS piping as sub-muxes, also from scrambled carriers.
- **Muxes as a tree** in the web interface (Configuration → DVB Inputs → Muxes): networks,
  transponders with their tuning, the sub-muxes carried in them (input streams, DAB, T2-MI) and
  the services of each mux, with scan per row, filter, add, edit, delete and live updates. The
  Services grid shows the type, the sub-mux a service comes from, its streams in short, ONID,
  TSID and HbbTV.
- **capmt fixes for OSCam:** ECM flush, CSA-ALT, message order, forced CA id.
- Smaller fixes: the timeshift option *Include teletext*, the local language codes qaa..qtz, a higher
  service priority beats an already tuned mux.

### HbbTV through a proxy (TVF)

The HbbTV apps of some channels only work from their home country. TVF can therefore send the
HbbTV traffic of a channel (pages, videos, licences) through a proxy. There are three ways, chosen in
TVF's settings under *Internet access*:

1. **No proxy** (*direct*): enough when the apps of all channels are reachable from where you are.
2. **One global proxy**: all HbbTV traffic goes to one proxy, typically a "smart" proxy that decides
   by destination itself (for example `*.de` through a German exit, everything else direct). Nothing
   to set up in tvheadend.
3. **A proxy per channel, set in tvheadend**: create an *internal* channel tag named
   `hbbtv-proxy <proxy URL>`, for example `hbbtv-proxy http://192.0.2.10:8906`, and put the channels
   into it; `hbbtv-proxy direct` keeps a channel off a global proxy. TVF asks tvheadend for the
   internal tags with the HTSP extension of the branch `pr/htsp-internal-tags` (`internalTags` in
   `enableAsyncMetadata`) and sends the HbbTV traffic of each channel through the proxy of its tag;
   that tag wins over the setting under *Internet access*, which applies to the channels without
   one. Internal tags are not sent to other HTSP clients: Kodi sees neither the tags nor any change
   to its channels or groups.

### Branches

| Branch | Contents |
|---|---|
| `master` | the same as `v3/all` |
| `v3/base` | upstream `master` + our open upstream pull requests + the fork's own topics |
| `v3/tvf` | `v3/base` + instant zapping, recommended for TVF |
| `v3/neumo` | `v3/base` + the Neumo driver support |
| `v3/all` | `v3/tvf` + Neumo + T2-MI + service priority |
| `pr/*` | one topic each on upstream `master`, as sent upstream |
| `fork/*` | topics only for this fork (this note, the mux tree) |

Our pull requests upstream:
[tvheadend/tvheadend, author L-S-D](https://github.com/tvheadend/tvheadend/pulls?q=is%3Apr+author%3AL-S-D).

---

<div align="center">

[![Build Status](https://github.com/tvheadend/tvheadend/actions/workflows/build-cloudsmith.yml/badge.svg?branch=master)](https://github.com/tvheadend/tvheadend/actions/workflows/build-cloudsmith.yml)
[![Coverity Scan](https://scan.coverity.com/projects/2114/badge.svg)](https://scan.coverity.com/projects/2114)
[![Github last commit](https://img.shields.io/github/last-commit/tvheadend/tvheadend)](https://github.com/tvheadend/tvheadend)

[![Releases](https://img.shields.io/github/tag/tvheadend/tvheadend.svg?style=flat-square)](https://github.com/tvheadend/tvheadend/releases)
[![License](https://img.shields.io/badge/license-GPLv3-blue)](./LICENSE.md) 
[![GitHub Activity](https://img.shields.io/github/commit-activity/y/tvheadend/tvheadend.svg?label=commits)](https://github.com/tvheadend/tvheadend/commits)

[![Hosted By: Cloudsmith](https://img.shields.io/badge/Packages%20hosted%20by-cloudsmith-blue?logo=cloudsmith&style=flat-square)](https://cloudsmith.io/~tvheadend/repos/tvheadend/packages/)

</div>

Tvheadend
=========

Tvheadend is the leading TV streaming server and Digital Video Recorder for Linux.

![tvheadend front page](https://github.com/tvheadend/tvheadend/raw/master/src/webui/static/img/epg.png)

It supports the following inputs:

  * ATSC
  * DVB-C(2)
  * DVB-S(2)
  * DVB-T(2)
  * HDHomeRun
  * IPTV
    * UDP
    * HTTP
  * SAT>IP
  * Unix Pipe

It supports the following outputs:

  * HTSP (native protocol)
  * HTTP
  * SAT>IP

Documentation
-------------

Tvheadend documentation can be found here: [https://docs.tvheadend.org](https://docs.tvheadend.org).

Support
-------

Please triage issues and ask questions in the forum: [https://tvheadend.org](https://tvheadend.org) or use the `#hts` IRC channel on Libera.Chat to speak with project staff: [https://web.libera.chat/#hts](https://web.libera.chat/#hts).

Please report triaged bugs via GitHub Issues. 

Building for Linux
------------------

First you need to configure:

	$ ./configure

If build dependencies are missing the configure script will complain or attempt
to disable optional features.

To build the binary:

	$ make

After compiling the Tvheadend binary is in the `build.linux` directory.

To run the Tvheadend binary:

	$ ./build.linux/tvheadend

Settings are stored in `$HOME/.hts/tvheadend`.

To install the newly compiled Tvheadend binary and associated files onto your system:

	$ sudo make install

Running on Linux
----------------

Instructions for popular distributions are in our public [documentation](https://docs.tvheadend.org/documentation/installation/linux).

Running in Docker
-----------------

Running in Docker can be as simple as:

	$ docker run --rm ghcr.io/tvheadend/tvheadend:latest

See [README.Docker.md](README.Docker.md) for more details.
