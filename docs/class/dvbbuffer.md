<tvh_include>inc/stream_contents</tvh_include>

---

## Overview

Instant zapping keeps chosen DVB muxes tuned in the background and holds the
last seconds of their whole transport stream in RAM, in a ring buffer in
front of the input processing. A service started on such a *warm* mux does
not wait for the next PAT, PMT, ECM and keyframe: it gets the buffered
seconds at once, through the same demultiplexing and descrambling as live
data, and plays within a fraction of a second.

---

## How it works

* **Warm muxes**: every mux with *Prebuffer (instant zapping)* enabled
  (Configuration → DVB Inputs → Muxes) gets a full mux subscription with the
  *Warm mux weight*. It stays tuned while a tuner is free; viewers and
  recordings always win, and the warm mux comes back when a tuner is free
  again.

* **Backlog**: a service, table or CA client that starts on a warm mux gets
  the last *Buffer (seconds)* of its PIDs first, then the live data. A
  scrambled service starts at the first ECM in the backlog, so that every
  packet it gets has its key.

* **PMTs**: while a mux has a ring buffer, the stored PMTs of all its
  services are kept current (tvh reads PMTs only of running services
  otherwise). A service started there does not restart its streams for a
  PMT that changed since (an audio track came or went) right after its
  backlog.

* **Recently used muxes**: with *Recently used muxes kept warm* above 0, the
  muxes last watched stay tuned with a ring buffer as well (with the lower
  *Recently used mux weight*), so zapping back starts from the buffer.

* **Live start**: a client should not play from the start of the backlog,
  seconds behind live. Profiles with *Instant zapping: start live* (Stream
  Profiles) pass the stream on from the newest keyframe at least *Live start
  behind live* behind live; audio starts there too, teletext comes whole.
  Recordings always get the whole backlog.

---

## Notes

* Each warm mux needs a tuner and RAM: the ring is sized for the bitrate of
  the mux from its tuning parameters (at most *Maximum mux bitrate*), about
  12 MB per second of buffer for a 80 Mbit/s mux.

* Keep the buffer below the shortest key period of the pay TV services in
  use (7 s for many of them): the backlog then holds one key change at most.

* The *Warm mux weight* decides what else may take a warm tuner: up to 3
  the OTA EPG grabber, up to 4 also initial scans; from 5 on (the default)
  only viewers, recordings and user scans. Keep a tuner free of warm muxes
  for the EPG grabber, or lower the weight.

* Settings that size the ring apply to muxes started afterwards.

---

## Buttons

<tvh_include>inc/buttons</tvh_include>

---
