// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Tvheadend contributors

/*
 * Blindscan of a DVB-S network (Neumo driver): the result of
 * `mpegts/network/blindscan_result` and what the graph makes of it.
 * Frequencies in kHz, levels in 0.01 dB (as the server sends them).
 */

import { reactive } from 'vue'
import type { MuxRow } from './muxTree'

export type Pol = 'H' | 'V'

export interface BlindscanCarrier {
  freq: number
  /* width between the edges 3 dB down, kHz */
  bw: number
  /* symbols/s: the width (half the power of a root raised cosine carrier
   * is at its symbol rate apart) */
  sr: number
  /* 0.01 dB above the noise floor */
  snr: number
  peak: number
}

export interface BlindscanPol {
  freq: number[]
  level: number[]
  /* the 10th percentile of the levels */
  noise?: number
  /* the noise floor along the band: its levels at these frequencies */
  floor_freq?: number[]
  floor?: number[]
  /* seconds since 1970: when this polarisation was taken (a run replaces
   * only the polarisations it takes) */
  time?: number
  carriers?: BlindscanCarrier[]
}

export interface BlindscanResult {
  /* running, done, failed, cancelled; none without a blindscan since the start */
  state?: string
  slice?: number
  slices?: number
  /* the polarisations of the blindscan (running or the last one): HV, H, V */
  run_pols?: string
  /* the one being taken */
  pol?: string
  error?: string
  /* seconds since 1970 of the result kept */
  time?: number
  frontend?: string
  pols?: Partial<Record<Pol, BlindscanPol>>
}

export type PolSel = 'HV' | 'H' | 'V'

/* H, V or both, from a string of them (HV, VH, H, ...); null without one */
export function polSelOf(pols: string | undefined | null): PolSel | null {
  const h = !!pols?.includes('H')
  const v = !!pols?.includes('V')
  return h && v ? 'HV' : h ? 'H' : v ? 'V' : null
}

/* a transponder of the network, for the graph */
export interface GraphMux {
  uuid: string
  freq: number
  pol: Pol
  sr: number
  /* 1 OK, 2 failed, else not scanned */
  scanResult: number
  /* 1 on, 0 off, -1 ignored */
  enabled: number
  name: string
}

/* the transponders of a network: no sub-muxes, satellite frequencies (kHz),
 * H and V only (circular L/R count as H/V: 13 V and 18 V as well) */
export function graphMuxes(rows: MuxRow[]): GraphMux[] {
  const out: GraphMux[] = []
  for (const r of rows) {
    if (r.parent_mux || !r.frequency || !r.polarisation) continue
    const p =
      r.polarisation === 'V' || r.polarisation === 'R'
        ? 'V'
        : r.polarisation === 'H' || r.polarisation === 'L'
          ? 'H'
          : null
    if (!p) continue
    out.push({
      uuid: r.uuid,
      freq: Number(r.frequency),
      pol: p,
      sr: Number(r.symbolrate ?? 0),
      scanResult: Number(r.scan_result ?? 0),
      enabled: Number(r.enabled ?? 1),
      name: String(r.name ?? ''),
    })
  }
  return out.sort((a, b) => a.freq - b.freq)
}

/* occupied width of a carrier of this symbol rate (rolloff 0.35), kHz */
export function muxWidth(sr: number): number {
  return (sr * 1.35) / 1000
}

/* the transponder a carrier is: same polarisation, each one's middle in
 * the band of the other (1 MHz at least) - two carriers of one satellite
 * and polarisation do not overlap; a narrow carrier in the band of a wide
 * transponder (an outdated one) is not it. As the server picks the mux of
 * a tune */
export function muxOfCarrier(c: BlindscanCarrier, pol: Pol, muxes: GraphMux[]): GraphMux | null {
  let best: GraphMux | null = null
  let bestD = Infinity
  for (const m of muxes) {
    if (m.pol !== pol) continue
    const d = Math.abs(m.freq - c.freq)
    const sr = c.sr > 0 && (!m.sr || c.sr < m.sr) ? c.sr : m.sr
    const tol = Math.max(1000, muxWidth(sr) / 2)
    if (d <= tol && d < bestD) {
      best = m
      bestD = d
    }
  }
  return best
}

/* linear between the points (xp ascending), the values at the ends beyond */
export function interpolate(xp: number[], fp: number[], x: number): number {
  const n = xp.length
  if (x <= xp[0]) return fp[0]
  if (x >= xp[n - 1]) return fp[n - 1]
  let lo = 0
  let hi = n - 1
  while (hi - lo > 1) {
    const m = (lo + hi) >> 1
    if (xp[m] <= x) lo = m
    else hi = m
  }
  return fp[lo] + ((fp[hi] - fp[lo]) * (x - xp[lo])) / (xp[hi] - xp[lo])
}

/* the levels above the noise floor along the band (above the one noise
 * level of an older result): a Unicable LNB levels each user band on its
 * own, a cable dampens the upper frequencies more, the polarisations differ
 * - above the floor the levels compare along the band and between them */
export function aboveFloor(p: BlindscanPol): BlindscanPol {
  const ff = p.floor_freq
  const fl = p.floor
  if (ff && fl && ff.length > 0 && ff.length === fl.length)
    return { ...p, level: p.level.map((l, i) => l - interpolate(ff, fl, p.freq[i])) }
  const n = p.noise ?? 0
  return { ...p, level: p.level.map((l) => l - n) }
}

/* the curve within [lo, hi] reduced to at most `buckets` points: the highest
 * level of each (a narrow carrier stays visible) */
export function decimate(
  freq: number[],
  level: number[],
  lo: number,
  hi: number,
  buckets: number
): Array<[number, number]> {
  const out: Array<[number, number]> = []
  if (hi <= lo || buckets <= 0) return out
  const width = (hi - lo) / buckets
  let cur = -1
  let maxL = -Infinity
  let atF = 0
  for (let i = 0; i < freq.length; i++) {
    const f = freq[i]
    if (f < lo || f > hi) continue
    const b = Math.min(buckets - 1, Math.floor((f - lo) / width))
    if (b !== cur) {
      if (cur >= 0) out.push([atF, maxL])
      cur = b
      maxL = -Infinity
    }
    if (level[i] > maxL) {
      maxL = level[i]
      atF = f
    }
  }
  if (cur >= 0) out.push([atF, maxL])
  return out
}

/* the level at f: of the point nearest to it */
export function levelAt(freq: number[], level: number[], f: number): number | null {
  if (!freq.length || f < freq[0] || f > freq[freq.length - 1]) return null
  let lo = 0
  let hi = freq.length - 1
  while (hi - lo > 1) {
    const m = (lo + hi) >> 1
    if (freq[m] <= f) lo = m
    else hi = m
  }
  return f - freq[lo] <= freq[hi] - f ? level[lo] : level[hi]
}

/* ticks every 1, 2 or 5 times a power of ten, about `count` of them (the
 * step nearest to an even split) */
export function ticks(lo: number, hi: number, count: number): number[] {
  if (!(hi > lo) || count <= 0) return []
  const raw = (hi - lo) / count
  const p = Math.pow(10, Math.floor(Math.log10(raw)))
  const step = [1, 2, 5, 10]
    .map((m) => m * p)
    .reduce((a, b) => (Math.abs(b - raw) < Math.abs(a - raw) ? b : a))
  const out: number[] = []
  for (let v = Math.ceil(lo / step) * step; v <= hi + step * 1e-9; v += step)
    out.push(Math.round(v / step) * step)
  return out
}

/* the range of the levels within [lo, hi] (0.01 dB), with some room */
export function levelRange(pols: BlindscanPol[], lo: number, hi: number): [number, number] {
  let min = Infinity
  let max = -Infinity
  for (const p of pols) {
    for (let i = 0; i < p.freq.length; i++) {
      if (p.freq[i] < lo || p.freq[i] > hi) continue
      if (p.level[i] < min) min = p.level[i]
      if (p.level[i] > max) max = p.level[i]
    }
  }
  if (!isFinite(min)) return [-500, 2500]
  const pad = Math.max(100, (max - min) * 0.05)
  return [Math.floor((min - pad) / 100) * 100, Math.ceil((max + pad) / 100) * 100]
}

/* a tune from the dialog, or of the blindscan for a new carrier */
export interface TuneState {
  /* kHz: asked for, and where the demodulator locked */
  reqFreq: number
  freq: number
  pol: Pol
  /* locked with data; locked, but no data; no lock */
  state: 'scanning' | 'ok' | 'nodata' | 'fail'
  text: string
  /* locked: the mux kept, or the one of the network it is */
  mux?: string
}

/* the tunes by network and mux, for as long as the page is open (not saved:
 * what was tried already shows in the graph) */
export const tunesByNetwork = reactive<Record<string, Record<string, TuneState>>>({})

/* the last tune at a carrier or transponder (middle freq, half its width):
 * same polarisation, asked for or locked within it (500 kHz at least) */
export function triedAt(
  tunes: TuneState[],
  pol: Pol,
  freq: number,
  half: number
): TuneState | null {
  const tol = Math.max(500, half)
  let last: TuneState | null = null
  for (const s of tunes)
    if (
      s.pol === pol &&
      (Math.abs(s.reqFreq - freq) <= tol ||
        (s.state !== 'scanning' && Math.abs(s.freq - freq) <= tol))
    )
      last = s
  return last
}

/* the MATYPE of a DVB-S2 stream: TS or a generic stream (GSE comes as
 * one), single or multistream, CCM or ACM/VCM - the names of the mux */
export function matypeText(m: number | undefined): string {
  if (m === undefined || m < 0) return ''
  const types = ['generic packetized', 'generic continuous', 'reserved', 'TS']
  const out = [types[(m >> 6) & 3]]
  if (!(m & 0x20)) out.push('multistream')
  if (!(m & 0x10)) out.push('ACM/VCM')
  return out.join(', ')
}
