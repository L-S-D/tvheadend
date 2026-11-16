// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Tvheadend contributors

/*
 * Blindscan of a DVB-S network (Neumo driver): the result of
 * `mpegts/network/blindscan_result` and what the graph makes of it.
 * Frequencies in kHz, levels in 0.01 dB (as the server sends them).
 */

import type { MuxRow } from './muxTree'

export type Pol = 'H' | 'V'

export interface BlindscanCarrier {
  freq: number
  /* width between the edges, kHz */
  bw: number
  /* symbols/s, from the width */
  sr: number
  /* 0.01 dB above the noise floor */
  snr: number
  peak: number
}

export interface BlindscanPol {
  freq: number[]
  level: number[]
  noise?: number
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
  pol?: string
  error?: string
  /* seconds since 1970 of the result kept */
  time?: number
  frontend?: string
  pols?: Partial<Record<Pol, BlindscanPol>>
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

/* the transponder a carrier is: same polarisation, within a quarter of the
 * symbol rates (1 MHz at least) - as the server picks the mux of a tune */
export function muxOfCarrier(c: BlindscanCarrier, pol: Pol, muxes: GraphMux[]): GraphMux | null {
  let best: GraphMux | null = null
  let bestD = Infinity
  for (const m of muxes) {
    if (m.pol !== pol) continue
    const d = Math.abs(m.freq - c.freq)
    const tol = Math.max(1000, Math.min(m.sr || c.sr, c.sr) / 4000)
    if (d <= tol && d < bestD) {
      best = m
      bestD = d
    }
  }
  return best
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
  if (!isFinite(min)) return [-7000, -4000]
  const pad = Math.max(100, (max - min) * 0.05)
  return [Math.floor((min - pad) / 100) * 100, Math.ceil((max + pad) / 100) * 100]
}
