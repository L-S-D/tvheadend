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

/* what the PIDs of a tune carried while its scan ran (the server's
 * statistics of the PIDs): the total, and the PIDs with the most packets */
export interface PidStat {
  pid: number
  packets: number
  scrambled?: number
  /* continuity errors */
  cc?: number
  /* a PES: its stream id, what its elementary stream starts with */
  stream_id?: number
  es?: string
  /* table ids of sections with a valid CRC, and of short form ones */
  tables?: number[]
  short?: number[]
  /* the BBFrames the demodulator passes (a generic stream) */
  bbframes?: { frames: number; hem: number; dfl_bits: number; matype?: number; isis?: number[] }
}
export interface TsContent {
  /* microseconds from the first packet to the last */
  us: number
  packets: number
  nulls: number
  tei: number
  pids: number
  list?: PidStat[]
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
  content?: TsContent
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
 * one), single or multistream, CCM or ACM/VCM; TS/GS 10, reserved in
 * DVB-S2, is GSE-HEM in DVB-S2X */
export function matypeText(m: number | undefined): string {
  if (m === undefined || m < 0) return ''
  const types = ['generic packetized', 'generic continuous', 'GSE-HEM', 'TS']
  const out = [types[(m >> 6) & 3]]
  if (!(m & 0x20)) out.push('multistream')
  if (!(m & 0x10)) out.push('ACM/VCM')
  return out.join(', ')
}

type Translate = (text: string, ...args: string[]) => string

/* EN 300 468 / ISO 13818-1 table ids */
const TABLE_NAMES: Array<[number, number, string]> = [
  [0x00, 0x00, 'PAT'],
  [0x01, 0x01, 'CAT'],
  [0x02, 0x02, 'PMT'],
  [0x03, 0x03, 'TSDT'],
  [0x3a, 0x3d, 'DSM-CC'],
  [0x3e, 0x3e, 'MPE'],
  [0x40, 0x41, 'NIT'],
  [0x42, 0x42, 'SDT'],
  [0x46, 0x46, 'SDT'],
  [0x4a, 0x4a, 'BAT'],
  [0x4e, 0x6f, 'EIT'],
  [0x70, 0x70, 'TDT'],
  [0x71, 0x71, 'RST'],
  [0x72, 0x72, 'ST'],
  [0x73, 0x73, 'TOT'],
  [0x74, 0x74, 'AIT'],
  [0x7e, 0x7e, 'DIT'],
  [0x7f, 0x7f, 'SIT'],
  [0x80, 0x81, 'ECM'],
  [0x82, 0x8f, 'EMM'],
]

const ES_NAMES: Record<string, string> = {
  mpeg2video: 'MPEG-2 video',
  h264: 'H.264 video',
  hevc: 'HEVC video',
  mpegaudio: 'MPEG audio',
  'aac-adts': 'AAC audio (ADTS)',
  'aac-latm': 'AAC audio (LATM)',
  ac3: 'AC-3 audio',
  ac4: 'AC-4 audio',
  teletext: 'teletext',
  subtitles: 'DVB subtitles',
}

function hex(v: number, n: number): string {
  return `0x${v.toString(16).toUpperCase().padStart(n, '0')}`
}

/* the names of table ids, each once */
export function tableNames(ids: number[]): string[] {
  const out: string[] = []
  for (const id of ids) {
    const n = TABLE_NAMES.find(([a, b]) => id >= a && id <= b)?.[2] ?? hex(id, 2)
    if (!out.includes(n)) out.push(n)
  }
  return out
}

/* bits in a time: "4.2 Mbit/s", "192 kbit/s" */
export function rateText(bits: number, us: number): string {
  if (us <= 0) return ''
  const kbps = (bits * 1000) / us
  return kbps >= 1000 ? `${(kbps / 1000).toFixed(1)} Mbit/s` : `${Math.round(kbps)} kbit/s`
}

/* a PES by its stream id */
function pesName(sid: number): string {
  if (sid >= 0xe0 && sid <= 0xef) return 'video'
  if (sid >= 0xc0 && sid <= 0xdf) return 'audio'
  if (sid === 0xbd) return 'private stream 1'
  return `PES ${hex(sid, 2)}`
}

/* the statistics of the PIDs as lines: the total, then a line per PID */
export function contentLines(c: TsContent, t: Translate): string[] {
  if (!c.packets) return [t('No TS packets')]
  const lines = [
    t(
      '{0} in {1} s, {2} PIDs, null packets {3} %',
      rateText(c.packets * 1504, c.us),
      (c.us / 1e6).toFixed(1),
      String(c.pids),
      ((100 * c.nulls) / c.packets).toFixed(0)
    ) + (c.tei ? `, ${t('{0} packets with errors', String(c.tei))}` : ''),
  ]
  for (const p of c.list ?? []) {
    const what: string[] = []
    if (p.bbframes) {
      const b = p.bbframes
      what.push(
        t('BBFrames of the demodulator: {0}/s', ((b.frames * 1e6) / Math.max(1, c.us)).toFixed(0)),
        t('data {0}', rateText(b.dfl_bits, c.us))
      )
      const mt = matypeText(b.matype)
      if (mt) what.push(mt)
      if (b.isis?.length) what.push(`ISI ${b.isis.join(', ')}`)
      if (b.hem) what.push(t('high efficiency mode'))
    } else if (p.es && ES_NAMES[p.es]) what.push(ES_NAMES[p.es])
    else if (p.stream_id !== undefined) what.push(pesName(p.stream_id))
    const tables = tableNames([...(p.tables ?? []), ...(p.short ?? [])])
    if (tables.length) what.push(t('sections {0}', tables.join(', ')))
    if (p.scrambled) what.push(t('scrambled {0} %', ((100 * p.scrambled) / p.packets).toFixed(0)))
    if (!what.length) what.push(t('no PES, no sections'))
    if (p.cc) what.push(t('{0} continuity errors', String(p.cc)))
    lines.push(
      `PID ${p.pid} (${hex(p.pid, 4)}): ${rateText(p.packets * 1504, c.us)} · ${what.join(' · ')}`
    )
  }
  return lines
}
