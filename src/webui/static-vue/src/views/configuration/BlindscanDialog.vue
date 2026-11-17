<!--
  SPDX-License-Identifier: GPL-3.0-or-later
  Copyright (C) 2026 Tvheadend contributors
-->
<script setup lang="ts">
/*
 * BlindscanDialog — the blindscan of a DVB-S network (Neumo driver).
 *
 * Starts one (polarisations, range) and follows it; shows the spectrum of
 * each polarisation above its noise floor, and below it in lanes the
 * transponders of the network and the carriers found. A click picks a
 * frequency (a carrier, a transponder, or a point of a curve) and tunes
 * there blind, with the symbol rate of the carrier or one given: the mux
 * there, or a new one that is kept only when it locks
 * (`mpegts/network/blindscan_tune`). Drag to zoom, double click for all. A
 * click on a tune that locked, or on the services of a mux picked, shows
 * what the mux carries: its tuning, sub-muxes and services.
 */

import { computed, onBeforeUnmount, ref, watch } from 'vue'
import Dialog from 'primevue/dialog'
import Button from 'primevue/button'
import { Loader2, Lock } from 'lucide-vue-next'
import { apiCall } from '@/api/client'
import { cometClient } from '@/api/comet'
import { useI18n } from '@/composables/useI18n'
import { useToastNotify } from '@/composables/useToastNotify'
import { apiErrorMessage } from '@/utils/apiErrorMessage'
import {
  caidList,
  exactMatch,
  MUX_FIELDS,
  scanResultName,
  SERVICE_FIELDS,
  serviceTypeName,
  type MuxRow,
  type ServiceRow,
} from './muxTree'
import {
  aboveFloor,
  decimate,
  graphMuxes,
  levelAt,
  levelRange,
  muxOfCarrier,
  contentLines,
  matypeText,
  polSelOf,
  ticks,
  triedAt,
  tunesByNetwork,
  type BlindscanCarrier,
  type BlindscanPol,
  type BlindscanResult,
  type GraphMux,
  type Pol,
  type PolSel,
  type TsContent,
  type TuneState,
} from './blindscan'

const { t } = useI18n()
const toast = useToastNotify()

interface Props {
  visible: boolean
  networkUuid: string | null
  networkName: string
}

const props = defineProps<Props>()
const emit = defineEmits<{
  'update:visible': [value: boolean]
  /* a tune changed the muxes of the network */
  changed: []
}>()

const visibleProxy = computed({
  get: () => props.visible,
  set: (v) => emit('update:visible', v),
})

const POLS: Pol[] = ['H', 'V']
const POINTS = 20000

const result = ref<BlindscanResult | null>(null)
const progress = ref<BlindscanResult | null>(null)
/* the polarisations to take and to show: those of a running blindscan */
const polSel = ref<PolSel>('HV')
/* after the spectrum: a tune at each new carrier this far above the noise */
const lockNew = ref(false)
const lockSnrDb = ref(3)
const startMhz = ref(10700)
const endMhz = ref(12750)
const view = ref<[number, number] | null>(null)
const busy = ref(false)

/* the frequency picked in the graph, for a tune */
interface Pick {
  freqMhz: number
  pol: Pol
  found: Found | null
  mux: GraphMux | null
  srAuto: boolean
  srK: number
  rangeMhz: number | null
}
const pick = ref<Pick | null>(null)

const running = computed(() => progress.value?.state === 'running')
const shown = (p: Pol) => polSel.value.includes(p)
watch(
  () => (running.value ? polSelOf(progress.value?.run_pols) : null),
  (rp) => {
    if (rp) polSel.value = rp
  }
)
/* the share of the slices taken, for the bar */
const progressPct = computed(() => {
  const p = progress.value
  if (!p?.slices) return 0
  return Math.min(100, Math.max(0, (100 * Math.max(0, (p.slice ?? 0) - 1)) / p.slices))
})
const muxRows = ref<MuxRow[]>([])
const muxes = computed<GraphMux[]>(() => graphMuxes(muxRows.value))

/* the muxes of the network: its transponders in the lanes */
async function loadMuxes() {
  if (!props.networkUuid) return
  const uuid = props.networkUuid
  try {
    const grid = await apiCall<{ entries?: MuxRow[] }>('mpegts/mux/grid', {
      start: 0,
      limit: 100000,
      hidemode: 'none',
      list: MUX_FIELDS,
      filter: [{ type: 'string', field: 'network_uuid', value: exactMatch(uuid) }],
    })
    if (uuid === props.networkUuid)
      muxRows.value = (grid.entries ?? []).filter((r) => r.network_uuid === uuid)
  } catch (e) {
    toast.error(`${t('Failed to load the muxes')}: ${apiErrorMessage(e)}`)
  }
}

/* the polarisations of the result, their levels above the noise floor */
const aboveFloorPols = computed(() => {
  const out: Partial<Record<Pol, BlindscanPol>> = {}
  for (const p of POLS) {
    const d = result.value?.pols?.[p]
    if (d && d.freq.length) out[p] = aboveFloor(d)
  }
  return out
})

function polData(p: Pol): BlindscanPol | null {
  return aboveFloorPols.value[p] ?? null
}

/* the frequencies of the result, or of the range asked for */
const fullRange = computed<[number, number]>(() => {
  let lo = Infinity
  let hi = -Infinity
  for (const p of POLS) {
    const d = polData(p)
    if (!d) continue
    lo = Math.min(lo, d.freq[0])
    hi = Math.max(hi, d.freq[d.freq.length - 1])
  }
  return isFinite(lo) ? [lo, hi] : [startMhz.value * 1000, endMhz.value * 1000]
})
const range = computed<[number, number]>(() => view.value ?? fullRange.value)

/* ---- loading and following ---- */

let pollTimer: number | undefined

async function load(points: number): Promise<BlindscanResult | null> {
  if (!props.networkUuid) return null
  try {
    return await apiCall<BlindscanResult>('mpegts/network/blindscan_result', {
      uuid: props.networkUuid,
      points,
    })
  } catch (e) {
    toast.error(`${t('Failed to load the blindscan')}: ${apiErrorMessage(e)}`)
    return null
  }
}

async function loadFull() {
  const r = await load(POINTS)
  if (r) {
    result.value = r
    progress.value = r
  }
}

function follow() {
  window.clearTimeout(pollTimer)
  pollTimer = window.setTimeout(async () => {
    const r = await load(10)
    if (!r || !props.visible) return
    progress.value = r
    if (r.state === 'running') follow()
    else await loadFull()
  }, 1500)
}

watch(
  () => [props.visible, props.networkUuid] as const,
  async ([v]) => {
    window.clearTimeout(pollTimer)
    if (!v) return
    result.value = null
    progress.value = null
    view.value = null
    pick.value = null
    muxRows.value = []
    void loadMuxes()
    await loadFull()
    /* what the result has (a running blindscan: its own, by the watch) */
    if (!running.value) polSel.value = polSelOf(POLS.filter((p) => polData(p)).join('')) ?? 'HV'
    if (running.value) follow()
  },
  { immediate: true }
)

/* muxes changed (a tune, a scan): their lanes again, a second later */
let muxTimer: number | undefined
const muxCometOff = cometClient.on('mpegts_mux', () => {
  if (!props.visible || muxTimer !== undefined) return
  muxTimer = window.setTimeout(() => {
    muxTimer = undefined
    void loadMuxes()
  }, 1000)
})

const cometOff = cometClient.on('blindscan', (m) => {
  const msg = m as Record<string, unknown>
  /* the tunes of any network: they show when its dialog opens */
  if (typeof msg.mux === 'string' && typeof msg.uuid === 'string') {
    if (msg.state === 'tuning') onTuneStart(msg.uuid, msg)
    else onTuneResult(msg.uuid, msg)
    return
  }
  if (!props.visible || msg.uuid !== props.networkUuid) return
  if (msg.state === 'running') {
    progress.value = { ...(progress.value ?? {}), ...(msg as BlindscanResult) }
  } else if (typeof msg.state === 'string') {
    window.clearTimeout(pollTimer)
    void loadFull()
  }
})

onBeforeUnmount(() => {
  cometOff()
  muxCometOff()
  window.clearTimeout(pollTimer)
  window.clearTimeout(muxTimer)
})

async function start() {
  if (!props.networkUuid) return
  busy.value = true
  try {
    const r = await apiCall<{ started?: boolean; error?: string }>('mpegts/network/blindscan', {
      uuid: props.networkUuid,
      pols: polSel.value,
      start: Math.round(startMhz.value * 1000),
      end: Math.round(endMhz.value * 1000),
      lock_snr: lockNew.value ? Math.round(lockSnrDb.value * 100) : undefined,
    })
    if (!r.started) toast.error(r.error ?? t('The blindscan did not start'))
    else {
      progress.value = {
        ...(progress.value ?? {}),
        state: 'running',
        run_pols: polSel.value,
        slice: 0,
        slices: 0,
        error: undefined,
      }
      follow()
    }
  } catch (e) {
    toast.error(`${t('Failed to start the blindscan')}: ${apiErrorMessage(e)}`)
  } finally {
    busy.value = false
  }
}

async function stop() {
  if (!props.networkUuid) return
  try {
    await apiCall('mpegts/network/blindscan_stop', { uuid: props.networkUuid })
  } catch (e) {
    toast.error(`${t('Failed to stop the blindscan')}: ${apiErrorMessage(e)}`)
  }
}

const statusText = computed(() => {
  const p = progress.value
  if (!p) return ''
  if (p.state === 'running') {
    return p.slices
      ? t('Slice {0} of {1} ({2})', String(p.slice ?? 0), String(p.slices), p.pol ?? '')
      : t('Starting…')
  }
  const parts: string[] = []
  if (p.state === 'failed') parts.push(`${t('Failed')}: ${p.error ?? ''}`)
  else if (p.state === 'cancelled') parts.push(t('Stopped'))
  if (result.value?.time) {
    const when = new Date(result.value.time * 1000).toLocaleString()
    /* a polarisation from an earlier run: with its time */
    const older = (x: Pol) => {
      const pt = polData(x)?.time
      return pt && result.value?.time && result.value.time - pt > 60
        ? ` (${new Date(pt * 1000).toLocaleString()})`
        : ''
    }
    const counts = POLS.filter((x) => polData(x))
      .map((x) => `${x} ${polData(x)?.carriers?.length ?? 0}${older(x)}`)
      .join(', ')
    parts.push(
      t(
        'Spectrum of {0} ({1}), carriers {2}, {3} without a mux',
        when,
        result.value.frontend ?? '',
        counts,
        String(newCount.value)
      )
    )
  } else if (p.state !== 'failed') {
    parts.push(t('No blindscan of this network yet'))
  }
  return parts.join(' · ')
})

/* the carriers found, each with the transponder it is (null: none yet) */
interface Found {
  pol: Pol
  c: BlindscanCarrier
  mux: GraphMux | null
}
const found = computed<Found[]>(() => {
  const out: Found[] = []
  for (const p of POLS)
    for (const c of polData(p)?.carriers ?? [])
      out.push({ pol: p, c, mux: muxOfCarrier(c, p, muxes.value) })
  return out
})
const newCount = computed(() => found.value.filter((f) => !f.mux).length)

/* ---- the graph ---- */

const W = 1000
const X0 = 56
const X1 = W - 12
const Y0 = 12
const Y1 = 250
const LANE_Y = 286
const LANE_H = 13
const LANE_GAP = 5
const LANES: Array<{ pol: Pol; kind: 'mux' | 'found' }> = [
  { pol: 'H', kind: 'mux' },
  { pol: 'H', kind: 'found' },
  { pol: 'V', kind: 'mux' },
  { pol: 'V', kind: 'found' },
]
/* the lanes of the polarisations shown */
const lanes = computed(() => LANES.filter((ln) => shown(ln.pol)))
const H = computed(() => LANE_Y + lanes.value.length * (LANE_H + LANE_GAP) + 4)

function laneY(i: number): number {
  return LANE_Y + i * (LANE_H + LANE_GAP)
}

const levels = computed<[number, number]>(() => {
  const [lo, hi] = range.value
  return levelRange(
    POLS.filter((p) => shown(p))
      .map((p) => polData(p))
      .filter((d): d is BlindscanPol => !!d),
    lo,
    hi
  )
})

function x(f: number): number {
  const [lo, hi] = range.value
  return X0 + ((f - lo) / (hi - lo)) * (X1 - X0)
}
function fAt(px: number): number {
  const [lo, hi] = range.value
  return lo + ((px - X0) / (X1 - X0)) * (hi - lo)
}
function y(l: number): number {
  const [lo, hi] = levels.value
  return Y1 - ((l - lo) / (hi - lo)) * (Y1 - Y0)
}

const curves = computed(() =>
  POLS.filter((p) => shown(p) && polData(p)).map((p) => {
    const d = polData(p)!
    const pts = decimate(d.freq, d.level, range.value[0], range.value[1], (X1 - X0) * 2)
    return {
      pol: p,
      points: pts.map(([f, l]) => `${x(f).toFixed(1)},${y(l).toFixed(1)}`).join(' '),
    }
  })
)

const xTicks = computed(() => ticks(range.value[0] / 1000, range.value[1] / 1000, 10))
const yTicks = computed(() => ticks(levels.value[0] / 100, levels.value[1] / 100, 6))

interface LaneRect {
  key: string
  lane: number
  x: number
  w: number
  cls: string
  title: string
  found?: Found
  mux?: GraphMux
  /* the last tune there (in this page) */
  tried?: TuneState | null
}

function clipRect(lo: number, hi: number): { x: number; w: number } | null {
  const [a, b] = range.value
  if (hi < a || lo > b) return null
  const x0 = x(Math.max(lo, a))
  const x1 = x(Math.min(hi, b))
  return { x: x0, w: Math.max(1.5, x1 - x0) }
}

function muxState(m: GraphMux): string {
  if (m.enabled === -1) return t('ignored')
  if (m.enabled === 0) return t('disabled')
  return m.scanResult === 1
    ? t('scan OK')
    : m.scanResult === 2
      ? t('scan failed')
      : t('not scanned')
}

const laneRects = computed<LaneRect[]>(() => {
  const out: LaneRect[] = []
  lanes.value.forEach((ln, i) => {
    if (ln.kind === 'mux') {
      for (const m of muxes.value) {
        if (m.pol !== ln.pol) continue
        /* as wide as a carrier found: its symbol rate, where half the
         * power is (the occupied width is more by the rolloff) */
        const half = (m.sr || 1000000) / 1000 / 2
        const r = clipRect(m.freq - half, m.freq + half)
        if (!r) continue
        const cls =
          m.enabled !== 1 ? 'off' : m.scanResult === 1 ? 'ok' : m.scanResult === 2 ? 'fail' : 'none'
        const tried = triedAt(tuneArr.value, m.pol, m.freq, half)
        out.push({
          key: `m${m.uuid}`,
          lane: i,
          ...r,
          cls: `bs-mux bs-mux--${cls}`,
          title:
            `${m.name} · ${(m.sr / 1000).toFixed(0)} kS/s · ${muxState(m)}` +
            (tried ? ` · ${t('tried')}: ${tried.text}` : ''),
          mux: m,
          tried,
        })
      }
    } else {
      for (const f of found.value) {
        if (f.pol !== ln.pol) continue
        const r = clipRect(f.c.freq - f.c.bw / 2, f.c.freq + f.c.bw / 2)
        if (!r) continue
        const tried = triedAt(tuneArr.value, f.pol, f.c.freq, f.c.bw / 2)
        out.push({
          key: `c${f.pol}${f.c.freq}`,
          lane: i,
          ...r,
          cls: `bs-found bs-found--${f.pol} ${f.mux ? 'bs-found--known' : 'bs-found--new'}`,
          title:
            `${(f.c.freq / 1000).toFixed(3)} MHz ${f.pol} · ~${(f.c.sr / 1000).toFixed(0)} kS/s · ${(f.c.snr / 100).toFixed(1)} dB${f.mux ? ` · ${f.mux.name}` : ` · ${t('no mux')}`}` +
            (tried ? ` · ${t('tried')}: ${tried.text}` : ''),
          found: f,
          tried,
        })
      }
    }
  })
  return out
})

/* ---- pointer: hover, drag to zoom, click to pick ---- */

const svgEl = ref<SVGSVGElement | null>(null)
const hoverX = ref<number | null>(null)
const dragFrom = ref<number | null>(null)

function svgPoint(e: PointerEvent | MouseEvent): { px: number; py: number } {
  const r = svgEl.value!.getBoundingClientRect()
  return {
    px: ((e.clientX - r.left) * W) / r.width,
    py: ((e.clientY - r.top) * H.value) / r.height,
  }
}

function onDown(e: PointerEvent) {
  const { px, py } = svgPoint(e)
  if (px < X0 || px > X1 || py > Y1) return
  dragFrom.value = px
}
function onMove(e: PointerEvent) {
  const { px } = svgPoint(e)
  hoverX.value = px >= X0 && px <= X1 ? px : null
}
function onUp(e: PointerEvent) {
  const { px, py } = svgPoint(e)
  const from = dragFrom.value
  dragFrom.value = null
  if (from !== null && Math.abs(px - from) > 8) {
    const a = fAt(Math.max(X0, Math.min(from, px)))
    const b = fAt(Math.min(X1, Math.max(from, px)))
    if (b - a > 500) view.value = [a, b]
    return
  }
  if (px < X0 || px > X1) return
  pickAt(px, py)
}
function onLeave() {
  hoverX.value = null
  dragFrom.value = null
}
function resetZoom() {
  view.value = null
}

const hoverText = computed(() => {
  if (hoverX.value === null) return ''
  const f = fAt(hoverX.value)
  const parts = [`${(f / 1000).toFixed(3)} MHz`]
  for (const p of POLS) {
    const d = polData(p)
    if (!d || !shown(p)) continue
    const l = levelAt(d.freq, d.level, f)
    if (l !== null) parts.push(`${p} ${(l / 100).toFixed(1)} dB`)
  }
  return parts.join('  ')
})

/* ---- pick and tune ---- */

function carrierAt(p: Pol, f: number): Found | null {
  for (const fd of found.value)
    if (fd.pol === p && Math.abs(fd.c.freq - f) <= fd.c.bw / 2) return fd
  return null
}

function pickAt(px: number, py: number) {
  const f = fAt(px)
  /* a lane: what is there */
  const li = lanes.value.findIndex((_, i) => py >= laneY(i) - 1 && py <= laneY(i) + LANE_H + 1)
  if (li >= 0) {
    const hit = laneRects.value.find((r) => r.lane === li && px >= r.x - 2 && px <= r.x + r.w + 2)
    if (!hit) return
    if (hit.found) setPick(hit.found.c.freq, hit.found.pol, hit.found, hit.found.mux)
    else if (hit.mux)
      setPick(hit.mux.freq, hit.mux.pol, carrierAt(hit.mux.pol, hit.mux.freq), hit.mux)
    return
  }
  if (py > Y1) return
  /* the curve: the polarisation shown with the higher level there */
  let pol: Pol | null = null
  let best = -Infinity
  for (const p of POLS) {
    const d = polData(p)
    if (!d || !shown(p)) continue
    const l = levelAt(d.freq, d.level, f)
    if (l !== null && l > best) {
      best = l
      pol = p
    }
  }
  if (!pol) return
  const fd = carrierAt(pol, f)
  setPick(fd ? fd.c.freq : Math.round(f), pol, fd, fd?.mux ?? null)
}

function setPick(freq: number, pol: Pol, fd: Found | null, mux: GraphMux | null) {
  const sr = fd ? fd.c.sr : mux ? mux.sr : 27500000
  pick.value = {
    freqMhz: Math.round(freq) / 1000,
    pol,
    found: fd,
    mux,
    srAuto: !!fd,
    srK: Math.round(sr / 1000),
    rangeMhz: null,
  }
}

const pickX = computed(() => (pick.value ? x(pick.value.freqMhz * 1000) : null))
/* the last tune at the pick (the carrier, else the mux) */
const pickTried = computed(() => {
  const p = pick.value
  if (!p) return null
  const half = p.found ? p.found.c.bw / 2 : p.mux ? (p.mux.sr || 1000000) / 1000 / 2 : 0
  return triedAt(tuneArr.value, p.pol, Math.round(p.freqMhz * 1000), half)
})

/* the tunes of this network (kept while the page is open) */
const tunes = computed<Record<string, TuneState>>(
  () => tunesByNetwork[props.networkUuid ?? ''] ?? {}
)
const tuneArr = computed(() => Object.values(tunes.value))
function setTune(net: string, mux: string, st: TuneState) {
  tunesByNetwork[net] = { ...(tunesByNetwork[net] ?? {}), [mux]: st }
}

async function tune() {
  const p = pick.value
  if (!p || !props.networkUuid) return
  const sr = p.srAuto && p.found ? p.found.c.sr : p.srK * 1000
  const freq = Math.round(p.freqMhz * 1000)
  try {
    const r = await apiCall<{ started?: boolean; mux?: string; error?: string }>(
      'mpegts/network/blindscan_tune',
      {
        uuid: props.networkUuid,
        freq,
        pol: p.pol,
        sr: Math.round(sr),
        range: p.rangeMhz ? Math.round(p.rangeMhz * 1000) : 0,
      }
    )
    if (!r.started || !r.mux) {
      toast.error(r.error ?? t('The tune did not start'))
      return
    }
    setTune(props.networkUuid, r.mux, {
      reqFreq: freq,
      freq,
      pol: p.pol,
      state: 'scanning',
      text: t('Scanning {0} MHz {1}…', (freq / 1000).toFixed(3), p.pol),
    })
    emit('changed')
  } catch (e) {
    toast.error(`${t('Failed to tune')}: ${apiErrorMessage(e)}`)
  }
}

function polOf(v: unknown, def: Pol): Pol {
  return v === 'H' || v === 'V' ? v : def
}

/* a tune started (here, or by the blindscan for its new carriers) */
function onTuneStart(net: string, m: Record<string, unknown>) {
  const uuid = String(m.mux)
  if (tunesByNetwork[net]?.[uuid]?.state === 'scanning') return
  const freq = Number(m.freq ?? 0)
  const pol = polOf(m.pol, 'H')
  setTune(net, uuid, {
    reqFreq: freq,
    freq,
    pol,
    state: 'scanning',
    text: t('Scanning {0} MHz {1}…', (freq / 1000).toFixed(3), pol),
  })
}

/* what the demodulator locked: "DVB-S2 PSK/8 2/3, 29949 kS/s, TS" */
function lockedText(m: Record<string, unknown>): string {
  const mod = [m.delsys, m.modulation, m.fec]
    .filter((x) => typeof x === 'string' && x && x !== 'AUTO' && x !== 'NONE')
    .join(' ')
  return [
    mod,
    `${(Number(m.sr ?? 0) / 1000).toFixed(0)} kS/s`,
    matypeText(typeof m.matype === 'number' ? m.matype : undefined),
  ]
    .filter(Boolean)
    .join(', ')
}

function onTuneResult(net: string, m: Record<string, unknown>) {
  const uuid = String(m.mux)
  const prev = tunesByNetwork[net]?.[uuid]
  const ok = m.result === 'ok'
  const freq = Number(m.freq ?? prev?.freq ?? 0)
  const pol = polOf(m.pol, prev?.pol ?? 'H')
  const where = `${(freq / 1000).toFixed(3)} MHz ${pol}`
  const subs = Number(m.submuxes ?? 0)
  let text: string
  if (ok && typeof m.same_name === 'string')
    text = t('{0}: locked the transponder {1}, which the network has', where, m.same_name)
  else if (ok)
    text = t(
      subs
        ? '{0}: locked ({1}) - the mux is kept, with {2} sub-muxes'
        : '{0}: locked ({1}) - the mux is kept',
      where,
      lockedText(m),
      String(subs)
    )
  else if (m.locked)
    /* a carrier, but no tables in it (not a TS, or a TS without SI) */
    text = t(
      m.deleted
        ? '{0}: locked ({1}), but the scan got no data - the new mux is deleted again'
        : '{0}: locked ({1}), but the scan got no data',
      where,
      lockedText(m)
    )
  else
    text = t(
      m.deleted
        ? '{0}: no lock on the carrier - the new mux is deleted again'
        : '{0}: no lock on the carrier',
      where
    )
  const mux = !ok ? undefined : typeof m.same === 'string' ? m.same : m.deleted ? undefined : uuid
  setTune(net, uuid, {
    reqFreq: prev?.reqFreq ?? freq,
    freq,
    pol,
    state: ok ? 'ok' : m.locked ? 'nodata' : 'fail',
    text,
    mux,
    content: m.content && typeof m.content === 'object' ? (m.content as TsContent) : undefined,
  })
  if (!props.visible || net !== props.networkUuid) return
  emit('changed')
  void loadMuxes()
}

/* ---- what a mux carries ---- */

interface MuxDetail {
  /* the mux, none when the tune deleted it again */
  uuid: string | null
  /* what the scan of the tune got: the statistics of the PIDs */
  content: TsContent | null
  label: string
  loading: boolean
  services: ServiceRow[]
  error: string
}
const detail = ref<MuxDetail | null>(null)
const detailRow = computed(() => muxRows.value.find((r) => r.uuid === detail.value?.uuid) ?? null)
const detailSubs = computed(() => {
  const u = detail.value?.uuid
  return u ? muxRows.value.filter((r) => r.parent_mux === u) : []
})
/* the services of the mux, then those of its sub-muxes, by name */
const detailServices = computed(() => {
  const d = detail.value
  if (!d) return []
  const order = [d.uuid, ...detailSubs.value.map((r) => r.uuid)]
  return [...d.services].sort(
    (a, b) =>
      order.indexOf(a.multiplex_uuid ?? '') - order.indexOf(b.multiplex_uuid ?? '') ||
      (a.svcname ?? '').localeCompare(b.svcname ?? '') ||
      (a.sid ?? 0) - (b.sid ?? 0)
  )
})

/* a tune: its mux (if kept) and what its scan got */
function showTune(s: TuneState) {
  if (s.mux) void showMux(s.mux, s.content ?? null, s.text)
  else
    detail.value = {
      uuid: null,
      content: s.content ?? null,
      label: s.text,
      loading: false,
      services: [],
      error: '',
    }
}

async function showMux(uuid: string, content?: TsContent | null, label?: string) {
  const ids = [uuid, ...muxRows.value.filter((r) => r.parent_mux === uuid).map((r) => r.uuid)]
  /* the content of the last tune of the mux */
  const c =
    content ??
    tuneArr.value.filter((x) => x.mux === uuid && x.content).slice(-1)[0]?.content ??
    null
  const base = { uuid, content: c, label: label ?? '' }
  detail.value = { ...base, loading: true, services: [], error: '' }
  try {
    const grids = await Promise.all(
      ids.map((id) =>
        apiCall<{ entries?: ServiceRow[] }>('mpegts/service/grid', {
          start: 0,
          limit: 10000,
          hidemode: 'none',
          list: SERVICE_FIELDS,
          filter: [{ type: 'string', field: 'multiplex_uuid', value: exactMatch(id) }],
        })
      )
    )
    if (detail.value?.uuid !== uuid) return
    detail.value = {
      ...base,
      loading: false,
      services: grids.flatMap((g) => g.entries ?? []),
      error: '',
    }
  } catch (e) {
    if (detail.value?.uuid === uuid)
      detail.value = { ...base, loading: false, services: [], error: apiErrorMessage(e) }
  }
}

function subMuxName(uuid: string | undefined): string {
  return muxRows.value.find((r) => r.uuid === uuid)?.name ?? ''
}

/* delivery system, modulation, FEC, symbol rate, input stream, stream type */
function tuningText(r: MuxRow): string {
  return [
    r.delsys,
    r.modulation,
    r.fec,
    r.symbolrate ? `${Math.round(r.symbolrate / 1000)} kS/s` : '',
    r.stream_id !== undefined && r.stream_id >= 0 && r.multistream ? `ISI ${r.stream_id}` : '',
    r.matype ? r.matype.replace(/^0x[0-9A-Fa-f]+: /, '') : '',
  ]
    .filter((x) => x && x !== 'AUTO' && x !== 'NONE')
    .join(' · ')
}

/* the tunes, newest first, and how they went */
const tuneList = computed(() => Object.entries(tunes.value).reverse())
const tunesRunning = computed(
  () => Object.values(tunes.value).filter((x) => x.state === 'scanning').length
)
const tuneSummary = computed(() => {
  const all = Object.values(tunes.value)
  const n = (s: TuneState['state']) => all.filter((x) => x.state === s).length
  return t(
    'Tunes: {0} running, {1} locked, {2} locked without data, {3} without a lock',
    String(n('scanning')),
    String(n('ok')),
    String(n('nodata')),
    String(n('fail'))
  )
})

const header = computed(() => t('Blindscan: {0}', props.networkName))
</script>

<template>
  <Dialog
    v-model:visible="visibleProxy"
    modal
    maximizable
    :draggable="false"
    :header="header"
    class="blindscan-dialog"
    :style="{ width: '1200px', maxWidth: 'calc(100vw - 32px)' }"
    :breakpoints="{ '768px': '100vw' }"
  >
    <div class="bs-bar">
      <label class="bs-field">
        <span>{{ t('Polarisation') }}</span>
        <select v-model="polSel" :disabled="running">
          <option value="HV">H + V</option>
          <option value="H">H</option>
          <option value="V">V</option>
        </select>
      </label>
      <label class="bs-field">
        <span>{{ t('From (MHz)') }}</span>
        <input
          v-model.number="startMhz"
          type="number"
          min="3000"
          max="13000"
          step="10"
          :disabled="running"
        />
      </label>
      <label class="bs-field">
        <span>{{ t('To (MHz)') }}</span>
        <input
          v-model.number="endMhz"
          type="number"
          min="3000"
          max="13000"
          step="10"
          :disabled="running"
        />
      </label>
      <label
        class="bs-check"
        :title="
          t(
            'After the spectrum, tune at each carrier found where the network has no transponder; a new mux is kept only when it locks'
          )
        "
      >
        <input v-model="lockNew" type="checkbox" :disabled="running" />
        {{ t('Then lock the new carriers') }}
      </label>
      <label v-if="lockNew" class="bs-field">
        <span>{{ t('From SNR (dB)') }}</span>
        <input
          v-model.number="lockSnrDb"
          type="number"
          min="0"
          max="30"
          step="0.5"
          :disabled="running"
        />
      </label>
      <Button
        v-if="!running"
        size="small"
        :label="t('Start blindscan')"
        :disabled="busy"
        @click="start"
      />
      <Button v-else size="small" severity="danger" :label="t('Stop')" @click="stop" />
      <span class="bs-status" :class="{ 'bs-status--err': progress?.state === 'failed' }">
        <Loader2 v-if="running" :size="14" :stroke-width="2" class="bs-spin" aria-hidden="true" />
        {{ statusText }}
      </span>
    </div>
    <div
      v-if="running"
      class="bs-progress"
      role="progressbar"
      :aria-label="t('Blindscan')"
      aria-valuemin="0"
      aria-valuemax="100"
      :aria-valuenow="Math.round(progressPct)"
    >
      <div class="bs-progress__fill" :style="{ width: `${progressPct}%` }" />
    </div>
    <div class="bs-bar bs-bar--small">
      <Button v-if="view" size="small" text :label="t('Show all')" @click="resetZoom" />
      <span class="bs-hint">{{
        hoverText ||
        t(
          'Drag to zoom, double click to show all; click a carrier, a transponder or the curve to tune there'
        )
      }}</span>
    </div>

    <svg
      ref="svgEl"
      class="bs-graph"
      :class="{ 'bs-graph--stale': running }"
      :viewBox="`0 0 ${W} ${H}`"
      role="img"
      :aria-label="t('Spectrum')"
      @pointerdown="onDown"
      @pointermove="onMove"
      @pointerup="onUp"
      @pointerleave="onLeave"
      @dblclick="resetZoom"
    >
      <defs>
        <pattern
          id="bs-hatch"
          width="6"
          height="6"
          patternUnits="userSpaceOnUse"
          patternTransform="rotate(45)"
        >
          <line x1="0" y1="0" x2="0" y2="6" class="bs-hatch-line" />
        </pattern>
      </defs>
      <g class="bs-axis">
        <template v-for="v in yTicks" :key="`y${v}`">
          <line :x1="X0" :x2="X1" :y1="y(v * 100)" :y2="y(v * 100)" class="bs-grid" />
          <text :x="X0 - 6" :y="y(v * 100) + 4" text-anchor="end">{{ v }}</text>
        </template>
        <template v-for="v in xTicks" :key="`x${v}`">
          <line :x1="x(v * 1000)" :x2="x(v * 1000)" :y1="Y0" :y2="Y1" class="bs-grid" />
          <text :x="x(v * 1000)" :y="Y1 + 16" text-anchor="middle">{{ v }}</text>
        </template>
        <text :x="X0 + 4" :y="Y0 - 2" class="bs-unit">{{ t('dB above the noise floor') }}</text>
        <text :x="X1" :y="Y1 + 30" text-anchor="end" class="bs-unit">MHz</text>
      </g>
      <line v-if="curves.length" :x1="X0" :x2="X1" :y1="y(0)" :y2="y(0)" class="bs-noise" />
      <g v-for="c in curves" :key="c.pol">
        <polyline :points="c.points" :class="`bs-curve bs-curve--${c.pol}`" />
      </g>
      <g>
        <text
          v-for="(ln, i) in lanes"
          :key="`l${ln.pol}${ln.kind}`"
          :x="X0 - 6"
          :y="laneY(i) + LANE_H - 2"
          text-anchor="end"
          :class="`bs-lane-label bs-lane-label--${ln.pol}`"
        >
          {{ ln.pol }} {{ ln.kind === 'mux' ? t('TP') : t('found') }}
        </text>
        <rect
          v-for="r in laneRects"
          :key="r.key"
          :x="r.x"
          :y="laneY(r.lane)"
          :width="r.w"
          :height="LANE_H"
          :class="r.cls"
        >
          <title>{{ r.title }}</title>
        </rect>
        <!-- tried already (in this page): how it went -->
        <circle
          v-for="r in laneRects.filter((x) => x.tried)"
          :key="`t${r.key}`"
          :cx="r.x + r.w / 2"
          :cy="laneY(r.lane) + LANE_H / 2"
          r="3.4"
          :class="`bs-tried bs-tried--${r.tried?.state}`"
        >
          <title>{{ r.title }}</title>
        </circle>
      </g>
      <line
        v-if="hoverX !== null"
        :x1="hoverX"
        :x2="hoverX"
        :y1="Y0"
        :y2="H - 4"
        class="bs-cursor"
      />
      <line
        v-if="pickX !== null && pickX >= X0 && pickX <= X1"
        :x1="pickX"
        :x2="pickX"
        :y1="Y0"
        :y2="H - 4"
        class="bs-pick"
      />
      <rect
        v-if="dragFrom !== null && hoverX !== null"
        :x="Math.min(dragFrom, hoverX)"
        :y="Y0"
        :width="Math.abs(hoverX - dragFrom)"
        :height="Y1 - Y0"
        class="bs-drag"
      />
    </svg>

    <div v-if="pick" class="bs-pick-panel">
      <div class="bs-pick-info">
        <template v-if="pick.found">
          {{ t('Carrier') }} ~{{ (pick.found.c.sr / 1000).toFixed(0) }} kS/s,
          {{ (pick.found.c.snr / 100).toFixed(1) }} dB
        </template>
        <template v-else>{{ t('No carrier found here') }}</template>
        ·
        <template v-if="pick.mux"
          >{{ t('Mux') }} {{ pick.mux.name }} ({{ muxState(pick.mux) }})</template
        >
        <template v-else>{{ t('no mux: a new one is kept only when it locks') }}</template>
        <div v-if="pickTried" :class="`bs-tune--${pickTried.state}`">
          {{ t('Tried') }}: {{ pickTried.text }}
        </div>
      </div>
      <label class="bs-field">
        <span>{{ t('Frequency (MHz)') }}</span>
        <input v-model.number="pick.freqMhz" type="number" step="0.001" />
      </label>
      <label class="bs-field">
        <span>{{ t('Polarisation') }}</span>
        <select v-model="pick.pol">
          <option value="H">H</option>
          <option value="V">V</option>
        </select>
      </label>
      <label v-if="pick.found" class="bs-check">
        <input v-model="pick.srAuto" type="checkbox" />
        {{ t('Symbol rate of the carrier') }}
      </label>
      <label class="bs-field">
        <span>{{ t('Symbol rate (kS/s)') }}</span>
        <input
          v-model.number="pick.srK"
          type="number"
          min="100"
          max="100000"
          step="1"
          :disabled="pick.srAuto && !!pick.found"
        />
      </label>
      <label class="bs-field">
        <span>{{ t('Search range (MHz)') }}</span>
        <input
          v-model.number="pick.rangeMhz"
          type="number"
          min="1"
          max="40"
          step="0.5"
          :placeholder="t('auto')"
        />
      </label>
      <Button size="small" :label="t('Tune and scan')" @click="tune" />
      <Button
        v-if="pick.mux"
        size="small"
        text
        :label="t('Show the services')"
        @click="pick.mux && showMux(pick.mux.uuid)"
      />
      <Button size="small" text :label="t('Close')" @click="pick = null" />
    </div>
    <div v-if="detail" class="bs-detail">
      <div class="bs-detail-head">
        <Loader2
          v-if="detail.loading"
          :size="14"
          :stroke-width="2"
          class="bs-spin"
          aria-hidden="true"
        />
        <strong>{{ detailRow?.name ?? (detail.uuid ? t('Mux') : detail.label) }}</strong>
        <span v-if="detailRow" class="bs-muted">
          {{ tuningText(detailRow) }} · {{ scanResultName(detailRow.scan_result) }} ·
          {{
            t(
              '{0} services, {1} channels',
              String(detailRow.num_svc ?? 0),
              String(detailRow.num_chn ?? 0)
            )
          }}
        </span>
        <span v-else-if="!detail.loading && detail.uuid" class="bs-muted">{{
          t('the mux is gone')
        }}</span>
        <Button
          size="small"
          text
          class="bs-detail-close"
          :label="t('Close')"
          @click="detail = null"
        />
      </div>
      <div v-if="detail.error" class="bs-status--err">{{ detail.error }}</div>
      <div v-if="detail.content" class="bs-content">
        <div class="bs-muted">{{ t('What the scan got') }}</div>
        <div v-for="(ln, i) in contentLines(detail.content, t)" :key="i" class="bs-content-line">
          {{ ln }}
        </div>
      </div>
      <div v-if="detailSubs.length" class="bs-muted">
        {{ t('Sub-muxes') }}:
        {{ detailSubs.map((r) => `${r.name} (${scanResultName(r.scan_result)})`).join(', ') }}
      </div>
      <div
        v-if="detail.uuid && !detail.loading && !detail.error && !detailServices.length"
        class="bs-muted"
      >
        {{ t('No services') }}
      </div>
      <div v-if="detailServices.length" class="bs-services-wrap">
        <table class="bs-services">
          <thead>
            <tr>
              <th>{{ t('Service') }}</th>
              <th>{{ t('Type') }}</th>
              <th>SID</th>
              <th v-if="detailSubs.length">{{ t('Sub-mux') }}</th>
              <th>{{ t('Provider') }}</th>
              <th>{{ t('Encrypted') }}</th>
              <th>{{ t('Channel') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="sv in detailServices" :key="sv.uuid">
              <td>{{ sv.svcname || t('(no name)') }}</td>
              <td>{{ serviceTypeName(sv.dvb_servicetype) }}</td>
              <td class="bs-num">{{ sv.sid }}</td>
              <td v-if="detailSubs.length">
                {{ sv.multiplex_uuid === detail.uuid ? '' : subMuxName(sv.multiplex_uuid) }}
              </td>
              <td>{{ sv.provider }}</td>
              <td>
                <template v-if="sv.encrypted">
                  <Lock :size="12" :stroke-width="2" class="bs-lock" />
                  {{ caidList(sv.caid) }}
                </template>
              </td>
              <td>{{ sv.channel?.length ? t('yes') : '' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
    <div v-if="tuneList.length" class="bs-tunes-head">
      <Loader2
        v-if="tunesRunning"
        :size="14"
        :stroke-width="2"
        class="bs-spin"
        aria-hidden="true"
      />
      {{ tuneSummary }}
    </div>
    <ul v-if="tuneList.length" class="bs-tunes">
      <li v-for="[k, s] in tuneList" :key="k" :class="`bs-tune--${s.state}`">
        <Loader2
          v-if="s.state === 'scanning'"
          :size="12"
          :stroke-width="2"
          class="bs-spin"
          aria-hidden="true"
        />
        <button
          v-if="s.mux || s.content"
          type="button"
          class="bs-link"
          :title="s.mux ? t('Show the services') : t('Show what the scan got')"
          @click="showTune(s)"
        >
          {{ s.text }}
        </button>
        <template v-else>{{ s.text }}</template>
      </li>
    </ul>
  </Dialog>
</template>

<style scoped>
.bs-bar {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: var(--tvh-space-3, 12px);
  margin-bottom: var(--tvh-space-2, 8px);
}
.bs-bar--small {
  align-items: center;
}
.bs-field {
  display: flex;
  flex-direction: column;
  gap: 2px;
  font-size: var(--tvh-text-sm, 0.85rem);
  color: var(--tvh-text-muted);
}
.bs-field input,
.bs-field select {
  width: 9em;
  padding: 3px 6px;
  border: 1px solid var(--tvh-border);
  border-radius: var(--tvh-radius-sm, 4px);
  background: var(--tvh-bg-surface);
  color: var(--tvh-text);
  font: inherit;
}
.bs-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: var(--tvh-text-sm, 0.85rem);
  color: var(--tvh-text-muted);
}
/* running: a blindscan or a tune */
.bs-spin {
  flex: none;
  vertical-align: -2px;
  animation: bs-spin 1.2s linear infinite;
}
@keyframes bs-spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}
@media (prefers-reduced-motion: reduce) {
  .bs-spin {
    animation: none;
  }
}
.bs-progress {
  height: 3px;
  margin: 0 0 var(--tvh-space-2, 8px);
  overflow: hidden;
  border-radius: 2px;
  background: var(--tvh-border);
}
.bs-progress__fill {
  height: 100%;
  background: var(--tvh-primary);
  transition: width 0.4s ease;
}
/* the spectrum shown is the one before: a new one is being taken */
.bs-graph--stale {
  opacity: 0.55;
}
.bs-status--err {
  color: var(--tvh-error);
}
.bs-hint {
  font-size: var(--tvh-text-sm, 0.85rem);
  color: var(--tvh-text-muted);
  font-variant-numeric: tabular-nums;
}
.bs-graph {
  width: 100%;
  height: auto;
  display: block;
  user-select: none;
  touch-action: none;
  cursor: crosshair;
  background: var(--tvh-bg-surface);
  border: 1px solid var(--tvh-border);
  border-radius: var(--tvh-radius-sm, 4px);
}
.bs-axis text,
.bs-lane-label {
  font-size: 11px;
  fill: var(--tvh-text-muted);
}
.bs-unit {
  font-style: italic;
}
.bs-grid {
  stroke: var(--tvh-border);
  stroke-width: 0.5;
}
.bs-curve {
  fill: none;
  stroke-width: 1;
  vector-effect: non-scaling-stroke;
}
.bs-curve--H {
  stroke: var(--tvh-primary);
}
.bs-curve--V {
  stroke: var(--tvh-warning);
}
/* the labels of the lanes name the colours of the polarisations */
.bs-lane-label--H {
  fill: var(--tvh-primary);
}
.bs-lane-label--V {
  fill: var(--tvh-warning-text, var(--tvh-warning));
}
.bs-noise {
  stroke: var(--tvh-text-muted, currentColor);
  stroke-width: 0.8;
  stroke-dasharray: 4 3;
  opacity: 0.6;
}
.bs-mux--ok {
  fill: var(--tvh-success);
}
.bs-mux--fail {
  fill: var(--tvh-error);
  opacity: 0.6;
}
.bs-mux--none {
  fill: var(--tvh-text-muted);
  opacity: 0.6;
}
.bs-mux--off {
  fill: url(#bs-hatch);
  stroke: var(--tvh-text-muted);
  stroke-width: 0.5;
}
.bs-hatch-line {
  stroke: var(--tvh-text-muted);
  stroke-width: 2;
}
.bs-found--H.bs-found--new {
  fill: var(--tvh-primary);
}
.bs-found--V.bs-found--new {
  fill: var(--tvh-warning);
}
.bs-found--known {
  fill: none;
  stroke-width: 1;
}
.bs-found--H.bs-found--known {
  stroke: var(--tvh-primary);
}
.bs-found--V.bs-found--known {
  stroke: var(--tvh-warning);
}
.bs-mux,
.bs-found {
  cursor: pointer;
}
.bs-cursor {
  stroke: var(--tvh-text-muted);
  stroke-width: 0.5;
  pointer-events: none;
}
.bs-pick {
  stroke: var(--tvh-text);
  stroke-width: 1;
  stroke-dasharray: 3 2;
  pointer-events: none;
}
.bs-drag {
  fill: var(--tvh-primary);
  opacity: 0.12;
  pointer-events: none;
}
.bs-pick-panel {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: var(--tvh-space-3, 12px);
  margin-top: var(--tvh-space-2, 8px);
  padding: var(--tvh-space-2, 8px);
  border: 1px solid var(--tvh-border);
  border-radius: var(--tvh-radius-sm, 4px);
}
.bs-pick-info {
  flex-basis: 100%;
  font-size: var(--tvh-text-sm, 0.85rem);
}
.bs-check {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: var(--tvh-text-sm, 0.85rem);
}
.bs-muted {
  color: var(--tvh-text-muted);
}
.bs-detail {
  margin-top: var(--tvh-space-2, 8px);
  padding: var(--tvh-space-2, 8px);
  border: 1px solid var(--tvh-border);
  border-radius: var(--tvh-radius-sm, 4px);
  font-size: var(--tvh-text-sm, 0.85rem);
}
.bs-detail-head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px 10px;
  margin-bottom: var(--tvh-space-1, 4px);
}
.bs-detail-close {
  margin-left: auto;
}
.bs-content {
  margin: var(--tvh-space-1, 4px) 0;
  max-height: 12em;
  overflow-y: auto;
}
.bs-content-line {
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
}
.bs-services-wrap {
  max-height: 16em;
  overflow-y: auto;
  margin-top: var(--tvh-space-1, 4px);
}
.bs-services {
  width: 100%;
  border-collapse: collapse;
}
.bs-services th {
  position: sticky;
  top: 0;
  background: var(--tvh-bg-surface);
  text-align: left;
  font-weight: 600;
  color: var(--tvh-text-muted);
}
.bs-services th,
.bs-services td {
  padding: 2px 8px 2px 0;
  border-bottom: 1px solid var(--tvh-border);
  white-space: nowrap;
}
.bs-num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.bs-lock {
  vertical-align: -1px;
}
/* a tune that locked: what it found, by a click */
.bs-link {
  padding: 0;
  border: 0;
  background: none;
  color: inherit;
  font: inherit;
  text-align: left;
  text-decoration: underline dotted;
  text-underline-offset: 2px;
  cursor: pointer;
}
.bs-link:hover,
.bs-link:focus-visible {
  text-decoration-style: solid;
}
.bs-tunes-head {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-top: var(--tvh-space-2, 8px);
  font-size: var(--tvh-text-sm, 0.85rem);
  color: var(--tvh-text-muted);
}
.bs-tunes {
  max-height: 9em;
  overflow-y: auto;
  margin: var(--tvh-space-1, 4px) 0 0;
  padding-left: 1.2em;
  font-size: var(--tvh-text-sm, 0.85rem);
}
.bs-tune--ok {
  color: var(--tvh-success);
}
.bs-tried {
  stroke: var(--tvh-bg-surface);
  stroke-width: 1.2;
  fill: var(--tvh-text-muted);
}
.bs-tried--ok {
  fill: var(--tvh-success);
}
.bs-tried--nodata {
  fill: var(--tvh-warning);
}
.bs-tried--fail {
  fill: var(--tvh-error);
}
.bs-tune--nodata {
  color: var(--tvh-warning-text, var(--tvh-warning));
}
.bs-tune--fail {
  color: var(--tvh-error);
}
</style>
