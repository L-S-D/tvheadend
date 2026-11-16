<!--
  SPDX-License-Identifier: GPL-3.0-or-later
  Copyright (C) 2026 Tvheadend contributors
-->
<script setup lang="ts">
/*
 * BlindscanDialog — the blindscan of a DVB-S network (Neumo driver).
 *
 * Starts one (polarisations, range) and follows it; shows the spectrum of
 * each polarisation with its noise floor, and below it in lanes the
 * transponders of the network and the carriers found. A click picks a
 * frequency (a carrier, a transponder, or a point of a curve) and tunes
 * there blind, with the symbol rate of the carrier or one given: the mux
 * there, or a new one that is kept only when it locks
 * (`mpegts/network/blindscan_tune`). Drag to zoom, double click for all.
 */

import { computed, onBeforeUnmount, ref, watch } from 'vue'
import Dialog from 'primevue/dialog'
import Button from 'primevue/button'
import { apiCall } from '@/api/client'
import { cometClient } from '@/api/comet'
import { useI18n } from '@/composables/useI18n'
import { useToastNotify } from '@/composables/useToastNotify'
import { apiErrorMessage } from '@/utils/apiErrorMessage'
import { exactMatch, MUX_FIELDS, type MuxRow } from './muxTree'
import {
  decimate,
  graphMuxes,
  levelAt,
  levelRange,
  muxOfCarrier,
  muxWidth,
  ticks,
  type BlindscanCarrier,
  type BlindscanPol,
  type BlindscanResult,
  type GraphMux,
  type Pol,
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
const polSel = ref<'HV' | 'H' | 'V'>('HV')
const startMhz = ref(10700)
const endMhz = ref(12750)
const show = ref<Record<Pol, boolean>>({ H: true, V: true })
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

function polData(p: Pol): BlindscanPol | null {
  const d = result.value?.pols?.[p]
  return d && d.freq.length ? d : null
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
  if (!props.visible || msg.uuid !== props.networkUuid) return
  if (typeof msg.mux === 'string') {
    onTuneResult(msg)
    return
  }
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
    })
    if (!r.started) toast.error(r.error ?? t('The blindscan did not start'))
    else {
      progress.value = {
        ...(progress.value ?? {}),
        state: 'running',
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
const H = LANE_Y + LANES.length * (LANE_H + LANE_GAP) + 4

function laneY(i: number): number {
  return LANE_Y + i * (LANE_H + LANE_GAP)
}

const levels = computed<[number, number]>(() => {
  const [lo, hi] = range.value
  return levelRange(
    POLS.filter((p) => show.value[p])
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
  POLS.filter((p) => show.value[p] && polData(p)).map((p) => {
    const d = polData(p)!
    const pts = decimate(d.freq, d.level, range.value[0], range.value[1], (X1 - X0) * 2)
    return {
      pol: p,
      points: pts.map(([f, l]) => `${x(f).toFixed(1)},${y(l).toFixed(1)}`).join(' '),
      noise: d.noise !== undefined ? y(d.noise) : null,
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
  LANES.forEach((ln, i) => {
    if (ln.kind === 'mux') {
      for (const m of muxes.value) {
        if (m.pol !== ln.pol) continue
        const half = muxWidth(m.sr || 1000000) / 2
        const r = clipRect(m.freq - half, m.freq + half)
        if (!r) continue
        const cls =
          m.enabled !== 1 ? 'off' : m.scanResult === 1 ? 'ok' : m.scanResult === 2 ? 'fail' : 'none'
        out.push({
          key: `m${m.uuid}`,
          lane: i,
          ...r,
          cls: `bs-mux bs-mux--${cls}`,
          title: `${m.name} · ${(m.sr / 1000).toFixed(0)} kS/s · ${muxState(m)}`,
          mux: m,
        })
      }
    } else {
      for (const f of found.value) {
        if (f.pol !== ln.pol) continue
        const r = clipRect(f.c.freq - f.c.bw / 2, f.c.freq + f.c.bw / 2)
        if (!r) continue
        out.push({
          key: `c${f.pol}${f.c.freq}`,
          lane: i,
          ...r,
          cls: `bs-found bs-found--${f.pol} ${f.mux ? 'bs-found--known' : 'bs-found--new'}`,
          title: `${(f.c.freq / 1000).toFixed(3)} MHz ${f.pol} · ~${(f.c.sr / 1000).toFixed(0)} kS/s · ${(f.c.snr / 100).toFixed(1)} dB${f.mux ? ` · ${f.mux.name}` : ` · ${t('no mux')}`}`,
          found: f,
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
  return { px: ((e.clientX - r.left) * W) / r.width, py: ((e.clientY - r.top) * H) / r.height }
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
    if (!d || !show.value[p]) continue
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
  const li = LANES.findIndex((_, i) => py >= laneY(i) - 1 && py <= laneY(i) + LANE_H + 1)
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
    if (!d || !show.value[p]) continue
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

interface TuneState {
  freq: number
  pol: Pol
  state: 'scanning' | 'ok' | 'fail'
  text: string
}
const tunes = ref<Record<string, TuneState>>({})

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
    tunes.value = {
      ...tunes.value,
      [r.mux]: {
        freq,
        pol: p.pol,
        state: 'scanning',
        text: t('Scanning {0} MHz {1}…', (freq / 1000).toFixed(3), p.pol),
      },
    }
    emit('changed')
  } catch (e) {
    toast.error(`${t('Failed to tune')}: ${apiErrorMessage(e)}`)
  }
}

function onTuneResult(m: Record<string, unknown>) {
  const uuid = String(m.mux)
  const prev = tunes.value[uuid]
  const ok = m.result === 'ok'
  const freq = Number(m.freq ?? prev?.freq ?? 0)
  const where = `${(freq / 1000).toFixed(3)} MHz ${prev?.pol ?? ''}`
  let text: string
  if (ok && typeof m.same_name === 'string')
    text = t('{0}: locked the transponder {1}, which the network has', where, m.same_name)
  else if (ok)
    text = t(
      '{0}: locked, {1} kS/s - the mux is kept',
      where,
      (Number(m.sr ?? 0) / 1000).toFixed(0)
    )
  else if (m.deleted) text = t('{0}: nothing found - the new mux goes again', where)
  else text = t('{0}: the scan failed', where)
  tunes.value = {
    ...tunes.value,
    [uuid]: { freq, pol: prev?.pol ?? 'H', state: ok ? 'ok' : 'fail', text },
  }
  emit('changed')
  void loadMuxes()
}

const tuneList = computed(() => Object.entries(tunes.value).reverse().slice(0, 5))

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
      <Button
        v-if="!running"
        size="small"
        :label="t('Start blindscan')"
        :disabled="busy"
        @click="start"
      />
      <Button v-else size="small" severity="danger" :label="t('Stop')" @click="stop" />
      <span class="bs-status" :class="{ 'bs-status--err': progress?.state === 'failed' }">{{
        statusText
      }}</span>
    </div>
    <div class="bs-bar bs-bar--small">
      <label v-for="p in POLS" :key="p" class="bs-check" :class="`bs-check--${p}`">
        <input v-model="show[p]" type="checkbox" />
        {{ p }}
      </label>
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
        <text :x="X0 - 6" :y="Y0 - 2" text-anchor="end" class="bs-unit">dB</text>
        <text :x="X1" :y="Y1 + 30" text-anchor="end" class="bs-unit">MHz</text>
      </g>
      <g v-for="c in curves" :key="c.pol">
        <line
          v-if="c.noise !== null"
          :x1="X0"
          :x2="X1"
          :y1="c.noise"
          :y2="c.noise"
          :class="`bs-noise bs-noise--${c.pol}`"
        />
        <polyline :points="c.points" :class="`bs-curve bs-curve--${c.pol}`" />
      </g>
      <g>
        <text
          v-for="(ln, i) in LANES"
          :key="`l${i}`"
          :x="X0 - 6"
          :y="laneY(i) + LANE_H - 2"
          text-anchor="end"
          class="bs-lane-label"
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
      <Button size="small" text :label="t('Close')" @click="pick = null" />
    </div>
    <ul v-if="tuneList.length" class="bs-tunes">
      <li v-for="[k, s] in tuneList" :key="k" :class="`bs-tune--${s.state}`">{{ s.text }}</li>
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
.bs-check {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: var(--tvh-text-sm, 0.85rem);
}
.bs-status {
  font-size: var(--tvh-text-sm, 0.85rem);
  color: var(--tvh-text-muted);
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
.bs-curve--H,
.bs-check--H {
  stroke: var(--tvh-primary);
  color: var(--tvh-primary);
}
.bs-curve--V,
.bs-check--V {
  stroke: var(--tvh-warning);
  color: var(--tvh-warning-text, var(--tvh-warning));
}
.bs-noise {
  stroke-width: 0.8;
  stroke-dasharray: 4 3;
  opacity: 0.6;
}
.bs-noise--H {
  stroke: var(--tvh-primary);
}
.bs-noise--V {
  stroke: var(--tvh-warning);
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
.bs-tunes {
  margin: var(--tvh-space-2, 8px) 0 0;
  padding-left: 1.2em;
  font-size: var(--tvh-text-sm, 0.85rem);
}
.bs-tune--ok {
  color: var(--tvh-success);
}
.bs-tune--fail {
  color: var(--tvh-error);
}
</style>
