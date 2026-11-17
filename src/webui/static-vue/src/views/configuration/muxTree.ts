// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Tvheadend contributors

/*
 * The muxes of a network as a tree: transponders, the sub-muxes
 * carried in them (input streams, DAB, T2MI, ...) and theirs.
 *
 * A sub-mux names its transponder in `parent_mux` (uuid, DVB muxes
 * only); the depth is not limited here. The sub-muxes of a
 * multistream transponder that are not input streams themselves
 * (DAB-GSE ensembles, ...) but carry an ISI (`stream_id`) go below
 * the input stream of that ISI: below its sub-mux when there is one,
 * else below a node made up for it (`virtual`). So the tree shows
 * transponder → ISI → DAB as it is on air.
 *
 * Pure functions, no Vue: the view (DvbMuxTreeView.vue) renders the
 * nodes and keeps the expanded state. The labels go through the static
 * `t()`.
 */
import { t } from '@/composables/useI18n'

/* the fields of an `api/mpegts/mux/grid` row the tree uses */
export interface MuxRow {
  uuid: string
  name?: string
  network_uuid?: string
  parent_mux?: string
  multistream?: boolean
  tstype?: number
  stream_id?: number
  pid?: number
  dab_ip?: string
  dab_port?: number
  carrier_sid?: number
  /* DAB in a PID: the service whose PMT lists the PID */
  dab_pmt_sid?: number
  pnetwork_name?: string | null
  delsys?: string
  modulation?: string
  constellation?: string
  fec?: string
  symbolrate?: number
  /* Hz, or kHz for satellite muxes (see muxFrequency.ts) */
  frequency?: number
  polarisation?: string
  rolloff?: string
  pilot?: string
  pls_mode?: string
  pls_code?: number
  bandwidth?: string
  coding?: string
  scan_result?: number
  scan_state?: number
  num_svc?: number
  num_chn?: number
  enabled?: number
  /* a GSE stream (Neumo driver): what it carried at its last scan, one line */
  gse_content?: string
  [key: string]: unknown
}

/* the fields of the mux grid the tree uses: the `list` of its requests,
 * the server sends only these (and the uuid) */
export const MUX_FIELDS = [
  'name', 'network_uuid', 'parent_mux', 'multistream', 'tstype', 'stream_id', 'pid', 'dab_ip', 'dab_port',
  'carrier_sid', 'dab_pmt_sid', 'pnetwork_name', 'delsys', 'modulation', 'constellation', 'fec', 'symbolrate',
  'rolloff', 'pilot', 'pls_mode', 'pls_code', 'bandwidth', 'scan_result', 'scan_state', 'num_svc', 'num_chn',
  'enabled', 'frequency', 'polarisation', 'gse_content',
].join(',')

export interface MuxTreeNode {
  key: string
  /* the mux, null for a made-up input stream node */
  row: MuxRow | null
  label: string
  virtual: boolean
  /* has a parent_mux that is not among the rows */
  orphan: boolean
  isi: number | null
  children: MuxTreeNode[]
}

/* mm_type, MM_TYPE_* in src/input/mpegts.h */
export const MUX_TYPE_TS = 0
export const MUX_TYPE_T2MI = 1
export const MUX_TYPE_DAB_MPE = 2
export const MUX_TYPE_DAB_ETI = 3
export const MUX_TYPE_DAB_GSE = 4
export const MUX_TYPE_GSE = 5
export const MUX_TYPE_DAB_TSNI = 6
export const MUX_TYPE_ISI = 7

/* the type as the tree shows it */
const TYPE_LABELS: Record<number, () => string> = {
  [MUX_TYPE_TS]: () => t('TS'),
  [MUX_TYPE_ISI]: () => t('TS (ISI)'),
  [MUX_TYPE_T2MI]: () => t('T2MI'),
  [MUX_TYPE_DAB_MPE]: () => t('DAB-MPE'),
  [MUX_TYPE_DAB_ETI]: () => t('DAB-ETI'),
  [MUX_TYPE_DAB_TSNI]: () => t('DAB-TSNI'),
  [MUX_TYPE_DAB_GSE]: () => t('DAB-GSE'),
  [MUX_TYPE_GSE]: () => t('GSE'),
}

export function muxTypeName(type: number | undefined): string {
  return TYPE_LABELS[type ?? MUX_TYPE_TS]?.() ?? t('type {0}', type)
}

/* a string filter of the grids is a case-insensitive extended regular
 * expression on the server (`idnode_filter_add_str`): the one that finds
 * exactly this text */
export function exactMatch(text: string): string {
  return `^${containsMatch(text)}$`
}

/* ... and the one that finds the text anywhere */
export function containsMatch(text: string): string {
  return text.replace(/[.[\]()*+?{}|^$\\]/g, '\\$&')
}

/* an address typed for a DAB sub-mux: a.b.c.d with the octets 0 to 255,
 * an optional :port 1 to 65535 (0: none); null when it is none - the
 * server would take 300.1.2.3 as 44.1.2.3 and cut the port to 16 bits */
export function parseAddress(text: string): { ip: string; port: number } | null {
  const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})(?::(\d{1,5}))?$/.exec(text.trim())
  if (!m) return null
  const octets = m.slice(1, 5).map(Number)
  if (octets.some((o) => o > 255)) return null
  const port = m[5] === undefined ? 0 : Number(m[5])
  if (m[5] !== undefined && (port < 1 || port > 65535)) return null
  return { ip: octets.join('.'), port }
}

/* a PID typed for a sub-mux, decimal or 0x hex: 1 to 0x1FFE (0x1FFF is
 * the null packet); null when it is none */
export function parsePid(text: string): number | null {
  const v = text.trim()
  const pid = /^0x[0-9a-f]+$/i.test(v) ? parseInt(v, 16) : /^\d+$/.test(v) ? Number(v) : NaN
  return Number.isInteger(pid) && pid > 0 && pid < 0x1fff ? pid : null
}

/* the IP address and port of a DAB sub-mux (MPE, GSE), else '' */
export function addressLabel(row: MuxRow): string {
  if (!row.dab_ip) return ''
  return row.dab_port ? `${row.dab_ip}:${row.dab_port}` : row.dab_ip
}

/* mm_scan_state (MM_SCAN_STATE_* in src/input/mpegts.h): idle, queued
 * for a scan, in the idle scan queue of the network (the muxes rotate
 * there with "Idle scan muxes"), scanning */
export const SCAN_STATE_IDLE = 0
export const SCAN_STATE_PEND = 1
export const SCAN_STATE_IPEND = 2
export const SCAN_STATE_ACTIVE = 3

/* a scan of the mux is queued or runs. Not so in the idle queue: a scan
 * asked for there queues the mux before the idle ones
 * (`mpegts_mux_class_scan_state_set`) */
export function scanQueued(row: MuxRow): boolean {
  const st = row.scan_state ?? SCAN_STATE_IDLE
  return st === SCAN_STATE_PEND || st === SCAN_STATE_ACTIVE
}

/* the server scans only an enabled mux (of an enabled network): a scan
 * of a disabled or ignored one is dropped without a word */
export function scanEnabled(row: MuxRow): boolean {
  return (row.enabled ?? 1) === 1
}

/* the muxes a scan of the node scans: its own, else (a made-up input
 * stream) those below it - the enabled ones */
export function scanTargets(node: MuxTreeNode): string[] {
  if (node.row) return scanEnabled(node.row) ? [node.row.uuid] : []
  const out: string[] = []
  for (const c of node.children) out.push(...scanTargets(c))
  return out
}

/* the node or one below it is queued or scanning */
export function scanBusy(node: MuxTreeNode): boolean {
  if (node.row) return scanQueued(node.row)
  return node.children.some(scanBusy)
}

/* the muxes of the trees queued for a scan or being scanned (not the
 * ones in the idle queue) */
export function scanPending(nodes: MuxTreeNode[]): number {
  let n = 0
  for (const x of nodes) n += (x.row && scanQueued(x.row) ? 1 : 0) + scanPending(x.children)
  return n
}

/* "PSK/8" → "8PSK", "QAM/256" → "256QAM", others as they are */
function modulationName(m: string | undefined): string {
  if (!m || m === 'AUTO' || m === 'NONE') return ''
  const x = /^(PSK|QAM|APSK)\/(\d+)$/.exec(m)
  return x ? `${x[2]}${x[1]}` : m
}

/* rolloff "35" → "0.35", "5" → "0.05" */
function rolloffName(r: string | undefined): string {
  if (!r || r === 'NONE') return ''
  if (r === 'AUTO') return 'auto'
  const n = Number(r)
  return Number.isFinite(n) ? (n / 100).toFixed(2) : r
}

/* the tuning of a transponder in one line: delivery system, modulation
 * and FEC, symbol rate (kSym/s), PLS (when scrambled), rolloff, pilot -
 * the PLS before the two that are mostly auto: a cut line keeps it */
export function tuningLabel(row: MuxRow): string {
  const parts: string[] = []
  if (row.delsys) parts.push(row.delsys)
  const mod = modulationName(row.modulation ?? row.constellation)
  const fec = row.fec && row.fec !== 'AUTO' && row.fec !== 'NONE' ? row.fec : ''
  if (mod || fec) parts.push([mod, fec].filter(Boolean).join(' '))
  if (row.bandwidth && row.bandwidth !== 'AUTO') parts.push(row.bandwidth)
  if (row.symbolrate) parts.push(`SR ${Math.round(row.symbolrate / 1000)}`)
  const s2 = row.delsys === 'DVB-S2' || row.delsys === 'ISDB-S'
  /* Root 1 and Gold 0 are the same sequence: not scrambled */
  const mode = row.pls_mode
  const code = row.pls_code
  if (s2 && mode && code !== undefined && !(mode === 'ROOT' && code === 1) && !(mode === 'GOLD' && code === 0))
    parts.push(`PLS ${mode.charAt(0) + mode.slice(1).toLowerCase()} ${code}`)
  const ro = s2 ? rolloffName(row.rolloff) : ''
  if (ro) parts.push(`RO ${ro}`)
  if (s2 && row.pilot) parts.push(`Pilot ${row.pilot.toLowerCase()}`)
  return parts.join(' · ')
}

/* a sub-mux that comes from a service of its transponder (T2MI ...) */
export interface CarrierInfo {
  name: string
  encrypted: boolean
  caid: string
}

/* the fields of an `api/mpegts/service/grid` row the tree uses */
export interface ServiceRow {
  uuid: string
  /* the name of its network (`mn_display_name`) */
  network?: string
  svcname?: string
  sid?: number
  provider?: string
  dvb_servicetype?: number
  encrypted?: boolean
  caid?: string
  channel?: string[]
  enabled?: boolean
  multiplex_uuid?: string
  streams?: string
}

/* the fields of the service grid the tree uses (`list`) */
export const SERVICE_FIELDS = [
  'svcname', 'sid', 'provider', 'dvb_servicetype', 'encrypted', 'caid', 'channel', 'enabled', 'multiplex_uuid',
  'network', 'streams',
].join(',')

/* service_type of the service descriptor (EN 300 468 table 87) */
const SERVICE_TYPES: Record<number, () => string> = {
  0x01: () => t('SD TV'),
  0x02: () => t('Radio'),
  0x03: () => t('Teletext'),
  0x0a: () => t('Radio'),
  0x0c: () => t('Data'),
  0x10: () => t('MHP'),
  0x11: () => t('HD TV'),
  0x16: () => t('SD TV'),
  0x19: () => t('HD TV'),
  0x1c: () => t('3D TV'),
  0x1f: () => t('UHD TV'),
  0x20: () => t('UHD TV'),
}

export function serviceTypeName(type: number | undefined): string {
  if (!type) return ''
  return SERVICE_TYPES[type]?.() ?? `0x${type.toString(16).padStart(2, '0')}`
}

/* "1883:003311,1863:003343" → "1883, 1863" (the CA systems, not the providers) */
export function caidList(caid: string | undefined): string {
  if (!caid) return ''
  const ids = caid
    .split(',')
    .map((c) => c.split(':')[0].trim())
    .filter(Boolean)
  return [...new Set(ids)].join(', ')
}

/* the services of a network by mux, by name */
export function servicesByMux(rows: ServiceRow[]): Map<string, ServiceRow[]> {
  const map = new Map<string, ServiceRow[]>()
  for (const r of rows) {
    const key = r.multiplex_uuid ?? ''
    const l = map.get(key)
    if (l) l.push(r)
    else map.set(key, [r])
  }
  for (const l of map.values())
    l.sort((a, b) => collator.compare(a.svcname ?? '', b.svcname ?? '') || (a.sid ?? 0) - (b.sid ?? 0))
  return map
}

/* mm_scan_result (MM_SCAN_* in src/input/mpegts.h), named as in
 * scan_result_tab in src/input/mpegts/mpegts_mux.c */
export const SCAN_RESULT_NONE = 0
export const SCAN_RESULT_OK = 1
export const SCAN_RESULT_FAIL = 2
export const SCAN_RESULT_PARTIAL = 3
export const SCAN_RESULT_IGNORE = 4

const SCAN_RESULTS: Record<number, () => string> = {
  [SCAN_RESULT_NONE]: () => '',
  [SCAN_RESULT_OK]: () => t('OK'),
  [SCAN_RESULT_FAIL]: () => t('FAIL'),
  [SCAN_RESULT_PARTIAL]: () => t('OK (partial)'),
  [SCAN_RESULT_IGNORE]: () => t('IGNORE'),
}

export function scanResultName(result: number | undefined): string {
  return SCAN_RESULTS[result ?? SCAN_RESULT_NONE]?.() ?? String(result)
}

/* scanned with success: OK, or OK in part (MM_SCAN_CHECK_OK) */
export function scanOk(row: MuxRow): boolean {
  return row.scan_result === SCAN_RESULT_OK || row.scan_result === SCAN_RESULT_PARTIAL
}

/* an input stream of its own: its children may hang below it */
function isInputStream(row: MuxRow): boolean {
  return row.tstype === MUX_TYPE_ISI || row.tstype === MUX_TYPE_GSE
}

function hasIsi(row: MuxRow): boolean {
  return typeof row.stream_id === 'number' && row.stream_id >= 0
}

const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' })

/* a sub-mux is named by what carries it, then its own part: the label is
 * that part, "12226V ISI 14" below "12226V" → "ISI 14" */
export function shortName(name: string, parentName: string | undefined): string {
  if (parentName && name.startsWith(parentName + ' ') && name.length > parentName.length + 1)
    return name.slice(parentName.length + 1)
  return name
}

/* input streams by ISI, then the others by type and name */
function compareNodes(a: MuxTreeNode, b: MuxTreeNode): number {
  const ai = a.isi !== null && (a.virtual || (a.row && isInputStream(a.row)))
  const bi = b.isi !== null && (b.virtual || (b.row && isInputStream(b.row)))
  if (ai !== bi) return ai ? -1 : 1
  if (ai && bi && a.isi !== b.isi) return (a.isi ?? 0) - (b.isi ?? 0)
  const at = a.row?.tstype ?? MUX_TYPE_TS
  const bt = b.row?.tstype ?? MUX_TYPE_TS
  if (!ai && at !== bt) return at - bt
  return collator.compare(a.label, b.label)
}

function sortTree(nodes: MuxTreeNode[]): void {
  nodes.sort(compareNodes)
  for (const n of nodes) sortTree(n.children)
}

export function buildMuxTree(rows: MuxRow[]): MuxTreeNode[] {
  const byUuid = new Map(rows.map((r) => [r.uuid, r]))

  /* the parent a row hangs below, null at the top (no parent, the
   * parent is not among the rows, or the chain loops) */
  function parentOf(row: MuxRow): MuxRow | null {
    const p = row.parent_mux ? byUuid.get(row.parent_mux) : undefined
    if (!p) return null
    const seen = new Set([row.uuid])
    for (let q: MuxRow | undefined = p; q; q = q.parent_mux ? byUuid.get(q.parent_mux) : undefined) {
      if (seen.has(q.uuid)) return null
      seen.add(q.uuid)
    }
    return p
  }

  const children = new Map<string, MuxRow[]>()
  const top: MuxRow[] = []
  for (const r of rows) {
    const p = parentOf(r)
    if (!p) {
      top.push(r)
      continue
    }
    const l = children.get(p.uuid)
    if (l) l.push(r)
    else children.set(p.uuid, [r])
  }

  function make(row: MuxRow, parentName: string | undefined): MuxTreeNode {
    return {
      key: row.uuid,
      row,
      label: shortName(row.name ?? row.uuid, parentName),
      virtual: false,
      orphan: !!row.parent_mux && !byUuid.has(row.parent_mux),
      isi: hasIsi(row) ? (row.stream_id as number) : null,
      children: [],
    }
  }

  function build(row: MuxRow, parentName: string | undefined): MuxTreeNode {
    const node = make(row, parentName)
    const name = row.name ?? row.uuid
    const kids = (children.get(row.uuid) ?? []).map((c) => build(c, name))
    if (!row.multistream) {
      node.children = kids
      return node
    }
    /* multistream: the sub-muxes with an ISI that are no input stream
     * go below the input stream of their ISI */
    const streams = new Map<number, MuxTreeNode>()
    for (const k of kids)
      if (k.row && isInputStream(k.row) && k.isi !== null && !streams.has(k.isi)) streams.set(k.isi, k)
    for (const k of kids) {
      if (!k.row || isInputStream(k.row) || k.isi === null) {
        node.children.push(k)
        continue
      }
      let s = streams.get(k.isi)
      if (!s) {
        s = {
          key: `${row.uuid}:isi:${k.isi}`,
          row: null,
          label: `ISI ${k.isi}`,
          virtual: true,
          orphan: false,
          isi: k.isi,
          children: [],
        }
        streams.set(k.isi, s)
        node.children.push(s)
      }
      /* named by the transponder and the ISI: "10717V ISI 171 Riks" → "Riks" */
      k.label = shortName(k.row.name ?? k.row.uuid, `${name} ISI ${k.isi}`)
      s.children.push(k)
    }
    return node
  }

  const tree = top.map((r) => build(r, undefined))
  sortTree(tree)
  return tree
}

/* the sub-muxes below a node (all levels, made-up nodes not counted) and
 * how many of them were scanned with success (OK or partial) */
export function subMuxScan(node: MuxTreeNode): { ok: number; total: number } {
  let ok = 0
  let total = 0
  for (const c of node.children) {
    if (c.row) {
      total++
      if (scanOk(c.row)) ok++
    }
    const sub = subMuxScan(c)
    ok += sub.ok
    total += sub.total
  }
  return { ok, total }
}

/* the services and channels of a node and of all sub-muxes below it */
export function subtreeCounts(node: MuxTreeNode): { svc: number; chn: number } {
  let svc = node.row?.num_svc ?? 0
  let chn = node.row?.num_chn ?? 0
  for (const c of node.children) {
    const sub = subtreeCounts(c)
    svc += sub.svc
    chn += sub.chn
  }
  return { svc, chn }
}

/* every node key with children: for "expand all" */
export function expandableKeys(nodes: MuxTreeNode[], out: string[] = []): string[] {
  for (const n of nodes) {
    if (n.children.length) {
      out.push(n.key)
      expandableKeys(n.children, out)
    }
  }
  return out
}

/* the number of muxes in the tree (made-up nodes not counted) */
export function countMuxes(nodes: MuxTreeNode[]): number {
  let n = 0
  for (const x of nodes) n += (x.row ? 1 : 0) + countMuxes(x.children)
  return n
}

/* the text a mux is found by: its name and label, type, provider network,
 * address and tuning (lower case) */
export function muxSearchText(node: MuxTreeNode): string {
  const r = node.row
  if (!r) return node.label.toLowerCase()
  return [r.name, node.label, muxTypeName(r.tstype), r.pnetwork_name, addressLabel(r), tuningLabel(r)]
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
}

export interface TreeFilterResult {
  /* the nodes shown: a match, or a match below them */
  show: Set<string>
  /* the nodes opened for a match below them (sub-muxes or services) */
  open: Set<string>
  /* the nodes that match themselves: all below them stays browsable */
  self: Set<string>
}

/* which nodes a filter shows: `match` for the node itself, `servicesMatch`
 * for a match among the services of its mux */
export function filterTree(
  nodes: MuxTreeNode[],
  match: (n: MuxTreeNode) => boolean,
  servicesMatch: (n: MuxTreeNode) => boolean = () => false,
): TreeFilterResult {
  const res: TreeFilterResult = { show: new Set(), open: new Set(), self: new Set() }
  const walk = (n: MuxTreeNode): boolean => {
    let below = false
    for (const c of n.children) if (walk(c)) below = true
    const svc = servicesMatch(n)
    const self = match(n)
    if (self) res.self.add(n.key)
    if (below || svc) res.open.add(n.key)
    if (self || below || svc) {
      res.show.add(n.key)
      return true
    }
    return false
  }
  for (const n of nodes) walk(n)
  return res
}
