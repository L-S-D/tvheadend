<!--
  SPDX-License-Identifier: GPL-3.0-or-later
  Copyright (C) 2026 Tvheadend contributors
-->
<script setup lang="ts">
/*
 * Configuration → DVB Inputs → Muxes.
 *
 * The muxes as a tree: network → transponder → the sub-muxes carried in
 * it (input streams of a multistream transponder, DAB, T2MI ...) → theirs,
 * and the services of each mux. A double click opens the editor of the
 * mux, service or network.
 *
 * A full mux grid: rows are selected by a click (Ctrl/Cmd adds one,
 * Shift a range) or their check box; the toolbar adds a mux (to the
 * network of the selection, else one picked), edits the selection (several
 * of a kind at once), deletes it, scans its muxes. The filter finds muxes
 * by name, type, provider network, address and tuning (all networks are
 * loaded for it), by state (scan failed, disabled), and optionally by
 * the names of their services; the rows above a match open.
 *
 * Live: Comet notifications of muxes, services and networks reload the
 * networks they belong to (collected for a second). In the rows: the
 * "On" box enables a mux or service at once; the PID and the address of
 * a sub-mux are edited in their cell (Enter or leaving it saves, Esc
 * cancels).
 *
 * The networks come from `mpegts/network/grid` (IPTV networks left
 * out: their class from `idnode/load`), the muxes of a network from
 * `mpegts/mux/grid` when it is first expanded. The tree is built in
 * `muxTree.ts`. No live updates: "Reload", and the network of an
 * edited mux reloads when its editor closes.
 *
 * Scan: the button of a row queues a scan of its mux (`scan_state` =
 * pending, as the mux editor does), of a made-up input stream those of
 * the muxes below it. The network is then reloaded every 2 s until these
 * scans are over.
 *
 * Services: expanding a mux shows its services (type, SID, CA systems,
 * channels) - right below it when it has no sub-muxes (a DAB ensemble, a
 * transponder without), else in a "Services (n)" row of their own,
 * collapsed, above the sub-muxes.
 * The services of a network are loaded when the first such row of it is
 * expanded (or for the carrier services of its sub-muxes), and again after
 * a scan or an edit.
 */
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import Button from 'primevue/button'
import { ChevronDown, ChevronRight, Lock, Pencil, Plus, RefreshCw, ScanSearch, Trash2 } from 'lucide-vue-next'
import IdnodeEditor from '@/components/IdnodeEditor.vue'
import IdnodePickEntityDialog from '@/components/IdnodePickEntityDialog.vue'
import SearchInput from '@/components/SearchInput.vue'
import { apiCall } from '@/api/client'
import { cometClient } from '@/api/comet'
import type { IdnodeNotification } from '@/types/comet'
import { useStaleDataRecovery } from '@/composables/useStaleDataRecovery'
import { useAccessStore } from '@/stores/access'
import { useConfirmDialog } from '@/composables/useConfirmDialog'
import { useI18n } from '@/composables/useI18n'
import {
  addressLabel,
  buildMuxTree,
  caidList,
  countMuxes,
  expandableKeys,
  filterTree,
  muxSearchText,
  muxTypeName,
  scanBusy,
  scanResultName,
  scanTargets,
  SCAN_STATE_ACTIVE,
  SCAN_STATE_IDLE,
  servicesByMux,
  serviceTypeName,
  subMuxScan,
  subtreeCounts,
  tuningLabel,
  type CarrierInfo,
  type MuxRow,
  type MuxTreeNode,
  type ServiceRow,
  type TreeFilterResult,
} from './muxTree'

const { t } = useI18n()
const access = useAccessStore()
const confirmDialog = useConfirmDialog()

interface NetworkEntry {
  uuid: string
  name: string
  numMux: number
  numSvc: number
  numChn: number
}

interface GridResponse<T> {
  entries?: T[]
}

interface FlatRow {
  key: string
  depth: number
  network?: NetworkEntry
  node?: MuxTreeNode
  hasChildren: boolean
  open: boolean
  loading?: boolean
  /* a network or transponder row: its name uses the columns type, PID
   * and address (a transponder: its tuning there) */
  wide?: boolean
  /* the "Services (n)" row of a mux, and a service */
  svcGroup?: { mux: MuxRow; count: number }
  service?: ServiceRow
  net?: NetworkEntry
}

const networks = ref<NetworkEntry[]>([])
const trees = ref<Record<string, MuxTreeNode[]>>({})
const loadingNets = ref(new Set<string>())
const expanded = ref(new Set<string>())
/* per network: its services by mux uuid (loaded when needed) */
const services = ref<Record<string, Map<string, ServiceRow[]>>>({})
const loadingServices = ref(new Set<string>())
const onlyWithSubMuxes = ref(false)
const error = ref('')

async function loadNetworks() {
  error.value = ''
  try {
    const grid = await apiCall<GridResponse<Record<string, unknown>>>('mpegts/network/grid', {
      start: 0,
      limit: 10000,
    })
    const rows = grid.entries ?? []
    const uuids = rows.map((r) => String(r.uuid))
    const classes = new Map<string, string>()
    if (uuids.length) {
      const nodes = await apiCall<GridResponse<{ uuid: string; class: string }>>('idnode/load', {
        uuid: uuids,
        list: 'networkname',
      })
      for (const n of nodes.entries ?? []) classes.set(n.uuid, n.class)
    }
    networks.value = rows
      .filter((r) => !(classes.get(String(r.uuid)) ?? '').startsWith('iptv'))
      .map((r) => ({
        uuid: String(r.uuid),
        name: String(r.networkname ?? r.uuid),
        numMux: Number(r.num_mux ?? 0),
        numSvc: Number(r.num_svc ?? 0),
        numChn: Number(r.num_chn ?? 0),
      }))
      .sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true }))
  } catch (e) {
    error.value = String(e)
  }
}

async function loadMuxes(net: NetworkEntry, expandNew: boolean) {
  const loading = new Set(loadingNets.value)
  loading.add(net.uuid)
  loadingNets.value = loading
  try {
    const grid = await apiCall<GridResponse<MuxRow>>('mpegts/mux/grid', {
      start: 0,
      limit: 100000,
      hidemode: 'none',
      sort: 'name',
      dir: 'ASC',
      filter: [{ type: 'string', field: 'network', value: net.name }],
    })
    const rows = (grid.entries ?? []).filter((r) => r.network_uuid === net.uuid)
    if (rows.some((r) => (r.carrier_sid ?? 0) > 0) && !services.value[net.uuid]) await loadServices(net)
    const tree = buildMuxTree(rows)
    trees.value = { ...trees.value, [net.uuid]: tree }
    if (expandNew) {
      const next = new Set(expanded.value)
      for (const k of expandableKeys(tree)) next.add(k)
      expanded.value = next
    }
  } catch (e) {
    error.value = String(e)
  } finally {
    const done = new Set(loadingNets.value)
    done.delete(net.uuid)
    loadingNets.value = done
  }
}

/* the services of a network, by mux */
async function loadServices(net: NetworkEntry) {
  const loading = new Set(loadingServices.value)
  loading.add(net.uuid)
  loadingServices.value = loading
  try {
    const grid = await apiCall<GridResponse<ServiceRow>>('mpegts/service/grid', {
      start: 0,
      limit: 100000,
      hidemode: 'none',
      filter: [{ type: 'string', field: 'network', value: net.name }],
    })
    services.value = { ...services.value, [net.uuid]: servicesByMux(grid.entries ?? []) }
  } catch (e) {
    error.value = String(e)
  } finally {
    const done = new Set(loadingServices.value)
    done.delete(net.uuid)
    loadingServices.value = done
  }
}

/* once loaded, the services follow scans and edits */
function refreshServices(net: NetworkEntry) {
  if (services.value[net.uuid]) void loadServices(net)
}

/* the service a sub-mux comes from (T2MI ...): name, encrypted */
function carrierOf(row: MuxRow): CarrierInfo | null {
  if (!row.carrier_sid || !row.parent_mux || !row.network_uuid) return null
  const s = services.value[row.network_uuid]?.get(row.parent_mux)?.find((x) => x.sid === row.carrier_sid)
  return s ? { name: s.svcname ?? '', encrypted: s.encrypted === true, caid: caidList(s.caid) } : null
}

function serviceTitle(s: ServiceRow): string {
  const parts = [`SID ${s.sid}`]
  if (s.encrypted) parts.push(t('encrypted: {0}', s.caid ?? ''))
  return parts.join(' — ')
}

/* the Address column: ip:port, or the carrier service */
function sourceLabel(row: MuxRow): string {
  const addr = addressLabel(row)
  if (addr) return addr
  if (!row.carrier_sid) return ''
  const c = carrierOf(row)
  return c?.name ? `SID ${row.carrier_sid} · ${c.name}` : `SID ${row.carrier_sid}`
}

function carrierTitle(row: MuxRow): string {
  const c = carrierOf(row)
  if (!c) return sourceLabel(row)
  return c.encrypted
    ? t('Carrier service {0} (SID {1}), encrypted: {2}', c.name, row.carrier_sid, c.caid)
    : t('Carrier service {0} (SID {1})', c.name, row.carrier_sid)
}

function setOpen(key: string, open: boolean) {
  const next = new Set(expanded.value)
  if (open) next.add(key)
  else next.delete(key)
  expanded.value = next
}

async function toggleNetwork(net: NetworkEntry) {
  const open = !expanded.value.has(net.uuid)
  setOpen(net.uuid, open)
  if (open && !trees.value[net.uuid]) await loadMuxes(net, true)
}

function toggle(row: FlatRow) {
  if (row.network) {
    void toggleNetwork(row.network)
    return
  }
  setOpen(row.key, !row.open)
  /* opening shows services: a "Services (n)" row, a mux without sub-muxes */
  const showsServices = !!row.svcGroup || (!!row.node?.row && !row.node.children.length)
  if (showsServices && !row.open && row.net && !services.value[row.net.uuid]) void loadServices(row.net)
}

async function expandAll() {
  const next = new Set(expanded.value)
  for (const n of networks.value) if (n.numMux > 0) next.add(n.uuid)
  expanded.value = next
  await Promise.all(
    networks.value.filter((n) => n.numMux > 0 && !trees.value[n.uuid]).map((n) => loadMuxes(n, false)),
  )
  const all = new Set(expanded.value)
  for (const tree of Object.values(trees.value)) for (const k of expandableKeys(tree)) all.add(k)
  expanded.value = all
}

function collapseAll() {
  expanded.value = new Set()
}

async function reload() {
  await loadNetworks()
  const loaded = networks.value.filter((n) => trees.value[n.uuid])
  trees.value = {}
  services.value = {}
  await Promise.all(loaded.map((n) => loadMuxes(n, false)))
}

/* the top nodes shown: with "only with sub-muxes" the transponders
 * that carry some, and the sub-muxes without their transponder */
function topNodes(tree: MuxTreeNode[]): MuxTreeNode[] {
  if (!onlyWithSubMuxes.value) return tree
  return tree.filter((n) => n.children.length > 0 || n.orphan)
}

/* Filter: text (muxes, optionally service names), state */
const filterText = ref('')
const filterQuery = ref('')
let filterTimer: number | undefined
watch(filterText, (v) => {
  window.clearTimeout(filterTimer)
  filterTimer = window.setTimeout(() => (filterQuery.value = v.trim().toLowerCase()), 250)
})
const stateFilter = ref<'all' | 'failed' | 'disabled'>('all')
const searchServices = ref(false)
const filterOn = computed(() => filterQuery.value !== '' || stateFilter.value !== 'all')

function muxMatches(n: MuxTreeNode): boolean {
  if (filterQuery.value && !muxSearchText(n).includes(filterQuery.value)) return false
  if (stateFilter.value === 'all') return true
  if (!n.row) return false
  return stateFilter.value === 'failed' ? n.row.scan_result === 2 : n.row.enabled !== 1
}

function serviceMatches(svc: ServiceRow): boolean {
  return !!filterQuery.value && (svc.svcname ?? '').toLowerCase().includes(filterQuery.value)
}

const filterResults = computed<Record<string, TreeFilterResult>>(() => {
  const out: Record<string, TreeFilterResult> = {}
  if (!filterOn.value) return out
  for (const net of networks.value) {
    const svcs = searchServices.value && filterQuery.value ? services.value[net.uuid] : undefined
    out[net.uuid] = filterTree(topNodes(trees.value[net.uuid] ?? []), muxMatches, (n) =>
      !!svcs && !!n.row && (svcs.get(n.row.uuid) ?? []).some(serviceMatches),
    )
  }
  return out
})

/* the filter needs all networks; the service search their services */
watch(filterOn, (on) => {
  if (!on) return
  for (const n of networks.value) if (n.numMux > 0 && !trees.value[n.uuid]) void loadMuxes(n, false)
})
watch([searchServices, filterQuery], ([on, q]) => {
  if (!on || !q) return
  for (const n of networks.value) if (n.numMux > 0 && !services.value[n.uuid]) void loadServices(n)
})

const visible = computed<FlatRow[]>(() => {
  const out: FlatRow[] = []
  /* the services of a mux (loading: a row that says so); a filter on
   * service names shows only the ones it finds */
  const serviceList = (net: NetworkEntry, row: MuxRow, depth: number, only?: (s: ServiceRow) => boolean) => {
    const list = services.value[net.uuid]
    if (!list) {
      out.push({ key: `${row.uuid}:services:loading`, depth, hasChildren: false, open: false, loading: true })
      return
    }
    for (const svc of list.get(row.uuid) ?? [])
      if (!only || only(svc)) out.push({ key: svc.uuid, depth, service: svc, net, hasChildren: false, open: false })
  }
  /* a mux with sub-muxes: its services in a "Services (n)" row */
  const serviceGroup = (net: NetworkEntry, row: MuxRow, depth: number, forced: boolean, only?: (s: ServiceRow) => boolean) => {
    const key = `${row.uuid}:services`
    const open = forced || expanded.value.has(key)
    out.push({ key, depth, svcGroup: { mux: row, count: row.num_svc ?? 0 }, net, hasChildren: true, open })
    if (open) serviceList(net, row, depth + 1, only)
  }
  const walk = (net: NetworkEntry, nodes: MuxTreeNode[], depth: number, f: TreeFilterResult | null, inMatch: boolean) => {
    for (const node of nodes) {
      if (f && !inMatch && !f.show.has(node.key)) continue
      const forced = !!f && !inMatch && f.open.has(node.key)
      const open = forced || expanded.value.has(node.key)
      const wide = depth === 1 && !!node.row && !node.orphan && (node.row.tstype ?? 0) === 0
      const svc = (node.row?.num_svc ?? 0) > 0
      out.push({ key: node.key, depth, node, net, hasChildren: node.children.length > 0 || svc, open, wide })
      if (!open) continue
      const self = !!f && f.self.has(node.key)
      /* opened for a service match: only the services found */
      const only = f && !inMatch && !self && searchServices.value ? serviceMatches : undefined
      if (svc && node.row) {
        const hit = !!only && (services.value[net.uuid]?.get(node.row.uuid) ?? []).some(serviceMatches)
        if (!only || hit) {
          if (node.children.length) serviceGroup(net, node.row, depth + 1, hit, only)
          else serviceList(net, node.row, depth + 1, only)
        }
      }
      walk(net, node.children, depth + 1, f, inMatch || self)
    }
  }
  for (const net of networks.value) {
    const f = filterOn.value ? filterResults.value[net.uuid] : null
    /* a filter shows the networks with a match (and those still loading) */
    if (f && (net.numMux === 0 || (trees.value[net.uuid] && f.show.size === 0))) continue
    const open = !!f || expanded.value.has(net.uuid)
    out.push({ key: net.uuid, depth: 0, network: net, hasChildren: net.numMux > 0, open, wide: true })
    if (!open) continue
    if (loadingNets.value.has(net.uuid) && !trees.value[net.uuid])
      out.push({ key: `${net.uuid}:loading`, depth: 1, hasChildren: false, open: false, loading: true })
    walk(net, topNodes(trees.value[net.uuid] ?? []), 1, f, false)
  }
  return out
})

const muxTotal = computed(() =>
  Object.values(trees.value).reduce((n, tree) => n + countMuxes(tree), 0),
)

/* Selection: muxes and services, by uuid */
type SelKind = 'mux' | 'service'
interface SelEntry {
  kind: SelKind
  net: NetworkEntry
}
const selected = ref(new Set<string>())
let anchor = -1
/* the network of the last network row clicked: where Add puts a mux */
const focusNet = ref<NetworkEntry | null>(null)

/* every loaded mux and service: its kind and network */
const index = computed(() => {
  const map = new Map<string, SelEntry>()
  for (const net of networks.value) {
    const add = (nodes: MuxTreeNode[]) => {
      for (const n of nodes) {
        if (n.row) map.set(n.row.uuid, { kind: 'mux', net })
        add(n.children)
      }
    }
    add(trees.value[net.uuid] ?? [])
    for (const list of services.value[net.uuid]?.values() ?? [])
      for (const svc of list) map.set(svc.uuid, { kind: 'service', net })
  }
  return map
})

/* the selection that still exists (a reload may have removed some) */
const selection = computed(() =>
  [...selected.value].filter((u) => index.value.has(u)).map((u) => ({ uuid: u, ...(index.value.get(u) as SelEntry) })),
)

function selectableUuid(r: FlatRow): string | null {
  return r.service?.uuid ?? r.node?.row?.uuid ?? null
}

function setSelected(next: Set<string>) {
  selected.value = next
}

function onRowClick(r: FlatRow, ev: MouseEvent, i: number) {
  if (r.network) {
    focusNet.value = r.network
    return
  }
  const uuid = selectableUuid(r)
  if (!uuid) return
  focusNet.value = r.net ?? null
  if (ev.shiftKey && anchor >= 0) {
    const [a, b] = anchor < i ? [anchor, i] : [i, anchor]
    const next = new Set(ev.ctrlKey || ev.metaKey ? selected.value : [])
    for (const row of visible.value.slice(a, b + 1)) {
      const u = selectableUuid(row)
      if (u) next.add(u)
    }
    setSelected(next)
    return
  }
  anchor = i
  if (ev.ctrlKey || ev.metaKey) toggleSelected(uuid)
  else setSelected(new Set([uuid]))
}

function toggleSelected(uuid: string) {
  const next = new Set(selected.value)
  if (next.has(uuid)) next.delete(uuid)
  else next.add(uuid)
  setSelected(next)
}

const visibleSelectable = computed(() => visible.value.map(selectableUuid).filter((u): u is string => !!u))
const allSelected = computed(
  () => visibleSelectable.value.length > 0 && visibleSelectable.value.every((u) => selected.value.has(u)),
)

function toggleAll() {
  setSelected(allSelected.value ? new Set() : new Set(visibleSelectable.value))
}

/* Editor: edit (one or several of a kind), create (a mux in a network) */
const editingUuid = ref<string | null>(null)
const editingUuids = ref<string[] | null>(null)
const editingTitle = ref('')
const creatingNet = ref<NetworkEntry | null>(null)
let editingNets: NetworkEntry[] = []

const createScope = computed(() =>
  creatingNet.value
    ? {
        classEndpoint: 'mpegts/network/mux_class',
        createEndpoint: 'mpegts/network/mux_create',
        params: { uuid: creatingNet.value.uuid },
      }
    : undefined,
)

function networkOf(row: MuxRow): NetworkEntry | null {
  return networks.value.find((n) => n.uuid === row.network_uuid) ?? null
}

function onRowDblclick(row: FlatRow) {
  if (row.service) {
    openEdit([row.service.uuid], 'service', row.net ? [row.net] : [])
  } else if (row.network) {
    editingUuid.value = row.network.uuid
    editingTitle.value = t('Edit Network')
    editingNets = []
  } else if (row.node?.row) {
    const net = networkOf(row.node.row)
    openEdit([row.node.row.uuid], 'mux', net ? [net] : [])
  }
}

function openEdit(uuids: string[], kind: SelKind, nets: NetworkEntry[]) {
  editingNets = nets
  if (uuids.length === 1) {
    editingUuid.value = uuids[0]
    editingTitle.value = kind === 'mux' ? t('Edit Mux') : t('Edit Service')
  } else {
    editingUuids.value = uuids
    editingTitle.value = kind === 'mux' ? t('Edit {0} muxes', uuids.length) : t('Edit {0} services', uuids.length)
  }
}

/* edit: one kind only (a mux and a service have different fields) */
const canEdit = computed(() => {
  const kinds = new Set(selection.value.map((x) => x.kind))
  return kinds.size === 1
})

function onEdit() {
  if (!canEdit.value) return
  const sel = selection.value
  openEdit(
    sel.map((x) => x.uuid),
    sel[0].kind,
    [...new Set(sel.map((x) => x.net))],
  )
}

/* add: to the network of the selection (or the network row clicked),
 * else to one picked */
const pickerVisible = ref(false)

function onAdd() {
  const net = focusNet.value ?? selection.value[0]?.net ?? null
  if (net) openCreate(net)
  else pickerVisible.value = true
}

function onNetworkPicked(uuid: string, name: string) {
  pickerVisible.value = false
  openCreate(networks.value.find((n) => n.uuid === uuid) ?? { uuid, name, numMux: 0, numSvc: 0, numChn: 0 })
}

function openCreate(net: NetworkEntry) {
  creatingNet.value = net
  editingTitle.value = t('Add Mux to {0}', net.name)
  editingNets = [net]
}

/* the new mux is saved: the editor goes on with it */
function onCreated(uuid: string) {
  creatingNet.value = null
  editingUuid.value = uuid
  editingTitle.value = t('Edit Mux')
}

function onEditorClose() {
  editingUuid.value = null
  editingUuids.value = null
  creatingNet.value = null
  if (editingNets.length) {
    for (const net of editingNets) {
      void loadMuxes(net, false)
      refreshServices(net)
    }
  } else void loadNetworks()
  editingNets = []
}

async function onDelete() {
  const sel = selection.value
  if (!sel.length) return
  const muxes = sel.filter((x) => x.kind === 'mux').length
  const svcs = sel.length - muxes
  const what = [muxes ? t('{0} mux(es)', muxes) : '', svcs ? t('{0} service(s)', svcs) : '']
    .filter(Boolean)
    .join(t(' and '))
  const ok = await confirmDialog.ask(t('Do you really want to delete {0}?', what), { severity: 'danger' })
  if (!ok) return
  try {
    await apiCall('idnode/delete', { uuid: JSON.stringify(sel.map((x) => x.uuid)) })
  } catch (e) {
    error.value = String(e)
  }
  setSelected(new Set())
  for (const net of new Set(sel.map((x) => x.net))) {
    void loadMuxes(net, false)
    refreshServices(net)
  }
}

/* scan: the muxes of the selection */
const selectedMuxes = computed(() => selection.value.filter((x) => x.kind === 'mux'))

async function onScanSelection() {
  const byNet = new Map<NetworkEntry, string[]>()
  for (const x of selectedMuxes.value) byNet.set(x.net, [...(byNet.get(x.net) ?? []), x.uuid])
  for (const [net, uuids] of byNet) await scanUuids(net, uuids)
}

/* Scans started here: their muxes per network, and the reload timers */
const SCAN_POLL_MS = 2000
const SCAN_POLL_MAX = 300 /* 10 min */
const scanning = new Map<string, Set<string>>()
const scanPolls = new Map<string, number>()

function pollScans(net: NetworkEntry) {
  if (scanPolls.has(net.uuid)) return
  let rounds = 0
  const id = window.setInterval(async () => {
    await loadMuxes(net, false)
    const uuids = scanning.get(net.uuid) ?? new Set<string>()
    const busy = (nodes: MuxTreeNode[]): boolean =>
      nodes.some(
        (n) =>
          (n.row !== null && uuids.has(n.row.uuid) && (n.row.scan_state ?? SCAN_STATE_IDLE) !== SCAN_STATE_IDLE) ||
          busy(n.children),
      )
    if (++rounds >= SCAN_POLL_MAX || !busy(trees.value[net.uuid] ?? [])) {
      window.clearInterval(id)
      scanPolls.delete(net.uuid)
      scanning.delete(net.uuid)
      refreshServices(net)
    }
  }, SCAN_POLL_MS)
  scanPolls.set(net.uuid, id)
}

async function scanRow(r: FlatRow) {
  const node = r.node
  if (!node) return
  const first = node.row ?? scanTargetsRow(node)
  const net = first ? networkOf(first) : null
  if (!net) return
  await scanUuids(net, scanTargets(node))
}

async function scanUuids(net: NetworkEntry, uuids: string[]) {
  try {
    for (const uuid of uuids)
      await apiCall('idnode/save', { node: JSON.stringify({ uuid, scan_state: 1 }) })
  } catch (e) {
    error.value = String(e)
    return
  }
  const set = scanning.get(net.uuid) ?? new Set<string>()
  for (const u of uuids) set.add(u)
  scanning.set(net.uuid, set)
  await loadMuxes(net, false)
  pollScans(net)
}

/* a mux below a made-up node: for its network */
function scanTargetsRow(node: MuxTreeNode): MuxRow | null {
  for (const c of node.children) {
    if (c.row) return c.row
    const r = scanTargetsRow(c)
    if (r) return r
  }
  return null
}

function scanStateLabel(row: MuxRow): string {
  const st = row.scan_state ?? SCAN_STATE_IDLE
  if (st === SCAN_STATE_IDLE) return ''
  return st === SCAN_STATE_ACTIVE ? t('scanning…') : t('queued')
}

onBeforeUnmount(() => {
  for (const id of scanPolls.values()) window.clearInterval(id)
  scanPolls.clear()
})

/* Live updates: the networks whose muxes or services changed reload */
const pendingMux = new Set<string>()
const pendingSvc = new Set<string>()
let pendingMuxCreate = false
let pendingSvcCreate = false
let pendingNetworks = false
let cometTimer: number | undefined

function onNotification(kind: 'mux' | 'service' | 'network', msg: IdnodeNotification) {
  const uuids = [...(msg.change ?? []), ...(msg.delete ?? [])]
  const created = (msg.create ?? []).length > 0
  if (!uuids.length && !created) return
  if (kind === 'network') pendingNetworks = true
  else if (kind === 'mux') {
    for (const u of uuids) pendingMux.add(u)
    if (created) pendingMuxCreate = true
  } else {
    for (const u of uuids) pendingSvc.add(u)
    if (created) pendingSvcCreate = true
  }
  if (cometTimer === undefined) cometTimer = window.setTimeout(applyNotifications, 1000)
}

async function applyNotifications() {
  cometTimer = undefined
  const muxNets = new Set<NetworkEntry>()
  const svcNets = new Set<NetworkEntry>()
  for (const u of pendingMux) {
    const e = index.value.get(u)
    if (e?.kind === 'mux') muxNets.add(e.net)
  }
  for (const u of pendingSvc) {
    const e = index.value.get(u)
    if (e?.kind === 'service') svcNets.add(e.net)
  }
  /* a new one: its network is not known here, the loaded ones reload */
  if (pendingMuxCreate) for (const n of networks.value) if (trees.value[n.uuid]) muxNets.add(n)
  if (pendingSvcCreate) for (const n of networks.value) if (services.value[n.uuid]) svcNets.add(n)
  const nets = pendingNetworks
  pendingMux.clear()
  pendingSvc.clear()
  pendingMuxCreate = pendingSvcCreate = pendingNetworks = false
  if (nets) await loadNetworks()
  for (const n of muxNets) void loadMuxes(n, false)
  for (const n of svcNets) void loadServices(n)
}

const cometOff = [
  cometClient.on('mpegts_mux', (m) => onNotification('mux', m as IdnodeNotification)),
  cometClient.on('service', (m) => onNotification('service', m as IdnodeNotification)),
  cometClient.on('mpegts_network', (m) => onNotification('network', m as IdnodeNotification)),
]
onBeforeUnmount(() => {
  for (const off of cometOff) off()
  window.clearTimeout(cometTimer)
})
/* after a long Comet gap (tab asleep, server restarted): all again */
useStaleDataRecovery({ refetch: () => void reload() })

/* In the rows: enable, PID, address */
async function saveFields(uuid: string, fields: Record<string, unknown>) {
  try {
    await apiCall('idnode/save', { node: JSON.stringify({ uuid, ...fields }) })
  } catch (e) {
    error.value = String(e)
  }
}

/* "On": a mux 1/0 (-1 = ignore: half set, a click enables it), a service */
function enabledState(r: FlatRow): 'on' | 'off' | 'ignore' | null {
  if (r.service) return r.service.enabled === false ? 'off' : 'on'
  const row = r.node?.row
  if (!row) return null
  return row.enabled === 1 ? 'on' : row.enabled === -1 ? 'ignore' : 'off'
}

function toggleEnabled(r: FlatRow) {
  if (r.service) {
    const v = r.service.enabled === false
    r.service.enabled = v
    void saveFields(r.service.uuid, { enabled: v })
  } else if (r.node?.row) {
    const v = r.node.row.enabled === 1 ? 0 : 1
    r.node.row.enabled = v
    void saveFields(r.node.row.uuid, { enabled: v })
  }
}

/* the sub-muxes in a PID (T2MI, DAB in a PID), DAB over IP (address) */
function pidEditable(row: MuxRow): boolean {
  return (row.tstype === 1 || row.tstype === 2 || row.tstype === 3 || row.tstype === 6) && !!row.parent_mux
}

function addressEditable(row: MuxRow): boolean {
  return (row.tstype === 2 || row.tstype === 4) && !!row.parent_mux
}

const cellEdit = ref<{ uuid: string; field: 'pid' | 'address'; value: string } | null>(null)

function startCellEdit(row: MuxRow, field: 'pid' | 'address') {
  cellEdit.value = { uuid: row.uuid, field, value: field === 'pid' ? String(row.pid ?? '') : addressLabel(row) }
}

function cellEditable(row: MuxRow, field: 'pid' | 'address'): boolean {
  return field === 'pid' ? pidEditable(row) : addressEditable(row)
}

/* a click in an editable cell edits it (no row selection) */
function onEditableCell(ev: MouseEvent, row: MuxRow, field: 'pid' | 'address') {
  if (!cellEditable(row, field)) return
  ev.stopPropagation()
  startCellEdit(row, field)
}

/* ... and its double click does not open the editor */
function onEditableDblclick(ev: MouseEvent, row: MuxRow, field: 'pid' | 'address') {
  if (cellEditable(row, field)) ev.stopPropagation()
}

function cancelCellEdit() {
  cellEdit.value = null
}

async function commitCellEdit(row: MuxRow) {
  const e = cellEdit.value
  if (!e || e.uuid !== row.uuid) return
  cellEdit.value = null
  const v = e.value.trim()
  if (e.field === 'pid') {
    const pid = Number(v.startsWith('0x') ? parseInt(v, 16) : v)
    if (!Number.isInteger(pid) || pid <= 0 || pid >= 0x1fff || pid === row.pid) return
    row.pid = pid
    await saveFields(row.uuid, { pid })
  } else {
    const m = /^(\d{1,3}(?:\.\d{1,3}){3})(?::(\d{1,5}))?$/.exec(v)
    if (!m || v === addressLabel(row)) return
    const port = m[2] ? Number(m[2]) : 0
    row.dab_ip = m[1]
    row.dab_port = port
    await saveFields(row.uuid, { dab_ip: m[1], dab_port: port })
  }
}

function enabledLabel(row: MuxRow): string {
  if (row.enabled === 0) return t('disabled')
  if (row.enabled === -1) return t('ignored')
  return ''
}

/* the full name, the provider network and the tuning */
function nameTitle(r: FlatRow): string | undefined {
  if (r.network) return r.network.name
  const row = r.node?.row
  if (!row) return undefined
  return [row.name, r.wide ? tuningLabel(row) : '', row.pnetwork_name].filter(Boolean).join(' — ')
}

/* Services and Channels: a mux with sub-muxes counts all levels below */
function countLabel(node: MuxTreeNode, what: 'svc' | 'chn'): string {
  if (!node.children.length) return String((what === 'svc' ? node.row?.num_svc : node.row?.num_chn) ?? '')
  return String(subtreeCounts(node)[what])
}

function countTitle(node: MuxTreeNode, what: 'svc' | 'chn'): string | undefined {
  if (!node.children.length) return undefined
  const own = (what === 'svc' ? node.row?.num_svc : node.row?.num_chn) ?? 0
  const all = subtreeCounts(node)[what]
  return t('{0} own + {1} in sub-muxes', own, all - own)
}

function scanSummary(node: MuxTreeNode): string {
  const s = subMuxScan(node)
  return `${s.ok}/${s.total}`
}

function pidLabel(row: MuxRow): string {
  return typeof row.pid === 'number' && row.pid > 0 ? String(row.pid) : ''
}

function indent(depth: number): Record<string, string> {
  return { paddingLeft: `calc(${depth} * 1.25rem + var(--tvh-space-2))` }
}

/* the cell input gets the focus when it appears */
const vFocus = { mounted: (el: HTMLElement) => el.focus() }

onMounted(loadNetworks)
</script>

<template>
  <section class="mux-tree">
    <div class="mux-tree__toolbar">
      <Button severity="secondary" outlined size="small" :title="t('Add a mux to the network of the selection')" @click="onAdd">
        <Plus :size="14" :stroke-width="2" />
        {{ t('Add') }}
      </Button>
      <Button severity="secondary" outlined size="small" :disabled="!canEdit" @click="onEdit">
        <Pencil :size="14" :stroke-width="2" />
        {{ t('Edit') }}
      </Button>
      <Button severity="secondary" outlined size="small" :disabled="!selection.length" @click="onDelete">
        <Trash2 :size="14" :stroke-width="2" />
        {{ t('Delete') }}
      </Button>
      <Button severity="secondary" outlined size="small" :disabled="!selectedMuxes.length" @click="onScanSelection">
        <ScanSearch :size="14" :stroke-width="2" />
        {{ t('Scan') }}
      </Button>
      <span class="mux-tree__sep" />
      <Button severity="secondary" outlined size="small" @click="expandAll">
        {{ t('Expand all') }}
      </Button>
      <Button severity="secondary" outlined size="small" @click="collapseAll">
        {{ t('Collapse all') }}
      </Button>
      <Button severity="secondary" outlined size="small" @click="reload">
        <RefreshCw :size="14" :stroke-width="2" />
        {{ t('Reload') }}
      </Button>
      <span class="mux-tree__count">
        <template v-if="selection.length">{{ t('{0} selected', selection.length) }} · </template>
        {{ t('{0} networks, {1} muxes loaded', networks.length, muxTotal) }}
      </span>
    </div>
    <div class="mux-tree__toolbar mux-tree__filters">
      <SearchInput
        v-model="filterText"
        class="mux-tree__search"
        :placeholder="t('Filter: name, type, provider, address, tuning')"
        :aria-label="t('Filter')"
      />
      <label class="mux-tree__check">
        <input v-model="searchServices" type="checkbox" />
        {{ t('Also service names') }}
      </label>
      <select v-model="stateFilter" class="mux-tree__select" :aria-label="t('State')">
        <option value="all">{{ t('All muxes') }}</option>
        <option value="failed">{{ t('Scan failed') }}</option>
        <option value="disabled">{{ t('Disabled') }}</option>
      </select>
      <label class="mux-tree__check">
        <input v-model="onlyWithSubMuxes" type="checkbox" />
        {{ t('Only transponders with sub-muxes') }}
      </label>
    </div>
    <p v-if="error" class="mux-tree__error" role="alert">{{ error }}</p>
    <div class="mux-tree__scroll">
      <table class="mux-tree__table">
        <colgroup>
          <col class="mux-tree__col-select" />
          <col />
          <col class="mux-tree__col-type" />
          <col class="mux-tree__col-pid" />
          <col class="mux-tree__col-address" />
          <col class="mux-tree__col-modcod" />
          <col class="mux-tree__col-scan" />
          <col class="mux-tree__col-count" />
          <col class="mux-tree__col-count" />
          <col class="mux-tree__col-on" />
          <col class="mux-tree__col-action" />
        </colgroup>
        <thead>
          <tr>
            <th class="mux-tree__select-cell">
              <input
                type="checkbox"
                :checked="allSelected"
                :aria-label="t('Select all shown')"
                @change="toggleAll"
              />
            </th>
            <th class="mux-tree__name">{{ t('Name') }}</th>
            <th>{{ t('Type') }}</th>
            <th class="mux-tree__num">{{ t('PID') }}</th>
            <th>{{ t('Address') }}</th>
            <th>{{ t('MODCOD') }}</th>
            <th>{{ t('Scan result') }}</th>
            <th class="mux-tree__num">{{ t('Services') }}</th>
            <th class="mux-tree__num">{{ t('Channels') }}</th>
            <th class="mux-tree__select-cell" :title="t('Enabled')">{{ t('On') }}</th>
            <th><span class="mux-tree__sr-only">{{ t('Scan') }}</span></th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="(r, i) in visible"
            :key="r.key"
            :class="{
              'mux-tree__row--selected': !!selectableUuid(r) && selected.has(selectableUuid(r) as string),
              'mux-tree__row--network': !!r.network,
              'mux-tree__row--virtual': !!r.node?.virtual,
              'mux-tree__row--off':
                (!!r.node?.row && r.node.row.enabled !== 1) || r.service?.enabled === false,
              'mux-tree__row--group': !!r.svcGroup,
            }"
            @click="onRowClick(r, $event, i)"
            @dblclick="onRowDblclick(r)"
          >
            <td class="mux-tree__select-cell">
              <input
                v-if="selectableUuid(r)"
                type="checkbox"
                :checked="selected.has(selectableUuid(r) as string)"
                :aria-label="t('Select')"
                @click.stop="toggleSelected(selectableUuid(r) as string)"
                @dblclick.stop
              />
            </td>
            <!-- network and transponder rows: the name uses type, PID, address -->
            <td
              class="mux-tree__name"
              :colspan="r.wide ? 4 : undefined"
              :style="indent(r.depth)"
              :title="nameTitle(r)"
            >
              <button
                v-if="r.hasChildren"
                type="button"
                class="mux-tree__toggle"
                :aria-expanded="r.open"
                :aria-label="r.open ? t('Collapse') : t('Expand')"
                @click.stop="toggle(r)"
                @dblclick.stop
              >
                <ChevronDown v-if="r.open" :size="14" :stroke-width="2" />
                <ChevronRight v-else :size="14" :stroke-width="2" />
              </button>
              <span v-else class="mux-tree__toggle-space" />
              <template v-if="r.network">
                {{ r.network.name }}
                <span class="mux-tree__muted">({{ r.network.numMux }})</span>
              </template>
              <template v-else-if="r.loading">
                <span class="mux-tree__muted">{{ t('Loading…') }}</span>
              </template>
              <template v-else-if="r.svcGroup">
                {{ t('Services') }}
                <span class="mux-tree__muted">({{ r.svcGroup.count }})</span>
              </template>
              <template v-else-if="r.service">
                <template v-if="r.service.svcname">{{ r.service.svcname }}</template>
                <span v-else class="mux-tree__muted">–</span>
                <span v-if="r.service.enabled === false" class="mux-tree__badge">{{ t('disabled') }}</span>
                <span v-if="r.service.provider" class="mux-tree__provider">{{ r.service.provider }}</span>
              </template>
              <template v-else-if="r.node">
                <template v-if="r.node.label">{{ r.node.label }}</template>
                <span v-else class="mux-tree__muted">–</span>
                <span v-if="r.node.row?.multistream" class="mux-tree__badge">{{ t('multistream') }}</span>
                <span v-if="r.node.virtual" class="mux-tree__badge" :title="t('No mux of its own: the sub-muxes below name the transponder and this ISI')">
                  {{ t('input stream') }}
                </span>
                <span v-if="r.node.orphan" class="mux-tree__badge mux-tree__badge--warn">{{ t('transponder missing') }}</span>
                <span v-if="r.node.row && enabledLabel(r.node.row)" class="mux-tree__badge">{{ enabledLabel(r.node.row) }}</span>
                <span v-if="r.wide && r.node.row" class="mux-tree__tuning">{{ tuningLabel(r.node.row) }}</span>
                <!-- after the tuning of a transponder, after the name of a sub-mux -->
                <span v-if="r.node.row?.pnetwork_name && !r.node.providerLabel" class="mux-tree__provider">
                  {{ r.node.row.pnetwork_name }}
                </span>
              </template>
            </td>
            <template v-if="r.node?.row">
              <template v-if="!r.wide">
                <td>{{ muxTypeName(r.node.row.tstype) }}</td>
                <td
                  class="mux-tree__num"
                  :class="{ 'mux-tree__editable': pidEditable(r.node.row) }"
                  @click="onEditableCell($event, r.node.row, 'pid')"
                  @dblclick="onEditableDblclick($event, r.node.row, 'pid')"
                >
                  <input
                    v-if="cellEdit?.uuid === r.node.row.uuid && cellEdit.field === 'pid'"
                    v-model="cellEdit.value"
                    v-focus
                    class="mux-tree__cell-input mux-tree__num"
                    :aria-label="t('PID')"
                    @keydown.enter.prevent="commitCellEdit(r.node.row)"
                    @keydown.esc.prevent="cancelCellEdit"
                    @blur="commitCellEdit(r.node.row)"
                    @click.stop
                  />
                  <template v-else>{{ pidLabel(r.node.row) }}</template>
                </td>
                <td
                  class="mux-tree__address"
                  :class="{ 'mux-tree__editable': addressEditable(r.node.row) }"
                  :title="carrierTitle(r.node.row)"
                  @click="onEditableCell($event, r.node.row, 'address')"
                  @dblclick="onEditableDblclick($event, r.node.row, 'address')"
                >
                  <input
                    v-if="cellEdit?.uuid === r.node.row.uuid && cellEdit.field === 'address'"
                    v-model="cellEdit.value"
                    v-focus
                    class="mux-tree__cell-input"
                    :aria-label="t('Address')"
                    placeholder="239.1.2.3:1234"
                    @keydown.enter.prevent="commitCellEdit(r.node.row)"
                    @keydown.esc.prevent="cancelCellEdit"
                    @blur="commitCellEdit(r.node.row)"
                    @click.stop
                  />
                  <template v-else>
                    <Lock
                      v-if="carrierOf(r.node.row)?.encrypted"
                      class="mux-tree__lock"
                      :size="12"
                      :stroke-width="2"
                    />
                    {{ sourceLabel(r.node.row) }}
                  </template>
                </td>
              </template>
              <td>{{ r.node.row.modcod ?? '' }}</td>
              <td>
                <span v-if="scanStateLabel(r.node.row)" class="mux-tree__busy">
                  {{ scanStateLabel(r.node.row) }}
                </span>
                <template v-else>{{ scanResultName(r.node.row.scan_result) }}</template>
                <span v-if="r.node.children.length" class="mux-tree__muted" :title="t('Sub-muxes scanned with success')">
                  · {{ scanSummary(r.node) }}
                </span>
              </td>
              <td class="mux-tree__num" :title="countTitle(r.node, 'svc')">{{ countLabel(r.node, 'svc') }}</td>
              <td class="mux-tree__num" :title="countTitle(r.node, 'chn')">{{ countLabel(r.node, 'chn') }}</td>
            </template>
            <template v-else-if="r.node?.virtual">
              <td class="mux-tree__muted">{{ t('input stream') }}</td>
              <td colspan="3" />
              <td class="mux-tree__muted" :title="t('Sub-muxes scanned with success')">
                {{ scanSummary(r.node) }}
              </td>
              <td class="mux-tree__num mux-tree__muted">{{ countLabel(r.node, 'svc') }}</td>
              <td class="mux-tree__num mux-tree__muted">{{ countLabel(r.node, 'chn') }}</td>
            </template>
            <template v-else-if="r.service">
              <td>{{ serviceTypeName(r.service.dvb_servicetype) }}</td>
              <td />
              <td class="mux-tree__address" :title="serviceTitle(r.service)">
                <Lock v-if="r.service.encrypted" class="mux-tree__lock" :size="12" :stroke-width="2" />
                SID {{ r.service.sid }}<template v-if="r.service.encrypted"> · {{ caidList(r.service.caid) }}</template>
              </td>
              <td colspan="3" class="mux-tree__streams" :title="r.service.streams">{{ r.service.streams }}</td>
              <td class="mux-tree__num">{{ r.service.channel?.length || '' }}</td>
            </template>
            <template v-else-if="r.network">
              <td colspan="2" />
              <td class="mux-tree__num">{{ r.network.numSvc }}</td>
              <td class="mux-tree__num">{{ r.network.numChn }}</td>
            </template>
            <td v-else colspan="7" />
            <td class="mux-tree__select-cell">
              <input
                v-if="enabledState(r)"
                type="checkbox"
                :checked="enabledState(r) === 'on'"
                :indeterminate="enabledState(r) === 'ignore'"
                :title="enabledState(r) === 'ignore' ? t('Ignored: a click enables it') : t('Enabled')"
                :aria-label="t('Enabled')"
                @click.stop="toggleEnabled(r)"
                @dblclick.stop
              />
            </td>
            <td class="mux-tree__action">
              <button
                v-if="r.node && (r.node.row || r.node.virtual)"
                type="button"
                class="mux-tree__scan"
                :disabled="scanBusy(r.node)"
                :title="r.node.row ? t('Scan this mux') : t('Scan the muxes of this input stream')"
                :aria-label="t('Scan')"
                @click.stop="scanRow(r)"
                @dblclick.stop
              >
                <ScanSearch :size="15" :stroke-width="2" />
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
  <IdnodePickEntityDialog
    :visible="pickerVisible"
    grid-endpoint="mpegts/network/grid"
    display-field="networkname"
    :title="t('Add Mux')"
    :label="t('Network')"
    @pick="onNetworkPicked"
    @close="pickerVisible = false"
  />
  <IdnodeEditor
    :uuid="editingUuid"
    :uuids="editingUuids"
    :create-base="creatingNet ? 'mpegts/network' : null"
    :parent-scoped="createScope"
    :level="access.uilevel"
    :title="editingTitle"
    @close="onEditorClose"
    @created="onCreated"
  />
</template>

<style scoped>
.mux-tree {
  display: flex;
  flex-direction: column;
  flex: 1 1 auto;
  min-height: 0;
  gap: var(--tvh-space-2);
}

.mux-tree__toolbar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: var(--tvh-space-2);
  padding: var(--tvh-space-2) 0;
}

.mux-tree__check {
  display: inline-flex;
  align-items: center;
  gap: var(--tvh-space-1);
  font-size: var(--tvh-text-sm);
  margin-left: var(--tvh-space-2);
}

.mux-tree__count {
  margin-left: auto;
  color: var(--tvh-text-muted);
  font-size: var(--tvh-text-sm);
}

.mux-tree__error {
  color: var(--tvh-error);
  font-size: var(--tvh-text-sm);
}

.mux-tree__scroll {
  flex: 1 1 auto;
  min-height: 0;
  overflow: auto;
  border: 1px solid var(--tvh-border);
  border-radius: var(--tvh-radius-sm);
  background: var(--tvh-bg-surface);
}

/* fixed column widths: expanding a node does not move the columns */
.mux-tree__table {
  width: 100%;
  min-width: 900px;
  table-layout: fixed;
  border-collapse: collapse;
  font-size: var(--tvh-text-sm);
}

.mux-tree__col-type {
  width: 88px;
}

.mux-tree__col-pid {
  width: 52px;
}

.mux-tree__col-address {
  width: 148px;
}

.mux-tree__col-modcod {
  width: 88px;
}

.mux-tree__col-scan {
  width: 128px;
}

.mux-tree__col-count {
  width: 78px;
}

.mux-tree__col-action {
  width: 40px;
}

.mux-tree__col-select {
  width: 30px;
}

.mux-tree__col-on {
  width: 34px;
}

.mux-tree__editable {
  cursor: text;
}

.mux-tree__editable:hover {
  box-shadow: inset 0 -1px 0 var(--tvh-primary);
}

.mux-tree__cell-input {
  width: 100%;
  box-sizing: border-box;
  font: inherit;
  padding: 1px 4px;
  border: 1px solid var(--tvh-primary);
  border-radius: var(--tvh-radius-sm);
  background: var(--tvh-bg-surface);
  color: var(--tvh-text);
}

.mux-tree__select-cell {
  text-align: center;
  padding: 0 !important;
}

.mux-tree__row--selected td {
  background: color-mix(in srgb, var(--tvh-primary) 14%, transparent);
}

.mux-tree__streams {
  color: var(--tvh-text-muted);
  font-size: var(--tvh-text-xs);
}

.mux-tree__sep {
  width: 1px;
  height: 1.4rem;
  background: var(--tvh-border);
  margin: 0 var(--tvh-space-1);
}

.mux-tree__filters {
  padding-top: 0;
}

.mux-tree__search {
  width: 22rem;
  max-width: 100%;
}

.mux-tree__select {
  font: inherit;
  font-size: var(--tvh-text-sm);
  padding: 2px var(--tvh-space-1);
  border: 1px solid var(--tvh-border);
  border-radius: var(--tvh-radius-sm);
  background: var(--tvh-bg-surface);
  color: var(--tvh-text);
}

.mux-tree__action {
  text-align: center;
}

.mux-tree__scan {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 1.6rem;
  height: 1.6rem;
  padding: 0;
  border: none;
  border-radius: var(--tvh-radius-sm);
  background: none;
  color: var(--tvh-text-muted);
  cursor: pointer;
}

.mux-tree__scan:hover:not(:disabled) {
  color: var(--tvh-primary);
  background: color-mix(in srgb, var(--tvh-primary) var(--tvh-hover-strength), transparent);
}

.mux-tree__scan:disabled {
  opacity: 0.35;
  cursor: default;
}

.mux-tree__scan:focus-visible {
  outline: 2px solid var(--tvh-focus);
}

.mux-tree__busy {
  color: var(--tvh-primary);
}

.mux-tree__sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  overflow: hidden;
  clip: rect(0 0 0 0);
  white-space: nowrap;
}

.mux-tree__table th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: var(--tvh-bg-surface);
  border-bottom: 1px solid var(--tvh-border);
  text-align: left;
  font-weight: 600;
  padding: 0 var(--tvh-space-2);
  height: var(--tvh-row-height);
  white-space: nowrap;
}

.mux-tree__table td {
  padding: 0 var(--tvh-space-2);
  height: var(--tvh-row-height);
  border-bottom: 1px solid color-mix(in srgb, var(--tvh-border) 50%, transparent);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

/* after the general th rule: numeric headers align with their values */
.mux-tree__table th.mux-tree__num {
  text-align: right;
}

.mux-tree__address {
  font-variant-numeric: tabular-nums;
}

.mux-tree__lock {
  vertical-align: -1px;
  margin-right: 2px;
  color: var(--tvh-warning);
}

.mux-tree__provider {
  margin-left: var(--tvh-space-2);
  color: var(--tvh-text-muted);
  font-weight: normal;
}

.mux-tree__tuning {
  margin-left: var(--tvh-space-3);
  color: var(--tvh-text-muted);
  font-size: var(--tvh-text-xs);
  font-weight: normal;
  font-variant-numeric: tabular-nums;
}

.mux-tree__table tbody tr {
  cursor: default;
  user-select: none;
}

.mux-tree__table tbody tr:hover {
  background: color-mix(in srgb, var(--tvh-primary) var(--tvh-hover-strength), transparent);
}


.mux-tree__num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.mux-tree__row--network td {
  font-weight: 600;
}

.mux-tree__row--virtual td,
.mux-tree__row--off td {
  color: var(--tvh-text-muted);
}

.mux-tree__row--virtual .mux-tree__name {
  font-style: italic;
}

.mux-tree__row--group td {
  color: var(--tvh-text-muted);
}

.mux-tree__toggle,
.mux-tree__toggle-space {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 1.25rem;
  height: 1.25rem;
  margin-right: var(--tvh-space-1);
  vertical-align: middle;
}

.mux-tree__toggle {
  padding: 0;
  border: none;
  background: none;
  color: var(--tvh-text);
  cursor: pointer;
  border-radius: var(--tvh-radius-sm);
}

.mux-tree__toggle:focus-visible {
  outline: 2px solid var(--tvh-focus);
}

.mux-tree__muted {
  color: var(--tvh-text-muted);
  font-weight: normal;
}

.mux-tree__badge {
  margin-left: var(--tvh-space-2);
  padding: 0 var(--tvh-space-1);
  border: 1px solid var(--tvh-border);
  border-radius: var(--tvh-radius-sm);
  font-size: var(--tvh-text-xs);
  font-style: normal;
  font-weight: normal;
  color: var(--tvh-text-muted);
}

.mux-tree__badge--warn {
  border-color: var(--tvh-warning);
  color: var(--tvh-warning);
}
</style>
