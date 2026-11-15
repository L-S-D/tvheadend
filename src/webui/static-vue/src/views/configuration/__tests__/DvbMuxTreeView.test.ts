// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Tvheadend contributors

/*
 * DvbMuxTreeView — the Muxes tab as a tree. The API answers come from
 * a small fake server below: it keeps networks, muxes and services and
 * applies the grid filters the way tvheadend does (a string filter is
 * a case-insensitive extended regular expression, `api_idnode.c`).
 */
import { describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import PrimeVue from 'primevue/config'
import { fireComet } from '@/test/__helpers__/cometClientMock'

const stubs = await vi.hoisted(() => import('@/test/__helpers__/gridViewTestUtils'))
const spies = vi.hoisted(() => ({ helpToggle: vi.fn(() => Promise.resolve()), openPlay: vi.fn() }))
vi.mock('@/api/client', () => stubs.apiClient)
vi.mock('@/api/comet', () => import('@/test/__helpers__/cometClientMock'))
vi.mock('@/components/IdnodeEditor.vue', () => stubs.emptyComponent)
vi.mock('@/components/IdnodePickEntityDialog.vue', () => stubs.emptyComponent)
vi.mock('vue-router', () => stubs.vueRouter)
vi.mock('@/composables/useConfirmDialog', () => stubs.confirmDialog)
vi.mock('@/composables/useToastNotify', () => stubs.toastNotify)
vi.mock('@/composables/useHelp', async () => {
  const { ref } = await import('vue')
  return { useHelp: () => ({ isOpen: ref(false), toggle: spies.helpToggle }) }
})
vi.mock('@/utils/playUrl', () => ({ openPlay: spies.openPlay }))

import DvbMuxTreeView from '../DvbMuxTreeView.vue'

stubs.resetEachTest()

type Row = Record<string, unknown>

interface FakeServer {
  networks: Row[]
  muxes: Row[]
  services: Row[]
  /* when set: an answer waits for the promise it returns */
  gate?: (ep: string) => Promise<void> | undefined
}

/* a DVB-S network with one transponder and its service, an IPTV
 * network with one mux */
function server(): FakeServer {
  return {
    networks: [
      { uuid: 'n1', class: 'dvb_network_dvbs', networkname: 'Astra', num_mux: 1, num_svc: 1, num_chn: 1 },
      {
        uuid: 'n2', class: 'iptv_auto_network', networkname: 'IPTV Automatic Network',
        num_mux: 1, num_svc: 0, num_chn: 0,
      },
    ],
    muxes: [
      {
        uuid: 'm1', name: '11494H', network: 'Astra', network_uuid: 'n1', tstype: 0, delsys: 'DVB-S2',
        frequency: 11494000, polarisation: 'H', num_svc: 1, num_chn: 1, scan_state: 0, scan_result: 1, enabled: 1,
      },
      {
        uuid: 'i1', name: 'Channel One', network: 'IPTV Automatic Network', network_uuid: 'n2', tstype: 0,
        num_svc: 0, num_chn: 0, scan_state: 0, scan_result: 0, enabled: 1,
      },
    ],
    services: [
      {
        uuid: 's1', svcname: 'Das Erste HD', sid: 10301, network: 'Astra', multiplex_uuid: 'm1',
        enabled: true, channel: ['c1'],
      },
    ],
  }
}

/* the grid filters as the server applies them */
function filtered(rows: Row[], params: Row): Row[] {
  const filter = (params.filter ?? []) as Array<{ field: string; type: string; value: string }>
  return rows.filter((r) =>
    filter.every((f) => {
      if (f.type !== 'string') return true
      let re: RegExp
      try {
        re = new RegExp(f.value, 'i')
      } catch {
        return true /* the server drops a filter that does not compile */
      }
      return re.test(String(r[f.field] ?? ''))
    }),
  )
}

function serve(s: FakeServer) {
  stubs.apiMock.mockImplementation(async (ep: string, params: Row = {}) => {
    const wait = s.gate?.(ep)
    if (wait) await wait
    if (ep === 'mpegts/network/grid') return { entries: s.networks, total: s.networks.length }
    if (ep === 'idnode/load') {
      const uuids = (params.uuid ?? []) as string[]
      return { entries: s.networks.filter((n) => uuids.includes(String(n.uuid))) }
    }
    if (ep === 'mpegts/mux/grid' || ep === 'mpegts/service/grid') {
      const rows = filtered(ep === 'mpegts/mux/grid' ? s.muxes : s.services, params)
      const limit = typeof params.limit === 'number' ? params.limit : rows.length
      return { entries: rows.slice(0, limit), total: rows.length }
    }
    return {}
  })
}

function calls(ep: string): Row[] {
  return stubs.apiMock.mock.calls.filter((c) => c[0] === ep).map((c) => (c[1] ?? {}) as Row)
}

async function mountTree(s: FakeServer = server()) {
  serve(s)
  const w = mount(DvbMuxTreeView, { global: { plugins: [PrimeVue] } })
  await flushPromises()
  return w
}

type Wrapper = Awaited<ReturnType<typeof mountTree>>

/* the text of the rows shown, one string per row */
function rowTexts(w: Wrapper): string[] {
  return w.findAll('tbody tr').map((tr) => tr.text().replace(/\s+/g, ' ').trim())
}

function rowOf(w: Wrapper, text: string) {
  const row = w.findAll('tbody tr').find((tr) => tr.text().includes(text))
  if (!row) throw new Error(`no row with "${text}": ${rowTexts(w).join(' | ')}`)
  return row
}

/* click the expand toggle of the row that shows `text` */
async function expand(w: Wrapper, text: string) {
  await rowOf(w, text).find('button.mux-tree__toggle').trigger('click')
  await flushPromises()
}

function button(w: Wrapper, label: string) {
  const b = w.findAll('button').find((x) => x.text().includes(label))
  if (!b) throw new Error(`no button "${label}"`)
  return b
}

describe('DvbMuxTreeView networks', () => {
  it('lists the IPTV networks too, their muxes as a flat list', async () => {
    const w = await mountTree()
    expect(rowTexts(w).some((r) => r.includes('Astra'))).toBe(true)
    expect(rowTexts(w).some((r) => r.includes('IPTV Automatic Network'))).toBe(true)
    await expand(w, 'IPTV Automatic Network')
    expect(rowTexts(w).some((r) => r.includes('Channel One'))).toBe(true)
    /* no class lookup to leave them out */
    expect(calls('idnode/load')).toHaveLength(0)
  })

  it('switches to the list view of the tab', async () => {
    const w = await mountTree()
    await button(w, 'List view').trigger('click')
    expect(stubs.router.push).toHaveBeenCalledWith({ name: 'config-dvb-mux-list' })
  })
})

/* rename network n1 (in its mux and service rows too) */
function renameAstra(s: FakeServer, name: string | undefined) {
  if (name === undefined) delete s.networks[0].networkname
  else s.networks[0].networkname = name
  const shown = name || 'unknown'
  for (const m of s.muxes) if (m.network_uuid === 'n1') m.network = shown
  for (const x of s.services) if (x.multiplex_uuid === 'm1') x.network = shown
}

describe('DvbMuxTreeView filters by network', () => {
  it('finds the muxes and services of a network with ( ) in its name', async () => {
    const s = server()
    renameAstra(s, 'Astra 1 (19.2E)')
    const w = await mountTree(s)
    await expand(w, 'Astra 1 (19.2E)')
    expect(rowTexts(w).some((r) => r.includes('11494H'))).toBe(true)
    expect(calls('mpegts/mux/grid')[0].filter).toEqual([{ type: 'string', field: 'network_uuid', value: '^n1$' }])
    await expand(w, '11494H')
    expect(rowTexts(w).some((r) => r.includes('Das Erste HD'))).toBe(true)
    expect(calls('mpegts/service/grid')[0].filter).toEqual([
      { type: 'string', field: 'network', value: '^Astra 1 \\(19\\.2E\\)$' },
    ])
  })

  it('loads a network without a name, its services by the name the server gives it', async () => {
    const s = server()
    renameAstra(s, undefined)
    const w = await mountTree(s)
    await expand(w, 'n1')
    await expand(w, '11494H')
    expect(rowTexts(w).some((r) => r.includes('Das Erste HD'))).toBe(true)
    expect(calls('mpegts/service/grid')[0].filter).toEqual([{ type: 'string', field: 'network', value: '^unknown$' }])
  })

  it('keeps out the services of another network the name filter finds', async () => {
    const s = server()
    s.networks.push({ uuid: 'n3', networkname: 'ASTRA', num_mux: 1, num_svc: 1, num_chn: 0 })
    s.muxes.push({
      uuid: 'm3', name: '10744H', network: 'ASTRA', network_uuid: 'n3', tstype: 0,
      num_svc: 1, num_chn: 0, scan_state: 0, enabled: 1,
    })
    s.services.push({ uuid: 's3', svcname: 'Other', sid: 1, network: 'ASTRA', multiplex_uuid: 'm3', enabled: true })
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    await expand(w, '11494H')
    const before = calls('mpegts/service/grid').length
    /* a change of the other network's service: not one of Astra's */
    vi.useFakeTimers()
    try {
      fireComet('service', { change: ['s3'] })
      await vi.advanceTimersByTimeAsync(1500)
    } finally {
      vi.useRealTimers()
    }
    await flushPromises()
    expect(calls('mpegts/service/grid')).toHaveLength(before)
    fireComet('service', { change: ['s1'] })
    await new Promise((r) => setTimeout(r, 1100))
    await flushPromises()
    expect(calls('mpegts/service/grid')).toHaveLength(before + 1)
  })
})

/* the answers of the mux grid wait until released, one by one */
function holdMuxGrid(s: FakeServer) {
  const held: Array<() => void> = []
  s.gate = (ep) => (ep === 'mpegts/mux/grid' ? new Promise<void>((r) => held.push(r)) : undefined)
  return {
    release: () => held.shift()?.(),
    stop: () => {
      s.gate = undefined
      while (held.length) held.shift()?.()
    },
  }
}

describe('DvbMuxTreeView loads in order', () => {
  it('loads a network once at a time, the later state wins', async () => {
    const s = server()
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    const before = calls('mpegts/mux/grid').length
    const gate = holdMuxGrid(s)
    const scan = () => rowOf(w, '11494H').find('button.mux-tree__scan').trigger('click')
    await scan()
    await flushPromises()
    await scan()
    await flushPromises()
    /* the second load waits for the first */
    expect(calls('mpegts/mux/grid')).toHaveLength(before + 1)
    s.muxes[0].scan_result = 2
    gate.release()
    await flushPromises()
    expect(calls('mpegts/mux/grid')).toHaveLength(before + 2)
    gate.stop()
    await flushPromises()
    expect(rowOf(w, '11494H').text()).toContain('FAIL')
  })

  it('times the next scan poll when the last reload is done', async () => {
    const s = server()
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] })
    try {
      /* queued: the poll goes on */
      s.muxes[0].scan_state = 1
      await rowOf(w, '11494H').find('button.mux-tree__scan').trigger('click')
      await flushPromises()
      const before = calls('mpegts/mux/grid').length
      const gate = holdMuxGrid(s)
      await vi.advanceTimersByTimeAsync(2000)
      expect(calls('mpegts/mux/grid')).toHaveLength(before + 1)
      /* a slow server: no more reloads meanwhile */
      await vi.advanceTimersByTimeAsync(6000)
      expect(calls('mpegts/mux/grid')).toHaveLength(before + 1)
      gate.stop()
      await flushPromises()
      expect(calls('mpegts/mux/grid')).toHaveLength(before + 1)
      await vi.advanceTimersByTimeAsync(2000)
      expect(calls('mpegts/mux/grid')).toHaveLength(before + 2)
    } finally {
      vi.useRealTimers()
    }
  })
})

describe('DvbMuxTreeView Reload', () => {
  it('keeps the services of an open mux and loads them again', async () => {
    const w = await mountTree()
    await expand(w, 'Astra (1)')
    await expand(w, '11494H')
    expect(rowTexts(w).some((r) => r.includes('Das Erste HD'))).toBe(true)
    const before = calls('mpegts/service/grid').length
    await button(w, 'Reload').trigger('click')
    await flushPromises()
    expect(calls('mpegts/service/grid')).toHaveLength(before + 1)
    expect(rowTexts(w).some((r) => r.includes('Das Erste HD'))).toBe(true)
    expect(w.text()).not.toContain('Loading…')
  })

  it('loads the services again that failed to load', async () => {
    const s = server()
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    s.gate = (ep) => (ep === 'mpegts/service/grid' ? Promise.reject(new Error('busy')) : undefined)
    await expand(w, '11494H')
    expect(w.text()).toContain('Loading…')
    s.gate = undefined
    await button(w, 'Reload').trigger('click')
    await flushPromises()
    expect(rowTexts(w).some((r) => r.includes('Das Erste HD'))).toBe(true)
  })
})

/* n more transponders in network n1, one service each */
function manyMuxes(s: FakeServer, n: number) {
  for (let j = 0; j < n; j++) {
    s.muxes.push({
      uuid: `x${j}`, name: `${10700 + j}V`, network: 'Astra', network_uuid: 'n1', tstype: 0,
      num_svc: 1, num_chn: 0, scan_state: 0, scan_result: 1, enabled: 1,
    })
    s.services.push({ uuid: `y${j}`, svcname: `Radio ${j}`, sid: j + 1, network: 'Astra', multiplex_uuid: `x${j}` })
  }
  s.networks[0].num_mux = n + 1
}

async function searchServiceNames(w: Wrapper, text: string) {
  await w.find('label.mux-tree__check input').setValue(true)
  await w.find('.mux-tree__search input').setValue(text)
  /* the filter waits 250 ms for the typing to end */
  await new Promise((r) => setTimeout(r, 300))
  await flushPromises()
}

describe('DvbMuxTreeView with many muxes and services', () => {
  it('asks only for the fields it shows', async () => {
    const w = await mountTree()
    await expand(w, 'Astra (1)')
    await expand(w, '11494H')
    const muxList = String(calls('mpegts/mux/grid')[0].list).split(',')
    expect(muxList).toEqual(expect.arrayContaining(['name', 'tstype', 'parent_mux', 'scan_state', 'num_svc', 'enabled']))
    const svcList = String(calls('mpegts/service/grid')[0].list).split(',')
    expect(svcList).toEqual(expect.arrayContaining(['svcname', 'sid', 'multiplex_uuid', 'network', 'streams']))
    expect(svcList).not.toContain('source')
  })

  it('renders the rows in view only, a spacer for the rest', async () => {
    const s = server()
    manyMuxes(s, 1000)
    const w = await mountTree(s)
    await expand(w, 'Astra (1001)')
    const rows = w.findAll('tbody tr.mux-tree__row')
    expect(rows.length).toBeGreaterThan(20)
    expect(rows.length).toBeLessThan(150)
    expect(w.find('tbody tr.mux-tree__pad').exists()).toBe(true)
  })

  it('searches the service names on the server, once, and shows the first matches', async () => {
    const s = server()
    manyMuxes(s, 600)
    const w = await mountTree(s)
    await searchServiceNames(w, 'radio')
    const search = calls('mpegts/service/grid').filter((c) => JSON.stringify(c.filter).includes('svcname'))
    expect(search).toHaveLength(1)
    expect(search[0].filter).toEqual([{ type: 'string', field: 'svcname', value: 'radio' }])
    expect(search[0].limit).toBe(500)
    /* no services of whole networks for it */
    expect(calls('mpegts/service/grid')).toHaveLength(1)
    expect(w.find('.mux-tree__hits').text()).toContain('500')
    expect(w.find('.mux-tree__hits').text()).toContain('600')
    expect(rowTexts(w).some((r) => r.includes('Radio 0'))).toBe(true)
  })

  it('escapes the search text for the server', async () => {
    const s = server()
    s.services.push({ uuid: 's9', svcname: 'ORF 2 (W)', sid: 9, network: 'Astra', multiplex_uuid: 'm1' })
    s.muxes[0].num_svc = 2
    const w = await mountTree(s)
    await searchServiceNames(w, '2 (w')
    expect(rowTexts(w).some((r) => r.includes('ORF 2 (W)'))).toBe(true)
    expect(rowTexts(w).some((r) => r.includes('Das Erste HD'))).toBe(false)
  })
})

describe('DvbMuxTreeView scan button', () => {
  const scanButton = (w: Wrapper, text: string) => rowOf(w, text).find('button.mux-tree__scan')

  it('scans a mux in the idle scan queue, and says it is there', async () => {
    const s = server()
    s.muxes[0].scan_state = 2
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    expect(scanButton(w, '11494H').attributes('disabled')).toBeUndefined()
    expect(rowOf(w, '11494H').text()).toContain('idle queue')
  })

  it('is off for a disabled mux and in a disabled network, and says why', async () => {
    const s = server()
    s.muxes[0].enabled = 0
    s.networks[1].enabled = false
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    expect(scanButton(w, '11494H').attributes('disabled')).toBeDefined()
    expect(scanButton(w, '11494H').attributes('title')).toContain('not enabled')
    await expand(w, 'IPTV Automatic Network')
    expect(scanButton(w, 'Channel One').attributes('disabled')).toBeDefined()
    expect(scanButton(w, 'Channel One').attributes('title')).toContain('network is disabled')
  })

  it('stops reloading when the scanned mux is back in the idle queue', async () => {
    const s = server()
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] })
    try {
      s.muxes[0].scan_state = 1
      await scanButton(w, '11494H').trigger('click')
      await flushPromises()
      const before = calls('mpegts/mux/grid').length
      /* scanned: the mux rotates in the idle queue again */
      s.muxes[0].scan_state = 2
      await vi.advanceTimersByTimeAsync(2000)
      expect(calls('mpegts/mux/grid')).toHaveLength(before + 1)
      await vi.advanceTimersByTimeAsync(20000)
      expect(calls('mpegts/mux/grid')).toHaveLength(before + 1)
    } finally {
      vi.useRealTimers()
    }
  })
})

describe('DvbMuxTreeView Scan of the selection', () => {
  const select = (w: Wrapper, text: string) => rowOf(w, text).findAll('td.mux-tree__select-cell input')[0].trigger('click')

  async function mountTwoSelected(s: FakeServer) {
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    await expand(w, 'IPTV Automatic Network')
    await select(w, '11494H')
    await select(w, 'Channel One')
    return w
  }

  it('queues all the muxes in one request', async () => {
    const w = await mountTwoSelected(server())
    await button(w, 'Scan').trigger('click')
    await flushPromises()
    const saves = calls('idnode/save')
    expect(saves).toHaveLength(1)
    expect(JSON.parse(String(saves[0].node))).toEqual({ uuid: ['m1', 'i1'], scan_state: 1 })
  })

  it('leaves out a mux that is not enabled', async () => {
    const s = server()
    s.muxes[1].enabled = -1
    const w = await mountTwoSelected(s)
    await button(w, 'Scan').trigger('click')
    await flushPromises()
    expect(JSON.parse(String(calls('idnode/save')[0].node)).uuid).toEqual(['m1'])
  })

  it('follows the scans after a save that failed', async () => {
    const s = server()
    const w = await mountTwoSelected(s)
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] })
    try {
      s.gate = (ep) => (ep === 'idnode/save' ? Promise.reject(new Error('timeout')) : undefined)
      s.muxes[0].scan_state = 1
      await button(w, 'Scan').trigger('click')
      await flushPromises()
      const before = calls('mpegts/mux/grid').length
      await vi.advanceTimersByTimeAsync(2000)
      expect(calls('mpegts/mux/grid').length).toBeGreaterThan(before)
    } finally {
      vi.useRealTimers()
    }
  })
})

/* the saves fail */
function failSaves(s: FakeServer) {
  s.gate = (ep) => (ep === 'idnode/save' ? Promise.reject(new Error('session expired')) : undefined)
}

describe('DvbMuxTreeView changes in the rows', () => {
  const onBox = (w: Wrapper, text: string) => rowOf(w, text).findAll('td.mux-tree__select-cell input')[1]

  it('takes back an "On" click whose save fails, and says so', async () => {
    const s = server()
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    await expand(w, '11494H')
    failSaves(s)
    const before = calls('mpegts/mux/grid').length
    await onBox(w, '11494H').trigger('click')
    await flushPromises()
    expect((onBox(w, '11494H').element as HTMLInputElement).checked).toBe(true)
    expect(rowOf(w, '11494H').classes()).not.toContain('mux-tree__row--off')
    expect(stubs.toast.error).toHaveBeenCalledWith('Failed to save: session expired')
    /* and the network as the server has it */
    expect(calls('mpegts/mux/grid').length).toBeGreaterThan(before)
    await onBox(w, 'Das Erste HD').trigger('click')
    await flushPromises()
    expect((onBox(w, 'Das Erste HD').element as HTMLInputElement).checked).toBe(true)
  })

  it('takes back a PID whose save fails', async () => {
    const s = server()
    s.muxes.push({
      uuid: 't2', name: '11494H T2MI 4096', network: 'Astra', network_uuid: 'n1', parent_mux: 'm1', tstype: 1,
      pid: 4096, num_svc: 0, num_chn: 0, scan_state: 0, enabled: 1,
    })
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    failSaves(s)
    const cell = () => rowOf(w, 'T2MI').findAll('td.mux-tree__num')[0]
    await cell().trigger('click')
    const input = cell().find('input')
    await input.setValue('4099')
    await input.trigger('keydown', { key: 'Enter' })
    await flushPromises()
    expect(cell().text()).toBe('4096')
    expect(stubs.toast.error).toHaveBeenCalledWith('Failed to save: session expired')
  })

  it('says when a delete fails', async () => {
    const s = server()
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    await rowOf(w, '11494H').findAll('td.mux-tree__select-cell input')[0].trigger('click')
    s.gate = (ep) => (ep === 'idnode/delete' ? Promise.reject(new Error('in use')) : undefined)
    await button(w, 'Delete').trigger('click')
    await flushPromises()
    expect(stubs.toast.error).toHaveBeenCalledWith('Failed to delete: in use')
  })
})

describe('DvbMuxTreeView load errors', () => {
  it('shows a failed load until that load succeeds', async () => {
    const s = server()
    const w = await mountTree(s)
    s.gate = (ep) => (ep === 'mpegts/mux/grid' ? Promise.reject(new Error('busy')) : undefined)
    await expand(w, 'Astra (1)')
    expect(w.find('.mux-tree__error').text()).toBe('Failed to load: busy')
    /* the networks load fine: the error of the muxes stays */
    s.gate = (ep) => (ep === 'mpegts/mux/grid' ? Promise.reject(new Error('busy')) : undefined)
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    try {
      fireComet('mpegts_network', { change: ['n1'] })
      await vi.advanceTimersByTimeAsync(1500)
    } finally {
      vi.useRealTimers()
    }
    await flushPromises()
    expect(w.find('.mux-tree__error').exists()).toBe(true)
    s.gate = undefined
    await button(w, 'Reload').trigger('click')
    await flushPromises()
    expect(w.find('.mux-tree__error').exists()).toBe(false)
  })
})

describe('DvbMuxTreeView address of a DAB sub-mux', () => {
  async function mountMpe() {
    const s = server()
    s.muxes.push({
      uuid: 'd1', name: '11494H DAB-MPE', network: 'Astra', network_uuid: 'n1', parent_mux: 'm1', tstype: 2,
      pid: 1001, dab_ip: '239.1.2.3', dab_port: 5000, num_svc: 0, num_chn: 0, scan_state: 0, enabled: 1,
    })
    const w = await mountTree(s)
    await expand(w, 'Astra (1)')
    return w
  }

  async function typeAddress(w: Wrapper, text: string) {
    const cell = rowOf(w, 'DAB-MPE').find('td.mux-tree__address')
    await cell.trigger('click')
    const input = rowOf(w, 'DAB-MPE').find('td.mux-tree__address input')
    await input.setValue(text)
    await input.trigger('keydown', { key: 'Enter' })
    await flushPromises()
  }

  it('saves an address in range', async () => {
    const w = await mountMpe()
    await typeAddress(w, '239.1.2.4:5001')
    expect(JSON.parse(String(calls('idnode/save')[0].node))).toEqual({ uuid: 'd1', dab_ip: '239.1.2.4', dab_port: 5001 })
  })

  it('saves no octet over 255 and no port over 65535, and says so', async () => {
    const w = await mountMpe()
    await typeAddress(w, '300.1.2.3')
    await typeAddress(w, '239.1.2.3:99999')
    expect(calls('idnode/save')).toHaveLength(0)
    expect(stubs.toast.warn).toHaveBeenCalledTimes(2)
    expect(rowOf(w, 'DAB-MPE').find('td.mux-tree__address').text()).toContain('239.1.2.3:5000')
  })
})

describe('DvbMuxTreeView what the grid had', () => {
  it('opens the help page of the muxes', async () => {
    spies.helpToggle.mockClear()
    const w = await mountTree()
    await w.find('button.mux-tree__help').trigger('click')
    expect(spies.helpToggle).toHaveBeenCalledWith('class/mpegts_mux')
  })

  it('plays a mux and a service', async () => {
    spies.openPlay.mockClear()
    const w = await mountTree()
    await expand(w, 'Astra (1)')
    await expand(w, '11494H')
    const playOf = (text: string) => rowOf(w, text).find('button[aria-label="Play this stream"]')
    await playOf('11494H').trigger('click')
    expect(spies.openPlay).toHaveBeenLastCalledWith(`stream/mux/m1?title=${encodeURIComponent('11494H / Astra')}`)
    await playOf('Das Erste HD').trigger('click')
    expect(spies.openPlay).toHaveBeenLastCalledWith(`stream/service/s1?title=${encodeURIComponent('Das Erste HD')}`)
    /* a click on Play selects nothing */
    expect(w.findAll('tr.mux-tree__row--selected')).toHaveLength(0)
  })

  it('shows the frequency of a transponder with its unit', async () => {
    const w = await mountTree()
    await expand(w, 'Astra (1)')
    expect(rowOf(w, '11494H').find('td.mux-tree__name').attributes('title')).toContain('11494000 kHz')
  })

  it('says how to get muxes when there are none', async () => {
    const s = server()
    s.networks = []
    const w = await mountTree(s)
    expect(w.find('.mux-tree__empty').text()).toContain('No muxes defined')
  })
})

describe('DvbMuxTreeView selection', () => {
  const pick = (w: Wrapper, text: string, opts: Record<string, boolean> = {}) =>
    rowOf(w, text).trigger('click', opts)
  const selectedTexts = (w: Wrapper) =>
    w.findAll('tr.mux-tree__row--selected').map((tr) => tr.text().replace(/\s+/g, ' ').trim())

  it('selects a Shift range from the row clicked, after rows above it opened', async () => {
    const w = await mountTree()
    await expand(w, 'IPTV Automatic Network')
    await pick(w, 'Channel One')
    /* opening Astra moves the rows below it */
    await expand(w, 'Astra (1)')
    await pick(w, '11494H', { shiftKey: true })
    const sel = selectedTexts(w)
    expect(sel.some((r) => r.includes('Channel One'))).toBe(true)
    expect(sel.some((r) => r.includes('11494H'))).toBe(true)
  })

  it('closes a row the filter opened', async () => {
    const w = await mountTree()
    await w.find('.mux-tree__search input').setValue('11494')
    await new Promise((r) => setTimeout(r, 300))
    await flushPromises()
    expect(rowTexts(w).some((r) => r.includes('11494H'))).toBe(true)
    await expand(w, 'Astra (1)')
    expect(rowTexts(w).some((r) => r.includes('11494H'))).toBe(false)
    await expand(w, 'Astra (1)')
    expect(rowTexts(w).some((r) => r.includes('11494H'))).toBe(true)
  })
})
