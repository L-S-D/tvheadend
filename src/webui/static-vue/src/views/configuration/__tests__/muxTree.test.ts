// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Tvheadend contributors

import { describe, expect, it } from 'vitest'
import {
  addressLabel,
  buildMuxTree,
  caidList,
  servicesByMux,
  serviceTypeName,
  countMuxes,
  expandableKeys,
  filterTree,
  muxSearchText,
  scanBusy,
  scanTargets,
  MUX_TYPE_DAB_ETI,
  MUX_TYPE_DAB_GSE,
  MUX_TYPE_DAB_MPE,
  MUX_TYPE_GSE,
  MUX_TYPE_ISI,
  MUX_TYPE_T2MI,
  shortName,
  subMuxScan,
  tuningLabel,
  withoutAddress,
  withoutType,
  withoutPid,
  type MuxRow,
  type MuxTreeNode,
} from '../muxTree'

function labels(nodes: MuxTreeNode[]): string[] {
  return nodes.map((n) => n.label)
}

describe('buildMuxTree', () => {
  it('puts the sub-muxes below their transponder, sorted by name', () => {
    const rows: MuxRow[] = [
      { uuid: 'b', name: '12092H' },
      { uuid: 'a', name: '11727V' },
      { uuid: 'd2', name: '12092H SDL National', parent_mux: 'b', tstype: MUX_TYPE_DAB_ETI },
      { uuid: 'd1', name: '12092H D1 National', parent_mux: 'b', tstype: MUX_TYPE_DAB_ETI },
    ]
    const tree = buildMuxTree(rows)
    expect(labels(tree)).toEqual(['11727V', '12092H'])
    expect(labels(tree[1].children)).toEqual(['D1 National', 'SDL National'])
    expect(countMuxes(tree)).toBe(4)
  })

  it('sorts frequencies numerically', () => {
    const tree = buildMuxTree([
      { uuid: '1', name: '9750H' },
      { uuid: '2', name: '10714H' },
    ])
    expect(labels(tree)).toEqual(['9750H', '10714H'])
  })

  it('shows the input streams of a multistream transponder by ISI', () => {
    const tree = buildMuxTree([
      { uuid: 't', name: '12111V', multistream: true },
      { uuid: 'i4', name: '12111V ISI 4', parent_mux: 't', tstype: MUX_TYPE_ISI, stream_id: 4 },
      { uuid: 'i1', name: '12111V ISI 1', parent_mux: 't', tstype: MUX_TYPE_ISI, stream_id: 1 },
      { uuid: 'i3', name: '12111V ISI 3', parent_mux: 't', tstype: MUX_TYPE_ISI, stream_id: 3 },
    ])
    expect(labels(tree[0].children)).toEqual(['ISI 1', 'ISI 3', 'ISI 4'])
  })

  it('groups the DAB of a multistream transponder below a made-up input stream', () => {
    /* 1°W 10717V: the DAB-GSE ensembles name the transponder, ISI 171 */
    const tree = buildMuxTree([
      { uuid: 't', name: '10717V', multistream: true },
      { uuid: 'r', name: '10717V Riks', parent_mux: 't', tstype: MUX_TYPE_DAB_GSE, stream_id: 171 },
      { uuid: 'n', name: '10717V NRK Reg1', parent_mux: 't', tstype: MUX_TYPE_DAB_GSE, stream_id: 171 },
    ])
    const isi = tree[0].children
    expect(isi).toHaveLength(1)
    expect(isi[0].virtual).toBe(true)
    expect(isi[0].row).toBeNull()
    expect(isi[0].label).toBe('ISI 171')
    expect(labels(isi[0].children)).toEqual(['NRK Reg1', 'Riks'])
    expect(countMuxes(tree)).toBe(3)
  })

  it('groups them below the input stream sub-mux of their ISI when there is one', () => {
    const tree = buildMuxTree([
      { uuid: 't', name: '10717V', multistream: true },
      { uuid: 'g', name: '10717V ISI 171', parent_mux: 't', tstype: MUX_TYPE_GSE, stream_id: 171 },
      { uuid: 'r', name: '10717V Riks', parent_mux: 't', tstype: MUX_TYPE_DAB_GSE, stream_id: 171 },
    ])
    expect(labels(tree[0].children)).toEqual(['ISI 171'])
    expect(tree[0].children[0].virtual).toBe(false)
    expect(labels(tree[0].children[0].children)).toEqual(['Riks'])
  })

  it('follows deeper chains: transponder, input stream, DAB in a PID', () => {
    const tree = buildMuxTree([
      { uuid: 't', name: '12226V', multistream: true },
      { uuid: 'i', name: '12226V ISI 14', parent_mux: 't', tstype: MUX_TYPE_ISI, stream_id: 14 },
      { uuid: 'd', name: '12226V ISI 14 DAB', parent_mux: 'i', tstype: MUX_TYPE_DAB_MPE, pid: 1001 },
    ])
    expect(tree[0].children[0].children[0].label).toBe('DAB')
    expect(expandableKeys(tree)).toEqual(['t', 'i'])
  })

  it('keeps a sub-mux whose transponder is missing at the top, marked', () => {
    const tree = buildMuxTree([{ uuid: 'x', name: '12345H ISI 2', parent_mux: 'gone', tstype: MUX_TYPE_ISI }])
    expect(tree).toHaveLength(1)
    expect(tree[0].orphan).toBe(true)
  })

  it('does not loop on a parent chain that loops', () => {
    const tree = buildMuxTree([
      { uuid: 'a', name: 'A', parent_mux: 'b' },
      { uuid: 'b', name: 'B', parent_mux: 'a' },
    ])
    expect(countMuxes(tree)).toBe(2)
  })
})

describe('shortName', () => {
  it('drops the name of the transponder', () => {
    expect(shortName('12226V ISI 14', '12226V')).toBe('ISI 14')
    expect(shortName('12226V', '12226V')).toBe('12226V')
    expect(shortName('11727V', undefined)).toBe('11727V')
  })
})

describe('withoutPid', () => {
  it('drops the PID the PID column shows', () => {
    expect(withoutPid('DAB Italia / PID 7031', 7031)).toBe('DAB Italia')
    expect(withoutPid('DAB Italia / PID 7031', 7131)).toBe('DAB Italia / PID 7031')
    expect(withoutPid('Salisbury', 701)).toBe('Salisbury')
  })
})

describe('subMuxScan', () => {
  it('counts the sub-muxes of all levels scanned with success', () => {
    const tree = buildMuxTree([
      { uuid: 't', name: '10717V', multistream: true, scan_result: 1 },
      { uuid: 'r', name: '10717V Riks', parent_mux: 't', tstype: MUX_TYPE_DAB_GSE, stream_id: 171, scan_result: 1 },
      { uuid: 'n', name: '10717V NRK', parent_mux: 't', tstype: MUX_TYPE_DAB_GSE, stream_id: 171, scan_result: 2 },
      { uuid: 'i', name: '10717V ISI 3', parent_mux: 't', tstype: MUX_TYPE_ISI, stream_id: 3, scan_result: 3 },
    ])
    expect(subMuxScan(tree[0])).toEqual({ ok: 2, total: 3 })
    const isi171 = tree[0].children.find((c) => c.virtual)
    expect(isi171 && subMuxScan(isi171)).toEqual({ ok: 1, total: 2 })
  })
})

describe('addressLabel', () => {
  it('shows ip:port of DAB over IP, nothing else', () => {
    expect(addressLabel({ uuid: 'a', dab_ip: '239.199.2.1', dab_port: 1234 })).toBe('239.199.2.1:1234')
    expect(addressLabel({ uuid: 'b', dab_ip: '239.199.2.1', dab_port: 0 })).toBe('239.199.2.1')
    expect(addressLabel({ uuid: 'c', dab_ip: '', pid: 1062 })).toBe('')
  })
})

describe('withoutAddress', () => {
  it('drops the address the address column shows', () => {
    const row = { uuid: 'a', dab_ip: '239.199.2.12', dab_port: 5555 }
    expect(withoutAddress('DAB-GSE 239.199.2.12:5555', row)).toBe('DAB-GSE')
    expect(withoutAddress('NRK Reg1', row)).toBe('NRK Reg1')
  })
})

describe('scanTargets / scanBusy', () => {
  const tree = buildMuxTree([
    { uuid: 't', name: '10717V', multistream: true },
    { uuid: 'r', name: '10717V Riks', parent_mux: 't', tstype: MUX_TYPE_DAB_GSE, stream_id: 171 },
    { uuid: 'n', name: '10717V NRK', parent_mux: 't', tstype: MUX_TYPE_DAB_GSE, stream_id: 171, scan_state: 1 },
  ])
  const isi = tree[0].children[0]

  it('scans a mux itself, a made-up input stream through the muxes below it', () => {
    expect(scanTargets(tree[0])).toEqual(['t'])
    expect(scanTargets(isi).sort()).toEqual(['n', 'r'])
  })

  it('is busy while a mux of it is queued or scanning', () => {
    expect(scanBusy(isi)).toBe(true)
    expect(scanBusy(tree[0])).toBe(false)
  })
})

describe('tuningLabel', () => {
  it('shows the DVB-S2 tuning, PLS only when scrambled', () => {
    expect(
      tuningLabel({
        uuid: 'a', delsys: 'DVB-S2', modulation: 'PSK/8', fec: '2/3', symbolrate: 31400000,
        rolloff: '20', pilot: 'ON', pls_mode: 'GOLD', pls_code: 262140,
      }),
    ).toBe('DVB-S2 · 8PSK 2/3 · SR 31400 · PLS Gold 262140 · RO 0.20 · Pilot on')
    expect(
      tuningLabel({
        uuid: 'b', delsys: 'DVB-S2', modulation: 'PSK/8', fec: '3/4', symbolrate: 4500000,
        rolloff: 'NONE', pilot: 'AUTO', pls_mode: 'ROOT', pls_code: 1,
      }),
    ).toBe('DVB-S2 · 8PSK 3/4 · SR 4500 · Pilot auto')
  })

  it('leaves out what DVB-S and DVB-C do not have', () => {
    expect(
      tuningLabel({ uuid: 'c', delsys: 'DVB-S', modulation: 'QPSK', fec: '3/4', symbolrate: 27500000, rolloff: '35', pilot: 'AUTO' }),
    ).toBe('DVB-S · QPSK 3/4 · SR 27500')
    expect(tuningLabel({ uuid: 'd', delsys: 'DVB-C', constellation: 'QAM/256', fec: 'AUTO', symbolrate: 6900000 })).toBe(
      'DVB-C · 256QAM · SR 6900',
    )
  })
})

describe('withoutType', () => {
  it('keeps no type and PID in the name, they have columns', () => {
    expect(withoutType('T2MI 4096', { uuid: 'a', tstype: MUX_TYPE_T2MI, pid: 4096 })).toBe('')
    expect(withoutType('T2MI 4096 / PLP 1', { uuid: 'a', tstype: MUX_TYPE_T2MI, pid: 4096 })).toBe('PLP 1')
    expect(withoutType('GSE ISI 171', { uuid: 'b', tstype: MUX_TYPE_GSE })).toBe('ISI 171')
    expect(withoutType('DAB-GSE', { uuid: 'c', tstype: MUX_TYPE_DAB_GSE })).toBe('')
    expect(withoutType('ISI 14', { uuid: 'd', tstype: MUX_TYPE_ISI })).toBe('ISI 14')
    expect(withoutType('Salisbury', { uuid: 'e', tstype: MUX_TYPE_DAB_MPE, pid: 701 })).toBe('Salisbury')
  })

  it('names a T2MI stream by the provider network of its TS', () => {
    const tree = buildMuxTree([
      { uuid: 't', name: '12285V' },
      { uuid: 'x', name: '12285V T2MI 4096', parent_mux: 't', tstype: MUX_TYPE_T2MI, pid: 4096, pnetwork_name: 'MX-7' },
    ])
    expect(tree[0].children[0].label).toBe('MX-7')
    expect(tree[0].children[0].providerLabel).toBe(true)
  })
})

describe('services', () => {
  it('names the service types and the CA systems', () => {
    expect(serviceTypeName(0x19)).toBe('HD TV')
    expect(serviceTypeName(0x02)).toBe('Radio')
    expect(serviceTypeName(0x86)).toBe('0x86')
    expect(serviceTypeName(0)).toBe('')
    expect(caidList('1883:003311,1863:003343,1883:000000')).toBe('1883, 1863')
    expect(caidList('')).toBe('')
  })

  it('groups the services by mux, sorted by name', () => {
    const m = servicesByMux([
      { uuid: '1', svcname: 'ORF2', sid: 2, multiplex_uuid: 'a' },
      { uuid: '2', svcname: 'ORF1', sid: 1, multiplex_uuid: 'a' },
      { uuid: '3', svcname: 'X', sid: 9, multiplex_uuid: 'b' },
    ])
    expect(m.get('a')?.map((s) => s.svcname)).toEqual(['ORF1', 'ORF2'])
    expect(m.get('b')?.length).toBe(1)
  })
})

describe('filterTree', () => {
  const tree = buildMuxTree([
    { uuid: 't', name: '10717V', multistream: true, pnetwork_name: 'Thor' },
    { uuid: 'r', name: '10717V Riks', parent_mux: 't', tstype: MUX_TYPE_DAB_GSE, stream_id: 171, dab_ip: '239.199.2.8', dab_port: 1234 },
    { uuid: 'n', name: '10717V NRK Reg1', parent_mux: 't', tstype: MUX_TYPE_DAB_GSE, stream_id: 171 },
    { uuid: 'o', name: '11034V' },
  ])
  const text = (q: string) => (n: Parameters<typeof muxSearchText>[0]) => muxSearchText(n).includes(q)

  it('shows a match with the nodes above it opened', () => {
    const f = filterTree(tree, text('riks'))
    expect([...f.show].sort()).toEqual(['r', 't', 't:isi:171'].sort())
    expect(f.open.has('t')).toBe(true)
    expect(f.open.has('t:isi:171')).toBe(true)
    expect(f.self.has('r')).toBe(true)
    expect(f.show.has('o')).toBe(false)
  })

  it('finds a mux by its address and type', () => {
    expect(filterTree(tree, text('239.199.2.8')).self.has('r')).toBe(true)
    expect(filterTree(tree, text('dab-gse')).self.size).toBe(2)
  })

  it('opens a mux for a match among its services', () => {
    const f = filterTree(tree, () => false, (n) => n.key === 'o')
    expect(f.show.has('o')).toBe(true)
    expect(f.open.has('o')).toBe(true)
  })
})
