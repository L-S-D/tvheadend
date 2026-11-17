// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Tvheadend contributors

import { describe, expect, it } from 'vitest'
import {
  aboveFloor,
  contentLines,
  gseLines,
  rateText,
  tableNames,
  decimate,
  graphMuxes,
  interpolate,
  levelAt,
  levelRange,
  matypeText,
  muxOfCarrier,
  muxWidth,
  polSelOf,
  ticks,
  triedAt,
  type TuneState,
} from '../blindscan'
import type { MuxRow } from '../muxTree'

describe('graphMuxes', () => {
  it('keeps the transponders, sorted, circular as H/V', () => {
    const rows: MuxRow[] = [
      {
        uuid: 'b',
        frequency: 12000000,
        polarisation: 'V',
        symbolrate: 27500000,
        scan_result: 1,
        enabled: 1,
        name: '12000V',
      },
      { uuid: 'a', frequency: 11000000, polarisation: 'L', symbolrate: 22000000, name: '11000L' },
      { uuid: 's', frequency: 12000000, polarisation: 'V', parent_mux: 'b', name: 'ISI 1' },
      { uuid: 'i', name: 'IPTV' },
    ]
    const m = graphMuxes(rows)
    expect(m.map((x) => [x.uuid, x.pol])).toEqual([
      ['a', 'H'],
      ['b', 'V'],
    ])
    expect(m[0].enabled).toBe(1)
    expect(m[1].scanResult).toBe(1)
  })
})

describe('muxOfCarrier', () => {
  const muxes = graphMuxes([
    { uuid: 'm1', frequency: 11461000, polarisation: 'H', symbolrate: 5780000 },
    { uuid: 'm2', frequency: 12535500, polarisation: 'H', symbolrate: 35294000 },
    { uuid: 'm3', frequency: 12535500, polarisation: 'V', symbolrate: 35294000 },
  ])
  const c = (freq: number, sr: number) => ({
    freq,
    sr,
    bw: (sr * 1.35) / 1000,
    snr: 900,
    peak: -5000,
  })

  it('finds the transponder within a quarter of the symbol rate', () => {
    expect(muxOfCarrier(c(11461063, 4315000), 'H', muxes)?.uuid).toBe('m1')
    expect(muxOfCarrier(c(12534201, 23926000), 'H', muxes)?.uuid).toBe('m2')
  })
  it('not on the other polarisation, not too far away', () => {
    expect(muxOfCarrier(c(11461063, 4315000), 'V', muxes)).toBeNull()
    expect(muxOfCarrier(c(11464963, 537000), 'H', muxes)).toBeNull()
  })
  it('a carrier in the band of a transponder is that one', () => {
    const wide = graphMuxes([
      { uuid: 'w', frequency: 11096000, polarisation: 'V', symbolrate: 29950000 },
    ])
    expect(muxOfCarrier(c(11091638, 15314815), 'V', wide)?.uuid).toBe('w')
    expect(muxOfCarrier(c(11120000, 5000000), 'V', wide)).toBeNull()
  })
  it('a narrow carrier in the band of a wide transponder is not it', () => {
    const wide = graphMuxes([
      { uuid: 'w', frequency: 11656000, polarisation: 'V', symbolrate: 22000000 },
    ])
    expect(muxOfCarrier(c(11643500, 850000), 'V', wide)).toBeNull()
    expect(muxOfCarrier(c(11655500, 21800000), 'V', wide)?.uuid).toBe('w')
  })
  it('1 MHz at least for narrow carriers', () => {
    expect(muxOfCarrier(c(11461900, 500000), 'H', muxes)?.uuid).toBe('m1')
  })
})

describe('decimate', () => {
  it('keeps the highest level of each bucket', () => {
    const f = [0, 1, 2, 3, 4, 5, 6, 7]
    const l = [1, 5, 2, 0, 9, 3, 3, 8]
    expect(decimate(f, l, 0, 8, 2)).toEqual([
      [1, 5],
      [4, 9],
    ])
  })
  it('only the range', () => {
    expect(decimate([0, 1, 2, 3], [4, 3, 2, 1], 1, 2, 10)).toEqual([
      [1, 3],
      [2, 2],
    ])
    expect(decimate([0, 1], [0, 0], 5, 4, 10)).toEqual([])
  })
})

describe('levelAt', () => {
  it('the nearest point', () => {
    expect(levelAt([0, 10, 20], [1, 2, 3], 6)).toBe(2)
    expect(levelAt([0, 10, 20], [1, 2, 3], 4)).toBe(1)
    expect(levelAt([0, 10, 20], [1, 2, 3], 25)).toBeNull()
  })
})

describe('ticks and ranges', () => {
  it('ticks on 1, 2, 5 steps', () => {
    expect(ticks(10700, 12750, 10)).toEqual([
      10800, 11000, 11200, 11400, 11600, 11800, 12000, 12200, 12400, 12600,
    ])
    expect(ticks(-66, -44, 6)).toEqual([-65, -60, -55, -50, -45])
    expect(ticks(5, 5, 3)).toEqual([])
  })
  it('level range with room', () => {
    expect(levelRange([{ freq: [1, 2, 3], level: [-6500, -5000, -4600] }], 0, 10)).toEqual([
      -6600, -4500,
    ])
    expect(levelRange([], 0, 10)).toEqual([-500, 2500])
  })
  it('the width of a carrier', () => {
    expect(muxWidth(27500000)).toBeCloseTo(37125)
  })
})

describe('the noise floor', () => {
  it('interpolates, the ends beyond', () => {
    expect(interpolate([0, 10, 20], [0, 100, 300], 5)).toBe(50)
    expect(interpolate([0, 10, 20], [0, 100, 300], 15)).toBe(200)
    expect(interpolate([0, 10, 20], [0, 100, 300], -5)).toBe(0)
    expect(interpolate([0, 10, 20], [0, 100, 300], 25)).toBe(300)
    expect(interpolate([7], [42], 3)).toBe(42)
  })
  it('levels above the floor along the band', () => {
    const p = aboveFloor({
      freq: [0, 10, 20],
      level: [-6000, -6500, -7000],
      noise: -9000,
      floor_freq: [0, 20],
      floor: [-8000, -9000],
    })
    expect(p.level).toEqual([2000, 2000, 2000])
    expect(p.freq).toEqual([0, 10, 20])
  })
  it('above the one noise level of an older result', () => {
    expect(aboveFloor({ freq: [0, 1], level: [-6000, -7000], noise: -8000 }).level).toEqual([
      2000, 1000,
    ])
    expect(aboveFloor({ freq: [0], level: [-6000] }).level).toEqual([-6000])
  })
})

describe('polSelOf', () => {
  it('H, V or both', () => {
    expect(polSelOf('HV')).toBe('HV')
    expect(polSelOf('VH')).toBe('HV')
    expect(polSelOf('H')).toBe('H')
    expect(polSelOf('V')).toBe('V')
    expect(polSelOf('')).toBeNull()
    expect(polSelOf(undefined)).toBeNull()
  })
})

describe('triedAt', () => {
  const tune = (reqFreq: number, freq: number, pol: 'H' | 'V', state: TuneState['state']) => ({
    reqFreq,
    freq,
    pol,
    state,
    text: '',
  })
  it('the last tune at a carrier, by the frequency asked for or locked', () => {
    const tunes = [
      tune(11643500, 11643500, 'V', 'fail'),
      tune(11067163, 11067214, 'H', 'nodata'),
      tune(11643400, 11643450, 'V', 'ok'),
    ]
    expect(triedAt(tunes, 'V', 11643500, 400)?.state).toBe('ok')
    expect(triedAt(tunes, 'H', 11067200, 5700)?.state).toBe('nodata')
    expect(triedAt(tunes, 'H', 11643500, 400)).toBeNull()
    expect(triedAt(tunes, 'V', 11650000, 400)).toBeNull()
  })
  it('a running tune by the frequency asked for only', () => {
    expect(triedAt([tune(12000000, 0, 'H', 'scanning')], 'H', 12000100, 100)?.state).toBe(
      'scanning'
    )
    expect(triedAt([tune(12000000, 0, 'H', 'scanning')], 'H', 0, 100)).toBeNull()
  })
})

describe('matypeText', () => {
  it('stream type, multistream, ACM', () => {
    expect(matypeText(0xf2)).toBe('TS')
    expect(matypeText(0x72)).toBe('generic continuous')
    expect(matypeText(0xd0)).toBe('TS, multistream')
    expect(matypeText(0x02)).toBe('generic packetized, multistream, ACM/VCM')
    expect(matypeText(0xb3)).toBe('GSE-HEM')
    expect(matypeText(-1)).toBe('')
    expect(matypeText(undefined)).toBe('')
  })
})

describe('the content of a tune', () => {
  const t = (text: string, ...args: string[]) =>
    text.replace(/\{(\d+)\}/g, (_, i: string) => args[Number(i)] ?? '')
  it('names tables once, rates', () => {
    expect(tableNames([0x4e, 0x50, 0x51, 0x00, 0x99])).toEqual(['EIT', 'PAT', '0x99'])
    expect(rateText(1504 * 1000, 1e6)).toBe('1.5 Mbit/s')
    expect(rateText(1504 * 100, 1e6)).toBe('150 kbit/s')
    expect(rateText(1, 0)).toBe('')
  })
  it('a TS: the total, then the PIDs', () => {
    const l = contentLines(
      {
        us: 2e6,
        packets: 10000,
        nulls: 1000,
        tei: 0,
        pids: 3,
        list: [
          { pid: 256, packets: 6000, stream_id: 0xe0, es: 'h264' },
          { pid: 18, packets: 500, tables: [0x4e, 0x50] },
          { pid: 512, packets: 2500, scrambled: 2500, cc: 2 },
        ],
      },
      t
    )
    expect(l[0]).toBe('7.5 Mbit/s in 2.0 s, 3 PIDs, null packets 10 %')
    expect(l[1]).toBe('PID 256 (0x0100): 4.5 Mbit/s · H.264 video')
    expect(l[2]).toBe('PID 18 (0x0012): 376 kbit/s · sections EIT')
    expect(l[3]).toBe('PID 512 (0x0200): 1.9 Mbit/s · scrambled 100 % · 2 continuity errors')
  })
  it('the BBFrames of a generic stream', () => {
    const l = contentLines(
      {
        us: 1e6,
        packets: 4000,
        nulls: 0,
        tei: 0,
        pids: 1,
        list: [
          {
            pid: 270,
            packets: 4000,
            bbframes: { frames: 800, hem: 800, dfl_bits: 8e6, matype: 0xb3 },
          },
        ],
      },
      t
    )
    expect(l[1]).toBe(
      'PID 270 (0x010E): 6.0 Mbit/s · BBFrames of the demodulator: 800/s · data 8.0 Mbit/s · GSE-HEM · high efficiency mode'
    )
    expect(contentLines({ us: 0, packets: 0, nulls: 0, tei: 0, pids: 0 }, t)).toEqual([
      'No TS packets',
    ])
  })
})

describe('what generic streams carry', () => {
  const t = (text: string, ...args: string[]) =>
    text.replace(/\{(\d+)\}/g, (_, i: string) => args[Number(i)] ?? '')
  const base = {
    packets: 0,
    pdus: 0,
    fragmented: 0,
    crc_errors: 0,
    length_errors: 0,
    corrupt: 0,
    hem_errors: 0,
    protocols: [],
    labels: { six: 0, three: 0, broadcast: 0, distinct: 0 },
    ip: {},
    flows: [],
  }
  it('GSE: protocols, labels, multicast groups', () => {
    const l = gseLines(
      {
        ...base,
        frames: [0, 100, 0, 0],
        packets: 50,
        pdus: 40,
        fragmented: 5,
        crc_errors: 1,
        protocols: [{ type: 0x0800, pdus: 40, bytes: 125000 }],
        labels: { six: 0, three: 0, broadcast: 40, distinct: 0 },
        ip: { v4_multicast: { packets: 40, bytes: 125000 } },
        flows: [
          { addr: '239.1.2.3', port: 5000, packets: 40, bytes: 120000, kind: 'rtp-ts', rtp_pt: 33 },
        ],
      },
      1e6,
      t
    )
    expect(l).toEqual([
      'Generic stream frames: 100 generic continuous',
      'GSE: 50 packets, 40 PDUs (5 put together from fragments) · 1 CRC errors',
      'Protocols: IPv4 100 % (1.0 Mbit/s)',
      'Labels: 0 of 6 bytes, 0 of 3 bytes, 40 broadcast; 0 different',
      'IPv4 multicast 1.0 Mbit/s',
      '239.1.2.3:5000: 960 kbit/s · MPEG-TS in RTP',
    ])
  })
  it('a generic stream of 188 byte packets: its TS', () => {
    const l = gseLines(
      {
        ...base,
        frames: [0, 57, 0, 0],
        frames_upl: 57,
        upl: 1504,
        isis: [0],
        ts: {
          us: 1e6,
          packets: 59,
          nulls: 0,
          tei: 0,
          pids: 1,
          list: [{ pid: 0, packets: 59, tables: [0] }],
        },
      },
      1e6,
      t
    )
    expect(l[0]).toBe('Generic stream frames: 57 generic continuous · ISI 0')
    expect(l[1]).toBe('57 frames with user packets of 188 bytes: no GSE')
    expect(l[2]).toBe('The TS in them:')
    expect(l[4]).toBe('  PID 0 (0x0000): 89 kbit/s · sections PAT')
    expect(l.length).toBe(5)
  })
})
