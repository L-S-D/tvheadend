// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Tvheadend contributors

import { describe, expect, it } from 'vitest'
import {
  aboveFloor,
  decimate,
  graphMuxes,
  interpolate,
  levelAt,
  levelRange,
  muxOfCarrier,
  muxWidth,
  polSelOf,
  ticks,
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
