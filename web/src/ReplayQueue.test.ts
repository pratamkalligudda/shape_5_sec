import { describe, expect, it } from 'vitest'
import { ReplayQueue } from './ReplayQueue'
import type { FoldedBatch } from './types'

const T0 = Date.parse('2026-09-23T12:00:00Z')

function batch(seq: number, opts: { session?: string; n?: number; gap?: boolean; startSec?: number } = {}): FoldedBatch {
  const n = opts.n ?? 5
  const start = opts.startSec ?? seq * 5
  return {
    sessionId: opts.session ?? 's1',
    batchSeq: seq,
    gap: opts.gap ?? false,
    partial: false,
    samples: Array.from({ length: n }, (_, i) => ({
      ts: new Date(T0 + (start + i) * 1000).toISOString(),
      values: { P: start + i },
      changed: ['P'],
    })),
  }
}

const values = (q: ReplayQueue) => q.points.map(p => (p.sample ? (p.sample.values.P as number) : null))

describe('ReplayQueue', () => {
  it('waits before the first batch', () => {
    const q = new ReplayQueue()
    q.tick()
    expect(q.state).toBe('waiting')
    expect(q.points).toHaveLength(0)
  })

  it('plays one sample per tick', () => {
    const q = new ReplayQueue()
    q.push(batch(0))
    expect(q.queueDepth).toBe(5)
    q.tick()
    q.tick()
    expect(values(q)).toEqual([0, 1])
    expect(q.queueDepth).toBe(3)
    expect(q.state).toBe('playing')
    expect(q.latest?.values.P).toBe(1)
  })

  it('goes buffering when starved and resumes', () => {
    const q = new ReplayQueue()
    q.push(batch(0, { n: 1 }))
    q.tick()
    q.tick()
    expect(q.state).toBe('buffering')
    expect(q.points).toHaveLength(1)
    q.push(batch(1, { n: 1, startSec: 1 }))
    q.tick()
    expect(q.state).toBe('playing')
    expect(values(q)).toEqual([0, 1])
  })

  it('dedupes by session and batchSeq', () => {
    const q = new ReplayQueue()
    q.push(batch(0))
    q.push(batch(0))
    expect(q.queueDepth).toBe(5)
  })

  it('catches up on backlog, keeping the newest 5 queued', () => {
    const q = new ReplayQueue()
    q.push(batch(0))
    q.push(batch(1))
    q.push(batch(2)) // 15 queued > 10
    q.tick()
    expect(q.points).toHaveLength(11) // 10 drained + 1 played
    expect(q.queueDepth).toBe(4)
  })

  it('inserts a null break before a gap batch', () => {
    const q = new ReplayQueue()
    q.push(batch(0, { n: 1 }))
    q.push(batch(3, { n: 1, startSec: 15, gap: true }))
    q.tick()
    q.tick()
    expect(values(q)).toEqual([0, null, 15])
    expect(q.points[1].t).toBe(q.points[2].t - 1)
  })

  it('does not insert a leading null for a gap on the very first batch', () => {
    const q = new ReplayQueue()
    q.push(batch(7, { n: 1, gap: true }))
    q.tick()
    expect(values(q)).toEqual([35])
  })

  it('records a session start marker only for batchSeq 0 and breaks the line between sessions', () => {
    const q = new ReplayQueue()
    q.push(batch(4, { n: 1, session: 'old' })) // joined mid-session: no marker
    q.push(batch(0, { n: 1, session: 'new', startSec: 100 }))
    q.tick()
    q.tick()
    expect(q.sessionStarts).toEqual([T0 + 100_000])
    expect(values(q)).toEqual([20, null, 100])
    expect(q.sessionId).toBe('new')
  })

  it('ignores late batches from a superseded session', () => {
    const q = new ReplayQueue()
    q.push(batch(0, { n: 1, session: 'a' }))
    q.push(batch(0, { n: 1, session: 'b', startSec: 50 }))
    q.push(batch(1, { n: 1, session: 'a', startSec: 5 }))
    expect(q.queueDepth).toBe(2)
    expect(q.sessionId).toBe('b')
  })

  it('ignores an older unseen session arriving after the current one', () => {
    const q = new ReplayQueue()
    q.push(batch(0, { n: 1, session: 'b', startSec: 100 }))
    q.push(batch(3, { n: 1, session: 'a', startSec: 15 })) // late flush from a session this page never saw
    q.push(batch(1, { n: 1, session: 'b', startSec: 101 }))
    expect(q.sessionId).toBe('b')
    expect(q.queueDepth).toBe(2)
  })

  it('still switches to a genuinely newer unseen session', () => {
    const q = new ReplayQueue()
    q.push(batch(4, { n: 1, session: 'a', startSec: 20 }))
    q.push(batch(0, { n: 1, session: 'b', startSec: 100 }))
    expect(q.sessionId).toBe('b')
  })

  it('plays partial batches sample by sample', () => {
    const q = new ReplayQueue()
    q.push(batch(0, { n: 2 }))
    q.tick()
    q.tick()
    q.tick()
    expect(values(q)).toEqual([0, 1])
    expect(q.state).toBe('buffering')
  })

  it('trims points and markers outside the window', () => {
    const q = new ReplayQueue(10)
    q.push(batch(0))
    q.push(batch(1))
    q.push(batch(2, { n: 5 }))
    for (let i = 0; i < 15; i++) q.tick()
    const ts = q.points.map(p => p.t)
    const latest = Date.parse(q.latest!.ts)
    expect(Math.min(...ts)).toBeGreaterThanOrEqual(latest - 10_000)
    expect(q.sessionStarts).toEqual([])
  })

  it('exposes partial flag of the current session', () => {
    const q = new ReplayQueue()
    q.push({ ...batch(3), partial: true })
    expect(q.partial).toBe(true)
  })
})
