import type { FoldedBatch, FoldedSample } from './types'

export type Point = { t: number; sample: FoldedSample | null } // sample null = line break
export type QueueState = 'waiting' | 'playing' | 'buffering'

type Item = { sample: FoldedSample; breakBefore: boolean; sessionStart: boolean }

/**
 * Buffers 5-sample batches and releases one sample per tick (1 s), so the chart moves second by second.
 * Pure: the caller owns the timer.
 */
export class ReplayQueue {
  private items: Item[] = []
  private seen = new Set<string>()
  private retired = new Set<string>()
  private current: string | null = null
  private currentStart = Number.NEGATIVE_INFINITY // ts of the first sample seen for the current session
  private currentPartial = false
  private _points: Point[] = []
  private _sessionStarts: number[] = []
  private _latest: FoldedSample | null = null
  private _state: QueueState = 'waiting'
  private started = false
  private _windowSeconds: number

  private readonly maxBacklog: number
  private readonly keepOnCatchUp: number

  // No TS parameter properties: the Vite template enables `erasableSyntaxOnly`.
  constructor(windowSeconds = 300, maxBacklog = 10, keepOnCatchUp = 5) {
    this._windowSeconds = windowSeconds
    this.maxBacklog = maxBacklog
    this.keepOnCatchUp = keepOnCatchUp
  }

  push(batch: FoldedBatch): void {
    const key = `${batch.sessionId}:${batch.batchSeq}`
    if (this.seen.has(key) || this.retired.has(batch.sessionId) || batch.samples.length === 0) return
    this.seen.add(key)

    const newSession = batch.sessionId !== this.current
    if (newSession) {
      // A session we have never seen may still be older than the current one (e.g. a late flush from before a
      // page reload). Order sessions by sample time, not arrival: data that ends before the current session began
      // belongs to a superseded session.
      const batchEnd = Date.parse(batch.samples[batch.samples.length - 1].ts)
      if (this.current && batchEnd < this.currentStart) {
        this.retired.add(batch.sessionId)
        return
      }
      if (this.current) this.retired.add(this.current)
      this.current = batch.sessionId
      this.currentStart = Date.parse(batch.samples[0].ts)
    }
    this.currentPartial = batch.partial

    batch.samples.forEach((sample, i) =>
      this.items.push({
        sample,
        breakBefore: i === 0 && (batch.gap || newSession),
        sessionStart: i === 0 && newSession && batch.batchSeq === 0,
      }),
    )
    this.started = true
  }

  tick(): void {
    if (!this.started) return
    if (this.items.length === 0) {
      this._state = 'buffering'
      return
    }
    if (this.items.length > this.maxBacklog) {
      while (this.items.length > this.keepOnCatchUp) this.play(this.items.shift()!)
    }
    this.play(this.items.shift()!)
    this._state = 'playing'
    this.trim()
  }

  setWindow(seconds: number): void {
    this._windowSeconds = seconds
    this.trim()
  }

  get points(): Point[] { return this._points }
  get sessionStarts(): number[] { return this._sessionStarts }
  get latest(): FoldedSample | null { return this._latest }
  get state(): QueueState { return this._state }
  get queueDepth(): number { return this.items.length }
  get sessionId(): string | null { return this.current }
  get partial(): boolean { return this.currentPartial }
  get windowSeconds(): number { return this._windowSeconds }

  private play(item: Item): void {
    const t = Date.parse(item.sample.ts)
    if (item.breakBefore && this._points.length > 0) this._points.push({ t: t - 1, sample: null })
    if (item.sessionStart) this._sessionStarts.push(t)
    this._points.push({ t, sample: item.sample })
    this._latest = item.sample
  }

  private trim(): void {
    if (!this._latest) return
    const cutoff = Date.parse(this._latest.ts) - this._windowSeconds * 1000
    const firstKept = this._points.findIndex(p => p.t >= cutoff)
    this._points = firstKept === -1 ? [] : this._points.slice(firstKept)
    this._sessionStarts = this._sessionStarts.filter(t => t >= cutoff)
  }
}
