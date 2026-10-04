/**
 * Paces playback: one item per tick. A backlog above maxBacklog (a background tab, a burst after reconnect) is
 * released at once down to the newest drainTo, so the chart catches up without replaying minutes of history.
 */
export class FrameQueue<T> {
  private items: T[] = [];

  constructor(private readonly maxBacklog = 10, private readonly drainTo = 5) {}

  get size(): number {
    return this.items.length;
  }

  push(items: T[]): void {
    this.items.push(...items);
  }

  tick(): T[] {
    if (this.items.length > this.maxBacklog) return this.items.splice(0, this.items.length - this.drainTo);
    return this.items.length > 0 ? this.items.splice(0, 1) : [];
  }

  clear(): void {
    this.items = [];
  }
}
