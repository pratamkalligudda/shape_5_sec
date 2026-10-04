import { describe, expect, it } from 'vitest';
import { FrameQueue } from './FrameQueue';

describe('FrameQueue', () => {
  it('releases one item per tick and nothing when empty', () => {
    const q = new FrameQueue<number>();
    q.push([1, 2, 3]);
    expect([q.tick(), q.tick(), q.tick(), q.tick()]).toEqual([[1], [2], [3], []]);
  });

  it('drains a backlog above 10 down to the newest 5 in one tick', () => {
    const q = new FrameQueue<number>();
    q.push(Array.from({ length: 12 }, (_, i) => i));

    expect(q.tick()).toEqual([0, 1, 2, 3, 4, 5, 6]);
    expect(q.size).toBe(5);
    expect(q.tick()).toEqual([7]);
  });

  it('does not drain at exactly 10', () => {
    const q = new FrameQueue<number>();
    q.push(Array.from({ length: 10 }, (_, i) => i));
    expect(q.tick()).toEqual([0]);
  });
});
