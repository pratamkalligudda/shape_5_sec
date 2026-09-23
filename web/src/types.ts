export type FoldedSample = {
  ts: string
  values: Record<string, unknown>
  changed: string[]
}

export type FoldedBatch = {
  sessionId: string
  batchSeq: number
  gap: boolean
  partial: boolean
  samples: FoldedSample[]
}
