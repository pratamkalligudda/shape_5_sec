import type { LiveBatch, LiveEnvelope } from './types';

export interface DecodedBatch {
  batch: LiveBatch;
  envelopeBytes: number;
  decodedBytes: number;
}

/** base64 → gzip inflate → UTF-8 → JSON, with DecompressionStream as the integration doc prescribes. */
export async function decodeEnvelope(envelope: LiveEnvelope): Promise<DecodedBatch> {
  if (envelope.encoding !== 'gzip+base64') throw new Error(`Unsupported envelope encoding: ${envelope.encoding}`);
  const compressed = Uint8Array.from(atob(envelope.payload), c => c.charCodeAt(0));
  const stream = new Blob([compressed]).stream().pipeThrough(new DecompressionStream('gzip'));
  const bytes = new Uint8Array(await new Response(stream).arrayBuffer());
  return {
    batch: JSON.parse(new TextDecoder().decode(bytes)) as LiveBatch,
    envelopeBytes: JSON.stringify(envelope).length,
    decodedBytes: bytes.length,
  };
}
