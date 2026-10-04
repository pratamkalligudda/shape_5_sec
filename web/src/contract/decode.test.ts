import { describe, expect, it } from 'vitest';
import { decodeEnvelope } from './decode';
import { batch, envelope } from '../test/fixtures';

describe('decodeEnvelope', () => {
  it('inflates a gzip+base64 envelope into the batch and reports both sizes', async () => {
    const b = batch(0, { P: 1 });
    const env = envelope(b);

    const decoded = await decodeEnvelope(env);

    expect(decoded.batch).toEqual(b);
    expect(decoded.decodedBytes).toBe(new TextEncoder().encode(JSON.stringify(b)).length);
    expect(decoded.envelopeBytes).toBe(JSON.stringify(env).length);
  });

  it('rejects any other encoding', async () => {
    await expect(decodeEnvelope({ encoding: 'json', payload: '{}' })).rejects.toThrow('Unsupported envelope encoding: json');
  });
});
