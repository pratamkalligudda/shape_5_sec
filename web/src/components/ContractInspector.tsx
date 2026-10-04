import type { InspectorState } from '../live/useLiveSession';

/** Dev panel: the last decoded batch, its sizes, and every contract violation seen (spec §8.2). */
export function ContractInspector({ inspector }: { inspector: InspectorState }) {
  const last = inspector.last;
  return (
    <section className="card">
      <h2>Contract inspector</h2>
      <dl className="facts">
        <dt>Batches</dt><dd>{inspector.batches}</dd>
        <dt>Violations</dt><dd data-testid="violation-count" className={inspector.violationCount ? 'error' : ''}>{inspector.violationCount}</dd>
        <dt>Envelope</dt><dd>{last ? `${last.envelopeBytes.toLocaleString()} B` : '—'}</dd>
        <dt>Decoded</dt><dd>{last ? `${last.decodedBytes.toLocaleString()} B` : '—'}</dd>
        <dt>Entries</dt><dd>{last ? last.batch.entries.length : '—'}</dd>
      </dl>
      {inspector.violations.length > 0 && (
        <ul className="violations">{inspector.violations.map((v, i) => <li key={i}>{v}</li>)}</ul>
      )}
      {last && (
        <details>
          <summary>Last decoded batch</summary>
          <pre>{JSON.stringify(last.batch, null, 2)}</pre>
        </details>
      )}
    </section>
  );
}
