import type { FoldedSample } from '../types'

export function RawDrawer({ latest }: { latest: FoldedSample | null }) {
  if (!latest) return null
  const changed = new Set(latest.changed)
  return (
    <details className="card">
      <summary>Raw folded snapshot · {new Date(latest.ts).toLocaleTimeString()} · {latest.changed.length} changed</summary>
      <pre className="raw">
        {Object.entries(latest.values).map(([k, v]) => (
          <div key={k} className={changed.has(k) ? 'hl' : ''}>{k}: {JSON.stringify(v)}</div>
        ))}
      </pre>
    </details>
  )
}
