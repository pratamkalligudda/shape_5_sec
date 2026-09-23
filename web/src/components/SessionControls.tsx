import { useEffect, useState } from 'react'

type Response = { status: number; body: string } | { error: string }

export function SessionControls({ simulatorUrl }: { simulatorUrl: string }) {
  const [duration, setDuration] = useState(10)
  const [response, setResponse] = useState<Response | null>(null)
  const [active, setActive] = useState<boolean | null>(null)

  const refresh = async () => {
    try {
      const res = await fetch(`${simulatorUrl}/fast-polling/status`)
      setActive((await res.json()).active)
    } catch {
      setActive(null)
    }
  }

  const call = async (path: string, body?: unknown) => {
    try {
      const res = await fetch(`${simulatorUrl}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
      })
      setResponse({ status: res.status, body: await res.text() })
    } catch (e) {
      setResponse({ error: `simulator unreachable: ${String(e)}` })
    }
    void refresh()
  }

  useEffect(() => { void refresh() }, [])

  return (
    <section className="card">
      <h2>Session {active === null ? '(simulator offline)' : active ? '(active)' : '(idle)'}</h2>
      <label>
        duration (min){' '}
        <input type="number" min={10} max={30} value={duration} onChange={e => setDuration(Number(e.target.value))} />
      </label>{' '}
      <button onClick={() => call('/fast-polling/enable', { sessionDurationMinutes: duration })}>Enable</button>{' '}
      <button onClick={() => call('/fast-polling/disable')}>Disable</button>
      {response && (
        <pre className="response">
          {'error' in response ? response.error : `${response.status}\n${response.body}`}
        </pre>
      )}
    </section>
  )
}
