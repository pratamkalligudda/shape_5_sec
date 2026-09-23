import { useEffect, useRef, useState } from 'react'
import { HubConnectionBuilder, LogLevel } from '@microsoft/signalr'
import { ReplayQueue } from './ReplayQueue'
import type { FoldedBatch } from './types'

export type ConnState = 'connecting' | 'live' | 'reconnecting' | 'down'

/** Connects to the SignalR hub via the Functions negotiate endpoint and ticks the replay queue every second. */
export function useTelemetry(functionsBaseUrl: string) {
  const queueRef = useRef(new ReplayQueue())
  const [conn, setConn] = useState<ConnState>('connecting')
  const [, setFrame] = useState(0)

  useEffect(() => {
    const hub = new HubConnectionBuilder()
      .withUrl(`${functionsBaseUrl}/api`)
      .withAutomaticReconnect([0, 2000, 5000, 10000, 30000])
      .configureLogging(LogLevel.Warning)
      .build()

    hub.on('batch', (batch: FoldedBatch) => queueRef.current.push(batch))
    hub.onreconnecting(() => setConn('reconnecting'))
    hub.onreconnected(() => setConn('live'))
    hub.onclose(() => setConn('down'))

    let disposed = false
    let retry: ReturnType<typeof setTimeout> | undefined
    const start = () => {
      setConn('connecting')
      hub.start()
        .then(() => !disposed && setConn('live'))
        .catch(() => {
          if (disposed) return
          setConn('down')
          retry = setTimeout(start, 5000) // automatic reconnect only covers drops after a first connect
        })
    }
    start()

    const timer = setInterval(() => {
      queueRef.current.tick()
      setFrame(f => f + 1)
    }, 1000)

    return () => {
      disposed = true
      clearInterval(timer)
      clearTimeout(retry)
      void hub.stop()
    }
  }, [functionsBaseUrl])

  return { queue: queueRef.current, conn }
}
