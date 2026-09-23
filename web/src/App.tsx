import { useState } from 'react'
import { useTelemetry } from './useTelemetry'
import { toRows } from './rows'
import { Header } from './components/Header'
import { TrendChart } from './components/TrendChart'
import { StatusPanel } from './components/StatusPanel'
import { SessionControls } from './components/SessionControls'
import { RawDrawer } from './components/RawDrawer'

const FUNCTIONS_URL = import.meta.env.VITE_FUNCTIONS_URL ?? 'http://localhost:7071'
const SIMULATOR_URL = import.meta.env.VITE_SIMULATOR_URL ?? 'http://localhost:5100'

export default function App() {
  const { queue, conn } = useTelemetry(FUNCTIONS_URL)
  const [unit, setUnit] = useState<'PSI' | 'Bar'>('PSI')
  const [, setWindowTick] = useState(0)

  const latest = queue.latest
  const end = latest ? Date.parse(latest.ts) : Date.now()
  const domain: [number, number] = [end - queue.windowSeconds * 1000, end]
  const num = (k: string) => (typeof latest?.values[k] === 'number' ? (latest.values[k] as number) : null)

  const pressureKey = `OperatingPressure${unit}`
  const high = num(`HighPressureSetpoint${unit}`)
  const low = num(`LowPressureSetpoint${unit}`)
  const refLines = [
    ...(high !== null ? [{ y: high, label: 'high setpoint' }] : []),
    ...(low !== null ? [{ y: low, label: 'low setpoint' }] : []),
  ]

  return (
    <main>
      <Header
        conn={conn}
        sessionId={queue.sessionId}
        partial={queue.partial}
        lagSeconds={latest ? (Date.now() - Date.parse(latest.ts)) / 1000 : null}
        queueDepth={queue.queueDepth}
        state={queue.state}
        windowSeconds={queue.windowSeconds}
        onWindowChange={s => { queue.setWindow(s); setWindowTick(x => x + 1) }}
      />
      <div className="grid">
        <div className="charts">
          <div className="unit-toggle">
            <button className={unit === 'PSI' ? 'on' : ''} onClick={() => setUnit('PSI')}>PSI</button>
            <button className={unit === 'Bar' ? 'on' : ''} onClick={() => setUnit('Bar')}>Bar</button>
          </div>
          <TrendChart title={`Operating pressure (${unit})`} rows={toRows(queue.points, [pressureKey])} domain={domain}
            sessionStarts={queue.sessionStarts} refLines={refLines}
            series={[{ key: pressureKey, name: pressureKey, color: 'var(--s1)' }]} />
          <TrendChart title="Stroke rate" rows={toRows(queue.points, ['StrokeRateLeft', 'StrokeRateRight'])} domain={domain}
            sessionStarts={queue.sessionStarts}
            series={[
              { key: 'StrokeRateLeft', name: 'Left', color: 'var(--s1)' },
              { key: 'StrokeRateRight', name: 'Right', color: 'var(--s2)' },
            ]} />
          <TrendChart title="Oil temperature & proportional output"
            rows={toRows(queue.points, ['HydraulicOilTemperatureF', 'ProportionalOutputValue'])} domain={domain}
            sessionStarts={queue.sessionStarts}
            series={[
              { key: 'HydraulicOilTemperatureF', name: 'Oil temp °F', color: 'var(--s3)' },
              { key: 'ProportionalOutputValue', name: 'Prop. output', color: 'var(--s2)', axis: 'right' },
            ]} />
        </div>
        <aside>
          <SessionControls simulatorUrl={SIMULATOR_URL} />
          <StatusPanel latest={latest} />
        </aside>
      </div>
      <RawDrawer latest={latest} />
    </main>
  )
}
