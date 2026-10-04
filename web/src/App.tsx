import { useMemo } from 'react';
import type { FieldMap } from './contract/types';
import { ContractInspector } from './components/ContractInspector';
import { ErrorBanner } from './components/ErrorBanner';
import { Header } from './components/Header';
import { HoursCards } from './components/HoursCards';
import { LiveMetrics } from './components/LiveMetrics';
import { OpenedWithBanner } from './components/OpenedWithBanner';
import { ParameterGraph } from './components/ParameterGraph';
import { SimPanel } from './components/SimPanel';
import { useLiveSession } from './live/useLiveSession';

export function App() {
  const live = useLiveSession();
  const lastFrame = live.timeline.frames[live.timeline.frames.length - 1];
  // The /series seed carries only the charted metrics; GET's snapshot fills the rest until live frames arrive.
  const latest: FieldMap = useMemo(() => ({ ...(live.status?.snapshot ?? {}), ...(lastFrame?.values ?? {}) }), [live.status, lastFrame]);

  return (
    <div className="page">
      <Header status={live.status} connection={live.connection} onStart={live.start} onExtend={live.extend} onStop={live.stop} />
      {live.error && <ErrorBanner error={live.error} onDismiss={live.dismissError} />}
      <OpenedWithBanner events={live.timeline.events} />
      <main className="layout">
        <div className="main-col">
          <ParameterGraph timeline={live.timeline} intervalMs={live.intervalMs} />
          <LiveMetrics latest={latest} timeline={live.timeline} sessionId={live.status?.active ? live.status.sessionId : null} />
        </div>
        <aside className="side-col">
          <HoursCards latest={latest} />
          <SimPanel />
          <ContractInspector inspector={live.inspector} />
        </aside>
      </main>
    </div>
  );
}
