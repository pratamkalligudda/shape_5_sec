import type { TimedEvent } from '../live/frames';
import { eventText } from '../live/events';

/** `active` events: the condition the session opened in. Shown here, never as a transition marker. */
export function OpenedWithBanner({ events }: { events: TimedEvent[] }) {
  const active = events.filter(e => e.event.state === 'active');
  if (active.length === 0) return null;
  return (
    <div className="banner banner-info">
      <strong>Opened with:</strong> {active.map(e => eventText(e.event)).join(' · ')}
    </div>
  );
}
