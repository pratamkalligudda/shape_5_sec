import type { UiError } from '../live/useLiveSession';

export function ErrorBanner({ error, onDismiss }: { error: UiError; onDismiss: () => void }) {
  return (
    <div className="banner banner-error" role="alert">
      <strong>{error.code}</strong> {error.message}
      <button className="link" onClick={onDismiss}>Dismiss</button>
    </div>
  );
}
