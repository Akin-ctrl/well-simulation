import { AlertTriangleIcon, CheckCircle2Icon, CircleSlashIcon } from 'lucide-react';
import { useElapsedSeconds } from '../../../hooks/use-elapsed-seconds';

/**
 * Whether the telemetry on screen is current.
 *
 * The pipeline stops writing when its source goes stale rather than recording
 * frozen values as fresh readings, so an outage shows up as readings that stop
 * arriving. Without this indicator the dashboard would keep showing the last
 * good values with nothing to say they had stopped updating, which is the
 * failure the Phase 1 heartbeat work exists to prevent.
 *
 * The age is computed on the server. A wrong clock in the browser would
 * otherwise make stale data look fresh.
 */

/** Older than this and the data is treated as stale, in seconds. */
export const STALE_AFTER_SECONDS = 30;

function describe(ageSeconds: number): string {
  if (ageSeconds < 60) return `${ageSeconds}s ago`;
  if (ageSeconds < 3600) return `${Math.floor(ageSeconds / 60)}m ago`;
  return `${Math.floor(ageSeconds / 3600)}h ago`;
}

export function FreshnessIndicator({
  ageSeconds,
  latestReadingAt,
  announce = false,
}: {
  ageSeconds: number | null;
  latestReadingAt: string | null;
  announce?: boolean;
}) {
  const elapsed = useElapsedSeconds(`${latestReadingAt}:${ageSeconds}`);
  const category =
    ageSeconds === null || !latestReadingAt
      ? 'No readings ingested'
      : ageSeconds + elapsed > STALE_AFTER_SECONDS
        ? 'Telemetry is stale'
        : 'Telemetry is current';
  const announcement = announce ? (
    <span className='sr-only' role='status'>
      {category}
    </span>
  ) : null;
  if (ageSeconds === null || !latestReadingAt) {
    return (
      <span className='inline-flex items-center gap-1.5 text-sm text-fgColor-muted'>
        <CircleSlashIcon aria-hidden='true' className='size-4' />
        {announcement}
        No readings ingested
      </span>
    );
  }

  const currentAge = ageSeconds + elapsed;
  const isStale = currentAge > STALE_AFTER_SECONDS;

  return (
    <span
      className={`inline-flex items-center gap-1.5 text-sm ${
        isStale ? 'text-red-700' : 'text-green-700'
      }`}
      title={new Date(latestReadingAt).toLocaleString()}
    >
      {announcement}
      {isStale ? (
        <AlertTriangleIcon aria-hidden='true' className='size-4' />
      ) : (
        <CheckCircle2Icon aria-hidden='true' className='size-4' />
      )}
      {isStale
        ? `Telemetry stale, last reading ${describe(currentAge)}`
        : `Live, updated ${describe(currentAge)}`}
    </span>
  );
}
