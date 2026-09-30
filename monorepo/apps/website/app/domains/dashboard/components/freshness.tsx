import { AlertTriangleIcon, CheckCircle2Icon, CircleSlashIcon } from 'lucide-react';

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
const STALE_AFTER_SECONDS = 30;

function describe(ageSeconds: number): string {
  if (ageSeconds < 60) return `${ageSeconds}s ago`;
  if (ageSeconds < 3600) return `${Math.floor(ageSeconds / 60)}m ago`;
  return `${Math.floor(ageSeconds / 3600)}h ago`;
}

export function FreshnessIndicator({
  ageSeconds,
  latestReadingAt,
}: {
  ageSeconds: number | null;
  latestReadingAt: string | null;
}) {
  if (ageSeconds === null || !latestReadingAt) {
    return (
      <span className='inline-flex items-center gap-1.5 text-sm text-fgColor-muted'>
        <CircleSlashIcon className='size-4' />
        No readings ingested
      </span>
    );
  }

  const isStale = ageSeconds > STALE_AFTER_SECONDS;

  return (
    <span
      className={`inline-flex items-center gap-1.5 text-sm ${
        isStale ? 'text-red-600' : 'text-green-700'
      }`}
      title={new Date(latestReadingAt).toLocaleString()}
    >
      {isStale ? (
        <AlertTriangleIcon className='size-4' />
      ) : (
        <CheckCircle2Icon className='size-4' />
      )}
      {isStale
        ? `Telemetry stale, last reading ${describe(ageSeconds)}`
        : `Live, updated ${describe(ageSeconds)}`}
    </span>
  );
}
