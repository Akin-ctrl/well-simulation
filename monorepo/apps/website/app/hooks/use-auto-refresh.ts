import { useEffect } from 'react';
import { useRevalidator } from 'react-router';

/**
 * Re-run the current route's loader on a timer.
 *
 * Every route loads through `clientLoader`, so revalidating the route is how
 * the page gets new data. Before this the dashboard never refreshed at all: it
 * showed whatever was true when the page was opened, while a mounted React
 * Query provider implied otherwise. That provider has been removed.
 *
 * Refreshing pauses while the tab is hidden. A background tab polling an
 * operations API for hours is waste, and the first thing it does on return is
 * fetch anyway.
 */
export function useAutoRefresh(intervalMs: number): void {
  const revalidator = useRevalidator();

  useEffect(() => {
    const tick = () => {
      if (document.visibilityState !== 'visible') return;
      // Skip if a revalidation is already in flight, so a slow response cannot
      // stack requests on top of each other.
      if (revalidator.state !== 'idle') return;
      revalidator.revalidate();
    };

    const onVisible = () => {
      if (document.visibilityState === 'visible') tick();
    };

    const timer = window.setInterval(tick, intervalMs);
    document.addEventListener('visibilitychange', onVisible);

    return () => {
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisible);
    };
    // revalidator is recreated on each render; depending on it would reset the
    // timer constantly. The interval is the only real input.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs]);
}

/**
 * How often each screen refreshes, in milliseconds.
 *
 * Matched to how fast the underlying data can actually change. Readings land
 * every 5 seconds. The analytics charts read continuous aggregates that rebuild
 * every 2 minutes, so polling them faster returns identical rows.
 */
export const REFRESH_INTERVALS = {
  overview: 10_000,
  wellhead: 10_000,
  alarms: 10_000,
  analytics: 60_000,
} as const;
