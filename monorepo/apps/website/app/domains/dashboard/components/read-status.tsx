import { ApiError } from '@well-simulation/dto/error';
import { useRevalidator } from 'react-router';
import { errorState, timeText } from '../../twin/presentation';
import { useElapsedSeconds } from '../../../hooks/use-elapsed-seconds';

/** Keep the time of the last successful read; authorization failures must propagate. */
export async function readDashboardView<T>(read: () => Promise<T>) {
  try {
    return {
      value: { data: await read(), checkedAt: new Date().toISOString() },
      error: null,
    };
  } catch (error) {
    if (error instanceof ApiError && [401, 403].includes(error.statusCode ?? 0)) throw error;
    return { value: null, error: errorState(error) };
  }
}

/** A successful API read and the freshness of its underlying samples differ. */
export function ReadStatus({
  checkedAt,
  intervalMs,
}: {
  checkedAt: string;
  intervalMs: number;
}) {
  const refresh = useRevalidator();
  const elapsed = useElapsedSeconds(checkedAt);
  const overdue = elapsed * 1000 > intervalMs * 2;
  return (
    <div className='twin-inline'>
      <span className='twin-muted'>
        Last checked {timeText(checkedAt)}. Check reading timestamps for data age.
      </span>
      <span role='status' className={overdue ? 'twin-badge twin-attention' : 'sr-only'}>
        {overdue ? 'Refresh overdue' : ''}
      </span>
      <button
        className='twin-button'
        disabled={refresh.state !== 'idle'}
        onClick={() => refresh.revalidate()}
      >
        {refresh.state === 'idle' ? 'Refresh view' : 'Refreshing…'}
      </button>
    </div>
  );
}
