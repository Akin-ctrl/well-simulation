import { useRevalidator } from 'react-router';
import type { errorState } from './presentation';
import { RequestMessage } from './messages';

/** An explicit GET retry is safe; model submissions have their own recovery flow. */
export function ReadFailure({
  error,
  retained = false,
}: {
  error: ReturnType<typeof errorState>;
  retained?: boolean;
}) {
  const refresh = useRevalidator();
  return (
    <section className='twin-workspace twin-stack'>
      {!retained && <h1>This view could not load</h1>}
      <RequestMessage error={error} />
      {retained && (
        <p className='twin-message'>
          Refresh failed. These values come from the last successful read. Check their
          timestamps.
        </p>
      )}
      <div>
        <button
          className='twin-button'
          disabled={refresh.state !== 'idle'}
          onClick={() => refresh.revalidate()}
        >
          {refresh.state === 'idle' ? 'Try again' : 'Retrying…'}
        </button>
      </div>
    </section>
  );
}
