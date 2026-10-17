import { useEffect, useRef, useState } from 'react';
import type { SnapshotSelection } from '@well-simulation/dto/req/snapshots';
import {
  snapshotListSchema,
  type SnapshotList,
} from '@well-simulation/dto/res/snapshots';
import { client, fetchFn } from '../../utils/api';
import { errorState, timeText } from './presentation';

/** Convert browser-local form time to UTC only at the request boundary. */
function localNow() {
  const now = new Date();
  return new Date(now.getTime() - now.getTimezoneOffset() * 60_000)
    .toISOString()
    .slice(0, 16);
}

export function SnapshotPicker({
  wellheadId,
  allowed,
  pending,
  onSelection,
  rejection,
}: {
  wellheadId: number;
  allowed: boolean;
  pending: boolean;
  rejection?: ReturnType<typeof errorState>;
  onSelection: (selection: SnapshotSelection | null, historical: boolean) => void;
}) {
  const [historical, setHistorical] = useState(false);
  const [at, setAt] = useState(localNow);
  const [result, setResult] = useState<SnapshotList | null>(null);
  const [selected, setSelected] = useState('');
  const [visibleCount, setVisibleCount] = useState(3);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');
  const [restricted, setRestricted] = useState(false);
  const historicalRef = useRef(historical);
  historicalRef.current = historical;
  const selectionCallback = useRef(onSelection);
  selectionCallback.current = onSelection;
  const sequence = useRef(0);
  const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  function clear(isHistorical = historical) {
    sequence.current += 1;
    setResult(null);
    setVisibleCount(3);
    setSelected('');
    setLoading(false);
    onSelection(null, isHistorical);
  }
  useEffect(() => {
    if (!allowed) {
      selectionCallback.current(null, historicalRef.current);
      sequence.current += 1;
      setResult(null);
      setSelected('');
      setLoading(false);
    }
    return () => {
      sequence.current += 1;
    };
  }, [allowed]);
  useEffect(() => {
    if (
      !rejection ||
      !historicalRef.current ||
      ![401, 403, 404, 409].includes(rejection.status ?? 0)
    )
      return;
    sequence.current += 1;
    setResult(null);
    setSelected('');
    setLoading(false);
    selectionCallback.current(null, true);
    setRestricted(rejection.status === 401 || rejection.status === 403);
    setMessage(
      rejection.status === 404
        ? 'This snapshot is no longer available. Find another starting state.'
        : rejection.message
    );
  }, [rejection]);
  async function search() {
    clear(true);
    const generation = sequence.current;
    const instant = new Date(at);
    if (!Number.isFinite(instant.getTime())) {
      setMessage('Choose a valid search date and time.');
      return;
    }
    setLoading(true);
    setMessage('Finding saved snapshots…');
    try {
      const response = await fetchFn<unknown>(
        client.twin.wellheads[':wellheadId'].snapshots.$get({
          param: { wellheadId: String(wellheadId) },
          query: { at: instant.toISOString() },
        })
      );
      const data = snapshotListSchema.parse(response.data);
      if (generation !== sequence.current) return;
      setResult(data);
      setMessage(
        data.items.length
          ? `${data.items.length} snapshots found. Choose one below.`
          : 'No snapshots in this search window. Try another time.'
      );
    } catch (error) {
      if (generation !== sequence.current) return;
      const failure = errorState(error);
      setRestricted(failure.status === 401 || failure.status === 403);
      setMessage(
        failure.status === 400
          ? 'Choose a past time within the preceding 89 days.'
          : failure.message
      );
    } finally {
      if (generation === sequence.current) setLoading(false);
    }
  }
  return (
    <fieldset disabled={!allowed || restricted || pending} className='twin-stack mb-5'>
      <legend>Starting state</legend>
      <label className='twin-check'>
        <input
          type='radio'
          name='starting-state'
          checked={!historical}
          onChange={() => {
            setHistorical(false);
            clear(false);
            setMessage('');
          }}
        />
        Current state
      </label>
      <label className='twin-check'>
        <input
          type='radio'
          name='starting-state'
          checked={historical}
          onChange={() => {
            setHistorical(true);
            clear(true);
            setMessage('');
          }}
        />
        Saved snapshot
      </label>
      {historical && (
        <>
          <div className='twin-field'>
            <label htmlFor='snapshot-at'>Find snapshots at or before</label>
            <input
              id='snapshot-at'
              type='datetime-local'
              value={at}
              onChange={(event) => {
                setAt(event.target.value);
                clear(true);
                setMessage('');
              }}
              aria-describedby='snapshot-time-help'
            />
            <p id='snapshot-time-help' className='twin-caption'>
              Times in {zone}. Search covers the preceding 24 hours, up to 89 days ago.
            </p>
          </div>
          <button
            type='button'
            className='twin-button'
            disabled={loading}
            onClick={() => void search()}
          >
            {loading ? 'Finding snapshots…' : 'Find snapshots'}
          </button>
          <p role='status' className='twin-caption'>
            {message}
          </p>
          {result && (
            <div className='twin-stack' role='group' aria-label='Saved snapshots'>
              {result.items.slice(0, visibleCount).map((item) => {
                const key = `${item.snapshotId}/${item.simulatedAt}`;
                return (
                  <div className='twin-snapshot-card' key={key}>
                    <label className='twin-check'>
                      <input
                        type='radio'
                        name='base-snapshot'
                        checked={selected === key}
                        disabled={!item.compatible}
                        onChange={() => {
                          setSelected(key);
                          onSelection(
                            {
                              snapshotId: item.snapshotId,
                              simulatedAt: item.simulatedAt,
                            },
                            true
                          );
                        }}
                      />
                      {timeText(item.simulatedAt)}
                    </label>
                    <p className='twin-caption'>
                      {item.compatible
                        ? 'Compatible model and parameters.'
                        : item.reason === 'model_version_changed'
                          ? 'Different model version. Choose another snapshot.'
                          : 'Different parameters. Choose another snapshot.'}
                    </p>
                    <details>
                      <summary>Snapshot details</summary>
                      <p className='twin-caption'>
                        Model {item.modelVersion}. Snapshot {item.snapshotId}. UTC:{' '}
                        {item.simulatedAt}.
                      </p>
                    </details>
                  </div>
                );
              })}
              {result.items.length > visibleCount && (
                <button
                  type='button'
                  className='twin-button'
                  onClick={() => setVisibleCount((count) => count + 3)}
                >
                  Show more snapshots
                </button>
              )}
              {result.hasMore && (
                <p className='twin-caption'>
                  More snapshots exist in this window. Set an earlier search time to see
                  them.
                </p>
              )}
            </div>
          )}
          <p className='twin-caption'>
            This run uses alarm rules active when you run it. Chart points expire
            according to their simulated dates, so older scenarios may remain available
            for less time.
          </p>
        </>
      )}
    </fieldset>
  );
}
