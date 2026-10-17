import { useCallback, useEffect, useState } from 'react';
import { Link, useLoaderData, useNavigate, useRouteLoaderData } from 'react-router';
import { DEFAULT_ROLE, hasCapability } from '@well-simulation/dto/auth/roles';
import type { AlarmReceipt } from '@well-simulation/dto/res/alarms';
import type { CurrentUserData } from '../../routes/_dashboard';
import { PageHelp } from '../dashboard/components/page-help';
import { ReadStatus } from '../dashboard/components/read-status';
import { ReadFailure } from '../twin/read-failure';
import { useRetainedRead } from '../../hooks/use-retained-read';
import { useElapsedSeconds } from '../../hooks/use-elapsed-seconds';
import { REFRESH_INTERVALS, useAutoRefresh } from '../../hooks/use-auto-refresh';
import { ALARM_VIEWS, alarmUrl, type alarmLoader } from './route';
import { AlarmEditor } from './editor';
import { EventCard, ShelfCard, receiptText } from './cards';
import { eventReview, shelfReview, reviewKey, type AlarmReview } from './review';

type Loaded = Awaited<ReturnType<typeof alarmLoader>>;
export default function AlarmCenter() {
  const incoming = useLoaderData<typeof alarmLoader>();
  return (
    <AlarmPage
      key={`${incoming.view}:${incoming.wellheadId ?? 'all'}:${incoming.cursor ?? 'first'}`}
      incoming={incoming}
    />
  );
}
function AlarmPage({ incoming }: { incoming: Loaded }) {
  const read = useRetainedRead(
    incoming.read.value,
    `${incoming.view}:${incoming.wellheadId ?? 'all'}:${incoming.cursor ?? 'first'}`
  );
  const elapsed = useElapsedSeconds(read?.checkedAt);
  const user = useRouteLoaderData<CurrentUserData>('routes/_dashboard')?.user;
  const canOperate = hasCapability(user?.role ?? DEFAULT_ROLE, 'alarm:acknowledge');
  const navigate = useNavigate();
  const [selected, setSelected] = useState<AlarmReview | null>(null);
  const [outcome, setOutcome] = useState<AlarmReceipt | null>(null);
  useAutoRefresh(REFRESH_INTERVALS.alarms);
  useEffect(() => {
    if (selected) document.getElementById('alarm-editor-heading')?.focus();
  }, [selected]);
  useEffect(() => {
    if (outcome) document.getElementById('alarm-outcome')?.focus();
  }, [outcome]);
  const done = useCallback((receipt: AlarmReceipt) => {
    setSelected(null);
    setOutcome(receipt);
  }, []);
  const choose = (review: AlarmReview) => {
    setOutcome(null);
    setSelected(review);
  };
  const closeReview = () => {
    const key = selected ? reviewKey(selected) : null;
    setSelected(null);
    requestAnimationFrame(() => {
      const origin = key
        ? document.querySelector<HTMLButtonElement>(
            `[data-alarm-review="${CSS.escape(key)}"]`
          )
        : null;
      (origin ?? document.getElementById('alarm-list-heading'))?.focus();
    });
  };
  if (!read)
    return incoming.read.error ? (
      <ReadFailure error={incoming.read.error} />
    ) : (
      <div className='twin-workspace'>
        <h1>Alarm Center</h1>
        <p role='status'>Loading alarm state…</p>
      </div>
    );
  const data = read.data,
    verified = !incoming.read.error && elapsed <= 20;
  const current = selected
    ? selected.event
      ? data.events.find(
          (event) => reviewKey(eventReview(event)) === reviewKey(selected)
        )
      : data.shelves.find(
          (item) => reviewKey(shelfReview(item)) === reviewKey(selected)
        )
    : null;
  const currentReview = current
    ? 'eventId' in current
      ? eventReview(current)
      : shelfReview(current)
    : null;
  const count = data.events.length + data.shelves.length;
  const itemName = incoming.view === 'shelves' ? 'rule' : 'event';
  return (
    <div className='twin-workspace twin-stack'>
      <div>
        <h1>Alarm Center</h1>
        <p className='twin-muted'>
          Check the condition, record that you have seen it, and follow your operating
          procedure.
        </p>
      </div>
      {incoming.read.error && <ReadFailure error={incoming.read.error} retained />}
      <ReadStatus checkedAt={read.checkedAt} intervalMs={REFRESH_INTERVALS.alarms} />
      {outcome && (
        <p role='status' tabIndex={-1} id='alarm-outcome' className='twin-message'>
          {receiptText(outcome)}
        </p>
      )}
      <div className='grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4'>
        {[
          { label: 'All active', value: data.summary.active },
          { label: 'Active critical', value: data.summary.critical },
          { label: 'Needs acknowledgement', value: data.summary.attention },
          { label: 'Current shelves', value: data.summary.shelves },
        ].map((tile) => (
          <div key={tile.label} className='twin-panel twin-panel-body'>
            <p className='twin-muted'>{tile.label}</p>
            <strong className='text-2xl'>{tile.value}</strong>
          </div>
        ))}
      </div>
      <p className='twin-caption'>
        Active totals include acknowledged and shelved conditions. Needs acknowledgement
        also includes retained events that returned to normal. Counts follow the
        selected well filter.
      </p>
      <PageHelp title='How acknowledgement and shelving work'>
        <p>
          Acknowledgement records that an operator has seen one event. Normal readings
          clear its condition according to the rule’s consecutive-reading count. Each
          new event needs its own acknowledgement.
        </p>
        <p>
          A shelf temporarily removes this well and rule from Needs acknowledgement. It
          still appears in All active totals, and Shelved rules shows its deadline and
          reason. A shelf expires even if the service restarts.
        </p>
        <p>
          History shows retained events. The current retention policy keeps about 90
          days. Open the well to check its current readings and their age.
        </p>
      </PageHelp>
      <nav className='twin-inline' aria-label='Alarm views'>
        {Object.entries(ALARM_VIEWS).map(([view, label]) => (
          <Link
            key={view}
            className={`twin-button ${incoming.view === view ? 'twin-primary' : ''}`}
            aria-current={incoming.view === view ? 'page' : undefined}
            to={alarmUrl(view as keyof typeof ALARM_VIEWS, incoming.wellheadId)}
          >
            {label}
          </Link>
        ))}
      </nav>
      <div className='twin-field twin-filter'>
        <label htmlFor='alarm-well-filter'>Well</label>
        <select
          id='alarm-well-filter'
          value={incoming.wellheadId ?? ''}
          onChange={(e) =>
            navigate(
              alarmUrl(
                incoming.view,
                e.target.value ? Number(e.target.value) : undefined
              )
            )
          }
        >
          <option value=''>All wells</option>
          {data.wells.map((well) => (
            <option key={well.wellheadId} value={well.wellheadId}>
              {well.wellheadName}
            </option>
          ))}
        </select>
      </div>
      {!canOperate && (
        <p className='twin-message'>
          OPERATIONS can acknowledge events and manage shelves. Your account can inspect
          alarm state and action history.
        </p>
      )}
      {selected && canOperate && (
        <AlarmEditor
          key={reviewKey(selected)}
          selected={selected}
          current={currentReview}
          verified={verified}
          observedAt={data.observedAt}
          elapsed={elapsed}
          onDone={done}
          onCancel={closeReview}
        />
      )}
      <section className='twin-stack' aria-labelledby='alarm-list-heading'>
        <div>
          <h2 id='alarm-list-heading' tabIndex={-1}>
            {ALARM_VIEWS[incoming.view]}
          </h2>
          <p className='twin-caption'>
            {count} {itemName}
            {count === 1 ? '' : 's'} on this page, {data.total} in this view.{' '}
            {incoming.view === 'history'
              ? 'Newest events first.'
              : incoming.view === 'shelves'
                ? 'Ordered by well and rule.'
                : 'Most severe first, then oldest.'}
          </p>
        </div>
        {count === 0 && (
          <div className='twin-panel twin-empty'>
            {incoming.cursor
              ? 'No rows remain on this page. Return to the first page for the current list.'
              : incoming.view === 'attention'
                ? 'No retained events need acknowledgement for this filter. Check All active for conditions that remain active.'
                : incoming.view === 'shelves'
                  ? 'No current shelves for this filter.'
                  : incoming.view === 'history'
                    ? 'No retained events for this filter.'
                    : 'No active conditions for this filter.'}
          </div>
        )}
        {data.events.map((event) => (
          <EventCard
            key={`${event.eventId}:${event.triggeredAt}`}
            event={event}
            canOperate={canOperate}
            verified={verified}
            observedAt={data.observedAt}
            elapsed={elapsed}
            onReview={choose}
          />
        ))}
        {data.shelves.map((item) => (
          <ShelfCard
            key={`${item.target.wellheadId}:${item.target.ruleId}`}
            item={item}
            canOperate={canOperate}
            verified={verified}
            observedAt={data.observedAt}
            elapsed={elapsed}
            onReview={choose}
          />
        ))}
      </section>
      <nav className='twin-inline' aria-label='Alarm pages'>
        {incoming.cursor && (
          <Link
            className='twin-button'
            to={alarmUrl(incoming.view, incoming.wellheadId)}
          >
            First page
          </Link>
        )}
        {data.nextCursor && (
          <Link
            className='twin-button'
            to={alarmUrl(incoming.view, incoming.wellheadId, data.nextCursor)}
          >
            Next page
          </Link>
        )}
      </nav>
    </div>
  );
}
