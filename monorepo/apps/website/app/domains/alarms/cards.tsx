import { useState } from 'react';
import { Link, useRevalidator } from 'react-router';
import type {
  AlarmEvent,
  AlarmReceipt,
  AlarmShelfList,
} from '@well-simulation/dto/res/alarms';
import { timeText } from '../twin/presentation';
import { eventDetail, alarmError } from './route';
import {
  eventReview,
  shelfReview,
  shelfSeconds,
  reviewKey,
  type AlarmReview,
} from './review';

const valueText = (value: number) =>
  new Intl.NumberFormat('en', { maximumFractionDigits: 2 }).format(value);
export const receiptText = (receipt: AlarmReceipt) =>
  `${receipt.actorName} ${receipt.action === 'acknowledge' ? 'acknowledged the event' : receipt.action === 'shelve' ? 'shelved the rule' : 'ended the shelf'} for ${receipt.target.wellheadName}, ${receipt.target.parameterName}.`;
export function EventCard({
  event,
  canOperate,
  verified,
  observedAt,
  elapsed,
  onReview,
}: {
  event: AlarmEvent;
  canOperate: boolean;
  verified: boolean;
  observedAt: string;
  elapsed: number;
  onReview: (value: AlarmReview) => void;
}) {
  const [history, setHistory] = useState<Awaited<
    ReturnType<typeof eventDetail>
  > | null>(null);
  const [error, setError] = useState<string | null>(null),
    [busy, setBusy] = useState(false);
  const refresh = useRevalidator();
  const review = eventReview(event),
    remaining = shelfSeconds(review, observedAt, elapsed);
  const title = `event-${event.eventId}-${event.triggeredAt.replace(/[^0-9]/g, '')}`;
  return (
    <article className='twin-panel' aria-labelledby={title}>
      <div className='twin-panel-body twin-stack'>
        <div className='twin-inline'>
          <span
            className={`twin-badge ${event.severity === 'CRITICAL' ? 'twin-error' : 'twin-attention'}`}
          >
            {event.severity}
          </span>
          <span>{event.clearedAt ? 'Returned to normal' : 'Active condition'}</span>
          <span className='twin-badge'>
            {event.acknowledgement ? 'Acknowledged' : 'Not acknowledged'}
          </span>
          {event.shelf?.active && (
            <span className='twin-badge twin-attention'>
              {remaining !== null && remaining <= 0
                ? 'Shelf deadline reached'
                : 'Shelved'}
            </span>
          )}
        </div>
        <div>
          <h2 id={title}>
            {event.target.wellheadName} · {event.target.parameterName}
          </h2>
          <p className='twin-muted'>{event.target.locationName}</p>
        </div>
        <dl className='twin-facts'>
          <div>
            <dt>Value when triggered</dt>
            <dd>
              {valueText(event.triggeredValue)} {event.target.unit}
            </dd>
          </div>
          <div>
            <dt>Current rule threshold</dt>
            <dd>
              {event.target.operator} {valueText(event.target.threshold)}{' '}
              {event.target.unit}
            </dd>
          </div>
          <div>
            <dt>Triggered</dt>
            <dd>{timeText(event.triggeredAt)}</dd>
          </div>
          {event.clearedAt && (
            <div>
              <dt>Returned to normal</dt>
              <dd>{timeText(event.clearedAt)}</dd>
            </div>
          )}
        </dl>
        {event.acknowledgement && (
          <p>
            Acknowledged by {event.acknowledgement.actorName},{' '}
            {timeText(event.acknowledgement.at)}. Reason: {event.acknowledgement.reason}
          </p>
        )}
        {event.shelf?.active && (
          <p className='twin-message'>
            {remaining !== null && remaining <= 0
              ? 'The shelf deadline has passed. Refresh to verify its current state.'
              : `Shelved until ${timeText(event.shelf.until!)}.`}{' '}
            {event.shelf.actorName}: {event.shelf.reason}
          </p>
        )}
        <details className='twin-stack'>
          <summary>Event details and recent actions</summary>
          <p className='twin-caption break-all'>
            Event {event.eventId}, {event.triggeredAt}.
          </p>
          <p>
            The current rule opens after {event.target.triggerAfter} consecutive
            breaches and clears after {event.target.clearAfter} normal readings. The
            event stores its trigger value and severity; it does not store the threshold
            that applied at that time.
          </p>
          <button
            className='twin-button'
            disabled={busy}
            onClick={async () => {
              setBusy(true);
              try {
                setHistory(await eventDetail(event));
                setError(null);
              } catch (cause) {
                const state = alarmError(cause);
                setError(state.message);
                if ([401, 403].includes(state.status ?? 0)) void refresh.revalidate();
              } finally {
                setBusy(false);
              }
            }}
          >
            {busy
              ? 'Loading actions…'
              : history
                ? 'Refresh recent actions'
                : 'Load recent actions'}
          </button>
          {error && (
            <p role='status' className='twin-message'>
              {error}
            </p>
          )}
          {history && (
            <>
              <p className='twin-caption'>
                Latest 20 actions for this well and rule, checked{' '}
                {timeText(history.observedAt)}.
              </p>
              {history.actions.length === 0 ? (
                <p>No operator actions recorded for this rule.</p>
              ) : (
                <ol className='twin-stack'>
                  {history.actions.map((item) => (
                    <li key={item.actionId}>
                      <p>
                        {receiptText(item)} {timeText(item.occurredAt)}
                      </p>
                      <p className='twin-muted'>
                        Reason: {item.reason}
                        {item.shelvedUntil
                          ? `. Deadline: ${timeText(item.shelvedUntil)}`
                          : ''}
                      </p>
                    </li>
                  ))}
                </ol>
              )}
            </>
          )}
        </details>
        <div className='twin-inline'>
          <Link className='twin-button' to={`/wellheads/${event.target.wellheadId}`}>
            Open well
          </Link>
          {canOperate && (
            <button
              className='twin-button'
              disabled={!verified || (remaining !== null && remaining <= 0)}
              data-alarm-review={reviewKey(review)}
              onClick={() => onReview(review)}
            >
              Review action
            </button>
          )}
        </div>
      </div>
    </article>
  );
}
export function ShelfCard({
  item,
  canOperate,
  verified,
  observedAt,
  elapsed,
  onReview,
}: {
  item: AlarmShelfList['items'][number];
  canOperate: boolean;
  verified: boolean;
  observedAt: string;
  elapsed: number;
  onReview: (value: AlarmReview) => void;
}) {
  const review = shelfReview(item),
    remaining = shelfSeconds(review, observedAt, elapsed);
  return (
    <article className='twin-panel'>
      <div className='twin-panel-body twin-stack'>
        <span className='twin-badge twin-attention'>Shelved rule</span>
        <h2>
          {item.target.wellheadName} · {item.target.parameterName}
        </h2>
        <p>
          {remaining !== null && remaining <= 0
            ? 'The deadline has passed. Refresh to verify its current state.'
            : `Shelf ends ${timeText(item.state.until!)}.`}
        </p>
        <p>
          {item.state.actorName}: {item.state.reason}
        </p>
        <p className='twin-muted'>
          This shelf applies to new events for this well and rule, even if no condition
          is active now. Revision {item.state.revision}.
        </p>
        <div className='twin-inline'>
          <Link className='twin-button' to={`/wellheads/${item.target.wellheadId}`}>
            Open well
          </Link>
          {canOperate && (
            <button
              className='twin-button'
              disabled={!verified || remaining === null || remaining <= 0}
              data-alarm-review={reviewKey(review)}
              onClick={() => onReview(review)}
            >
              Review shelf
            </button>
          )}
        </div>
      </div>
    </article>
  );
}
