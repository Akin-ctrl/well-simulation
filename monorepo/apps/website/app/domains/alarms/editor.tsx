import { useEffect, useState } from 'react';
import { useFetcher, useRevalidator } from 'react-router';
import {
  alarmActionInputSchema,
  SHELF_MIN_MINUTES,
  SHELF_MAX_MINUTES,
  type AlarmActionInput,
} from '@well-simulation/dto/req/alarms';
import type { AlarmReceipt } from '@well-simulation/dto/res/alarms';
import { numberText, timeText } from '../twin/presentation';
import type { alarmPageAction } from './route';
import { reviewVersion, shelfSeconds, type AlarmReview } from './review';

export function AlarmEditor({
  selected,
  current,
  verified,
  observedAt,
  elapsed,
  onDone,
  onCancel,
}: {
  selected: AlarmReview;
  current: AlarmReview | null;
  verified: boolean;
  observedAt: string;
  elapsed: number;
  onDone: (receipt: AlarmReceipt) => void;
  onCancel: () => void;
}) {
  const fetcher = useFetcher<Awaited<ReturnType<typeof alarmPageAction>>>();
  const refresh = useRevalidator();
  const [baseline, setBaseline] = useState(selected);
  const [action, setAction] = useState<AlarmActionInput['action']>(
    selected.event && !selected.event.acknowledgement
      ? 'acknowledge'
      : selected.shelf?.active
        ? 'unshelve'
        : 'shelve'
  );
  const [reason, setReason] = useState('');
  const [duration, setDuration] = useState(30);
  const [confirmed, setConfirmed] = useState(false);
  const [pending, setPending] = useState<{
    key: string;
    input: AlarmActionInput;
  } | null>(null);
  const [uncertain, setUncertain] = useState(false);
  const [missing, setMissing] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const busy = fetcher.state !== 'idle';
  const changed = !current || reviewVersion(current) !== reviewVersion(baseline);
  const remaining = current ? shelfSeconds(current, observedAt, elapsed) : null;
  const canAcknowledge = !!baseline.event && !baseline.event.acknowledgement;
  const canUnshelve = !!baseline.shelf?.active && remaining !== null && remaining > 0;
  const disabled = !verified || busy || uncertain || changed;
  useEffect(() => {
    const result = fetcher.data;
    if (!result || busy) return;
    if (result.receipt) {
      onDone(result.receipt);
      return;
    }
    setUncertain(result.uncertain);
    setMissing(result.missing);
    setMessage(
      result.missing
        ? 'No committed result was found yet. You can check again or retry the same reviewed request.'
        : (result.error?.message ?? null)
    );
    if (!result.uncertain) setConfirmed(false);
    document.getElementById('alarm-editor-message')?.focus({ preventScroll: true });
  }, [fetcher.data, busy, onDone]);
  const submit = (request: NonNullable<typeof pending>, operation = 'write') =>
    fetcher.submit(
      { operation, requestKey: request.key, input: JSON.stringify(request.input) },
      { method: 'post' }
    );
  const reviewCurrent = () => {
    if (!current) {
      refresh.revalidate();
      return;
    }
    setBaseline(current);
    setConfirmed(false);
    setMessage(null);
    setPending(null);
    if (action === 'acknowledge' && current.event?.acknowledgement)
      setAction(current.shelf?.active ? 'unshelve' : 'shelve');
    if (action === 'unshelve' && !current.shelf?.active) setAction('shelve');
  };
  return (
    <section className='twin-panel' aria-labelledby='alarm-editor-heading'>
      <div className='twin-panel-heading'>
        <h2 id='alarm-editor-heading' tabIndex={-1}>
          Review action for {baseline.target.wellheadName}
        </h2>
      </div>
      <div className='twin-panel-body twin-stack'>
        <p>
          {baseline.target.parameterName}. Current rule: {baseline.target.operator}{' '}
          {numberText(baseline.target.threshold)} {baseline.target.unit}.
        </p>
        {baseline.event && (
          <p className='twin-muted'>
            Event triggered {timeText(baseline.event.triggeredAt)}. Condition:{' '}
            {baseline.event.clearedAt ? 'returned to normal' : 'active'}.
          </p>
        )}
        {baseline.shelf?.active && (
          <p className='twin-message'>
            Current shelf ends {timeText(baseline.shelf.until!)}. Setting another shelf
            replaces that deadline.
          </p>
        )}
        {message && (
          <p
            id='alarm-editor-message'
            tabIndex={-1}
            role='status'
            className='twin-message'
          >
            {message}
          </p>
        )}
        {!verified && (
          <p className='twin-message'>
            Refresh the alarm state before submitting an action.
          </p>
        )}
        {changed && !uncertain && (
          <div className='twin-stack'>
            <p className='twin-message'>
              The reviewed state changed. Your reason remains below.{' '}
              {current
                ? 'Review the current state before continuing.'
                : 'Refresh this view and select the current event or shelf.'}
            </p>
            {current && (
              <button
                className='twin-button'
                disabled={!verified || busy}
                onClick={reviewCurrent}
              >
                Review current state
              </button>
            )}
          </div>
        )}
        {uncertain && pending && (
          <div className='twin-stack'>
            <p>
              Keep this review open until you check the outcome. A missing reply does
              not prove the action failed.
            </p>
            <div className='twin-inline'>
              <button
                className='twin-button'
                disabled={busy}
                onClick={() => submit(pending, 'check')}
              >
                Check result
              </button>
              {missing && (
                <button
                  className='twin-button'
                  disabled={busy || !verified}
                  onClick={() => submit(pending)}
                >
                  Retry same request
                </button>
              )}
            </div>
          </div>
        )}
        <form
          className='twin-form'
          onSubmit={(event) => {
            event.preventDefault();
            if (disabled) return;
            const parsed = alarmActionInputSchema.safeParse(
              action === 'acknowledge'
                ? {
                    action,
                    eventId: baseline.event?.eventId,
                    triggeredAt: baseline.event?.triggeredAt,
                    reason,
                    confirmed,
                  }
                : {
                    action,
                    wellheadId: baseline.target.wellheadId,
                    ruleId: baseline.target.ruleId,
                    expectedRevision: baseline.shelf?.revision ?? 0,
                    ...(action === 'shelve' ? { durationMinutes: duration } : {}),
                    reason,
                    confirmed,
                  }
            );
            if (!parsed.success) {
              setMessage(
                'Check the reason, duration and confirmation before submitting.'
              );
              return;
            }
            const request = { key: crypto.randomUUID(), input: parsed.data };
            setPending(request);
            setMissing(false);
            setMessage(null);
            submit(request);
          }}
        >
          <fieldset disabled={disabled} className='twin-stack min-w-0'>
            <legend className='sr-only'>Reviewed alarm action</legend>
            <div className='twin-field'>
              <label htmlFor='alarm-action-kind'>Action</label>
              <select
                id='alarm-action-kind'
                value={action}
                onChange={(e) => {
                  setAction(e.target.value as AlarmActionInput['action']);
                  setConfirmed(false);
                }}
              >
                {canAcknowledge && (
                  <option value='acknowledge'>Acknowledge event</option>
                )}
                <option value='shelve'>Shelve rule</option>
                {baseline.shelf?.active && (
                  <option value='unshelve' disabled={!canUnshelve}>
                    End shelf now
                  </option>
                )}
              </select>
            </div>
            <p>
              {action === 'acknowledge'
                ? 'This records that you have seen this event. Normal readings clear its condition.'
                : action === 'shelve'
                  ? 'This removes this well and rule from Needs acknowledgement until the deadline, including any new events. All active totals still count the condition.'
                  : 'This ends the shelf now. Unacknowledged events return to Needs acknowledgement.'}
            </p>
            {action === 'shelve' && (
              <div className='twin-field'>
                <label htmlFor='alarm-shelf-duration'>Shelf duration (minutes)</label>
                <input
                  id='alarm-shelf-duration'
                  aria-describedby='alarm-shelf-duration-help'
                  type='number'
                  min={SHELF_MIN_MINUTES}
                  max={SHELF_MAX_MINUTES}
                  step={1}
                  required
                  value={duration}
                  onChange={(e) => {
                    setDuration(Number(e.target.value));
                    setConfirmed(false);
                  }}
                />
                <span className='twin-caption' id='alarm-shelf-duration-help'>
                  Choose {SHELF_MIN_MINUTES}–{SHELF_MAX_MINUTES} whole minutes. The
                  deadline starts when the database records the action.
                </span>
              </div>
            )}
            {action === 'unshelve' && !canUnshelve && (
              <p className='twin-message'>
                The shelf deadline has passed. Refresh to verify its current state.
              </p>
            )}
            <div className='twin-field'>
              <label htmlFor='alarm-action-reason'>Reason</label>
              <textarea
                id='alarm-action-reason'
                rows={3}
                maxLength={500}
                required
                value={reason}
                onChange={(e) => {
                  setReason(e.target.value);
                  setConfirmed(false);
                }}
              />
            </div>
            <label className='twin-check'>
              <input
                type='checkbox'
                required
                checked={confirmed}
                onChange={(e) => setConfirmed(e.target.checked)}
              />{' '}
              I checked the well, rule and effect of this action.
            </label>
            <div>
              <button
                type='submit'
                className='twin-button twin-primary'
                disabled={action === 'unshelve' && !canUnshelve}
              >
                {busy
                  ? 'Saving action…'
                  : action === 'acknowledge'
                    ? 'Acknowledge event'
                    : action === 'shelve'
                      ? 'Shelve rule'
                      : 'End shelf now'}
              </button>
            </div>
          </fieldset>
        </form>
        <div>
          <button
            className='twin-button'
            disabled={busy || uncertain}
            onClick={onCancel}
          >
            Close review
          </button>
        </div>
      </div>
    </section>
  );
}
