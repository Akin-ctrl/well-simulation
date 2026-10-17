import { useEffect, useState } from 'react';
import {
  Link,
  useFetcher,
  useOutletContext,
  useRevalidator,
  useRouteLoaderData,
} from 'react-router';
import { hasCapability, DEFAULT_ROLE } from '@well-simulation/dto/auth/roles';
import type { ForecastScheduleView } from '@well-simulation/dto/res/forecast-schedule';
import type { CurrentUserData } from '../../routes/_dashboard';
import type { WellheadContext } from '../../routes/_dashboard.wellheads.$wellheadId';
import { useRetainedRead } from '../../hooks/use-retained-read';
import { useElapsedSeconds } from '../../hooks/use-elapsed-seconds';
import { ReadStatus } from '../dashboard/components/read-status';
import { ReadFailure } from './read-failure';
import { timeText } from './presentation';
import { projectionUrl } from './projection-route';
import type { scheduleAction } from './schedule-route';

const reasons: Record<string, string> = {
  authority_revoked: 'The schedule owner no longer has active administrator access.',
  projection_busy: 'Other forecasts used the available calculation slots.',
  model_not_current: 'The model did not have a current state for this well.',
  database_unavailable: 'The database could not confirm a saved result.',
  projection_failed: 'The model could not calculate this forecast.',
  claim_expired: 'The job exceeded its allowed time.',
  worker_interrupted: 'The worker stopped or exceeded its allowed time.',
};
export function ForecastSchedulePanel() {
  const well = useOutletContext<WellheadContext>();
  const incoming = well.scheduleRead;
  const read = useRetainedRead(
    incoming?.value ?? null,
    String(well.detail.wellhead.wellheadId)
  );
  const user = useRouteLoaderData<CurrentUserData>('routes/_dashboard')?.user;
  const elapsed = useElapsedSeconds(read?.checkedAt);
  if (!read)
    return incoming?.error ? (
      <ReadFailure error={incoming.error} />
    ) : (
      <p role='status'>Loading forecast schedule…</p>
    );
  const { schedule, latestJob, latestCompleted } = read.data;
  const attemptExpired =
    latestJob?.status === 'running' &&
    Date.parse(latestJob.leaseExpiresAt) <= Date.now();
  const verified = !incoming?.error && !well.unverified && elapsed <= 20;
  const overdue =
    schedule?.enabled &&
    schedule.nextDueAt &&
    Date.parse(schedule.nextDueAt) < Date.now() - 30_000;
  return (
    <section className='twin-panel' aria-labelledby='schedule-heading'>
      <div className='twin-panel-heading'>
        <h2 id='schedule-heading'>Scheduled forecasts</h2>
        <span className='twin-badge'>{schedule?.enabled ? 'Enabled' : 'Off'}</span>
      </div>
      <div className='twin-panel-body twin-stack'>
        <p>
          Save a forecast at regular intervals using this well’s current state and
          controls. Each result appears in Saved runs.
        </p>
        {incoming?.error && <ReadFailure error={incoming.error} retained />}
        <ReadStatus checkedAt={read.checkedAt} intervalMs={10_000} />
        {schedule ? (
          <dl className='twin-facts'>
            <div>
              <dt>Interval</dt>
              <dd>Every {schedule.intervalSeconds / 60} minutes</dd>
            </div>
            <div>
              <dt>Forecast duration</dt>
              <dd>{schedule.horizonSeconds / 60} minutes</dd>
            </div>
            <div>
              <dt>Next due</dt>
              <dd>
                {schedule.nextDueAt ? timeText(schedule.nextDueAt) : 'Schedule is off'}
              </dd>
            </div>
            <div>
              <dt>Saved settings</dt>
              <dd>
                Revision {schedule.revision}, {timeText(schedule.updatedAt)}
              </dd>
            </div>
          </dl>
        ) : (
          <p>No schedule yet. An administrator can set one up below.</p>
        )}
        {overdue && (
          <p className='twin-message' role='status'>
            The next job is overdue. Check service health; the worker skips missed
            intervals when it resumes.
          </p>
        )}
        {latestJob ? (
          <div>
            <h3>Latest attempt: {attemptExpired ? 'overdue' : latestJob.status}</h3>
            {attemptExpired && (
              <p>
                This job exceeded its allowed time. Its last recorded status was
                running; the worker will mark it failed when it resumes.
              </p>
            )}
            <p className='twin-muted'>
              Due {timeText(latestJob.scheduledFor)}.{' '}
              {latestJob.completedAt
                ? `Finished ${timeText(latestJob.completedAt)}.`
                : `Started ${timeText(latestJob.startedAt)}.`}
            </p>
            {latestJob.reasonCode && (
              <p>
                {reasons[latestJob.reasonCode] ??
                  'The job could not finish. Check service logs using its job ID.'}
              </p>
            )}
            {latestJob.missedIntervals > 0 && (
              <p>
                Skipped {latestJob.missedIntervals} earlier intervals while the schedule
                was overdue.
              </p>
            )}
            <details>
              <summary>Job details</summary>
              <p className='twin-caption break-all'>
                Job {latestJob.jobId}. Schedule revision {latestJob.scheduleRevision}.
                Owner account {latestJob.ownerUserId}.
              </p>
            </details>
          </div>
        ) : (
          <p>No scheduled attempts yet.</p>
        )}
        {latestCompleted?.runId && (
          <div>
            <h3>Latest completed result</h3>
            <p className='twin-muted'>
              Saved {timeText(latestCompleted.completedAt!)}. Check this timestamp
              before using an older result.
            </p>
            <Link
              className='twin-button'
              to={projectionUrl(
                well.detail.wellhead.wellheadId,
                'forecast',
                latestCompleted.runId
              )}
            >
              Open scheduled result
            </Link>
          </div>
        )}
        {hasCapability(user?.role ?? DEFAULT_ROLE, 'asset:manage') ? (
          <ScheduleEditor schedule={schedule} verified={verified} />
        ) : (
          <p className='twin-caption'>
            Administrators manage this schedule. You can open its saved results.
          </p>
        )}
      </div>
    </section>
  );
}

function ScheduleEditor({
  schedule,
  verified,
}: {
  schedule: ForecastScheduleView['schedule'];
  verified: boolean;
}) {
  const fetcher = useFetcher<Awaited<ReturnType<typeof scheduleAction>>>();
  const refresh = useRevalidator();
  const [draft, setDraft] = useState(schedule);
  const [generation, setGeneration] = useState(0);
  const [uncertain, setUncertain] = useState(false);
  const busy = fetcher.state !== 'idle';
  const changed = (draft?.revision ?? 0) !== (schedule?.revision ?? 0);
  useEffect(() => {
    if (!fetcher.data || busy) return;
    setUncertain(fetcher.data.uncertain);
    if (!fetcher.data.error) {
      setDraft(fetcher.data.saved);
      setGeneration((value) => value + 1);
    }
    document.getElementById('schedule-outcome')?.focus({ preventScroll: true });
  }, [fetcher.data, busy]);
  const reload = () => {
    setDraft(schedule);
    setGeneration((value) => value + 1);
    refresh.revalidate();
  };
  return (
    <details>
      <summary className='cursor-pointer font-semibold'>Edit forecast schedule</summary>
      <p className='twin-muted my-3'>
        Saving starts a new interval from now and makes you the schedule owner.
        Disabling stops future jobs; a job already running may finish. Your schedule
        continues after you sign out, while your administrator access remains active.
      </p>
      {fetcher.data && (
        <p id='schedule-outcome' tabIndex={-1} role='status' className='twin-message'>
          {fetcher.data.error?.message ??
            (fetcher.data.checked
              ? 'Current schedule loaded. Review the settings before another save.'
              : 'Schedule saved.')}
        </p>
      )}
      {uncertain && (
        <button
          className='twin-button'
          disabled={busy}
          onClick={() =>
            fetcher.submit(
              { intent: 'schedule', operation: 'check' },
              { method: 'post' }
            )
          }
        >
          Check saved schedule
        </button>
      )}
      {changed && !uncertain && (
        <p className='twin-message'>
          The schedule changed since you began editing.{' '}
          <button className='twin-button' onClick={reload}>
            Use current settings
          </button>
        </p>
      )}
      {!verified && (
        <p className='twin-message'>Refresh the schedule before saving changes.</p>
      )}
      <fetcher.Form
        method='post'
        key={generation}
        className='twin-form twin-stack mt-4'
      >
        <input type='hidden' name='intent' value='schedule' />
        <input type='hidden' name='revision' value={draft?.revision ?? 0} />
        <fieldset
          disabled={busy || uncertain || changed || !verified}
          className='twin-stack'
        >
          <legend className='sr-only'>Forecast schedule settings</legend>
          <label className='twin-check'>
            <input
              type='checkbox'
              name='enabled'
              value='yes'
              defaultChecked={draft?.enabled ?? false}
            />{' '}
            Enable scheduled forecasts
          </label>
          <div className='twin-grid'>
            <label className='twin-field'>
              Run every (minutes)
              <input
                type='number'
                name='interval'
                min={5}
                max={1440}
                step={1}
                required
                defaultValue={(draft?.intervalSeconds ?? 900) / 60}
              />
            </label>
            <label className='twin-field'>
              Forecast ahead (minutes)
              <input
                type='number'
                name='duration'
                min={5}
                max={30}
                step={1}
                required
                defaultValue={(draft?.horizonSeconds ?? 900) / 60}
              />
            </label>
          </div>
          <p className='twin-caption'>
            Choose an interval from 5 to 1440 minutes and a duration from 5 to 30
            minutes. The model saves one chart point per minute. Missed jobs do not
            queue up.
          </p>
          <label className='twin-field'>
            Reason for this schedule change
            <textarea name='reason' maxLength={500} required />
          </label>
          <label className='twin-check'>
            <input type='checkbox' name='confirmed' value='yes' required /> I checked
            this well, the interval and the forecast duration.
          </label>
          <div>
            <button className='twin-button twin-primary' type='submit'>
              {busy ? 'Saving schedule…' : 'Save schedule'}
            </button>
          </div>
        </fieldset>
      </fetcher.Form>
    </details>
  );
}
