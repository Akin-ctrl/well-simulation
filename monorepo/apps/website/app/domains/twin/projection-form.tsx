import { useRef, useState } from 'react';
import { useFetcher, useRouteLoaderData } from 'react-router';
import { DEFAULT_ROLE, hasCapability } from '@well-simulation/dto/auth/roles';
import {
  forecastRequestSchema,
  MAX_SCENARIO_STEPS,
} from '@well-simulation/dto/req/projection';
import {
  SCENARIO_CONTROLS,
  type ScenarioControlKey,
} from '@well-simulation/dto/req/scenario-controls';
import type { CurrentUserData } from '../../routes/_dashboard';
import { routes } from '../../config/routes';
import { RequestMessage } from './messages';
import { scenarioInput, type ScheduleRow, type errorState } from './presentation';
import { fieldIssues } from './form-errors';
import type { ProjectionKind } from './projection-route';

/** Drafts survive status polls. Only explicit valid submissions execute a model. */
export function ProjectionForm({
  kind,
  wellheadId,
}: {
  kind: ProjectionKind;
  wellheadId: number;
}) {
  const user = useRouteLoaderData<CurrentUserData>('routes/_dashboard')?.user;
  const fetcher = useFetcher<{ error: ReturnType<typeof errorState> }>();
  const [name, setName] = useState('Choke adjustment');
  const [horizon, setHorizon] = useState('30');
  const [sample, setSample] = useState('60');
  const [rows, setRows] = useState<ScheduleRow[]>([
    { id: 1, offsetMinutes: '5', control: 'chokePositionPercent', value: '80' },
  ]);
  const nextId = useRef(2);
  const [attempted, setAttempted] = useState(false);
  const [stepMessage, setStepMessage] = useState('');
  const horizonSeconds = horizon.trim() === '' ? NaN : Number(horizon) * 60;
  const stepSeconds = sample.trim() === '' ? NaN : Number(sample);
  const validation =
    kind === 'forecast'
      ? forecastRequestSchema.safeParse({ horizonSeconds, stepSeconds })
      : scenarioInput(wellheadId, name, horizonSeconds, stepSeconds, rows);
  const issues =
    attempted && !validation.success
      ? fieldIssues(validation.error.issues, kind, rows)
      : [];
  const pending = fetcher.state !== 'idle';
  const allowed = Boolean(
    user && hasCapability(user.role ?? DEFAULT_ROLE, 'scenario:run')
  );
  const errorId = `${kind}-errors`;
  function update(id: number, patch: Partial<ScheduleRow>) {
    setRows((current) =>
      current.map((row) => (row.id === id ? { ...row, ...patch } : row))
    );
  }
  function focus(id: string) {
    requestAnimationFrame(() => document.getElementById(id)?.focus());
  }
  function attributes(id: string) {
    const invalid = issues.some((issue) => issue.field === id);
    return {
      'aria-invalid': invalid || undefined,
      'aria-describedby': invalid ? `${id}-error` : undefined,
    };
  }
  function errorFor(id: string) {
    const matching = issues.filter((issue) => issue.field === id);
    return matching.length ? (
      <p id={`${id}-error`} className='twin-field-error'>
        {matching.map((issue) => issue.message).join(' ')}
      </p>
    ) : null;
  }
  return (
    <section className='twin-panel'>
      <div className='twin-panel-heading'>
        <div>
          <h2 id={`${kind}-form-title`}>
            {kind === 'forecast' ? 'Forecast inputs' : 'New scenario inputs'}
          </h2>
          <p className='twin-caption'>New draft · Start from the current well state.</p>
        </div>
      </div>
      <fetcher.Form
        method='post'
        noValidate
        aria-labelledby={`${kind}-form-title`}
        className='twin-form twin-panel-body'
        onSubmit={(event) => {
          if (pending || !allowed) {
            event.preventDefault();
            return;
          }
          setAttempted(true);
          if (!validation.success) {
            event.preventDefault();
            focus(errorId);
          }
        }}
      >
        <input
          type='hidden'
          name='payload'
          value={JSON.stringify(validation.success ? validation.data : {})}
        />
        {issues.length > 0 && (
          <div
            id={errorId}
            tabIndex={-1}
            className='twin-message twin-error mb-4'
            aria-labelledby={`${errorId}-title`}
          >
            <h3 id={`${errorId}-title`}>Check your inputs</h3>
            <ul>
              {issues.map((issue, index) => (
                <li key={index}>
                  <a
                    href={`#${issue.field}`}
                    onClick={(event) => {
                      event.preventDefault();
                      document.getElementById(issue.field)?.focus();
                    }}
                  >
                    {issue.label}: {issue.message}
                  </a>
                </li>
              ))}
            </ul>
          </div>
        )}
        <fieldset disabled={!allowed}>
          <legend className='sr-only'>
            {kind === 'forecast' ? 'Forecast parameters' : 'Scenario parameters'}
          </legend>
          {kind === 'scenario' && (
            <div className='twin-field'>
              <label htmlFor='scenario-name'>Scenario name</label>
              <input
                id='scenario-name'
                value={name}
                onChange={(event) => setName(event.target.value)}
                {...attributes('scenario-name')}
                required
              />
              {errorFor('scenario-name')}
            </div>
          )}
          <div className='twin-grid'>
            <div className='twin-field'>
              <label htmlFor={`${kind}-horizon`}>Duration (minutes)</label>
              <input
                id={`${kind}-horizon`}
                type='number'
                step='any'
                value={horizon}
                onChange={(event) => setHorizon(event.target.value)}
                {...attributes(`${kind}-horizon`)}
                required
              />
              {errorFor(`${kind}-horizon`)}
            </div>
            <div className='twin-field'>
              <label htmlFor={`${kind}-sample`}>Save a point every (seconds)</label>
              <input
                id={`${kind}-sample`}
                type='number'
                step='1'
                value={sample}
                onChange={(event) => setSample(event.target.value)}
                {...attributes(`${kind}-sample`)}
                required
              />
              {errorFor(`${kind}-sample`)}
            </div>
          </div>
          <p className='twin-caption'>
            Calculate 5–120 minutes ahead. Save a chart point every 5–300 seconds. The
            interval must divide the duration evenly.
          </p>
          {kind === 'scenario' && (
            <div className='mt-5'>
              <h3>Proposed changes</h3>
              <p className='twin-caption'>
                Count minutes from the start of this run. Minute 0 applies a change
                immediately to the model copy.
              </p>
              {rows.map((row, index) => (
                <fieldset className='twin-step' key={row.id}>
                  <legend>Step {index + 1}</legend>
                  {rows.length > 1 && (
                    <button
                      type='button'
                      className='twin-button mb-3'
                      aria-label={`Remove step ${index + 1}`}
                      onClick={() => {
                        const remaining = rows.filter((item) => item.id !== row.id);
                        setRows(remaining);
                        setStepMessage(
                          `Step ${index + 1} removed. ${remaining.length} steps remain.`
                        );
                        focus(
                          `offset-${remaining[Math.min(index, remaining.length - 1)]!.id}`
                        );
                      }}
                    >
                      Remove
                    </button>
                  )}
                  <div className='twin-field'>
                    <label htmlFor={`offset-${row.id}`}>At minute</label>
                    <input
                      id={`offset-${row.id}`}
                      type='number'
                      step='1'
                      value={row.offsetMinutes}
                      onChange={(event) =>
                        update(row.id, { offsetMinutes: event.target.value })
                      }
                      {...attributes(`offset-${row.id}`)}
                      required
                    />
                    {errorFor(`offset-${row.id}`)}
                  </div>
                  <div className='twin-field'>
                    <label htmlFor={`control-${row.id}`}>Control</label>
                    <select
                      id={`control-${row.id}`}
                      value={row.control}
                      onChange={(event) =>
                        update(row.id, {
                          control: event.target.value as ScenarioControlKey,
                          value:
                            event.target.value === 'chokePositionPercent'
                              ? '80'
                              : 'false',
                        })
                      }
                    >
                      {Object.entries(SCENARIO_CONTROLS).map(([key, info]) => (
                        <option key={key} value={key}>
                          {info.label}
                        </option>
                      ))}
                    </select>
                  </div>
                  <label htmlFor={`value-${row.id}`}>
                    Target {SCENARIO_CONTROLS[row.control].numeric ? '(%)' : 'state'}
                  </label>
                  {SCENARIO_CONTROLS[row.control].numeric ? (
                    <input
                      id={`value-${row.id}`}
                      type='number'
                      step='any'
                      value={row.value}
                      onChange={(event) =>
                        update(row.id, { value: event.target.value })
                      }
                      {...attributes(`value-${row.id}`)}
                      required
                    />
                  ) : (
                    <select
                      id={`value-${row.id}`}
                      value={row.value}
                      onChange={(event) =>
                        update(row.id, { value: event.target.value })
                      }
                      {...attributes(`value-${row.id}`)}
                    >
                      <option value='false'>
                        {row.control === 'emergencyShutdown'
                          ? 'Normal'
                          : row.control === 'pumpOn'
                            ? 'Stopped'
                            : 'Closed'}
                      </option>
                      <option value='true'>
                        {row.control === 'emergencyShutdown'
                          ? 'Tripped'
                          : row.control === 'pumpOn'
                            ? 'Running'
                            : 'Open'}
                      </option>
                    </select>
                  )}
                  {errorFor(`value-${row.id}`)}
                </fieldset>
              ))}
              <button
                type='button'
                className='twin-button'
                disabled={rows.length >= MAX_SCENARIO_STEPS}
                onClick={() => {
                  const id = nextId.current++;
                  setRows((current) => [
                    ...current,
                    {
                      id,
                      offsetMinutes: String(
                        Number(current.at(-1)?.offsetMinutes ?? 0) + 5
                      ),
                      control: 'chokePositionPercent',
                      value: '80',
                    },
                  ]);
                  setStepMessage(`Step ${rows.length + 1} added.`);
                  focus(`offset-${id}`);
                }}
              >
                Add change
              </button>
              <p className='twin-caption'>
                Use ordered times. Up to {MAX_SCENARIO_STEPS} changes apply to this
                scenario’s model copy.
              </p>
              <p className='sr-only' role='status'>
                {stepMessage}
              </p>
            </div>
          )}
          <button
            type='submit'
            aria-disabled={pending}
            className='twin-button twin-primary mt-5 w-full'
          >
            {pending
              ? 'Calculating…'
              : kind === 'forecast'
                ? 'Run forecast'
                : 'Run scenario'}
          </button>
        </fieldset>
        {!allowed && (
          <p className='twin-message'>Your account cannot run model analysis.</p>
        )}
        <p role='status' className='twin-caption'>
          {pending
            ? 'Calculating and saving the result. Your draft stays here.'
            : 'Each run saves a new result with these inputs.'}
        </p>
        {fetcher.data?.error && (
          <RequestMessage
            error={fetcher.data.error}
            historyUrl={`${routes.dashboard.wellhead(wellheadId)}/runs?kind=${kind}`}
          />
        )}
      </fetcher.Form>
    </section>
  );
}
