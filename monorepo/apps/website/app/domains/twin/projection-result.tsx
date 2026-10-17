import { useState } from 'react';
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { ProjectionDetail } from '@well-simulation/dto/res/projection-detail';
import { BOOLEAN_STATE_LABELS } from '@well-simulation/dto/res/dashboard';
import { SCENARIO_CONTROLS } from '@well-simulation/dto/req/scenario-controls';
import {
  controlText,
  numberText,
  projectionRows,
  signalText,
  timeText,
} from './presentation';

const REASONS: Record<string, string> = {
  synthetic_uncalibrated:
    'This reference model uses synthetic parameters without field calibration.',
  forecast_horizon:
    'Confidence decreases as the forecast extends further into the future.',
  recent_recovery: 'The model recently recovered from a saved state or a clock gap.',
  no_active_alarm_rules: 'There are no active alarm rules for threshold comparison.',
  near_alarm_threshold: 'A projected value approaches an active threshold.',
  sharp_control_change: 'The schedule includes a large control change.',
};
const clockTime = (value: number) =>
  new Intl.DateTimeFormat('en', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(value));

/** Charts and the table read exactly the same immutable result points. */
export function ProjectionResult({ run }: { run: ProjectionDetail }) {
  const [code, setCode] = useState(
    run.signals.find((signal) => signal.code === 'tubing_pressure')?.code ??
      run.signals[0]?.code ??
      Object.keys(run.points[0]?.values ?? {})[0] ??
      ''
  );
  const [page, setPage] = useState(0);
  const signal = run.signals.find((item) => item.code === code);
  const rows = projectionRows(run, code);
  const last = rows.at(-1);
  const difference =
    last?.value != null && last.baseline != null ? last.value - last.baseline : null;
  const scenario = run.kind === 'scenario';
  const stateSignal = Boolean(BOOLEAN_STATE_LABELS[code]);
  const firstRisk = run.points.find(
    (point) => point.alarmRisk === 'warning' || point.alarmRisk === 'critical'
  );
  const allUnknown = run.points.every((point) => point.alarmRisk === 'unknown');
  return (
    <div className='twin-stack'>
      <section className='twin-panel'>
        <div className='twin-panel-heading'>
          <div>
            <h2 id='projection-title' tabIndex={-1}>
              {run.name ?? 'Saved forecast'}
            </h2>
            <p className='twin-caption'>Created {timeText(run.generatedAt)}</p>
          </div>
          <span className='twin-badge'>Saved result</span>
        </div>
        <div className='twin-panel-body'>
          <p className='twin-muted mb-4'>
            This saved result keeps its original inputs. Editing the draft does not
            change it.
          </p>
          <div className='twin-filter'>
            <div>
              <label htmlFor='projection-signal'>Signal</label>
              <select
                id='projection-signal'
                value={code}
                onChange={(event) => setCode(event.target.value)}
              >
                {run.signals.map((item) => (
                  <option value={item.code} key={item.code}>
                    {item.displayName}
                    {item.canonicalUnit ? ` (${item.canonicalUnit})` : ''}
                  </option>
                ))}
              </select>
            </div>
          </div>
          <p className='twin-caption'>
            Starts {timeText(run.baseSimulatedAt)} · {run.horizonSeconds / 60} minutes ·
            sampled every {run.stepSeconds} seconds
          </p>
          <figure
            className='twin-chart'
            aria-label={`${signal?.displayName ?? code} projection chart`}
          >
            <figcaption className='twin-chart-caption'>
              {signal?.displayName ?? code} ({signal?.canonicalUnit || 'state'}). Times
              in {Intl.DateTimeFormat().resolvedOptions().timeZone}.
              {scenario
                ? ' Dashed line: unchanged baseline. Solid line: scenario.'
                : ' Solid line: forecast.'}{' '}
              Missing values break the line. Plotted values are available in the table
              below.
            </figcaption>
            <ResponsiveContainer width='100%' height={310}>
              <LineChart
                data={rows}
                margin={{ top: 30, right: 20, bottom: 15, left: 5 }}
                accessibilityLayer
              >
                <CartesianGrid stroke='#e3e7ed' vertical={false} />
                <XAxis
                  dataKey='time'
                  type='number'
                  domain={[
                    Date.parse(run.baseSimulatedAt),
                    Date.parse(run.baseSimulatedAt) + run.horizonSeconds * 1000,
                  ]}
                  tickFormatter={clockTime}
                  minTickGap={35}
                  tick={{ fontSize: 12, fill: '#586477' }}
                />
                <YAxis
                  width={65}
                  tick={{ fontSize: 12, fill: '#586477' }}
                  tickFormatter={(value) =>
                    stateSignal
                      ? signalText(code, Number(value))
                      : numberText(Number(value))
                  }
                  domain={stateSignal ? [0, 1] : ['auto', 'auto']}
                  ticks={stateSignal ? [0, 1] : undefined}
                  label={
                    stateSignal
                      ? undefined
                      : {
                          value: signal?.canonicalUnit ?? 'Unit unavailable',
                          angle: -90,
                          position: 'insideLeft',
                        }
                  }
                />
                <Tooltip
                  labelFormatter={(value) => timeText(new Date(Number(value)))}
                  formatter={(value) =>
                    signalText(
                      code,
                      value == null ? null : Number(value),
                      signal?.canonicalUnit
                    )
                  }
                />
                <Legend />
                {run.scheduledControls.map((step, index) => (
                  <ReferenceLine
                    key={index}
                    x={Date.parse(run.baseSimulatedAt) + step.offsetMinutes * 60000}
                    stroke='#718096'
                    strokeDasharray='3 4'
                    label={{
                      value: `Change ${index + 1}`,
                      position: 'insideTopLeft',
                      fontSize: 10,
                    }}
                  />
                ))}
                {scenario && (
                  <Line
                    dataKey='baseline'
                    name='Unchanged baseline'
                    type={stateSignal ? 'stepAfter' : 'linear'}
                    stroke='var(--twin-baseline)'
                    strokeWidth={2}
                    strokeDasharray='7 5'
                    dot={false}
                    connectNulls={false}
                    isAnimationActive={false}
                  />
                )}
                <Line
                  dataKey='value'
                  name={scenario ? 'Scenario' : 'Forecast'}
                  type={stateSignal ? 'stepAfter' : 'linear'}
                  stroke='var(--twin-scenario)'
                  strokeWidth={2.5}
                  dot={false}
                  connectNulls={false}
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </figure>
        </div>
        <div className='twin-summary'>
          <div>
            <p className='twin-muted'>At +{run.horizonSeconds / 60} min</p>
            <p className='twin-number text-[#553afe]'>
              {signalText(code, last?.value, signal?.canonicalUnit)}
            </p>
          </div>
          {scenario ? (
            <>
              <div>
                <p className='twin-muted'>Baseline</p>
                <p className='twin-number'>
                  {signalText(code, last?.baseline, signal?.canonicalUnit)}
                </p>
              </div>
              <div>
                <p className='twin-muted'>Difference</p>
                <p className='twin-number'>
                  {stateSignal
                    ? difference == null
                      ? 'Unavailable'
                      : difference === 0
                        ? 'Unchanged'
                        : 'Changed'
                    : `${difference != null && difference > 0 ? '+' : ''}${numberText(difference)} ${signal?.canonicalUnit ?? ''}`}
                </p>
              </div>
            </>
          ) : (
            <div>
              <p className='twin-muted'>Samples</p>
              <p className='twin-number'>{run.points.length}</p>
            </div>
          )}
        </div>
      </section>
      <div className='twin-grid'>
        <section className='twin-panel'>
          <div className='twin-panel-heading'>
            <h2>Model confidence</h2>
            <span className='twin-badge'>{run.confidence.label}</span>
          </div>
          <div className='twin-panel-body'>
            <ul className='twin-muted list-disc pl-4 space-y-2'>
              {run.confidence.reasons.map((reason) => (
                <li key={reason}>{REASONS[reason] ?? reason.replaceAll('_', ' ')}</li>
              ))}
            </ul>
            <p className='twin-caption'>
              Rule-based score {numberText(run.confidence.score)}. It describes model
              confidence, not the probability of this outcome.
            </p>
          </div>
        </section>
        <section className='twin-panel'>
          <div className='twin-panel-heading'>
            <h2>Projected thresholds</h2>
          </div>
          <div className='twin-panel-body'>
            <p>
              {firstRisk
                ? `First sampled ${firstRisk.alarmRisk} exceedance at +${firstRisk.elapsedSeconds / 60} min`
                : allUnknown
                  ? 'Threshold risk unavailable'
                  : 'No exceedance in the returned samples'}
            </p>
            {firstRisk && (
              <p className='twin-caption'>
                {timeText(firstRisk.timestamp)} · Rule IDs{' '}
                {firstRisk.triggeredRuleIds.join(', ')}
              </p>
            )}
            <p className='twin-caption'>
              Across all projected signals. The model can miss events between saved
              points.
            </p>
          </div>
        </section>
      </div>
      <section className='twin-panel'>
        <details>
          <summary className='twin-panel-heading cursor-pointer'>
            Original run inputs
          </summary>
          <div className='twin-panel-body'>
            <p className='twin-caption'>Model version {run.modelVersion}</p>
            <h3>Baseline controls</h3>
            <p className='twin-muted'>
              The baseline keeps these starting controls unchanged.
            </p>
            <dl className='twin-facts mt-3'>
              {Object.entries(run.baselineControls).map(([key, value]) => (
                <div key={key}>
                  <dt>
                    {Object.values(SCENARIO_CONTROLS).find(
                      (info) => info.storedKey === key
                    )?.label ??
                      (key === 'swab_valve_open'
                        ? 'Swab valve'
                        : key.replaceAll('_', ' '))}
                  </dt>
                  <dd>{controlText(key, value)}</dd>
                </div>
              ))}
            </dl>
            {scenario && (
              <>
                <h3 className='mt-5'>Saved schedule</h3>
                <ol className='twin-muted mt-2 space-y-2'>
                  {run.scheduledControls.map((step, index) => (
                    <li key={index}>
                      Change {index + 1}, at +{step.offsetMinutes} min:{' '}
                      {Object.entries(step.input)
                        .map(
                          ([key, value]) =>
                            `${SCENARIO_CONTROLS[key as keyof typeof SCENARIO_CONTROLS].label}: ${controlText(key, value)}`
                        )
                        .join('; ')}
                    </li>
                  ))}
                </ol>
              </>
            )}
          </div>
        </details>
      </section>
      <section className='twin-panel'>
        <details>
          <summary className='twin-panel-heading cursor-pointer'>
            View plotted values as a table
          </summary>
          <div
            className='twin-scroll'
            tabIndex={0}
            role='region'
            aria-label='Projection values'
          >
            <table className='twin-table'>
              <caption className='sr-only'>
                {signal?.displayName} in {signal?.canonicalUnit}. Times in the browser
                timezone.
              </caption>
              <thead>
                <tr>
                  <th scope='col'>Time</th>
                  <th scope='col'>Elapsed</th>
                  <th scope='col'>{scenario ? 'Scenario' : 'Forecast'}</th>
                  {scenario && <th scope='col'>Baseline</th>}
                  <th scope='col'>All-signal risk</th>
                </tr>
              </thead>
              <tbody>
                {run.points.slice(page * 50, (page + 1) * 50).map((point) => (
                  <tr key={point.timestamp}>
                    <td>{timeText(point.timestamp)}</td>
                    <td>{point.elapsedSeconds}s</td>
                    <td>
                      {signalText(code, point.values[code], signal?.canonicalUnit)}
                    </td>
                    {scenario && (
                      <td>
                        {signalText(
                          code,
                          point.baselineValues?.[code],
                          signal?.canonicalUnit
                        )}
                      </td>
                    )}
                    <td>{point.alarmRisk}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className='twin-panel-body twin-inline'>
            <button
              className='twin-button'
              disabled={page === 0}
              onClick={() => setPage(page - 1)}
            >
              Previous values
            </button>
            <span className='twin-muted' role='status'>
              Page {page + 1} of {Math.ceil(run.points.length / 50)}
            </span>
            <button
              className='twin-button'
              disabled={(page + 1) * 50 >= run.points.length}
              onClick={() => setPage(page + 1)}
            >
              Next values
            </button>
          </div>
        </details>
      </section>
    </div>
  );
}
