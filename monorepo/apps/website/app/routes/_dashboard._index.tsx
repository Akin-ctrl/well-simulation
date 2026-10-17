import { GettingStarted } from '../domains/dashboard/components/page-help';
import { useRetainedRead } from '../hooks/use-retained-read';
import { ReadFailure } from '../domains/twin/read-failure';
import { useState } from 'react';
import { Link, useLoaderData } from 'react-router';
import { ApiError } from '@well-simulation/dto/error';
import type { DashboardOverviewResponse } from '@well-simulation/dto/res/dashboard';
import type { FleetModelStatus } from '@well-simulation/dto/res/model-status';
import { client, fetchFn } from '../utils/api';
import { routes } from '../config/routes';
import { REFRESH_INTERVALS, useAutoRefresh } from '../hooks/use-auto-refresh';
import {
  FreshnessIndicator,
  STALE_AFTER_SECONDS,
} from '../domains/dashboard/components/freshness';
import { ModelBadge, useModelCheckOverdue } from '../domains/twin/model-status';
import { useElapsedSeconds } from '../hooks/use-elapsed-seconds';
import {
  abnormal,
  signalText,
  timeText,
  errorState,
} from '../domains/twin/presentation';

export async function clientLoader() {
  try {
    const [data, models] = await Promise.all([
      fetchFn<DashboardOverviewResponse>(client.dashboard.overview.$get()),
      fetchFn<FleetModelStatus>(client.twin.status.$get()).catch((error: unknown) => {
        if (
          error instanceof ApiError &&
          (error.statusCode === 401 || error.statusCode === 403)
        )
          throw error;
        return null;
      }),
    ]);
    return { value: { data: data.data, models: models?.data ?? null }, error: null };
  } catch (error) {
    if (
      error instanceof ApiError &&
      (error.statusCode === 401 || error.statusCode === 403)
    )
      throw error;
    return { value: null, error: errorState(error) };
  }
}
export default function Overview() {
  const incoming = useLoaderData<typeof clientLoader>();
  const retained = useRetainedRead(incoming.value, 'fleet');
  useAutoRefresh(REFRESH_INTERVALS.overview);
  if (!retained) return incoming.error ? <ReadFailure error={incoming.error} /> : null;
  return (
    <LoadedOverview
      data={retained.data}
      models={retained.models}
      error={incoming.error}
    />
  );
}
function LoadedOverview({
  data,
  models,
  error,
}: {
  data: DashboardOverviewResponse;
  models: FleetModelStatus | null;
  error: ReturnType<typeof errorState> | null;
}) {
  const elapsed = useElapsedSeconds(data);
  const overdue = useModelCheckOverdue(models);
  const [search, setSearch] = useState('');
  const [attention, setAttention] = useState(false);
  const wells = data.wellheads
    .map((asset) => {
      const readings = data.latestReadings.filter(
        (item) => item.wellheadId === asset.wellheadId
      );
      const status = models?.wellheads.find(
        (item) => item.wellheadId === asset.wellheadId
      );
      return { asset, readings, status, abnormalReadings: readings.filter(abnormal) };
    })
    .filter((item) =>
      `${item.asset.wellheadName} ${item.asset.locationName} ${item.asset.fieldName}`
        .toLowerCase()
        .includes(search.toLowerCase())
    )
    .filter(
      (item) =>
        !attention ||
        item.asset.activeAlarmCount > 0 ||
        item.abnormalReadings.length > 0 ||
        !item.readings.length ||
        !item.status?.current ||
        !models?.available ||
        overdue ||
        (data.summary.latestReadingAgeSeconds ?? Infinity) + elapsed >
          STALE_AFTER_SECONDS
    );
  return (
    <div className='twin-workspace twin-stack'>
      {error && <ReadFailure error={error} retained />}
      <div className='twin-heading'>
        <div>
          <p className='twin-eyebrow'>Field operations</p>
          <h1>Wellhead overview</h1>
          <p className='twin-muted'>
            Choose a well to check its readings, forecast its behaviour, or try a
            change.
          </p>
        </div>
        <FreshnessIndicator
          announce
          ageSeconds={data.summary.latestReadingAgeSeconds}
          latestReadingAt={data.summary.latestReadingAt}
        />
      </div>
      <GettingStarted />
      <div className='grid gap-4 sm:grid-cols-3'>
        {[
          ['Configured wells', data.summary.totalWellheads],
          ['Tracked signals', data.summary.parametersTracked],
          ['Active alarms', data.summary.activeAlarms],
        ].map(([label, value]) => (
          <div className='twin-panel twin-panel-body' key={label}>
            <p className='twin-muted'>{label}</p>
            <p className='twin-number'>{value}</p>
          </div>
        ))}
      </div>
      <section>
        <div className='twin-heading'>
          <div>
            <h2>Your wells</h2>
            <p className='twin-muted'>
              Latest available readings. Open a well for details.
            </p>
          </div>
          <span className='twin-muted' role='status'>
            {wells.length} of {data.wellheads.length} wells
          </span>
        </div>
        <div className='twin-filter'>
          <div>
            <label htmlFor='well-search'>Find a well, pad, or field</label>
            <input
              id='well-search'
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder='Search wells'
            />
          </div>
          <label className='twin-inline' htmlFor='attention-filter'>
            <input
              id='attention-filter'
              type='checkbox'
              checked={attention}
              onChange={(event) => setAttention(event.target.checked)}
              style={{ width: 18, minHeight: 18 }}
            />
            Needs attention
          </label>
        </div>
        <div className='twin-panel'>
          <div
            className='twin-scroll'
            tabIndex={0}
            role='region'
            aria-label='Wellhead snapshot'
          >
            <table className='twin-table twin-fleet-table'>
              <thead>
                <tr>
                  <th scope='col'>Well</th>
                  <th scope='col' className='numeric'>
                    Tubing pressure
                  </th>
                  <th scope='col' className='numeric'>
                    Flow rate
                  </th>
                  <th scope='col' className='numeric'>
                    Water cut
                  </th>
                  <th scope='col'>Data age</th>
                  <th scope='col'>Model</th>
                  <th scope='col'>Conditions</th>
                </tr>
              </thead>
              <tbody>
                {wells.map(({ asset, readings, status, abnormalReadings }) => (
                  <tr key={asset.wellheadId}>
                    <td>
                      <Link to={routes.dashboard.wellhead(asset.wellheadId)}>
                        {asset.wellheadName}
                      </Link>
                      <span className='block twin-caption'>
                        {asset.fieldName} · {asset.locationName}
                      </span>
                    </td>
                    {['tubing_pressure', 'flow_rate', 'water_cut'].map((code) => {
                      const reading = readings.find(
                        (item) => item.parameterCode === code
                      );
                      return (
                        <td key={code} className='numeric'>
                          {signalText(code, reading?.rawValue, reading?.canonicalUnit)}
                        </td>
                      );
                    })}
                    <td>
                      <FreshnessIndicator
                        ageSeconds={
                          readings.length ? data.summary.latestReadingAgeSeconds : null
                        }
                        latestReadingAt={
                          readings.length ? data.summary.latestReadingAt : null
                        }
                      />
                    </td>
                    <td>
                      <ModelBadge
                        status={status}
                        available={models?.available ?? false}
                        overdue={overdue || Boolean(error)}
                      />
                    </td>
                    <td>
                      {asset.activeAlarmCount > 0 && (
                        <p className='text-red-800'>
                          {asset.activeAlarmCount} active alarms
                        </p>
                      )}
                      {abnormalReadings.length ? (
                        <details>
                          <summary className='cursor-pointer text-amber-800'>
                            {abnormalReadings.length} out of range
                          </summary>
                          <ul className='mt-2 space-y-1'>
                            {abnormalReadings.map((item) => (
                              <li key={item.parameterCode}>
                                {item.parameterDisplayName}:{' '}
                                {signalText(
                                  item.parameterCode ?? '',
                                  item.rawValue,
                                  item.canonicalUnit
                                )}
                              </li>
                            ))}
                          </ul>
                        </details>
                      ) : !readings.length ? (
                        'No readings in latest batch'
                      ) : asset.activeAlarmCount === 0 ? (
                        'No range warnings'
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!wells.length && (
            <p className='twin-empty'>
              {data.wellheads.length
                ? 'No wells match these filters.'
                : 'No wellheads are configured.'}
            </p>
          )}
        </div>
        <p className='twin-caption'>
          Model check: {timeText(models?.checkedAt)} · Telemetry sample:{' '}
          {timeText(data.summary.latestReadingAt)}
        </p>
      </section>
      <section className='twin-panel'>
        <div className='twin-panel-heading'>
          <h2>Active alarm preview</h2>
          <Link to={routes.dashboard.alarms} className='text-blue-800 underline'>
            Open Alarm Center
          </Link>
        </div>
        <div className='twin-panel-body'>
          {data.activeAlarms.length ? (
            <ul className='space-y-3'>
              {data.activeAlarms.map((alarm) => (
                <li key={alarm.eventId}>
                  <strong>
                    {alarm.severityLevel}: {alarm.wellheadName} ·{' '}
                    {alarm.parameterDisplayName}
                  </strong>
                  <p className='twin-muted'>
                    {signalText(
                      alarm.parameterCode ?? '',
                      alarm.triggeredValue,
                      alarm.canonicalUnit
                    )}{' '}
                    · {timeText(alarm.triggeredAt)}
                  </p>
                </li>
              ))}
            </ul>
          ) : (
            <p className='twin-muted'>No active alarms.</p>
          )}
        </div>
      </section>
    </div>
  );
}
