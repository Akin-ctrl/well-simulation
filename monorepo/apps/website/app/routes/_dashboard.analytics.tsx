import { useLoaderData } from 'react-router';
import type { DashboardAnalyticsResponse } from '@well-simulation/dto/res/dashboard';
import { TrendChart } from '../domains/dashboard/components/trend-chart';
import { PageHelp } from '../domains/dashboard/components/page-help';
import {
  ReadStatus,
  readDashboardView,
} from '../domains/dashboard/components/read-status';
import { ReadFailure } from '../domains/twin/read-failure';
import { useRetainedRead } from '../hooks/use-retained-read';
import { client, fetchFn } from '../utils/api';
import { REFRESH_INTERVALS, useAutoRefresh } from '../hooks/use-auto-refresh';

export async function clientLoader() {
  return readDashboardView(
    async () =>
      (await fetchFn<DashboardAnalyticsResponse>(client.dashboard.analytics.$get()))
        .data
  );
}

/** Fleet averages and alarm counts retain their own meaning and timestamps. */
export default function Analytics() {
  const incoming = useLoaderData<typeof clientLoader>();
  const retained = useRetainedRead(incoming.value, 'analytics');
  useAutoRefresh(REFRESH_INTERVALS.analytics);
  if (!retained) return incoming.error ? <ReadFailure error={incoming.error} /> : null;
  const data = retained.data;
  const totals = new Map<string, number>();
  for (const count of data.alarmCounts) {
    const severity = count.severityLevel ?? 'Unknown';
    totals.set(severity, (totals.get(severity) ?? 0) + count.alarmsTriggered);
  }
  return (
    <div className='twin-workspace twin-stack'>
      {incoming.error && <ReadFailure error={incoming.error} retained />}
      <header>
        <h1>Analytics</h1>
        <p className='twin-muted'>
          Compare recent trends across the fleet. Open a well for its individual
          history.
        </p>
      </header>
      <ReadStatus
        checkedAt={retained.checkedAt}
        intervalMs={REFRESH_INTERVALS.analytics}
      />
      <PageHelp title='What these charts show'>
        <p>
          Each point averages available readings across wells over two minutes. A fleet
          average can hide a change at one well.
        </p>
        <p>
          Check each chart’s timestamps. A line gap means missing data. Temperature and
          flow use separate axes because their units differ.
        </p>
        <p>
          Alarm counts include events triggered in the last 24 hours, including events
          that have cleared. Alarms shows only the active events.
        </p>
      </PageHelp>
      <section className='twin-panel'>
        <div className='twin-panel-heading'>
          <h2>Alarms triggered in the last 24 hours</h2>
        </div>
        <div className='twin-panel-body'>
          {totals.size ? (
            <dl className='twin-grid'>
              {[...totals].map(([severity, count]) => (
                <div key={severity}>
                  <dt className='twin-muted'>{severity}</dt>
                  <dd className='twin-number'>{count}</dd>
                </div>
              ))}
            </dl>
          ) : (
            <p className='twin-muted'>No alarm events in this period.</p>
          )}
        </div>
      </section>
      <div className='twin-grid'>
        {[
          {
            title: 'Pressure trends',
            points: data.pressureTrend,
            codes: ['tubing_pressure', 'casing_pressure', 'annulus_pressure'],
          },
          {
            title: 'Temperature and flow',
            points: data.temperatureFlowTrend,
            codes: ['wellhead_temperature', 'flow_rate'],
          },
          {
            title: 'Water cut and gas–oil ratio',
            points: data.waterCutGorTrend,
            codes: ['water_cut', 'gas_oil_ratio'],
          },
        ].map((chart) => (
          <section className='twin-panel' key={chart.title}>
            <div className='twin-panel-heading'>
              <div>
                <h2>{chart.title}</h2>
                <p className='twin-muted'>Two-minute averages across wells.</p>
              </div>
            </div>
            <div className='twin-panel-body'>
              <TrendChart
                points={chart.points}
                metricCodes={chart.codes}
                emptyMessage='No readings in this period yet.'
              />
            </div>
          </section>
        ))}
      </div>
    </div>
  );
}
