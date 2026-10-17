import { PageHelp } from '../domains/dashboard/components/page-help';
import {
  readDashboardView,
  ReadStatus,
} from '../domains/dashboard/components/read-status';
import { useRetainedRead } from '../hooks/use-retained-read';
import { ReadFailure } from '../domains/twin/read-failure';
import { AlertTriangleIcon } from 'lucide-react';
import { Link, useLoaderData } from 'react-router';
import type { ActiveAlarmsResponse } from '@well-simulation/dto/res/dashboard';

import { client, fetchFn } from '../utils/api';
import { routes } from '../config/routes';
import { REFRESH_INTERVALS, useAutoRefresh } from '../hooks/use-auto-refresh';

/**
 * The Alarm Center, scoped by ADR 0032.
 *
 * Shows active alarms as evidence an operator can act on: severity, the
 * affected asset, the parameter and its value, the threshold that was crossed,
 * how long it has been open, and a link to the wellhead.
 *
 * There are deliberately no acknowledge or shelve controls. The backend has no
 * alarm lifecycle model, so those buttons could not record anything. ADR 0032
 * decided that a control which pretends to work is worse than its absence.
 *
 * The API returns the list already ranked, most severe first and then oldest
 * first, and caps it. The tiles therefore use the API's totals, which count
 * every active alarm, rather than counting the rows on screen.
 */

export async function clientLoader() {
  return readDashboardView(
    async () =>
      (await fetchFn<ActiveAlarmsResponse>(client.dashboard['active-alarms'].$get()))
        .data
  );
}

/** How long an alarm has been open, which is what makes it urgent or not. */
function age(triggeredAt: string | Date | null | undefined): string {
  if (!triggeredAt) return 'unknown';

  const seconds = Math.max(
    0,
    Math.round((Date.now() - new Date(triggeredAt).getTime()) / 1000)
  );
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
}

function severityClass(severity: string | null | undefined): string {
  if (severity === 'CRITICAL') return 'twin-error';
  if (severity === 'WARNING') return 'twin-attention';
  return 'twin-badge';
}

function formatValue(value: number | null | undefined): string {
  if (value === null || value === undefined) return 'unknown';
  return new Intl.NumberFormat('en', { maximumFractionDigits: 2 }).format(value);
}

function AlarmCenter() {
  const incoming = useLoaderData<typeof clientLoader>();
  const retained = useRetainedRead(incoming.value, 'alarms');
  useAutoRefresh(REFRESH_INTERVALS.alarms);
  if (!retained) return incoming.error ? <ReadFailure error={incoming.error} /> : null;
  const { totals, alarms } = retained.data;

  return (
    <div className='twin-workspace twin-stack'>
      {incoming.error && <ReadFailure error={incoming.error} retained />}
      <div>
        <h1>Alarm Center</h1>
        <p className='text-sm text-fgColor-muted'>
          Check the most severe alarms first, then open the affected well for current
          readings.
        </p>
      </div>

      <ReadStatus
        checkedAt={retained.checkedAt}
        intervalMs={REFRESH_INTERVALS.alarms}
      />
      <PageHelp title='How alarms open and clear'>
        <p>
          An alarm opens after three consecutive readings cross a threshold. Three
          consecutive normal readings clear it.
        </p>
        <p>
          Value when triggered shows the reading that opened the alarm. Open the well to
          check its latest readings and their age.
        </p>
      </PageHelp>
      <div className='grid gap-4 md:grid-cols-3'>
        {[
          { label: 'Critical', value: totals.critical },
          { label: 'Warning', value: totals.warning },
          { label: 'Wellheads affected', value: totals.affectedWellheads },
        ].map((tile) => (
          <div key={tile.label} className='twin-panel twin-panel-body'>
            <p className='text-sm text-fgColor-muted'>{tile.label}</p>
            <span className='text-xl font-semibold'>{tile.value}</span>
          </div>
        ))}
      </div>

      {alarms.length < totals.total && (
        <p className='text-sm text-fgColor-muted'>
          Showing the first {alarms.length} of {totals.total} active alarms, most severe
          first.
        </p>
      )}

      {totals.total === 0 ? (
        <div className='twin-panel twin-empty'>No active alarms right now.</div>
      ) : (
        <div
          className='twin-panel twin-scroll'
          role='region'
          aria-label='Active alarms'
          tabIndex={0}
        >
          <table className='twin-table'>
            <thead className='text-left text-fgColor-muted'>
              <tr className='border-b border-b-neutral-200'>
                <th scope='col' className='pb-2 font-medium'>
                  Severity
                </th>
                <th scope='col' className='pb-2 font-medium'>
                  Wellhead
                </th>
                <th scope='col' className='pb-2 font-medium'>
                  Parameter
                </th>
                <th scope='col' className='pb-2 font-medium'>
                  Value when triggered
                </th>
                <th scope='col' className='pb-2 font-medium'>
                  Threshold
                </th>
                <th scope='col' className='pb-2 font-medium'>
                  Open for
                </th>
              </tr>
            </thead>
            <tbody>
              {alarms.map((alarm) => (
                <tr
                  key={alarm.eventId ?? `${alarm.wellheadId}-${alarm.triggeredAt}`}
                  className='border-b border-b-neutral-100 last:border-b-0'
                >
                  <td className='py-2.5'>
                    <span
                      className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${severityClass(
                        alarm.severityLevel
                      )}`}
                    >
                      <AlertTriangleIcon className='size-3' />
                      {alarm.severityLevel ?? 'unknown'}
                    </span>
                  </td>
                  <td className='py-2.5'>
                    {alarm.wellheadId ? (
                      <Link
                        to={routes.dashboard.wellhead(alarm.wellheadId)}
                        className='font-medium hover:underline'
                      >
                        {alarm.wellheadName}
                      </Link>
                    ) : (
                      alarm.wellheadName
                    )}
                    <span className='block text-xs text-fgColor-muted'>
                      {alarm.locationName}
                    </span>
                  </td>
                  <td className='py-2.5'>{alarm.parameterDisplayName}</td>
                  <td className='py-2.5 font-medium'>
                    {formatValue(alarm.triggeredValue)} {alarm.canonicalUnit}
                  </td>
                  <td className='py-2.5 text-fgColor-muted'>
                    {alarm.operator} {formatValue(alarm.thresholdValue)}{' '}
                    {alarm.canonicalUnit}
                  </td>
                  <td className='py-2.5'>{age(alarm.triggeredAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default AlarmCenter;
