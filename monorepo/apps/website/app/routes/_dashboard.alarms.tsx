import { AlertTriangleIcon } from 'lucide-react';
import { Link, useLoaderData } from 'react-router';
import type { ActiveAlarm } from '@well-simulation/dto/res/dashboard';

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
 */

type AlarmsData = { alarms: ActiveAlarm[] };

const SEVERITY_ORDER: Record<string, number> = { CRITICAL: 0, WARNING: 1 };

export async function clientLoader() {
  const response = await fetchFn<AlarmsData>(client.dashboard['active-alarms'].$get());
  return response.data;
}

/** Most severe first, then oldest first within a severity. */
function ranked(alarms: ActiveAlarm[]): ActiveAlarm[] {
  return [...alarms].sort((a, b) => {
    const severity =
      (SEVERITY_ORDER[a.severityLevel ?? ''] ?? 99) -
      (SEVERITY_ORDER[b.severityLevel ?? ''] ?? 99);
    if (severity !== 0) return severity;
    return (
      new Date(a.triggeredAt ?? 0).getTime() - new Date(b.triggeredAt ?? 0).getTime()
    );
  });
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
  if (severity === 'CRITICAL') return 'bg-red-100 text-red-700';
  if (severity === 'WARNING') return 'bg-amber-100 text-amber-700';
  return 'bg-neutral-100 text-fgColor-muted';
}

function formatValue(value: number | null | undefined): string {
  if (value === null || value === undefined) return 'unknown';
  return new Intl.NumberFormat('en', { maximumFractionDigits: 2 }).format(value);
}

function AlarmCenter() {
  const data = useLoaderData() as AlarmsData;
  useAutoRefresh(REFRESH_INTERVALS.alarms);

  const alarms = ranked(data.alarms);
  const critical = alarms.filter((a) => a.severityLevel === 'CRITICAL').length;
  const warning = alarms.filter((a) => a.severityLevel === 'WARNING').length;
  const affectedWells = new Set(alarms.map((a) => a.wellheadId)).size;

  return (
    <div className='space-y-5'>
      <div>
        <h1 className='text-lg font-semibold'>Alarm Center</h1>
        <p className='text-sm text-fgColor-muted'>
          Alarms currently open, most severe first. An alarm opens after three
          consecutive breaching readings and clears after three consecutive normal ones,
          so a single stray value does not raise one.
        </p>
      </div>

      <div className='grid gap-4 md:grid-cols-3'>
        {[
          { label: 'Critical', value: critical },
          { label: 'Warning', value: warning },
          { label: 'Wellheads affected', value: affectedWells },
        ].map((tile) => (
          <div key={tile.label} className='card py-3! px-5! rounded-lg'>
            <p className='text-sm text-fgColor-muted'>{tile.label}</p>
            <span className='text-xl font-semibold'>{tile.value}</span>
          </div>
        ))}
      </div>

      {alarms.length === 0 ? (
        <div className='card p-6! text-sm text-fgColor-muted'>
          No active alarms. Every parameter is inside its configured range.
        </div>
      ) : (
        <div className='card overflow-x-auto'>
          <table className='w-full text-sm'>
            <thead className='text-left text-fgColor-muted'>
              <tr className='border-b border-b-neutral-200'>
                <th className='pb-2 font-medium'>Severity</th>
                <th className='pb-2 font-medium'>Wellhead</th>
                <th className='pb-2 font-medium'>Parameter</th>
                <th className='pb-2 font-medium'>Value</th>
                <th className='pb-2 font-medium'>Threshold</th>
                <th className='pb-2 font-medium'>Open for</th>
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
