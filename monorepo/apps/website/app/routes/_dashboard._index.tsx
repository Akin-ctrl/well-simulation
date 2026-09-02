import {
  ActivityIcon,
  AlertTriangleIcon,
  GaugeIcon,
  GridIcon,
  RadioTowerIcon,
} from 'lucide-react';
import { Link, useLoaderData } from 'react-router';
import type {
  DashboardOverviewResponse,
  LatestReading,
} from '@well-simulation/dto/res/dashboard';
import { client, fetchFn } from '../utils/api';
import { routes } from '../config/routes';
import { REFRESH_INTERVALS, useAutoRefresh } from '../hooks/use-auto-refresh';
import { FreshnessIndicator } from '../domains/dashboard/components/freshness';

type WellheadCard = {
  wellheadId: number | null;
  wellheadName: string;
  locationName: string;
  fieldName: string;
  readings: LatestReading[];
};

export async function clientLoader() {
  const response = await fetchFn<DashboardOverviewResponse>(
    client.dashboard.overview.$get()
  );
  return response.data;
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return '—';
  }

  return new Intl.NumberFormat('en', {
    maximumFractionDigits: 2,
  }).format(value);
}

function formatDate(value: string | Date | null | undefined) {
  if (!value) {
    return 'No readings yet';
  }

  return new Intl.DateTimeFormat('en', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(value));
}

function readingStatus(reading: LatestReading) {
  if (
    reading.rawValue === null ||
    reading.rawValue === undefined ||
    reading.normalMin === null ||
    reading.normalMin === undefined ||
    reading.normalMax === null ||
    reading.normalMax === undefined
  ) {
    return 'unknown';
  }

  if (reading.rawValue < reading.normalMin || reading.rawValue > reading.normalMax) {
    return 'out of range';
  }

  return 'normal';
}

function readingStatusClass(status: string) {
  if (status === 'out of range') {
    return 'text-red-600';
  }
  if (status === 'unknown') {
    return 'text-fgColor-muted';
  }
  return 'text-green-700';
}

function alarmSeverityClass(severity: string | null | undefined) {
  const normalized = severity?.toLowerCase();
  if (normalized === 'critical') {
    return 'text-red-600';
  }
  if (normalized === 'warning') {
    return 'text-amber-600';
  }
  return 'text-fgColor-muted';
}

/**
 * The parameters shown on a card when nothing is wrong.
 *
 * The four an operator reads first on a producing well. Before this the card
 * showed whichever four parameters happened to come back first from the view,
 * which was arbitrary and differed by wellhead.
 */
const HEADLINE_PARAMETERS = [
  'tubing_pressure',
  'wellhead_temperature',
  'flow_rate',
  'water_cut',
] as const;

const CARD_READING_COUNT = 4;

/**
 * Order a wellhead's readings so anything abnormal is visible on the card.
 *
 * Out-of-range values come first, whichever parameter they belong to, then the
 * headline four, then whatever is left. An excursion on a parameter outside the
 * headline set would otherwise not appear on the overview at all.
 */
function cardReadings(readings: LatestReading[]): LatestReading[] {
  const abnormal = readings.filter((r) => readingStatus(r) === 'out of range');
  const headline = readings.filter(
    (r) =>
      readingStatus(r) !== 'out of range' &&
      HEADLINE_PARAMETERS.includes(
        (r.parameterCode ?? '') as (typeof HEADLINE_PARAMETERS)[number]
      )
  );
  const rest = readings.filter((r) => !abnormal.includes(r) && !headline.includes(r));

  const ordered = [...abnormal, ...headline, ...rest];
  return ordered.slice(0, Math.max(CARD_READING_COUNT, abnormal.length));
}

function wellheadCards(readings: LatestReading[]) {
  const cards = new Map<string, WellheadCard>();

  for (const reading of readings) {
    const key = reading.wellheadName ?? `Wellhead ${reading.wellheadId ?? 'unknown'}`;
    const existing = cards.get(key);

    if (existing) {
      existing.readings.push(reading);
      continue;
    }

    cards.set(key, {
      wellheadId: reading.wellheadId ?? null,
      wellheadName: key,
      locationName: reading.locationName ?? 'Unknown location',
      fieldName: reading.fieldName ?? 'Unknown field',
      readings: [reading],
    });
  }

  return [...cards.values()];
}

function Overview() {
  const data = useLoaderData() as DashboardOverviewResponse;
  useAutoRefresh(REFRESH_INTERVALS.overview);

  const cards = wellheadCards(data.latestReadings);
  const summaryCards = [
    {
      label: 'Total Wells',
      value: data.summary.totalWellheads,
      icon: GridIcon,
    },
    {
      label: 'Parameters',
      value: data.summary.parametersTracked,
      icon: GaugeIcon,
    },
    {
      label: 'Active Alarms',
      value: data.summary.activeAlarms,
      icon: AlertTriangleIcon,
    },
    {
      label: 'Telemetry',
      value: (
        <FreshnessIndicator
          ageSeconds={data.summary.latestReadingAgeSeconds}
          latestReadingAt={data.summary.latestReadingAt}
        />
      ),
      icon: RadioTowerIcon,
    },
  ];

  return (
    <div className='space-y-5'>
      <div className='grid gap-4 md:grid-cols-2 xl:grid-cols-4'>
        {summaryCards.map((card) => {
          const Icon = card.icon;

          return (
            <div key={card.label} className='py-3! px-5! rounded-lg card'>
              <div className='flex justify-between items-center gap-3'>
                <p className='text-sm text-fgColor-muted'>{card.label}</p>
                <Icon className='size-5' />
              </div>

              <span className='text-xl font-semibold'>{card.value}</span>
            </div>
          );
        })}
      </div>

      <div className='border-t border-t-neutral-300' />

      <div className='grid gap-6 xl:grid-cols-[2fr_1fr]'>
        <section className='space-y-4'>
          <div>
            <h2 className='text-lg font-semibold'>Wellhead Snapshot</h2>
            <p className='text-sm text-fgColor-muted'>
              Latest complete historian snapshot across {cards.length} of{' '}
              {data.summary.totalWellheads} wells.
            </p>
          </div>

          {cards.length ? (
            <div className='grid gap-4 md:grid-cols-2'>
              {cards.map((wellhead) => (
                <div className='card gap-4 flex flex-col' key={wellhead.wellheadName}>
                  <div>
                    {wellhead.wellheadId ? (
                      <Link
                        to={routes.dashboard.wellhead(wellhead.wellheadId)}
                        className='font-semibold hover:underline'
                      >
                        {wellhead.wellheadName}
                      </Link>
                    ) : (
                      <p className='font-semibold'>{wellhead.wellheadName}</p>
                    )}
                    <p className='text-sm text-fgColor-muted'>
                      {wellhead.fieldName} · {wellhead.locationName}
                    </p>
                  </div>

                  <div className='grid gap-2'>
                    {cardReadings(wellhead.readings).map((reading) => (
                      <div
                        key={`${reading.wellheadName}-${reading.parameterCode}-${reading.timestampUtc}`}
                        className='bg-neutral-100 p-3 rounded-lg flex items-center justify-between gap-3'
                      >
                        <div>
                          <p className='text-sm font-medium'>
                            {reading.parameterDisplayName ?? reading.parameterCode}
                          </p>
                          <p className='text-xs text-fgColor-muted'>
                            {formatDate(reading.timestampUtc)}
                          </p>
                        </div>
                        <div className='text-right'>
                          <p className='text-sm font-semibold'>
                            {formatNumber(reading.rawValue)} {reading.canonicalUnit}
                          </p>
                          <p className='text-xs capitalize text-fgColor-muted'>
                            <span
                              className={readingStatusClass(readingStatus(reading))}
                            >
                              {readingStatus(reading)}
                            </span>
                          </p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className='card p-6! text-sm text-fgColor-muted'>
              No readings have been ingested yet. Start the simulator and ingestion
              services.
            </div>
          )}
        </section>

        <section className='space-y-4'>
          <div>
            <h2 className='text-lg font-semibold'>Active Alarms</h2>
            <p className='text-sm text-fgColor-muted'>
              Open alarm events from the database.
            </p>
          </div>

          <div className='card flex flex-col gap-3'>
            {data.activeAlarms.length ? (
              data.activeAlarms.map((alarm) => (
                <div
                  key={alarm.eventId ?? `${alarm.wellheadName}-${alarm.triggeredAt}`}
                  className='border-b border-b-neutral-200 pb-3 last:border-b-0 last:pb-0'
                >
                  <div className='flex items-center gap-2'>
                    <ActivityIcon className='size-4 text-red-600' />
                    <p className='font-medium'>
                      {alarm.wellheadName ?? 'Unknown wellhead'}
                    </p>
                  </div>
                  <p className='text-sm text-fgColor-muted'>
                    {alarm.parameterDisplayName} {formatNumber(alarm.triggeredValue)}
                  </p>
                  <p
                    className={`text-xs uppercase ${alarmSeverityClass(alarm.severityLevel)}`}
                  >
                    {alarm.severityLevel ?? 'severity unknown'}
                  </p>
                </div>
              ))
            ) : (
              <p className='text-sm text-fgColor-muted'>No active alarms.</p>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}

export default Overview;
