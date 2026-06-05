import {
  ActivityIcon,
  AlertTriangleIcon,
  GaugeIcon,
  GridIcon,
  RadioTowerIcon,
} from 'lucide-react';
import { useLoaderData } from 'react-router';
import { client, fetchFn } from '../utils/api';

type DashboardSummary = {
  totalWellheads: number;
  parametersTracked: number;
  activeAlarms: number;
  latestReadingAt: string | null;
};

type LatestReading = {
  timestampUtc?: string | null;
  rawValue?: number | null;
  wellheadId?: number | null;
  wellheadName?: string | null;
  locationName?: string | null;
  fieldName?: string | null;
  parameterCode?: string | null;
  parameterDisplayName?: string | null;
  canonicalUnit?: string | null;
  normalMin?: number | null;
  normalMax?: number | null;
};

type ActiveAlarm = {
  eventId?: number | null;
  triggeredAt?: string | null;
  severityLevel?: string | null;
  triggeredValue?: number | null;
  wellheadName?: string | null;
  parameterDisplayName?: string | null;
  thresholdValue?: number | null;
};

type OverviewData = {
  summary: DashboardSummary;
  latestReadings: LatestReading[];
  activeAlarms: ActiveAlarm[];
};

type WellheadCard = {
  wellheadName: string;
  locationName: string;
  fieldName: string;
  readings: LatestReading[];
};

export async function clientLoader() {
  const response = await fetchFn<OverviewData>(client.dashboard.overview.$get());
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

function formatDate(value: string | null | undefined) {
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

function wellheadCards(readings: LatestReading[]) {
  const cards = new Map<string, WellheadCard>();

  for (const reading of readings) {
    const key = reading.wellheadName ?? `Wellhead ${reading.wellheadId ?? 'unknown'}`;
    const existing = cards.get(key);

    if (existing) {
      if (existing.readings.length < 4) {
        existing.readings.push(reading);
      }
      continue;
    }

    cards.set(key, {
      wellheadName: key,
      locationName: reading.locationName ?? 'Unknown location',
      fieldName: reading.fieldName ?? 'Unknown field',
      readings: [reading],
    });
  }

  return [...cards.values()].slice(0, 6);
}

function Overview() {
  const data = useLoaderData() as OverviewData;
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
      label: 'Latest Reading',
      value: formatDate(data.summary.latestReadingAt),
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
              Latest readings grouped by wellhead from the historian view.
            </p>
          </div>

          {cards.length ? (
            <div className='grid gap-4 md:grid-cols-2'>
              {cards.map((wellhead) => (
                <div className='card gap-4 flex flex-col' key={wellhead.wellheadName}>
                  <div>
                    <p className='font-semibold'>{wellhead.wellheadName}</p>
                    <p className='text-sm text-fgColor-muted'>
                      {wellhead.fieldName} · {wellhead.locationName}
                    </p>
                  </div>

                  <div className='grid gap-2'>
                    {wellhead.readings.map((reading) => (
                      <div
                        key={`${reading.wellheadName}-${reading.parameterCode}-${reading.timestampUtc}`}
                        className='bg-neutral-100 p-3 rounded-lg flex items-center justify-between gap-3'>
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
                            {readingStatus(reading)}
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
              No readings have been ingested yet. Start the simulator and ingestion services.
            </div>
          )}
        </section>

        <section className='space-y-4'>
          <div>
            <h2 className='text-lg font-semibold'>Active Alarms</h2>
            <p className='text-sm text-fgColor-muted'>Open alarm events from the database.</p>
          </div>

          <div className='card flex flex-col gap-3'>
            {data.activeAlarms.length ? (
              data.activeAlarms.map((alarm) => (
                <div
                  key={alarm.eventId ?? `${alarm.wellheadName}-${alarm.triggeredAt}`}
                  className='border-b border-b-neutral-200 pb-3 last:border-b-0 last:pb-0'>
                  <div className='flex items-center gap-2'>
                    <ActivityIcon className='size-4 text-red-600' />
                    <p className='font-medium'>{alarm.wellheadName ?? 'Unknown wellhead'}</p>
                  </div>
                  <p className='text-sm text-fgColor-muted'>
                    {alarm.parameterDisplayName} {formatNumber(alarm.triggeredValue)}
                  </p>
                  <p className='text-xs uppercase text-red-600'>
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
