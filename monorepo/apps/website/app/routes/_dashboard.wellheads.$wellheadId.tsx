import {
  AlertTriangleIcon,
  ArrowLeftIcon,
  GaugeIcon,
  MapPinIcon,
  RadioTowerIcon,
} from 'lucide-react';
import {
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Link, useLoaderData } from 'react-router';
import type {
  LatestReading,
  TrendPoint,
  WellheadDetailResponse,
} from '@corsight/dto/res/dashboard';
import { Title } from '../domains/dashboard/components/misc';
import { routes } from '../config/routes';
import { client, fetchFn } from '../utils/api';

type LoaderArgs = {
  params: {
    wellheadId?: string;
  };
};

type MultiMetricPoint = {
  date: string;
  [metricCode: string]: string | number;
};

const METRIC_LABELS: Record<string, string> = {
  tubing_pressure: 'Tubing Pressure',
  casing_pressure: 'Casing Pressure',
  annulus_pressure: 'Annulus Pressure',
  wellhead_temperature: 'Temperature',
  flow_rate: 'Flow Rate',
  water_cut: 'Water Cut',
  gas_oil_ratio: 'Gas/Oil Ratio',
};

const METRIC_COLORS: Record<string, string> = {
  tubing_pressure: '#553AFE',
  casing_pressure: '#01C0F6',
  annulus_pressure: '#10B981',
  wellhead_temperature: '#F97316',
  flow_rate: '#553AFE',
  water_cut: '#01C0F6',
  gas_oil_ratio: '#10B981',
};

export async function clientLoader({ params }: LoaderArgs) {
  const wellheadId = params.wellheadId ?? '';
  const response = await fetchFn<WellheadDetailResponse>(
    client.dashboard.wellheads[':wellheadId'].$get({
      param: { wellheadId },
    })
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

function statusClass(status: string) {
  if (status === 'out of range') {
    return 'text-red-600';
  }
  if (status === 'unknown') {
    return 'text-fgColor-muted';
  }
  return 'text-green-700';
}

function severityClass(severity: string | null | undefined) {
  const normalized = severity?.toLowerCase();
  if (normalized === 'critical') {
    return 'text-red-600';
  }
  if (normalized === 'warning') {
    return 'text-amber-600';
  }
  return 'text-fgColor-muted';
}

function multiSeries(points: TrendPoint[], metricCodes: string[]): MultiMetricPoint[] {
  const buckets = new Map<string, MultiMetricPoint>();

  for (const point of [...points].reverse()) {
    const metricCode = point.parameterCode;
    if (!metricCode || !metricCodes.includes(metricCode)) {
      continue;
    }

    const date = formatDate(point.bucketTime);
    const bucket = buckets.get(date) ?? { date };
    bucket[metricCode] = point.avgValue ?? 0;
    buckets.set(date, bucket);
  }

  return [...buckets.values()];
}

function TrendPanel({
  title,
  description,
  data,
  metricCodes,
}: {
  title: string;
  description: string;
  data: MultiMetricPoint[];
  metricCodes: string[];
}) {
  return (
    <section className='card'>
      <div className='mb-4'>
        <h2 className='font-semibold'>{title}</h2>
        <p className='text-sm text-fgColor-muted'>{description}</p>
      </div>

      {data.length ? (
        <ResponsiveContainer width='100%' height={260}>
          <LineChart data={data}>
            <XAxis dataKey='date' stroke='#636c76' tickLine={false} axisLine={false} />
            <YAxis
              stroke='#636c76'
              axisLine={false}
              tickLine={false}
              tickFormatter={(value: number) => formatNumber(value)}
            />
            <Tooltip
              formatter={(value: number, name: string) => [
                formatNumber(value),
                METRIC_LABELS[name] ?? name,
              ]}
            />
            {metricCodes.map((metricCode) => (
              <Line
                key={metricCode}
                type='monotone'
                dataKey={metricCode}
                stroke={METRIC_COLORS[metricCode] ?? '#553AFE'}
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      ) : (
        <div className='flex h-64 items-center justify-center text-sm text-fgColor-muted'>
          No trend data available for this well yet.
        </div>
      )}
    </section>
  );
}

export default function WellheadDetail() {
  const data = useLoaderData() as WellheadDetailResponse;
  const pressureData = multiSeries(data.pressureTrend, [
    'tubing_pressure',
    'casing_pressure',
    'annulus_pressure',
  ]);
  const productionData = multiSeries(data.temperatureFlowTrend, [
    'wellhead_temperature',
    'flow_rate',
  ]);
  const compositionData = multiSeries(data.waterCutGorTrend, [
    'water_cut',
    'gas_oil_ratio',
  ]);

  return (
    <div className='space-y-5'>
      <div className='flex items-center justify-between gap-4'>
        <div>
          <Link
            to={routes.dashboard.overview}
            className='mb-2 inline-flex items-center gap-2 text-sm text-fgColor-muted hover:text-fgColor-default'>
            <ArrowLeftIcon className='size-4' />
            Back to overview
          </Link>
          <Title>{data.wellhead.wellheadName}</Title>
          <p className='text-sm text-fgColor-muted'>
            {data.wellhead.fieldName} · {data.wellhead.locationName} ·{' '}
            {data.wellhead.wellheadType ?? 'Wellhead'}
          </p>
        </div>

        <div className='rounded-full border border-border-default px-3 py-1 text-sm capitalize'>
          {data.wellhead.status ?? 'status unknown'}
        </div>
      </div>

      <div className='grid gap-4 md:grid-cols-2 xl:grid-cols-4'>
        <div className='card gap-2'>
          <div className='flex items-center justify-between'>
            <p className='text-sm text-fgColor-muted'>Latest Reading</p>
            <RadioTowerIcon className='size-5' />
          </div>
          <p className='font-semibold'>{formatDate(data.latestReadingAt)}</p>
        </div>
        <div className='card gap-2'>
          <div className='flex items-center justify-between'>
            <p className='text-sm text-fgColor-muted'>Parameters</p>
            <GaugeIcon className='size-5' />
          </div>
          <p className='font-semibold'>{data.latestReadings.length}</p>
        </div>
        <div className='card gap-2'>
          <div className='flex items-center justify-between'>
            <p className='text-sm text-fgColor-muted'>Active Alarms</p>
            <AlertTriangleIcon className='size-5' />
          </div>
          <p className='font-semibold'>{data.activeAlarms.length}</p>
        </div>
        <div className='card gap-2'>
          <div className='flex items-center justify-between'>
            <p className='text-sm text-fgColor-muted'>Location</p>
            <MapPinIcon className='size-5' />
          </div>
          <p className='font-semibold'>{data.wellhead.locationName}</p>
        </div>
      </div>

      <div className='grid gap-6 xl:grid-cols-[2fr_1fr]'>
        <section className='space-y-4'>
          <div>
            <h2 className='text-lg font-semibold'>Current Parameters</h2>
            <p className='text-sm text-fgColor-muted'>
              Latest values from this wellhead’s most recent complete telemetry batch.
            </p>
          </div>

          <div className='grid gap-3 md:grid-cols-2 xl:grid-cols-3'>
            {data.latestReadings.map((reading) => {
              const status = readingStatus(reading);
              return (
                <div
                  key={reading.parameterCode ?? reading.parameterTypeId}
                  className='rounded-lg bg-neutral-100 p-3'>
                  <div className='flex items-start justify-between gap-3'>
                    <div>
                      <p className='text-sm font-medium'>
                        {reading.parameterDisplayName ?? reading.parameterCode}
                      </p>
                      <p className='text-xs text-fgColor-muted'>
                        Normal: {formatNumber(reading.normalMin)}–
                        {formatNumber(reading.normalMax)} {reading.canonicalUnit}
                      </p>
                    </div>
                    <p className={`text-xs capitalize ${statusClass(status)}`}>{status}</p>
                  </div>
                  <p className='mt-3 text-lg font-semibold'>
                    {formatNumber(reading.rawValue)} {reading.canonicalUnit}
                  </p>
                </div>
              );
            })}
          </div>
        </section>

        <section className='space-y-4'>
          <div>
            <h2 className='text-lg font-semibold'>Active Alarms</h2>
            <p className='text-sm text-fgColor-muted'>Open alarms for this wellhead.</p>
          </div>

          <div className='card flex flex-col gap-3'>
            {data.activeAlarms.length ? (
              data.activeAlarms.map((alarm) => (
                <div
                  key={alarm.eventId ?? `${alarm.parameterDisplayName}-${alarm.triggeredAt}`}
                  className='border-b border-b-neutral-200 pb-3 last:border-b-0 last:pb-0'>
                  <div className='flex items-center justify-between gap-3'>
                    <p className='font-medium'>{alarm.parameterDisplayName}</p>
                    <p className={`text-xs uppercase ${severityClass(alarm.severityLevel)}`}>
                      {alarm.severityLevel ?? 'unknown'}
                    </p>
                  </div>
                  <p className='text-sm text-fgColor-muted'>
                    {formatNumber(alarm.triggeredValue)} {alarm.operator}{' '}
                    {formatNumber(alarm.thresholdValue)}
                  </p>
                  <p className='text-xs text-fgColor-muted'>
                    Triggered {formatDate(alarm.triggeredAt)}
                  </p>
                </div>
              ))
            ) : (
              <p className='text-sm text-fgColor-muted'>No active alarms for this well.</p>
            )}
          </div>
        </section>
      </div>

      <div className='grid gap-4 xl:grid-cols-2'>
        <TrendPanel
          title='Pressure Trends'
          description='Two-minute tubing, casing, and annulus pressure aggregates for this well.'
          data={pressureData}
          metricCodes={['tubing_pressure', 'casing_pressure', 'annulus_pressure']}
        />
        <TrendPanel
          title='Temperature / Flow'
          description='Two-minute temperature and production flow aggregates for this well.'
          data={productionData}
          metricCodes={['wellhead_temperature', 'flow_rate']}
        />
        <TrendPanel
          title='Water Cut / Gas-Oil Ratio'
          description='Two-minute fluid composition indicators for this well.'
          data={compositionData}
          metricCodes={['water_cut', 'gas_oil_ratio']}
        />
      </div>
    </div>
  );
}
