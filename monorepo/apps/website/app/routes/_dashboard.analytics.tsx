import {
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { useLoaderData } from 'react-router';
import type {
  DailyAlarmCount,
  DashboardAnalyticsResponse,
  TrendPoint,
} from '@corsight/dto/res/dashboard';
import { Title } from '../domains/dashboard/components/misc';
import { client, fetchFn } from '../utils/api';

type MultiMetricPoint = {
  date: string;
  [metricCode: string]: string | number;
};

type AlarmSegment = {
  color: string;
  name: string;
  amount: number;
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

const SEVERITY_COLORS: Record<string, string> = {
  critical: '#DC2626',
  high: '#F97316',
  warning: '#F59E0B',
  medium: '#EAB308',
  low: '#01C0F6',
};

export async function clientLoader() {
  const response = await fetchFn<DashboardAnalyticsResponse>(client.dashboard.analytics.$get());
  return response.data;
}

function formatDate(value: string | Date | null | undefined) {
  if (!value) {
    return 'n/a';
  }

  return new Intl.DateTimeFormat('en', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
  }).format(new Date(value));
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return '—';
  }

  return new Intl.NumberFormat('en', {
    maximumFractionDigits: 1,
  }).format(value);
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

function alarmSegments(counts: DailyAlarmCount[]): AlarmSegment[] {
  const totals = new Map<string, number>();

  for (const count of counts) {
    const severity = count.severityLevel?.toLowerCase() ?? 'unknown';
    totals.set(severity, (totals.get(severity) ?? 0) + (count.totalAlarmsTriggered ?? 0));
  }

  if (!totals.size) {
    return [{ color: '#D1D5DB', name: 'No alarms', amount: 1 }];
  }

  return [...totals.entries()].map(([severity, total]) => ({
    color: SEVERITY_COLORS[severity] ?? '#6B7280',
    name: severity,
    amount: total,
  }));
}

function totalAlarms(segments: AlarmSegment[]) {
  if (segments.length === 1 && segments[0]?.name === 'No alarms') {
    return 0;
  }

  return segments.reduce((total, segment) => total + segment.amount, 0);
}

function EmptyChart({ message }: { message: string }) {
  return (
    <div className='flex h-64 items-center justify-center text-sm text-fgColor-muted'>
      {message}
    </div>
  );
}

function MultiMetricChart({
  data,
  metricCodes,
}: {
  data: MultiMetricPoint[];
  metricCodes: string[];
}) {
  if (!data.length) {
    return <EmptyChart message='No aggregate metric data available yet.' />;
  }

  return (
    <ResponsiveContainer width='100%' height={260}>
      <LineChart data={data}>
        <XAxis dataKey='date' stroke='#636c76' tickLine={false} axisLine={false} />
        <YAxis
          stroke='#636c76'
          axisLine={false}
          tickLine={false}
          tickFormatter={(value: number) => formatNumber(value)}
        />
        <Tooltip formatter={(value: number, name: string) => [formatNumber(value), METRIC_LABELS[name] ?? name]} />
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
  );
}

function Analytics() {
  const data = useLoaderData() as DashboardAnalyticsResponse;
  const pressureData = multiSeries(data.pressureTrend, [
    'tubing_pressure',
    'casing_pressure',
    'annulus_pressure',
  ]);
  const temperatureFlowData = multiSeries(data.temperatureFlowTrend, [
    'wellhead_temperature',
    'flow_rate',
  ]);
  const waterCutGorData = multiSeries(data.waterCutGorTrend, [
    'water_cut',
    'gas_oil_ratio',
  ]);
  const alarmData = alarmSegments(data.dailyAlarmCounts);
  const alarmTotal = totalAlarms(alarmData);

  return (
    <div className='space-y-5'>
      <div>
        <Title>Analytics</Title>
        <p className='text-fg-muted'>
          Two-minute aggregate trends from TimescaleDB continuous aggregates.
        </p>
      </div>

      <div className='grid grid-cols-1 gap-4 xl:grid-cols-5'>
        <div className='card xl:col-span-2'>
          <div className='mb-4'>
            <h2 className='font-semibold'>Pressure Trends</h2>
            <p className='text-sm text-fgColor-muted'>
              Tubing, casing, and annulus pressure averages across wells.
            </p>
          </div>
          <MultiMetricChart
            data={pressureData}
            metricCodes={['tubing_pressure', 'casing_pressure', 'annulus_pressure']}
          />
        </div>

        <div className='card xl:col-span-2'>
          <div className='mb-4'>
            <h2 className='font-semibold'>Temperature / Flow</h2>
            <p className='text-sm text-fgColor-muted'>
              Two-minute averages; values use their native units.
            </p>
          </div>
          <MultiMetricChart
            data={temperatureFlowData}
            metricCodes={['wellhead_temperature', 'flow_rate']}
          />
        </div>

        <div className='card'>
          <div className='mb-4'>
            <h2 className='font-semibold'>Alarm Severity</h2>
            <p className='text-sm text-fgColor-muted'>Five-minute alarm counts by severity.</p>
          </div>
          <div className='relative h-56'>
            <ResponsiveContainer>
              <PieChart width={100} height={100}>
                <Pie
                  data={alarmData}
                  dataKey='amount'
                  outerRadius={80}
                  innerRadius={65}
                  paddingAngle={5}
                  cornerRadius={5}>
                  {alarmData.map((segment) => (
                    <Cell
                      key={segment.name}
                      fill={segment.color}
                      strokeLinejoin='round'
                      strokeLinecap='round'
                    />
                  ))}
                </Pie>
                <Tooltip formatter={(value: number, name: string) => [formatNumber(value), name]} />
              </PieChart>
            </ResponsiveContainer>
            <div className='absolute left-0 top-0 flex h-full w-full flex-col items-center justify-center'>
              <p className='text-fgColor-muted'>Total</p>
              <p className='text-title-medium'>{formatNumber(alarmTotal)}</p>
            </div>
          </div>
        </div>

        <div className='card xl:col-span-3'>
          <div className='mb-4'>
            <h2 className='font-semibold'>Water Cut / Gas-Oil Ratio</h2>
            <p className='text-sm text-fgColor-muted'>
              Two-minute averages; values use their native units.
            </p>
          </div>
          <MultiMetricChart
            data={waterCutGorData}
            metricCodes={['water_cut', 'gas_oil_ratio']}
          />
        </div>
      </div>
    </div>
  );
}

export default Analytics;
