import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';
import { useLoaderData } from 'react-router';
import type {
  AlarmCount,
  DashboardAnalyticsResponse,
} from '@well-simulation/dto/res/dashboard';
import { Title } from '../domains/dashboard/components/misc';
import { TrendChart } from '../domains/dashboard/components/trend-chart';
import { client, fetchFn } from '../utils/api';
import { REFRESH_INTERVALS, useAutoRefresh } from '../hooks/use-auto-refresh';

type AlarmSegment = {
  color: string;
  name: string;
  amount: number;
};

const SEVERITY_COLORS: Record<string, string> = {
  critical: '#DC2626',
  high: '#F97316',
  warning: '#F59E0B',
  medium: '#EAB308',
  low: '#01C0F6',
};

export async function clientLoader() {
  const response = await fetchFn<DashboardAnalyticsResponse>(
    client.dashboard.analytics.$get()
  );
  return response.data;
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return '—';
  }

  return new Intl.NumberFormat('en', {
    maximumFractionDigits: 1,
  }).format(value);
}

function alarmSegments(counts: AlarmCount[]): AlarmSegment[] {
  const totals = new Map<string, number>();

  for (const count of counts) {
    const severity = count.severityLevel?.toLowerCase() ?? 'unknown';
    totals.set(severity, (totals.get(severity) ?? 0) + (count.alarmsTriggered ?? 0));
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

function Analytics() {
  const data = useLoaderData() as DashboardAnalyticsResponse;
  useAutoRefresh(REFRESH_INTERVALS.analytics);
  const alarmData = alarmSegments(data.alarmCounts);
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
          <TrendChart
            points={data.pressureTrend}
            metricCodes={['tubing_pressure', 'casing_pressure', 'annulus_pressure']}
            emptyMessage='No aggregate metric data available yet.'
          />
        </div>

        <div className='card xl:col-span-2'>
          <div className='mb-4'>
            <h2 className='font-semibold'>Temperature / Flow</h2>
            <p className='text-sm text-fgColor-muted'>
              Two-minute averages across wells. Each unit has its own axis.
            </p>
          </div>
          <TrendChart
            points={data.temperatureFlowTrend}
            metricCodes={['wellhead_temperature', 'flow_rate']}
            emptyMessage='No aggregate metric data available yet.'
          />
        </div>

        <div className='card'>
          <div className='mb-4'>
            <h2 className='font-semibold'>Alarm Severity</h2>
            <p className='text-sm text-fgColor-muted'>
              Alarms triggered in the last 24 hours, by severity.
            </p>
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
                  cornerRadius={5}
                >
                  {alarmData.map((segment) => (
                    <Cell
                      key={segment.name}
                      fill={segment.color}
                      strokeLinejoin='round'
                      strokeLinecap='round'
                    />
                  ))}
                </Pie>
                <Tooltip
                  formatter={(value: number, name: string) => [
                    formatNumber(value),
                    name,
                  ]}
                />
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
              Two-minute averages across wells. Each unit has its own axis.
            </p>
          </div>
          <TrendChart
            points={data.waterCutGorTrend}
            metricCodes={['water_cut', 'gas_oil_ratio']}
            emptyMessage='No aggregate metric data available yet.'
          />
        </div>
      </div>
    </div>
  );
}

export default Analytics;
