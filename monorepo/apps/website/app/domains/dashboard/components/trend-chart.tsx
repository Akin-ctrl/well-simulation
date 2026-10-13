import {
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { TrendPoint } from '@well-simulation/dto/res/dashboard';

/**
 * One trend chart for the Analytics and wellhead pages.
 *
 * Both pages previously carried their own copy of the chart, and both copies
 * hid gaps in the data. Buckets sat at equal steps whatever their time, lines
 * were joined across missing values, and a missing average was drawn as 0. A
 * day with the stack switched off looked like a day of steady readings.
 *
 * This chart plots on a real time axis and breaks the line wherever a bucket is
 * missing. Each unit gets its own labelled axis, so a percentage is not drawn
 * against a value in the hundreds.
 */

/** Width of the TimescaleDB trend buckets the API returns (mv_2min_*). */
const BUCKET_MS = 120_000;

/** Two buckets further apart than this have at least one missing between them. */
const GAP_MS = BUCKET_MS * 1.5;

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

/** One bucket time, with each metric's average or null when it is missing. */
type TrendRow = { time: number } & Record<string, number | null>;

/**
 * Turn API trend points into rows ordered by time, with a row of nulls placed
 * inside every gap so the chart breaks the line there.
 */
export function trendRows(points: TrendPoint[], metricCodes: string[]): TrendRow[] {
  const rows = new Map<number, TrendRow>();

  for (const point of points) {
    const code = point.parameterCode;
    if (!code || !metricCodes.includes(code) || !point.bucketTime) continue;

    const time = new Date(point.bucketTime).getTime();
    if (!Number.isFinite(time)) continue;

    const row = rows.get(time) ?? emptyRow(time, metricCodes);
    // A missing average stays missing. Zero is a real reading.
    row[code] = point.avgValue ?? null;
    rows.set(time, row);
  }

  const ordered = [...rows.values()].sort((a, b) => a.time - b.time);
  const withGaps: TrendRow[] = [];
  for (const row of ordered) {
    const previous = withGaps[withGaps.length - 1];
    if (previous && row.time - previous.time > GAP_MS) {
      withGaps.push(emptyRow(previous.time + BUCKET_MS, metricCodes));
    }
    withGaps.push(row);
  }
  return withGaps;
}

function emptyRow(time: number, metricCodes: string[]): TrendRow {
  const row: TrendRow = { time };
  for (const code of metricCodes) row[code] = null;
  return row;
}

/** The unit each metric is reported in, taken from the data itself. */
function unitsByMetric(points: TrendPoint[]): Map<string, string> {
  const units = new Map<string, string>();
  for (const point of points) {
    if (point.parameterCode && point.canonicalUnit) {
      units.set(point.parameterCode, point.canonicalUnit);
    }
  }
  return units;
}

function formatNumber(value: number) {
  return new Intl.NumberFormat('en', { maximumFractionDigits: 1 }).format(value);
}

function formatTime(time: number) {
  return new Intl.DateTimeFormat('en', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(time));
}

export function TrendChart({
  points,
  metricCodes,
  emptyMessage,
}: {
  points: TrendPoint[];
  metricCodes: string[];
  emptyMessage: string;
}) {
  const rows = trendRows(points, metricCodes);
  if (!rows.length) {
    return (
      <div className='flex h-64 items-center justify-center text-sm text-fgColor-muted'>
        {emptyMessage}
      </div>
    );
  }

  const units = unitsByMetric(points);
  // First unit on the left axis, second on the right. The charts on these
  // pages pair at most two units.
  const axisUnits = [...new Set(metricCodes.map((code) => units.get(code) ?? ''))];
  const axisFor = (code: string) =>
    axisUnits.indexOf(units.get(code) ?? '') === 1 ? 'right' : 'left';
  const labelFor = (code: string) => {
    const unit = units.get(code);
    return `${METRIC_LABELS[code] ?? code}${unit ? ` (${unit})` : ''}`;
  };

  return (
    <ResponsiveContainer width='100%' height={280}>
      <LineChart data={rows}>
        <XAxis
          dataKey='time'
          type='number'
          scale='time'
          domain={['dataMin', 'dataMax']}
          tickFormatter={formatTime}
          stroke='#636c76'
          tickLine={false}
          axisLine={false}
          minTickGap={40}
          // Keep the first and last time labels clear of the Y axis numbers.
          tickMargin={8}
          padding={{ left: 16, right: 16 }}
        />
        {axisUnits.slice(0, 2).map((unit, index) => (
          <YAxis
            key={unit || 'none'}
            yAxisId={index === 0 ? 'left' : 'right'}
            orientation={index === 0 ? 'left' : 'right'}
            stroke='#636c76'
            axisLine={false}
            tickLine={false}
            tickFormatter={formatNumber}
            label={{
              value: unit,
              angle: index === 0 ? -90 : 90,
              position: index === 0 ? 'insideLeft' : 'insideRight',
              fill: '#636c76',
            }}
          />
        ))}
        <Tooltip
          labelFormatter={(time) => formatTime(Number(time))}
          formatter={(value, name) => [
            typeof value === 'number' ? formatNumber(value) : 'missing',
            labelFor(String(name)),
          ]}
        />
        <Legend formatter={(name) => labelFor(String(name))} />
        {metricCodes.map((code) => (
          <Line
            key={code}
            yAxisId={axisFor(code)}
            type='monotone'
            dataKey={code}
            stroke={METRIC_COLORS[code] ?? '#553AFE'}
            strokeWidth={2}
            dot={false}
            connectNulls={false}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}
