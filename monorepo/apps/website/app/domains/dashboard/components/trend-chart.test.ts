import { describe, expect, it } from 'vitest';
import type { TrendPoint } from '@well-simulation/dto/res/dashboard';
import { trendRows } from './trend-chart';

const at = (minute: number) => new Date(Date.UTC(2026, 0, 1, 0, minute)).toISOString();

function point(minute: number, code: string, value: number | null): TrendPoint {
  return { bucketTime: at(minute), parameterCode: code, avgValue: value };
}

describe('trendRows', () => {
  it('breaks a line across missing two-minute buckets on a real time axis', () => {
    const rows = trendRows(
      [
        point(8, 'tubing_pressure', 103),
        point(0, 'tubing_pressure', 100),
        point(2, 'tubing_pressure', 101),
      ],
      ['tubing_pressure']
    );

    expect(rows.map((row) => row.time)).toEqual(
      [0, 2, 4, 8].map((minute) => Date.parse(at(minute)))
    );
    expect(rows.map((row) => row.tubing_pressure)).toEqual([100, 101, null, 103]);
  });

  it('keeps a zero reading distinct from a missing average', () => {
    const rows = trendRows(
      [point(0, 'flow_rate', 0), point(2, 'flow_rate', null), point(4, 'flow_rate', 5)],
      ['flow_rate']
    );

    expect(rows.map((row) => row.flow_rate)).toEqual([0, null, 5]);
  });

  it('aligns series by timestamp and ignores unrelated or invalid points', () => {
    const rows = trendRows(
      [
        point(2, 'flow_rate', 20),
        point(0, 'tubing_pressure', 100),
        point(0, 'other', 1),
        { bucketTime: 'invalid', parameterCode: 'flow_rate', avgValue: 9 },
      ],
      ['tubing_pressure', 'flow_rate']
    );

    expect(rows).toEqual([
      { time: Date.parse(at(0)), tubing_pressure: 100, flow_rate: null },
      { time: Date.parse(at(2)), tubing_pressure: null, flow_rate: 20 },
    ]);
  });
});
