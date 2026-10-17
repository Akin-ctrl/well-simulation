import { describe, expect, it } from 'vitest';
import type { ProjectionDetail } from '@well-simulation/dto/res/projection-detail';
import { projectionRows, scenarioInput, signalText } from './presentation';

const at = Date.parse('2026-10-16T12:00:00Z');
function result(stepSeconds: number): ProjectionDetail {
  return {
    runId: 'fdf20e93-ff4a-4db6-8db5-af9622314998',
    kind: 'scenario',
    branchOrigin: { kind: 'current' },
    executionOrigin: { kind: 'manual' },
    wellheadId: 1,
    name: 'Test',
    source: 'synthetic_reduced_order_model',
    fieldCalibrated: false,
    modelVersion: '0.3.0',
    baseSimulatedAt: new Date(at).toISOString(),
    generatedAt: new Date(at).toISOString(),
    horizonSeconds: 600,
    stepSeconds,
    confidence: {
      method: 'heuristic_uncalibrated',
      score: 0.5,
      label: 'moderate',
      reasons: [],
    },
    baselineControls: {},
    scheduledControls: [],
    signals: [],
    points: [1, 2, 4].map((n, index): ProjectionDetail['points'][number] => ({
      elapsedSeconds: n * stepSeconds,
      timestamp: new Date(at + n * stepSeconds * 1000).toISOString(),
      values: index === 1 ? {} : { flow_rate: 0 },
      baselineValues: { flow_rate: 100 },
      alarmRisk: 'none',
      triggeredRuleIds: [],
    })),
  };
}
describe('Projection presentation', () => {
  it.each([5, 60, 300])(
    'keeps the %is sample spacing, missing values, zero, and matching baselines',
    (step) => {
      const rows = projectionRows(result(step), 'flow_rate');
      expect(rows).toEqual([
        { time: at + step * 1000, value: 0, baseline: 100 },
        { time: at + step * 2000, value: null, baseline: 100 },
        { time: at + step * 3000, value: null, baseline: null },
        { time: at + step * 4000, value: 0, baseline: 100 },
      ]);
    }
  );
  it('converts false explicitly and lets the shared schema reject unordered steps', () => {
    const rows = [
      { id: 1, offsetMinutes: '0', control: 'pumpOn' as const, value: 'false' },
    ];
    const valid = scenarioInput(1, 'Stop pump', 300, 60, rows);
    expect(valid.success && valid.data.controls[0]?.input.pumpOn).toBe(false);
    expect(
      scenarioInput(1, 'Stop pump', 300, 60, [...rows, { ...rows[0]!, id: 2 }]).success
    ).toBe(false);
  });
  it('rejects empty numeric fields and invalid boolean targets', () => {
    expect(
      scenarioInput(1, 'Test', 300, 60, [
        { id: 1, offsetMinutes: '', control: 'chokePositionPercent', value: '80' },
      ]).success
    ).toBe(false);
    expect(
      scenarioInput(1, 'Test', 300, 60, [
        { id: 1, offsetMinutes: '0', control: 'chokePositionPercent', value: '' },
      ]).success
    ).toBe(false);
    expect(
      scenarioInput(1, 'Test', 300, 60, [
        { id: 1, offsetMinutes: '0', control: 'pumpOn', value: 'invalid' },
      ]).success
    ).toBe(false);
  });
  it('keeps missing and zero states distinct', () => {
    expect(signalText('pump_status', null, 'state')).toBe('Unavailable');
    expect(signalText('pump_status', 0, 'state')).toBe('Stopped');
    expect(signalText('emergency_shutdown', 1, 'state')).toBe('Tripped');
    expect(signalText('flow_rate', 0, 'bbl/day')).toBe('0 bbl/day');
  });
});
