import { describe, expect, it } from 'vitest';
import { forecastRequestSchema } from '@well-simulation/dto/req/projection';
import { fieldIssues } from './form-errors';
import { scenarioInput, type ScheduleRow } from './presentation';

const rows: ScheduleRow[] = [
  { id: 7, offsetMinutes: '0', control: 'chokePositionPercent', value: '101' },
];
describe('schema errors reach the corresponding editor controls', () => {
  it('links a target bound error to its stable row id after removals', () => {
    const result = scenarioInput(1, 'Adjustment', 300, 60, rows);
    expect(result.success).toBe(false);
    if (!result.success)
      expect(fieldIssues(result.error.issues, 'scenario', rows)).toEqual([
        expect.objectContaining({ field: 'value-7', label: 'Step 1 target' }),
      ]);
  });
  it('links divisibility errors to the interval instead of an unrelated field', () => {
    const result = forecastRequestSchema.safeParse({
      horizonSeconds: 300,
      stepSeconds: 70,
    });
    expect(result.success).toBe(false);
    if (!result.success)
      expect(fieldIssues(result.error.issues, 'forecast', [])).toEqual([
        expect.objectContaining({
          field: 'forecast-sample',
          message: 'Choose an interval that divides the duration evenly.',
        }),
      ]);
  });
  it('makes a schedule ordering error reachable through every time field', () => {
    const unordered = [
      { ...rows[0]!, value: '80', offsetMinutes: '2' },
      { ...rows[0]!, id: 9, value: '70', offsetMinutes: '1' },
    ];
    const result = scenarioInput(1, 'Adjustment', 300, 60, unordered);
    expect(result.success).toBe(false);
    if (!result.success)
      expect(
        fieldIssues(result.error.issues, 'scenario', unordered).map(
          (issue) => issue.field
        )
      ).toEqual(['offset-7', 'offset-9']);
  });
});
