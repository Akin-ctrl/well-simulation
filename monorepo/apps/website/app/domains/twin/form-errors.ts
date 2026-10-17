import type { ScheduleRow } from './presentation';

export type FieldIssue = { field: string; label: string; message: string };

/** Translate schema paths to editor controls; numerical rules stay in the schema. */
export function fieldIssues(
  issues: { path: PropertyKey[]; message: string }[],
  kind: 'forecast' | 'scenario',
  rows: ScheduleRow[]
): FieldIssue[] {
  return issues.flatMap((issue) => {
    const [field, index, part] = issue.path;
    if (field === 'name')
      return [
        {
          field: 'scenario-name',
          label: 'Scenario name',
          message: 'Enter a name from 1 to 120 characters.',
        },
      ];
    if (field === 'horizonSeconds')
      return [
        {
          field: `${kind}-horizon`,
          label: 'Duration',
          message: 'Enter a duration from 5 to 120 minutes, in whole seconds.',
        },
      ];
    if (field === 'stepSeconds')
      return [
        {
          field: `${kind}-sample`,
          label: 'Chart interval',
          message: 'Enter a whole number from 5 to 300 seconds.',
        },
      ];
    if (field === 'controls' && typeof index === 'number' && rows[index]) {
      const row = rows[index];
      const target = part === 'input';
      return [
        {
          field: `${target ? 'value' : 'offset'}-${row.id}`,
          label: `Step ${index + 1} ${target ? 'target' : 'time'}`,
          message: target
            ? row.control === 'chokePositionPercent'
              ? 'Enter a target from 0 to 100%.'
              : 'Choose one of the available control states.'
            : 'Enter a whole-minute time of zero or greater.',
        },
      ];
    }
    if (issue.message === 'Control offsets must be ordered and inside the horizon') {
      return rows.map((row, i) => ({
        field: `offset-${row.id}`,
        label: `Step ${i + 1} time`,
        message: 'Use increasing whole-minute times before the end of the run.',
      }));
    }
    if (issue.message === 'horizonSeconds must be divisible by stepSeconds') {
      return [
        {
          field: `${kind}-sample`,
          label: 'Chart interval',
          message: 'Choose an interval that divides the duration evenly.',
        },
      ];
    }
    return [{ field: `${kind}-horizon`, label: 'Inputs', message: issue.message }];
  });
}
