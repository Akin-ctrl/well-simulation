import { describe, expect, it } from 'vitest';
import { snapshotSelectionSchema, snapshotTimeKey } from './req/snapshots';
import { storedBranchOrigin } from './res/snapshots';

const selection = {
  snapshotId: '9007199254740993',
  simulatedAt: '2026-10-15T00:00:00.123456Z',
};
describe('Exact snapshot provenance', () => {
  it('preserves bigint strings and rejects numerical or out-of-range identities', () => {
    expect(snapshotSelectionSchema.parse(selection)).toEqual(selection);
    for (const snapshotId of [
      Number('9007199254740993'),
      '0',
      '01',
      'x',
      '9223372036854775808',
    ])
      expect(
        snapshotSelectionSchema.safeParse({ ...selection, snapshotId }).success
      ).toBe(false);
  });
  it('compares UTC microseconds without millisecond rounding', () => {
    expect(snapshotTimeKey('2026-10-15T00:00:00.123456+00:00')).toBe(
      snapshotTimeKey(selection.simulatedAt)
    );
    expect(snapshotTimeKey('2026-10-15T00:00:00.123457Z')).not.toBe(
      snapshotTimeKey(selection.simulatedAt)
    );
    expect(() => snapshotTimeKey('2026-10-15T00:00:00.1234567Z')).toThrow();
    expect(() => snapshotTimeKey('2026-10-15T01:00:00+01:00')).toThrow();
  });
  it('recognizes legacy current rows without hiding malformed new metadata', () => {
    expect(storedBranchOrigin({})).toEqual({ kind: 'current' });
    expect(
      storedBranchOrigin({ branchOrigin: { kind: 'snapshot', ...selection } })
    ).toEqual({ kind: 'snapshot', ...selection });
    expect(() => storedBranchOrigin({ branchOrigin: null })).toThrow();
    expect(() => storedBranchOrigin({ branchOrigin: { kind: 'snapshot' } })).toThrow();
  });
});
