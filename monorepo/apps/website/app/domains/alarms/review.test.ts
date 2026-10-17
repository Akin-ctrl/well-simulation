import { describe, expect, it } from 'vitest';
import { alarmActionInputSchema } from '@well-simulation/dto/req/alarms';
import { reviewKey, reviewVersion, shelfSeconds, type AlarmReview } from './review';
const review: AlarmReview = {
  target: {
    wellheadId: 1,
    wellheadName: 'Well',
    locationName: 'Field',
    ruleId: 1,
    parameterName: 'Pressure',
    unit: 'psi',
    operator: '>',
    threshold: 3000,
    severity: 'CRITICAL',
    triggerAfter: 3,
    clearAfter: 3,
  },
  event: null,
  shelf: {
    revision: 1,
    until: '2026-10-17T06:30:00.000000Z',
    active: true,
    updatedAt: '2026-10-17T06:00:00.000000Z',
    actorName: 'Operator',
    reason: 'Inspection',
  },
};
describe('alarm review boundaries', () => {
  it('uses database observation time and elapsed time for the shelf deadline', () => {
    expect(shelfSeconds(review, '2026-10-17T06:29:50.000000Z', 9)).toBe(1);
    expect(shelfSeconds(review, '2026-10-17T06:29:50.000000Z', 10)).toBe(0);
    expect(shelfSeconds(review, '2026-10-17T06:29:50.000000Z', 11)).toBe(-1);
    expect(
      shelfSeconds({ ...review, shelf: null }, '2026-10-17T06:00:00Z', 0)
    ).toBeNull();
  });
  it('requires another review when evidence changes even at the same revision', () => {
    const changed = { ...review, target: { ...review.target, threshold: 3100 } };
    expect(reviewKey(changed)).toBe(reviewKey(review));
    expect(reviewVersion(changed)).not.toBe(reviewVersion(review));
  });
  it('keeps exact event identity in a reviewed acknowledgement', () => {
    const parsed = alarmActionInputSchema.parse({
      action: 'acknowledge',
      eventId: '9007199254740993',
      triggeredAt: '2026-10-17T06:00:00.123456+00:00',
      reason: ' Inspection ',
      confirmed: true,
    });
    expect(parsed).toMatchObject({
      eventId: '9007199254740993',
      triggeredAt: '2026-10-17T06:00:00.123456Z',
      reason: 'Inspection',
    });
  });
});
