import type {
  AlarmEvent,
  AlarmShelfList,
  AlarmTarget,
} from '@well-simulation/dto/res/alarms';
export type AlarmReview = {
  target: AlarmTarget;
  event: AlarmEvent | null;
  shelf: AlarmEvent['shelf'];
};
export const eventReview = (event: AlarmEvent): AlarmReview => ({
  target: event.target,
  event,
  shelf: event.shelf,
});
export const shelfReview = (item: AlarmShelfList['items'][number]): AlarmReview => ({
  target: item.target,
  shelf: item.state,
  event: null,
});
export const reviewKey = (item: AlarmReview) =>
  item.event
    ? `${item.event.eventId}:${item.event.triggeredAt}`
    : `shelf:${item.target.wellheadId}:${item.target.ruleId}`;
/** Include all values a user reviews, not only the concurrency token. */
export const reviewVersion = (item: AlarmReview) => JSON.stringify(item);
export function shelfSeconds(
  item: AlarmReview,
  observedAt: string,
  elapsed: number
): number | null {
  return item.shelf?.active && item.shelf.until
    ? (Date.parse(item.shelf.until) - Date.parse(observedAt)) / 1000 - elapsed
    : null;
}
