import z from 'zod';
import { ApiError } from '@well-simulation/dto/error';
import {
  alarmActionInputSchema,
  alarmQuerySchema,
  shelfQuerySchema,
} from '@well-simulation/dto/req/alarms';
import {
  alarmDetailSchema,
  alarmListSchema,
  alarmReceiptSchema,
  alarmShelfListSchema,
  type AlarmEvent,
  type AlarmShelfList,
} from '@well-simulation/dto/res/alarms';
import { client, fetchFn } from '../../utils/api';
import { readDashboardView } from '../dashboard/components/read-status';
import { errorState } from '../twin/presentation';

export function alarmError(error: unknown) {
  const state = errorState(error);
  return {
    ...state,
    message:
      state.status === 409
        ? 'The alarm state changed. Refresh and review the current event or shelf.'
        : state.status === 404
          ? 'The event is no longer retained, or this well or rule no longer exists.'
          : state.message,
  };
}

export const ALARM_VIEWS = {
  attention: 'Needs acknowledgement',
  active: 'All active',
  shelves: 'Shelved rules',
  history: 'History',
} as const;
export type AlarmView = keyof typeof ALARM_VIEWS;
export const alarmUrl = (view: AlarmView, wellheadId?: number, cursor?: string) => {
  const query = new URLSearchParams({ view });
  if (wellheadId) query.set('wellheadId', String(wellheadId));
  if (cursor) query.set('cursor', cursor);
  return `/alarms?${query}`;
};
export async function alarmLoader({ request }: { request: Request }) {
  const search = new URL(request.url).searchParams;
  const view = z
    .enum(['attention', 'active', 'shelves', 'history'])
    .parse(search.get('view') ?? 'attention');
  const wellheadId = search.get('wellheadId') ?? undefined,
    cursor = search.get('cursor') ?? undefined;
  const read = await readDashboardView(async () => {
    const query = {
      ...(wellheadId ? { wellheadId } : {}),
      ...(cursor ? { cursor } : {}),
    };
    if (view === 'shelves') {
      const parsed = shelfQuerySchema.parse(query);
      const response = await fetchFn<unknown>(
        client.alarms.shelves.$get({ query: parsed })
      );
      const data = alarmShelfListSchema.parse(response.data);
      return {
        ...data,
        events: [] as AlarmEvent[],
        shelves: data.items,
        total: data.total,
      };
    }
    const parsed = alarmQuerySchema.parse({ ...query, view });
    const response = await fetchFn<unknown>(client.alarms.$get({ query: parsed }));
    const data = alarmListSchema.parse(response.data);
    return {
      ...data,
      events: data.items,
      shelves: [] as AlarmShelfList['items'],
      total: view === 'history' ? data.summary.retained : data.summary[view],
    };
  });
  return {
    view,
    wellheadId: wellheadId ? Number(wellheadId) : undefined,
    cursor,
    read,
  };
}
export async function alarmPageAction({ request }: { request: Request }) {
  const fields = await request.formData();
  const operation = fields.get('operation');
  let attempted = false;
  try {
    const key = z.uuid().parse(fields.get('requestKey'));
    const mode = z.enum(['check', 'write']).parse(operation);
    const input =
      mode === 'write'
        ? alarmActionInputSchema.parse(JSON.parse(String(fields.get('input'))))
        : null;
    attempted = true;
    const response =
      input === null
        ? await fetchFn<unknown>(
            client.alarms.actions[':requestKey'].$get({ param: { requestKey: key } })
          )
        : await fetchFn<unknown>(
            client.alarms.actions.$post(
              {
                json: input,
              },
              { headers: { 'Idempotency-Key': key } }
            )
          );
    return {
      receipt: alarmReceiptSchema.parse(response.data),
      error: null,
      uncertain: false,
      missing: false,
    };
  } catch (error) {
    if (!attempted)
      return {
        receipt: null,
        error: { status: 400, message: 'Check the action fields before submitting.' },
        uncertain: false,
        missing: false,
      };
    if (error instanceof ApiError && [401, 403].includes(error.statusCode ?? 0))
      throw error;
    if (operation === 'check' && error instanceof ApiError && error.statusCode === 404)
      return { receipt: null, error: null, uncertain: true, missing: true };
    const state = alarmError(error);
    return {
      receipt: null,
      error: state,
      uncertain:
        operation === 'check' ||
        state.status == null ||
        state.status < 400 ||
        state.status >= 500,
      missing: false,
    };
  }
}
export async function eventDetail(event: AlarmEvent) {
  const response = await fetchFn<unknown>(
    client.alarms.events[':eventId'].$get({
      param: { eventId: event.eventId },
      query: { triggeredAt: event.triggeredAt },
    })
  );
  return alarmDetailSchema.parse(response.data);
}
