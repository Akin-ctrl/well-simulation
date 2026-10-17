import { ApiError } from '@well-simulation/dto/error';
import { forecastScheduleInputSchema } from '@well-simulation/dto/req/forecast-schedule';
import {
  forecastScheduleSchema,
  forecastScheduleViewSchema,
} from '@well-simulation/dto/res/forecast-schedule';
import { client, fetchFn } from '../../utils/api';
import { errorState } from './presentation';

/** Unknown write outcomes require a read and a new explicit review. */
export async function scheduleAction(wellheadId: string, fields: FormData) {
  try {
    const endpoint = client.twin.wellheads[':wellheadId']['forecast-schedule'];
    if (fields.get('operation') === 'check') {
      const response = await fetchFn<unknown>(endpoint.$get({ param: { wellheadId } }));
      return {
        saved: forecastScheduleViewSchema.parse(response.data).schedule,
        checked: true,
        error: null,
        uncertain: false,
      };
    }
    const parsed = forecastScheduleInputSchema.safeParse({
      expectedRevision: Number(fields.get('revision')),
      enabled: fields.get('enabled') === 'yes',
      intervalMinutes: Number(fields.get('interval')),
      durationMinutes: Number(fields.get('duration')),
      reason: fields.get('reason'),
      confirmed: fields.get('confirmed') === 'yes',
    });
    if (!parsed.success)
      throw new ApiError(
        null,
        'Check the interval, duration and reason, then confirm your changes.',
        400
      );
    const response = await fetchFn<unknown>(
      endpoint.$put({ param: { wellheadId }, json: parsed.data })
    );
    return {
      saved: forecastScheduleSchema.parse(response.data),
      checked: false,
      error: null,
      uncertain: false,
    };
  } catch (error) {
    if (error instanceof ApiError && [401, 403].includes(error.statusCode ?? 0))
      throw error;
    const state = errorState(error);
    return {
      saved: null,
      checked: false,
      error: state,
      uncertain: state.status == null || state.status >= 500,
    };
  }
}
