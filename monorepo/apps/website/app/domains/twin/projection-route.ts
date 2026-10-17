import { redirect, type ShouldRevalidateFunctionArgs } from 'react-router';
import { ApiError } from '@well-simulation/dto/error';
import {
  forecastRequestSchema,
  scenarioRequestSchema,
} from '@well-simulation/dto/req/projection';
import type { ProjectionResponse } from '@well-simulation/dto/res/projection';
import {
  projectionDetailSchema,
  type ProjectionDetail,
} from '@well-simulation/dto/res/projection-detail';
import { client, fetchFn } from '../../utils/api';
import { routes } from '../../config/routes';
import { errorState } from './presentation';

export type ProjectionKind = 'forecast' | 'scenario';
type Arguments = { params: { wellheadId?: string }; request: Request };
export type ResultData = {
  run: ProjectionDetail | null;
  error: ReturnType<typeof errorState> | null;
};
export function projectionUrl(
  wellheadId: string | number,
  kind: ProjectionKind,
  runId?: string
): string {
  return `${routes.dashboard.wellhead(wellheadId)}/${kind === 'forecast' ? 'forecast' : 'scenarios'}${runId ? `?run=${encodeURIComponent(runId)}` : ''}`;
}
/** Saved result GETs do not execute projections or repeat on status polling. */
export function projectionLoader(kind: ProjectionKind) {
  return async ({ params, request }: Arguments): Promise<ResultData> => {
    const runId = new URL(request.url).searchParams.get('run');
    if (!runId) return { run: null, error: null };
    try {
      const response =
        kind === 'forecast'
          ? await fetchFn<ProjectionDetail>(
              client.twin.forecasts[':runId'].$get({ param: { runId } })
            )
          : await fetchFn<ProjectionDetail>(
              client.twin.scenarios[':runId'].$get({ param: { runId } })
            );
      const run = projectionDetailSchema.parse(response.data);
      if (String(run.wellheadId) !== params.wellheadId)
        throw new ApiError(null, 'Result belongs to another well', 404);
      return { run, error: null };
    } catch (error) {
      return { run: null, error: errorState(error) };
    }
  };
}
/** One explicit form submission, with no retry after an uncertain response. */
export function projectionAction(kind: ProjectionKind) {
  return async ({ params, request }: Arguments) => {
    try {
      const fields = await request.formData();
      const raw: unknown = JSON.parse(String(fields.get('payload')));
      const wellheadId = params.wellheadId ?? '';
      let response;
      if (kind === 'forecast') {
        const body = forecastRequestSchema.parse(raw);
        response = await fetchFn<ProjectionResponse>(
          client.twin.wellheads[':wellheadId'].forecast.$post({
            param: { wellheadId },
            json: body,
          })
        );
      } else {
        const body = scenarioRequestSchema.parse(raw);
        if (String(body.wellheadId) !== wellheadId)
          throw new ApiError(null, 'Wrong well', 400);
        response = await fetchFn<ProjectionResponse>(
          client.twin.scenarios.$post({ json: body })
        );
      }
      return redirect(projectionUrl(wellheadId, kind, response.data.runId));
    } catch (error) {
      return { error: errorState(error) };
    }
  };
}
export function savedResultShouldRevalidate({
  currentUrl,
  nextUrl,
  formMethod,
  defaultShouldRevalidate,
}: ShouldRevalidateFunctionArgs) {
  return !formMethod && currentUrl.href === nextUrl.href
    ? false
    : defaultShouldRevalidate;
}
