import { Hono } from 'hono';
import { HTTPException } from 'hono/http-exception';
import { bodyLimit } from 'hono/body-limit';
import { asc, db, eq } from '@well-simulation/db/query';
import {
  predictionPoint,
  predictionRun,
  scenarioPoint,
  scenarioRun,
} from '@well-simulation/db/schemas/schema';
import {
  forecastRequestSchema,
  scenarioRequestSchema,
} from '@well-simulation/dto/req/projection';
import {
  projectionResponseSchema,
  type ProjectionResponse,
} from '@well-simulation/dto/res/projection';

import { recordAuditEvent } from '../audit';
import { config } from '../config';
import { requireCapability, sessionUserId } from '../middleware/authorize';
import { responseHandler } from '../utils/handler';

const MAX_TWIN_RESPONSE_BYTES = 5_000_000;
const TWIN_TIMEOUT_MS = 12_000;
const RUN_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function positiveWellheadId(raw: string | undefined): number {
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0 || value > 2_147_483_647) {
    throw new HTTPException(400, { message: 'Invalid wellhead id' });
  }
  return value;
}

function runId(raw: string | undefined): string {
  if (!raw || !RUN_ID_PATTERN.test(raw)) {
    throw new HTTPException(400, { message: 'Invalid run id' });
  }
  return raw;
}

async function requestBody(c: {
  req: { json: () => Promise<unknown> };
}): Promise<unknown> {
  try {
    return await c.req.json();
  } catch {
    throw new HTTPException(400, { message: 'Invalid JSON request' });
  }
}

/** The browser never receives the service token or calls twin-core directly. */
async function runTwin(
  path: string,
  actor: number,
  body: object
): Promise<ProjectionResponse> {
  let response: Response;
  try {
    response = await fetch(`${config.twinCoreUrl}${path}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-twin-internal-token': config.twinInternalToken,
        'x-twin-actor-id': String(actor),
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(TWIN_TIMEOUT_MS),
    });
  } catch (error) {
    if (error instanceof Error && error.name === 'TimeoutError') {
      throw new HTTPException(504, { message: 'Twin service timed out' });
    }
    throw new HTTPException(503, { message: 'Twin service unavailable' });
  }
  if (response.status === 400 || response.status === 404 || response.status === 503) {
    throw new HTTPException(response.status, {
      message: 'Twin request could not complete',
    });
  }
  if (response.status !== 201) {
    throw new HTTPException(502, { message: 'Twin service rejected the request' });
  }
  let text: string;
  try {
    text = await response.text();
  } catch {
    throw new HTTPException(503, { message: 'Twin service response unavailable' });
  }
  if (text.length > MAX_TWIN_RESPONSE_BYTES) {
    throw new HTTPException(502, { message: 'Twin service response is too large' });
  }
  try {
    return projectionResponseSchema.parse(JSON.parse(text));
  } catch {
    throw new HTTPException(502, { message: 'Twin service response is invalid' });
  }
}

function baseResponse(run: {
  runId: string;
  wellheadId: number;
  modelVersion: string;
  baseSimulatedAt: string;
  createdAt: string;
  horizonSeconds: number;
  stepSeconds: number;
  confidenceScore: number;
  confidenceReasons: string[];
}) {
  return {
    runId: run.runId,
    wellheadId: run.wellheadId,
    source: 'synthetic_reduced_order_model' as const,
    fieldCalibrated: false as const,
    modelVersion: run.modelVersion,
    baseSimulatedAt: run.baseSimulatedAt,
    generatedAt: run.createdAt,
    horizonSeconds: run.horizonSeconds,
    stepSeconds: run.stepSeconds,
    confidence: {
      method: 'heuristic_uncalibrated' as const,
      score: run.confidenceScore,
      label: run.confidenceScore >= 0.4 ? ('moderate' as const) : ('low' as const),
      reasons: run.confidenceReasons,
    },
  };
}

async function savedForecast(runId: string): Promise<ProjectionResponse> {
  const [run] = await db
    .select()
    .from(predictionRun)
    .where(eq(predictionRun.runId, runId))
    .limit(1);
  if (!run) throw new HTTPException(404, { message: 'Forecast not found' });
  const rows = await db
    .select()
    .from(predictionPoint)
    .where(eq(predictionPoint.runId, runId))
    .orderBy(asc(predictionPoint.forecastAt));
  if (rows.length !== run.horizonSeconds / run.stepSeconds)
    throw new HTTPException(410, { message: 'Forecast points have expired' });
  return projectionResponseSchema.parse({
    ...baseResponse(run),
    kind: 'forecast',
    name: null,
    points: rows.map((point) => ({
      elapsedSeconds: point.elapsedSeconds,
      timestamp: point.forecastAt,
      values: point.values,
      alarmRisk: point.alarmRisk,
      triggeredRuleIds: point.triggeredRuleIds,
    })),
  });
}

async function savedScenario(runId: string): Promise<ProjectionResponse> {
  const [run] = await db
    .select()
    .from(scenarioRun)
    .where(eq(scenarioRun.runId, runId))
    .limit(1);
  if (!run) throw new HTTPException(404, { message: 'Scenario not found' });
  const rows = await db
    .select()
    .from(scenarioPoint)
    .where(eq(scenarioPoint.runId, runId))
    .orderBy(asc(scenarioPoint.forecastAt));
  if (rows.length !== run.horizonSeconds / run.stepSeconds)
    throw new HTTPException(410, { message: 'Scenario points have expired' });
  return projectionResponseSchema.parse({
    ...baseResponse(run),
    kind: 'scenario',
    name: run.name,
    points: rows.map((point) => ({
      elapsedSeconds: point.elapsedSeconds,
      timestamp: point.forecastAt,
      values: point.values,
      alarmRisk: point.alarmRisk,
      triggeredRuleIds: point.triggeredRuleIds,
      baselineValues: point.baselineValues,
      baselineAlarmRisk: point.baselineAlarmRisk,
    })),
  });
}

/** Public analysis routes. They cannot apply a live actuator command. */
export const twinRouter = new Hono()
  .use('*', bodyLimit({ maxSize: 4096 }))
  .post(
    '/wellheads/:wellheadId/forecast',
    requireCapability('scenario:run'),
    responseHandler(async (c) => {
      const actor = sessionUserId(c);
      const wellheadId = positiveWellheadId(c.req.param('wellheadId'));
      const input = forecastRequestSchema.parse(await requestBody(c));
      try {
        const result = await runTwin(`/wellheads/${wellheadId}/forecast`, actor, input);
        await recordAuditEvent(c, {
          action: 'twin.forecast',
          outcome: 'success',
          actorUserId: actor,
          subjectType: 'prediction_run',
          subjectId: result.runId,
          detail: { wellheadId, horizonSeconds: input.horizonSeconds },
        });
        return result;
      } catch (error) {
        await recordAuditEvent(c, {
          action: 'twin.forecast',
          outcome: 'failure',
          actorUserId: actor,
          detail: { wellheadId },
        });
        throw error;
      }
    }, 'Forecast generated')
  )
  .post(
    '/scenarios',
    requireCapability('scenario:run'),
    responseHandler(async (c) => {
      const actor = sessionUserId(c);
      const input = scenarioRequestSchema.parse(await requestBody(c));
      try {
        const result = await runTwin('/scenarios', actor, input);
        await recordAuditEvent(c, {
          action: 'twin.scenario',
          outcome: 'success',
          actorUserId: actor,
          subjectType: 'scenario_run',
          subjectId: result.runId,
          detail: {
            wellheadId: input.wellheadId,
            horizonSeconds: input.horizonSeconds,
          },
        });
        return result;
      } catch (error) {
        await recordAuditEvent(c, {
          action: 'twin.scenario',
          outcome: 'failure',
          actorUserId: actor,
          detail: { wellheadId: input.wellheadId },
        });
        throw error;
      }
    }, 'Scenario generated')
  )
  .get(
    '/forecasts/:runId',
    requireCapability('dashboard:read'),
    responseHandler(
      async (c) => savedForecast(runId(c.req.param('runId'))),
      'Forecast loaded'
    )
  )
  .get(
    '/scenarios/:runId',
    requireCapability('dashboard:read'),
    responseHandler(
      async (c) => savedScenario(runId(c.req.param('runId'))),
      'Scenario loaded'
    )
  );
