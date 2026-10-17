import { Hono } from 'hono';
import { validator } from 'hono/validator';
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

import { projectionDetail, runHistory } from '../projection-details';
import { modelStatus, snapshotCandidates } from '../twin-control-client';
import {
  snapshotSearchSchema,
  snapshotSelectionSchema,
  snapshotTimeKey,
} from '@well-simulation/dto/req/snapshots';
import { storedBranchOrigin } from '@well-simulation/dto/res/snapshots';

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
  if ([400, 404, 409, 503].includes(response.status)) {
    throw new HTTPException(response.status as 400 | 404 | 409 | 503, {
      message:
        response.status === 409
          ? 'The selected snapshot no longer matches this model'
          : 'Twin request could not complete',
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
    const result = projectionResponseSchema.parse(JSON.parse(text));
    if ('baseSnapshot' in body && body.baseSnapshot !== undefined) {
      const selected = snapshotSelectionSchema.parse(body.baseSnapshot);
      if (
        result.kind !== 'scenario' ||
        result.branchOrigin.kind !== 'snapshot' ||
        result.branchOrigin.snapshotId !== selected.snapshotId ||
        snapshotTimeKey(result.branchOrigin.simulatedAt) !==
          snapshotTimeKey(selected.simulatedAt) ||
        result.baseSimulatedAt !== result.branchOrigin.simulatedAt
      )
        throw new Error('Historical source mismatch');
    } else if (result.branchOrigin.kind !== 'current') {
      throw new Error('Current source mismatch');
    }
    return result;
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
  baseState: Record<string, unknown>;
}) {
  return {
    runId: run.runId,
    wellheadId: run.wellheadId,
    source: 'synthetic_reduced_order_model' as const,
    fieldCalibrated: false as const,
    modelVersion: run.modelVersion,
    baseSimulatedAt: run.baseSimulatedAt,
    generatedAt: run.createdAt,
    branchOrigin: storedBranchOrigin(run.baseState),
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

async function savedForecast(runId: string) {
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
  return projectionDetail(
    projectionResponseSchema.parse({
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
    }),
    run
  );
}

async function savedScenario(runId: string) {
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
  return projectionDetail(
    projectionResponseSchema.parse({
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
    }),
    run
  );
}

/** Public analysis routes. They cannot apply a live actuator command. */
export const twinRouter = new Hono()
  .use('*', bodyLimit({ maxSize: 4096 }))
  .get('/status', requireCapability('dashboard:read'), responseHandler(modelStatus))
  .get(
    '/wellheads/:wellheadId/snapshots',
    requireCapability('scenario:run'),
    validator('query', (value) => snapshotSearchSchema.parse(value)),
    (c) =>
      responseHandler(() =>
        snapshotCandidates(
          positiveWellheadId(c.req.param('wellheadId')),
          c.req.valid('query').at
        )
      )(c)
  )
  .get(
    '/wellheads/:wellheadId/runs',
    requireCapability('dashboard:read'),
    validator('query', (value) => value),
    (c) =>
      responseHandler(() =>
        runHistory(positiveWellheadId(c.req.param('wellheadId')), c.req.query())
      )(c)
  )
  .post(
    '/wellheads/:wellheadId/forecast',
    requireCapability('scenario:run'),
    validator('json', (value) => forecastRequestSchema.parse(value)),
    (c) =>
      responseHandler(async () => {
        const actor = sessionUserId(c);
        const wellheadId = positiveWellheadId(c.req.param('wellheadId'));
        const input = c.req.valid('json');
        try {
          const result = await runTwin(
            `/wellheads/${wellheadId}/forecast`,
            actor,
            input
          );
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
      }, 'Forecast generated')(c)
  )
  .post(
    '/scenarios',
    requireCapability('scenario:run'),
    validator('json', (value) => scenarioRequestSchema.parse(value)),
    (c) =>
      responseHandler(async () => {
        const actor = sessionUserId(c);
        const input = c.req.valid('json');
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
      }, 'Scenario generated')(c)
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
