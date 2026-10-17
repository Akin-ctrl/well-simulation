/** Saved-run presentation uses stored inputs and the canonical signal catalogue. */
import { HTTPException } from 'hono/http-exception';
import { db, desc, eq, sql } from '@well-simulation/db/query';
import {
  parametertype,
  predictionRun,
  scenarioRun,
  wellhead,
} from '@well-simulation/db/schemas/schema';
import {
  projectionDetailSchema,
  runHistoryQuerySchema,
  storedScheduledControlSchema,
  type RunHistory,
} from '@well-simulation/dto/res/projection-detail';
import { SCENARIO_CONTROLS } from '@well-simulation/dto/req/scenario-controls';
import type { ProjectionResponse } from '@well-simulation/dto/res/projection';

export async function projectionDetail(
  result: ProjectionResponse,
  run: {
    controlValues: Record<string, number | boolean>;
    scheduledControls?: Record<string, unknown>[];
  }
) {
  const signals = await db
    .select({
      code: parametertype.code,
      displayName: parametertype.displayName,
      canonicalUnit: parametertype.canonicalUnit,
      dataType: parametertype.dataType,
    })
    .from(parametertype)
    .orderBy(parametertype.parameterTypeId);
  const keys = new Set(result.points.flatMap((point) => Object.keys(point.values)));
  return projectionDetailSchema.parse({
    ...result,
    baselineControls: run.controlValues,
    scheduledControls: (run.scheduledControls ?? []).map((raw) => {
      const step = storedScheduledControlSchema.parse(raw);
      return {
        offsetMinutes: step.offset_seconds / 60,
        input: Object.fromEntries(
          Object.entries(SCENARIO_CONTROLS).flatMap(([key, definition]) => {
            const value = step.change[definition.storedKey];
            return value === null || value === undefined ? [] : [[key, value]];
          })
        ),
      };
    }),
    signals: signals.filter((signal) => keys.has(signal.code)),
  });
}

/** Bound the query and select summaries only, never thousands of point rows. */
export async function runHistory(
  wellheadId: number,
  query: Record<string, string>
): Promise<RunHistory> {
  const { kind, limit, offset } = runHistoryQuerySchema.parse(query);
  const [asset] = await db
    .select({ id: wellhead.wellheadId })
    .from(wellhead)
    .where(eq(wellhead.wellheadId, wellheadId))
    .limit(1);
  if (!asset) throw new HTTPException(404, { message: 'Wellhead not found' });
  const table = kind === 'forecast' ? predictionRun : scenarioRun;
  const rows = await db
    .select({
      runId: table.runId,
      wellheadId: table.wellheadId,
      name: kind === 'scenario' ? scenarioRun.name : sql<string | null>`NULL`,
      createdAt: table.createdAt,
      baseSimulatedAt: table.baseSimulatedAt,
      modelVersion: table.modelVersion,
      horizonSeconds: table.horizonSeconds,
      stepSeconds: table.stepSeconds,
      confidenceScore: table.confidenceScore,
    })
    .from(table)
    .where(eq(table.wellheadId, wellheadId))
    .orderBy(desc(table.createdAt), desc(table.runId))
    .limit(limit + 1)
    .offset(offset);
  return {
    items: rows.slice(0, limit).map((row) => ({ ...row, kind })),
    hasMore: rows.length > limit,
  };
}
