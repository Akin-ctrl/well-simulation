/** Internal HTTP contracts for control status and parameter validation. */
import { HTTPException } from 'hono/http-exception';
import type { ParameterDraft } from '@well-simulation/dto/req/controls';
import {
  runningControlsSchema,
  parameterValidationSchema,
  parameterRuntimeSchema,
} from '@well-simulation/dto/res/controls';
import { config } from './config';

/** Bounded service calls carry only server-owned credentials. */
async function twinRequest(path: string, body?: object): Promise<unknown> {
  try {
    const response = await fetch(`${config.twinCoreUrl}${path}`, {
      method: body ? 'POST' : 'GET',
      headers: {
        'content-type': 'application/json',
        'x-twin-internal-token': config.twinInternalToken,
      },
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(2000),
    });
    if (response.status === 400)
      throw new HTTPException(400, { message: 'Model validation failed' });
    if (response.status === 404)
      throw new HTTPException(404, { message: 'Well not found' });
    if (!response.ok) throw new Error('Twin not ready');
    const text = await response.text();
    if (text.length > 16_384) throw new Error('Twin response too large');
    return JSON.parse(text) as unknown;
  } catch (error) {
    if (error instanceof HTTPException) throw error;
    throw new HTTPException(503, { message: 'Twin service unavailable' });
  }
}

/** A failed hint never rewrites a successfully committed command outcome. */
export async function notifyTwin(path: string): Promise<boolean> {
  try {
    await twinRequest(path, {});
    return true;
  } catch {
    return false;
  }
}

export async function runningControls(wellheadId: number) {
  const result = runningControlsSchema.safeParse(
    await twinRequest(`/wellheads/${wellheadId}/controls`)
  );
  if (!result.success || result.data.wellheadId !== wellheadId)
    throw new HTTPException(503, { message: 'Running controls unavailable' });
  return result.data;
}

/** Load lifecycle identity even if invalid parameters prevented model startup. */
export async function parameterIdentity(wellheadId: number) {
  const identity = parameterRuntimeSchema.safeParse(
    await twinRequest(`/wellheads/${wellheadId}/state`)
  );
  if (!identity.success || identity.data.wellheadId !== wellheadId)
    throw new HTTPException(503, { message: 'Model identity unavailable' });
  return identity.data;
}

/** Ask the authoritative Python resolver to validate the complete parameter set. */
export async function validateParameter(input: ParameterDraft) {
  const { wellheadId, modelVersion, parameterKey, value, unit } = input;
  const result = parameterValidationSchema.safeParse(
    await twinRequest('/parameter-changes/validate', {
      wellheadId,
      modelVersion,
      parameterKey,
      value,
      unit,
    })
  );
  if (!result.success || result.data.modelVersion !== input.modelVersion)
    throw new HTTPException(503, { message: 'Parameter validation unavailable' });
  return result.data;
}
