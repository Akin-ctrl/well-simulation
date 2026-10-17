import z from 'zod';
import { redirect } from 'react-router';
import { ApiError } from '@well-simulation/dto/error';
import {
  commandIdSchema,
  controlDraftSchema,
  parameterDraftSchema,
  proposalHistoryQuerySchema,
  controlActivateSchema,
} from '@well-simulation/dto/req/controls';
import {
  commandSummarySchema,
  parameterSummarySchema,
  parameterWorkspaceSchema,
  type CommandSummary,
  type ParameterSummary,
} from '@well-simulation/dto/res/controls';
import { client, fetchFn } from '../../utils/api';
import { readDashboardView } from '../dashboard/components/read-status';
import { errorState } from './presentation';

export type OperationKind = 'control' | 'parameter';
export type Proposal = CommandSummary | ParameterSummary;
type Arguments = { params: { wellheadId?: string }; request: Request };
export const proposalId = (proposal: Proposal) =>
  'commandId' in proposal ? proposal.commandId : proposal.changeId;
export const operationUrl = (
  well: string | number,
  kind: OperationKind,
  id?: string,
  offset = 0
) =>
  `/wellheads/${well}/${kind === 'control' ? 'controls' : 'parameters'}?offset=${offset}${id ? `&proposal=${encodeURIComponent(id)}` : ''}`;

async function history(
  kind: OperationKind,
  wellheadId: string,
  query: { offset: number; idempotencyKey?: string }
) {
  const args = {
    param: { wellheadId },
    query: {
      offset: String(query.offset),
      ...(query.idempotencyKey ? { idempotencyKey: query.idempotencyKey } : {}),
    },
  };
  const response =
    kind === 'control'
      ? await fetchFn<unknown>(
          client.twin.wellheads[':wellheadId']['control-commands'].$get(args)
        )
      : await fetchFn<unknown>(
          client.twin.wellheads[':wellheadId']['parameter-changes'].$get(args)
        );
  return z
    .object({
      items: z.array(
        kind === 'control' ? commandSummarySchema : parameterSummarySchema
      ),
      offset: z.number(),
      hasMore: z.boolean(),
    })
    .parse(response.data);
}

async function selected(
  kind: OperationKind,
  wellheadId: string,
  id: string
): Promise<Proposal> {
  commandIdSchema.parse(id);
  const response =
    kind === 'control'
      ? await fetchFn<unknown>(
          client.twin['control-commands'][':id'].$get({ param: { id } })
        )
      : await fetchFn<unknown>(
          client.twin['parameter-changes'][':id'].$get({ param: { id } })
        );
  const value = (
    kind === 'control' ? commandSummarySchema : parameterSummarySchema
  ).parse(response.data);
  if (String(value.wellheadId) !== wellheadId)
    throw new ApiError(null, 'Proposal belongs to another well', 404);
  return value;
}

export function operationLoader(kind: OperationKind) {
  return async ({ params, request }: Arguments) => {
    const wellheadId = params.wellheadId ?? '';
    const search = new URL(request.url).searchParams;
    const query = proposalHistoryQuerySchema.parse({
      offset: search.get('offset') ?? 0,
    });
    const id = search.get('proposal');
    return readDashboardView(async () => {
      const [proposals, proposal, parameters] = await Promise.all([
        history(kind, wellheadId, query),
        id ? selected(kind, wellheadId, id) : null,
        kind === 'parameter'
          ? readDashboardView(async () =>
              parameterWorkspaceSchema.parse(
                (
                  await fetchFn<unknown>(
                    client.twin.wellheads[':wellheadId'].parameters.$get({
                      param: { wellheadId },
                    })
                  )
                ).data
              )
            )
          : null,
      ]);
      return { proposals, proposal, parameters };
    });
  };
}
export type OperationRead = Awaited<ReturnType<ReturnType<typeof operationLoader>>>;

/** Each explicit write has one request; uncertain responses lead to a GET lookup. */
export function operationAction(kind: OperationKind) {
  return async ({ params, request }: Arguments) => {
    const fields = await request.formData();
    const intent = String(fields.get('intent'));
    const wellheadId = params.wellheadId ?? '';
    const key = String(fields.get('key') ?? '');
    const id = String(fields.get('id') ?? '');
    try {
      if (intent === 'check') {
        const proposal = id
          ? await selected(kind, wellheadId, id)
          : (
              await history(kind, wellheadId, {
                offset: 0,
                idempotencyKey: commandIdSchema.parse(key),
              })
            ).items[0];
        if (!proposal)
          return {
            error: {
              status: undefined,
              message:
                'No proposal found yet. Check again before submitting another change.',
            },
            uncertain: true,
            key,
          };
        if (id) return { error: null, uncertain: false, key };
        return redirect(operationUrl(wellheadId, kind, proposalId(proposal)));
      }
      if (intent === 'draft') {
        commandIdSchema.parse(key);
        const raw: unknown = JSON.parse(String(fields.get('payload')));
        if (kind === 'control') {
          const json = controlDraftSchema.parse(raw);
          if (String(json.wellheadId) !== wellheadId)
            throw new ApiError(null, 'Wrong well', 400);
          const response = await fetchFn<unknown>(
            client.twin['control-commands'].$post(
              { json },
              { headers: { 'Idempotency-Key': key } }
            )
          );
          return redirect(
            operationUrl(
              wellheadId,
              kind,
              commandSummarySchema.parse(response.data).commandId
            )
          );
        }
        const json = parameterDraftSchema.parse(raw);
        if (String(json.wellheadId) !== wellheadId)
          throw new ApiError(null, 'Wrong well', 400);
        const response = await fetchFn<unknown>(
          client.twin['parameter-changes'].$post(
            { json },
            { headers: { 'Idempotency-Key': key } }
          )
        );
        return redirect(
          operationUrl(
            wellheadId,
            kind,
            parameterSummarySchema.parse(response.data).changeId
          )
        );
      }
      commandIdSchema.parse(id);
      if (fields.get('confirm') !== 'yes')
        throw new ApiError(null, 'Confirm the well, target and reason first.', 400);
      await selected(kind, wellheadId, id);
      if (kind === 'control') {
        const endpoint = client.twin['control-commands'][':id'];
        if (intent === 'cancel')
          await fetchFn(
            endpoint.cancel.$post({ param: { id }, json: { confirm: true } })
          );
        else if (intent === 'activate') {
          const json = controlActivateSchema.parse({
            confirm: true,
            expectedRevision: Number(fields.get('revision')),
          });
          await fetchFn(endpoint.activate.$post({ param: { id }, json }));
        } else throw new ApiError(null, 'Unknown action', 400);
      } else {
        const endpoint = client.twin['parameter-changes'][':id'];
        if (intent === 'cancel')
          await fetchFn(
            endpoint.cancel.$post({ param: { id }, json: { confirm: true } })
          );
        else if (intent === 'publish')
          await fetchFn(
            endpoint.publish.$post({ param: { id }, json: { confirm: true } })
          );
        else throw new ApiError(null, 'Unknown action', 400);
      }
      return { error: null, uncertain: false, key };
    } catch (error) {
      if (error instanceof ApiError && [401, 403].includes(error.statusCode ?? 0))
        throw error;
      const state =
        error instanceof z.ZodError
          ? { status: 400, message: 'Check the proposal values and units.' }
          : errorState(error);
      const uncertain =
        intent === 'check' || state.status == null || state.status >= 500;
      return {
        error:
          state.status === 409
            ? {
                ...state,
                message:
                  'The state changed or another proposal exists. Refresh and review your proposals before continuing.',
              }
            : state,
        uncertain,
        key,
      };
    }
  };
}
