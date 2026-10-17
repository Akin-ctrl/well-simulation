import z from 'zod';
import { revisionSchema, wellIdSchema } from '../req/controls';

/** Internal running-state proof, checked before queuing a public command. */
export const runningControlsSchema = z
  .object({
    wellheadId: wellIdSchema,
    revision: revisionSchema,
    modelVersion: z.string(),
    parameterFingerprint: z.string().length(64),
    status: z.string(),
    lastTickAt: z.string().datetime({ offset: true }).nullable(),
    controls: z
      .object({
        chokePositionPercent: z.number().finite().min(0).max(100),
        masterValveOpen: z.boolean(),
        wingValveOpen: z.boolean(),
        swabValveOpen: z.boolean(),
        pumpOn: z.boolean(),
        emergencyShutdown: z.boolean(),
      })
      .strict(),
  })
  .strict();
export type RunningControls = z.infer<typeof runningControlsSchema>;

export const parameterValidationSchema = z
  .object({
    modelVersion: z.string(),
    runningParameterFingerprint: z.string().length(64).nullable(),
    proposedParameterFingerprint: z.string().length(64),
    restartRequired: z.literal(true),
  })
  .strict();

/** Only lifecycle identity is needed when an invalid parameter stopped startup. */
export const parameterRuntimeSchema = z.object({
  wellheadId: wellIdSchema,
  modelVersion: z.string(),
  status: z.string(),
});

/** Canonical Python parameter definitions, with current values when available. */
export const parameterCatalogueSchema = z
  .object({
    wellheadId: wellIdSchema,
    modelVersion: z.string(),
    parameters: z
      .array(
        z
          .object({
            key: z.string().min(1),
            description: z.string(),
            unit: z.string(),
            defaultValue: z.number().finite(),
            minimum: z.number().finite(),
            maximum: z.number().finite(),
            requiresRestart: z.boolean(),
            runningValue: z.number().finite().nullable(),
          })
          .strict()
      )
      .min(1)
      .max(200),
  })
  .strict();
export type ParameterCatalogue = z.infer<typeof parameterCatalogueSchema>;

const proposalFields = {
  wellheadId: wellIdSchema,
  createdBy: wellIdSchema,
  idempotencyKey: z.string().uuid(),
  reason: z.string(),
  status: z.string(),
  createdAt: z.string(),
  expiresAt: z.string(),
  resultAt: z.string().nullable(),
};
export const commandSummarySchema = z.object({
  ...proposalFields,
  commandId: z.string().uuid(),
  expectedRevision: revisionSchema,
  modelVersion: z.string(),
  chokePositionPercent: z.number().nullable(),
  masterValveOpen: z.boolean().nullable(),
  wingValveOpen: z.boolean().nullable(),
  pumpOn: z.boolean().nullable(),
  emergencyShutdown: z.boolean().nullable(),
  resultRevision: revisionSchema.nullable(),
  resultReason: z.string().nullable(),
  events: z
    .array(
      z.object({
        eventType: z.string(),
        occurredAt: z.string(),
        reasonCode: z.string(),
      })
    )
    .max(100)
    .optional(),
});
export type CommandSummary = z.infer<typeof commandSummarySchema>;
export const parameterSummarySchema = z.object({
  ...proposalFields,
  changeId: z.string().uuid(),
  modelVersion: z.string(),
  parameterKey: z.string(),
  value: z.number().nullable(),
  unit: z.string().nullable(),
  expectedOverrideId: wellIdSchema.nullable(),
  publishedOverrideId: wellIdSchema.nullable(),
});
export type ParameterSummary = z.infer<typeof parameterSummarySchema>;

export const controlWorkspaceSchema = z.object({
  running: runningControlsSchema,
  active: z
    .object({
      revision: revisionSchema,
      appliedCommandId: z.string().uuid().nullable(),
    })
    .nullable(),
  pendingCommandId: z.string().uuid().nullable(),
});
export type ControlWorkspace = z.infer<typeof controlWorkspaceSchema>;
export const parameterWorkspaceSchema = parameterRuntimeSchema.extend({
  catalogue: parameterCatalogueSchema,
  running: runningControlsSchema.nullable(),
  latestOverrides: z.array(
    z.object({
      parameterOverrideId: wellIdSchema,
      parameterKey: z.string(),
      value: z.number().nullable(),
      unit: z.string().nullable(),
    })
  ),
});
export type ParameterWorkspace = z.infer<typeof parameterWorkspaceSchema>;
