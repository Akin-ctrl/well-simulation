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
