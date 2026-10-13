import z from 'zod';
import { controlChangeSchema } from './projection';

export const commandIdSchema = z.string().uuid();

export const wellIdSchema = z.number().int().positive().max(2_147_483_647);
export const revisionSchema = z.number().int().min(0).max(2_147_483_646);
export const liveTargetsSchema = controlChangeSchema.refine(
  (value) => value.emergencyShutdown === undefined || Object.keys(value).length === 1,
  { message: 'Shutdown or reset must be a separate command' }
);
export const controlDraftSchema = z
  .object({
    wellheadId: wellIdSchema,
    expectedRevision: revisionSchema,
    reason: z.string().trim().min(1).max(500),
    targets: liveTargetsSchema,
  })
  .strict();
export const controlActivateSchema = z
  .object({
    confirm: z.literal(true),
    expectedRevision: revisionSchema,
  })
  .strict();
export const confirmationSchema = z.object({ confirm: z.literal(true) }).strict();
export const parameterDraftSchema = z
  .object({
    wellheadId: wellIdSchema,
    modelVersion: z.string().regex(/^\d+\.\d+\.\d+$/),
    parameterKey: z.string().min(1).max(100),
    value: z.number().finite().nullable(),
    unit: z.string().min(1).max(40).nullable(),
    expectedOverrideId: wellIdSchema.nullable(),
    reason: z.string().trim().min(1).max(500),
  })
  .strict()
  .refine((value) => (value.value === null) === (value.unit === null));

export type ControlDraft = z.infer<typeof controlDraftSchema>;
export type LiveTargets = z.infer<typeof liveTargetsSchema>;
export type ParameterDraft = z.infer<typeof parameterDraftSchema>;
