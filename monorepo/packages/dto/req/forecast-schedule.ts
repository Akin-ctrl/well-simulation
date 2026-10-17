import z from 'zod';

/** Whole minutes keep the editor and worker limits in the same units. */
export const forecastScheduleInputSchema = z
  .object({
    expectedRevision: z.number().int().min(0).max(2_147_483_646),
    enabled: z.boolean(),
    intervalMinutes: z.number().int().min(5).max(1440),
    durationMinutes: z.number().int().min(5).max(30),
    reason: z.string().trim().min(1).max(500),
    confirmed: z.literal(true),
  })
  .strict();
export type ForecastScheduleInput = z.infer<typeof forecastScheduleInputSchema>;
