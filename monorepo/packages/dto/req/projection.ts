import z from 'zod';

/** One bounded request uses the model's fixed one-second internal step. */
const windowFields = {
  horizonSeconds: z.number().int().min(300).max(7200).default(1800),
  stepSeconds: z.number().int().min(5).max(300).default(60),
};

export const forecastRequestSchema = z
  .object(windowFields)
  .strict()
  .refine((value) => value.horizonSeconds % value.stepSeconds === 0, {
    message: 'horizonSeconds must be divisible by stepSeconds',
  });

export const controlChangeSchema = z
  .object({
    chokePositionPercent: z.number().finite().min(0).max(100).optional(),
    masterValveOpen: z.boolean().optional(),
    wingValveOpen: z.boolean().optional(),
    pumpOn: z.boolean().optional(),
    emergencyShutdown: z.boolean().optional(),
  })
  .strict()
  .refine((value) => Object.keys(value).length > 0, {
    message: 'A control step must change at least one field',
  });

export const scenarioRequestSchema = z
  .object({
    wellheadId: z.number().int().positive().max(2_147_483_647),
    name: z.string().trim().min(1).max(120),
    ...windowFields,
    controls: z
      .array(
        z
          .object({
            offsetMinutes: z.number().int().min(0),
            input: controlChangeSchema,
          })
          .strict()
      )
      .min(1)
      .max(16),
  })
  .strict()
  .refine((value) => value.horizonSeconds % value.stepSeconds === 0, {
    message: 'horizonSeconds must be divisible by stepSeconds',
  })
  .refine(
    (value) =>
      value.controls.every(
        (step, index) =>
          step.offsetMinutes * 60 < value.horizonSeconds &&
          (index === 0 || step.offsetMinutes > value.controls[index - 1]!.offsetMinutes)
      ),
    { message: 'Control offsets must be ordered and inside the horizon' }
  );

export type ForecastRequest = z.infer<typeof forecastRequestSchema>;
export type ScenarioRequest = z.infer<typeof scenarioRequestSchema>;
