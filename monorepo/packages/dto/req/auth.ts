import z from 'zod';

import { ROLES } from '../auth/roles';

export const MIN_PASSWORD_LENGTH = 12;

const passwordSchema = z
  .string()
  .min(
    MIN_PASSWORD_LENGTH,
    `Password must be at least ${MIN_PASSWORD_LENGTH} characters`
  );

export const registerSchema = z
  .object({
    firstName: z.string().optional(),
    lastName: z.string().optional(),
    username: z.string().min(3, 'Username must be at least 3 characters'),
    email: z.email({ error: 'Please enter a valid email address' }),
    password: passwordSchema,
    confirmPassword: z.string(),
    // Only an administrator reaches this endpoint (ADR 0033), so the role is
    // theirs to set. Omitting it creates the least-privileged account.
    role: z.enum(ROLES).optional(),
  })
  .refine((data) => data.password === data.confirmPassword, {
    message: 'Passwords do not match',
    path: ['confirmPassword'],
  });

export const loginSchema = z.object({
  emailOrUsername: z.string().min(3, 'Identifier must be at least 3 characters'),
  password: passwordSchema,
});
