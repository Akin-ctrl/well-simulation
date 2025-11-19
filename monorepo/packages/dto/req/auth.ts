import z from 'zod';

export const registerSchema = z
  .object({
    firstName: z.string().optional(),
    lastName: z.string().optional(),
    username: z.string().min(3, 'Username must be at least 3 characters'),
    email: z.email({ error: 'Please enter a valid email address' }),
    password: z.string().min(6, 'Password must be at least 6 characters'),
    confirmPassword: z.string(),
  })
  .refine((data) => data.password === data.confirmPassword, {
    message: 'Passwords do not match',
    path: ['confirmPassword'],
  });

export const loginSchema = z.object({
  emailOrUsername: z.string().min(3, 'Identifier must be at least 3 characters'),
  password: z.string().min(6, 'Password must be at least 6 characters'),
});
