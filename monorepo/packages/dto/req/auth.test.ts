import { describe, expect, it } from 'vitest';
import { loginSchema, MIN_PASSWORD_LENGTH, registerSchema } from './auth';

const strongPassword = 'correct-horse-battery-staple';

describe('auth request schemas', () => {
  it('accepts a valid registration payload', () => {
    const result = registerSchema.safeParse({
      firstName: 'Ada',
      lastName: 'Lovelace',
      username: 'ada-ops',
      email: 'ada@example.com',
      password: strongPassword,
      confirmPassword: strongPassword,
    });

    expect(result.success).toBe(true);
  });

  it('rejects short passwords for registration', () => {
    const result = registerSchema.safeParse({
      username: 'ada-ops',
      email: 'ada@example.com',
      password: 'short123',
      confirmPassword: 'short123',
    });

    expect(result.success).toBe(false);
    expect(result.error?.issues[0]?.message).toContain(
      `${MIN_PASSWORD_LENGTH} characters`
    );
  });

  it('rejects mismatched confirmation passwords', () => {
    const result = registerSchema.safeParse({
      username: 'ada-ops',
      email: 'ada@example.com',
      password: strongPassword,
      confirmPassword: 'different-password-value',
    });

    expect(result.success).toBe(false);
    expect(result.error?.issues[0]?.path).toEqual(['confirmPassword']);
  });

  it('requires the same password length policy for login', () => {
    const result = loginSchema.safeParse({
      emailOrUsername: 'ada-ops',
      password: 'short123',
    });

    expect(result.success).toBe(false);
    expect(result.error?.issues[0]?.message).toContain(
      `${MIN_PASSWORD_LENGTH} characters`
    );
  });
});
