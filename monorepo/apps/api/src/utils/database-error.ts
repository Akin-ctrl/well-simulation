type DatabaseError = { code?: unknown; cause?: unknown };

/**
 * Find a Postgres error code, following the cause chain.
 *
 * Drizzle wraps driver failures in its own error, so the `code` that says a
 * unique constraint was violated is on `error.cause`, not the error itself.
 */
export function errorCode(error: unknown, depth = 0): string | undefined {
  if (typeof error !== 'object' || error === null || depth > 5) return undefined;

  const code = (error as DatabaseError).code;
  if (typeof code === 'string') return code;

  return errorCode((error as DatabaseError).cause, depth + 1);
}
