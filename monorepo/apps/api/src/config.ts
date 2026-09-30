/**
 * Configuration read and validated once, at startup.
 *
 * `JWT_SECRET` was previously read per request, so a misconfigured service
 * started cleanly, reported healthy, and failed every authenticated call with a
 * 500. Standard 9 asks for explicit startup behaviour: refuse to start instead.
 */

const MIN_SECRET_LENGTH = 32;

function required(name: string): string {
  const value = process.env[name];
  if (!value || value.trim().length === 0) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function readJwtSecret(): string {
  const secret = required('JWT_SECRET');
  if (secret.length < MIN_SECRET_LENGTH) {
    throw new Error(
      `JWT_SECRET must be at least ${MIN_SECRET_LENGTH} characters; got ${secret.length}`
    );
  }
  return secret;
}

/**
 * Whether session cookies carry the `Secure` flag.
 *
 * Previously tied to `NODE_ENV === 'production'`, which the compose stack sets
 * while serving plain HTTP on port 8082. That works only because browsers treat
 * localhost as a secure context; on any real host without TLS the browser drops
 * the cookie and login silently fails. Driving it from an explicit variable
 * makes the deployment state the truth rather than inferring it.
 */
function readCookieSecure(): boolean {
  const explicit = process.env.COOKIE_SECURE;
  if (explicit !== undefined) {
    return explicit.toLowerCase() === 'true';
  }
  return process.env.NODE_ENV === 'production';
}

/**
 * Origins permitted to make state-changing requests.
 *
 * Empty means same-origin only, which is how the dashboard reaches the API:
 * nginx proxies `/api` so the browser never makes a cross-origin call.
 */
function readAllowedOrigins(): string[] {
  return (process.env.ALLOWED_ORIGINS ?? '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
}

export const config = {
  jwtSecret: readJwtSecret(),
  cookieSecure: readCookieSecure(),
  allowedOrigins: readAllowedOrigins(),
  sessionMaxAgeSeconds: 60 * 60 * 24,
} as const;
