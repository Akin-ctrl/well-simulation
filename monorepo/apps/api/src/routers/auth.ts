import { Hono, type Context } from 'hono';
import { db } from '@well-simulation/db/query';
import { users } from '@well-simulation/db/schemas/user';
import { zValidator } from '@hono/zod-validator';
import { loginSchema, registerSchema } from '@well-simulation/dto/req/auth';
import { DEFAULT_ROLE } from '@well-simulation/dto/auth/roles';
import { responseHandler } from '../utils/handler';
import { requireCapability, sessionUserId } from '../middleware/authorize';
import bcryptjs from 'bcryptjs';
import { HTTPException } from 'hono/http-exception';
import { deleteCookie, setCookie } from 'hono/cookie';
import { sign } from 'hono/jwt';

import { config } from '../config';
import { recordAuditEvent } from '../audit';

const AUTH_COOKIE_NAME = 'auth_token';
const BCRYPT_ROUNDS = 11;

type UserRecord = typeof users.$inferSelect;

function publicUser(user: UserRecord) {
  // Destructured purely to drop the hash from the response. The underscore
  // marks it as deliberately unused; removing it would leak the password hash.
  const { encryptedPassword: _encryptedPassword, ...safeUser } = user;
  return safeUser;
}

async function issueSession(c: Context, user: UserRecord) {
  const expiresAtSeconds = Math.floor(Date.now() / 1000) + config.sessionMaxAgeSeconds;
  const token = await sign(
    {
      sub: String(user.id),
      email: user.email,
      role: user.role ?? DEFAULT_ROLE,
      exp: expiresAtSeconds,
    },
    config.jwtSecret,
    config.jwtAlgorithm
  );

  setCookie(c, AUTH_COOKIE_NAME, token, {
    httpOnly: true,
    sameSite: 'Lax',
    secure: config.cookieSecure,
    path: '/',
    maxAge: config.sessionMaxAgeSeconds,
  });
}

export const authRouter = new Hono()
  // Creating a user is an administrative action, not self-service. ADR 0033
  // closed open registration: an unauthenticated write endpoint on an
  // operations dashboard handed every caller a view of the whole fleet.
  .post(
    '/register',
    requireCapability('user:manage'),
    zValidator('json', registerSchema),
    (c) => {
      const payload = c.req.valid('json');

      return responseHandler(async () => {
        const encryptedPassword = await bcryptjs.hash(payload.password, BCRYPT_ROUNDS);

        const user = await db
          .insert(users)
          .values({
            firstName: payload.firstName,
            lastName: payload.lastName,
            userName: payload.username,
            email: payload.email,
            encryptedPassword: encryptedPassword,
            role: payload.role ?? DEFAULT_ROLE,
          })
          .returning();

        const createdUser = user?.[0];
        if (!createdUser) {
          throw new HTTPException(500, { message: 'User registration failed' });
        }

        await recordAuditEvent(c, {
          action: 'user.create',
          outcome: 'success',
          actorUserId: sessionUserId(c),
          subjectType: 'user',
          subjectId: createdUser.id,
          detail: { role: createdUser.role ?? DEFAULT_ROLE },
        });

        return { user: publicUser(createdUser) };
      })(c);
    }
  )
  .post('/login', zValidator('json', loginSchema), (c) => {
    const payload = c.req.valid('json');

    return responseHandler(async () => {
      const user = await db.query.users.findFirst({
        where(fields, { eq, or }) {
          return or(
            eq(fields.email, payload.emailOrUsername),
            eq(fields.userName, payload.emailOrUsername)
          );
        },
      });

      if (!user) {
        // Recorded even though no account matched. If rows only appeared for
        // real accounts, the presence of one would disclose that the account
        // exists (ADR 0034).
        await recordAuditEvent(c, {
          action: 'auth.login',
          outcome: 'failure',
          actorIdentifier: payload.emailOrUsername,
          detail: { reason: 'unknown_identifier' },
        });
        throw new HTTPException(401, { message: 'Invalid credentials' });
      }

      const encryptedPassword = user.encryptedPassword;
      if (!encryptedPassword) {
        throw new HTTPException(401, { message: 'Invalid credentials' });
      }

      const isPasswordValid = await bcryptjs.compare(
        payload.password,
        encryptedPassword
      );

      if (!isPasswordValid) {
        await recordAuditEvent(c, {
          action: 'auth.login',
          outcome: 'failure',
          actorUserId: user.id,
          actorIdentifier: payload.emailOrUsername,
          detail: { reason: 'bad_password' },
        });
        throw new HTTPException(401, { message: 'Invalid credentials' });
      }

      await issueSession(c, user);
      await recordAuditEvent(c, {
        action: 'auth.login',
        outcome: 'success',
        actorUserId: user.id,
        actorIdentifier: payload.emailOrUsername,
      });
      return { user: publicUser(user) };
    })(c);
  })
  .get('/me', (c) => {
    return responseHandler(async () => {
      const userId = sessionUserId(c);
      const user = await db.query.users.findFirst({
        where(fields, { eq }) {
          return eq(fields.id, userId);
        },
      });

      if (!user) {
        throw new HTTPException(401, { message: 'Unauthorized' });
      }

      return { user: publicUser(user) };
    })(c);
  })
  .post('/logout', (c) => {
    return responseHandler(async () => {
      // Read the session before the cookie is cleared, so the event carries an
      // actor rather than being anonymous.
      const userId = sessionUserId(c);

      deleteCookie(c, AUTH_COOKIE_NAME, { path: '/' });

      await recordAuditEvent(c, {
        action: 'auth.logout',
        outcome: 'success',
        actorUserId: userId,
      });

      return { authenticated: false };
    }, 'Logged out')(c);
  });
