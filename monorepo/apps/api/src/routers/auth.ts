import { Hono, type Context } from 'hono';
import { db } from '@corsight/db/query';
import { users } from '@corsight/db/schemas/user';
import { zValidator } from '@hono/zod-validator';
import { loginSchema, registerSchema } from '@corsight/dto/req/auth';
import { DEFAULT_ROLE } from '@corsight/dto/auth/roles';
import { responseHandler } from '../utils/handler';
import { requireCapability, sessionUserId } from '../middleware/authorize';
import bcryptjs from 'bcryptjs';
import { HTTPException } from 'hono/http-exception';
import { deleteCookie, setCookie } from 'hono/cookie';
import { sign } from 'hono/jwt';

import { config } from '../config';

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
    config.jwtSecret
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
        throw new HTTPException(401, { message: 'Invalid credentials' });
      }

      await issueSession(c, user);
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
    deleteCookie(c, AUTH_COOKIE_NAME, {
      path: '/',
    });

    return responseHandler(async () => {
      return { authenticated: false };
    }, 'Logged out')(c);
  });
