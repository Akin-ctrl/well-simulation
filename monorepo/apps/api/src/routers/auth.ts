import { Hono, type Context } from 'hono';
import { db } from '@corsight/db/query';
import { users } from '@corsight/db/schemas/user';
import { zValidator } from '@hono/zod-validator';
import { loginSchema, registerSchema } from '@corsight/dto/req/auth';
import { responseHandler } from '../utils/handler';
import bcryptjs from 'bcryptjs';
import { HTTPException } from 'hono/http-exception';
import { deleteCookie, setCookie } from 'hono/cookie';
import { sign } from 'hono/jwt';

const AUTH_COOKIE_NAME = 'auth_token';
const SESSION_MAX_AGE_SECONDS = 60 * 60 * 24;

type UserRecord = typeof users.$inferSelect;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function requireJwtSecret() {
  const secret = process.env.JWT_SECRET;
  if (!secret) {
    throw new HTTPException(500, { message: 'Authentication is not configured' });
  }
  return secret;
}

function publicUser(user: UserRecord) {
  const { encryptedPassword, ...safeUser } = user;
  return safeUser;
}

function sessionUserId(c: Context) {
  const session = c.get('session') as unknown;
  if (!isRecord(session) || typeof session.sub !== 'string') {
    throw new HTTPException(401, { message: 'Unauthorized' });
  }

  const userId = Number.parseInt(session.sub, 10);
  if (!Number.isInteger(userId)) {
    throw new HTTPException(401, { message: 'Unauthorized' });
  }

  return userId;
}

async function issueSession(c: Context, user: UserRecord) {
  const expiresAtSeconds = Math.floor(Date.now() / 1000) + SESSION_MAX_AGE_SECONDS;
  const token = await sign(
    {
      sub: String(user.id),
      email: user.email,
      role: user.role ?? 'USER',
      exp: expiresAtSeconds,
    },
    requireJwtSecret()
  );

  setCookie(c, AUTH_COOKIE_NAME, token, {
    httpOnly: true,
    sameSite: 'Lax',
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    maxAge: SESSION_MAX_AGE_SECONDS,
  });
}

export const authRouter = new Hono()
  .post(
    '/register',
    zValidator('json', registerSchema),
    (c) => {
      const payload = c.req.valid('json');

      return responseHandler(async () => {
        const encryptedPassword = await bcryptjs.hash(payload.password, 11);

        const user = await db
          .insert(users)
          .values({
            firstName: payload.firstName,
            lastName: payload.lastName,
            userName: payload.username,
            email: payload.email,
            encryptedPassword: encryptedPassword,
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

      const isPasswordValid = await bcryptjs.compare(payload.password, encryptedPassword);

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
