import { Hono } from 'hono';
import { db } from '@corsight/db/query';
import { vWellheadParameterReadings } from '@corsight/db/schemas/views';
import { users } from '@corsight/db/schemas/user';
import { zValidator } from '@hono/zod-validator';
import { loginSchema, registerSchema } from '@corsight/dto/req/auth';
import { responseHandler } from '../utils/handler';
import bcryptjs from 'bcryptjs';
import { HTTPException } from 'hono/http-exception';

export const authRouter = new Hono()
  .get('/', async (c) => {
    const data = await db.select().from(vWellheadParameterReadings).limit(10);
    return c.json({ message: 'Auth route', data });
  })
  .post(
    '/register',
    zValidator('json', registerSchema.omit({ confirmPassword: true })),
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

        return { user: user?.[0]! };
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

      const isPasswordValid = await bcryptjs.compare(
        payload.password,
        user.encryptedPassword!
      );

      if (!isPasswordValid) {
        throw new HTTPException(401, { message: 'Invalid credentials' });
      }
      return { user };
    })(c);
  });
