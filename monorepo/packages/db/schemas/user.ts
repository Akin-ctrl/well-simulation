import {
  boolean,
  pgTable,
  serial,
  text,
  timestamp,
  varchar,
} from 'drizzle-orm/pg-core';
import { statusEnum, userRoleEnum } from './enums';

export const users = pgTable('users', {
  id: serial('id').primaryKey(),
  email: varchar('email', {
    length: 256,
  })
    .notNull()
    .unique(),
  firstName: varchar('first_name', {
    length: 256,
  }),
  lastName: varchar('last_name', {
    length: 256,
  }),
  userName: varchar('user_name', {
    length: 256,
  })
    .notNull()
    .unique(),

  phoneNumber: varchar('phone_number', { length: 256 }).unique(),
  bio: text('bio'),
  image: varchar('image', {
    length: 256,
  }),
  role: userRoleEnum('role'),
  createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' })
    .notNull()
    .defaultNow(),
  status: statusEnum('status').default('ACTIVE'),
  deleted: boolean('deleted'),
  deletedAt: timestamp('deleted_at', { withTimezone: true, mode: 'date' }),
  encryptedPassword: varchar('encrypted_password', { length: 256 }).notNull(),
  lastSignedIn: timestamp('last_signed_in', {
    withTimezone: true,
    mode: 'date',
  }),
  updatedAt: timestamp('updated_at', {
    withTimezone: true,
    mode: 'date',
  })
    .defaultNow()
    .$onUpdateFn(() => new Date()),
});
