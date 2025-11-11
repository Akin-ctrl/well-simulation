import { pgEnum } from 'drizzle-orm/pg-core';

export const statusEnum = pgEnum('status', ['ACTIVE', 'DISABLED']);

export const userRoleEnum = pgEnum('role', ['USER', 'ADMIN', 'OPERATIONS']);
