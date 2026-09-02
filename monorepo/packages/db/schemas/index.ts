import * as _schema from './schema';
import * as relations from './relations';
import * as views from './views';
import * as users from './user';
import * as audit from './audit';

export const schema = { ..._schema, ...users, ...audit, ...views, ...relations };
