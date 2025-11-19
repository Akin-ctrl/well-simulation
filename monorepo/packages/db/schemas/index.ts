import * as _schema from './schema';
import * as relations from './relations';
import * as views from './views';
import * as users from './user';

export const schema = { ..._schema, ...users, ...views, ...relations };
