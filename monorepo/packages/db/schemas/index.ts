import * as _schema from './schema';
import * as relations from './relations';
import * as views from './views';

export const schema = { ..._schema, ...views, ...relations };
