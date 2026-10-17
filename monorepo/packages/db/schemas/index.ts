import * as _schema from './schema';
import * as relations from './relations';
import * as views from './views';
import * as users from './user';
import * as audit from './audit';
import * as controls from './controls';
import * as alarms from './alarms';
import * as schedules from './forecast-schedules';

export const schema = {
  ..._schema,
  ...users,
  ...audit,
  ...controls,
  ...alarms,
  ...schedules,
  ...views,
  ...relations,
};
