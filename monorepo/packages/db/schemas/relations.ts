import { relations } from 'drizzle-orm/relations';
import {
  field,
  location,
  device,
  wellhead,
  deviceparametermapping,
  parametertype,
  alarmrule,
  parameterreading,
  alarmevent,
} from './schema';

export const locationRelations = relations(location, ({ one, many }) => ({
  field: one(field, {
    fields: [location.fieldId],
    references: [field.fieldId],
  }),
  wellheads: many(wellhead),
}));

export const fieldRelations = relations(field, ({ many }) => ({
  locations: many(location),
}));

export const wellheadRelations = relations(wellhead, ({ one, many }) => ({
  device: one(device, {
    fields: [wellhead.deviceId],
    references: [device.deviceId],
  }),
  location: one(location, {
    fields: [wellhead.locationId],
    references: [location.locationId],
  }),
  parameterreadings: many(parameterreading),
  alarmevents: many(alarmevent),
}));

export const deviceRelations = relations(device, ({ many }) => ({
  wellheads: many(wellhead),
  deviceparametermappings: many(deviceparametermapping),
}));

export const deviceparametermappingRelations = relations(
  deviceparametermapping,
  ({ one, many }) => ({
    device: one(device, {
      fields: [deviceparametermapping.deviceId],
      references: [device.deviceId],
    }),
    parametertype: one(parametertype, {
      fields: [deviceparametermapping.parameterTypeId],
      references: [parametertype.parameterTypeId],
    }),
    parameterreadings: many(parameterreading),
  })
);

export const parametertypeRelations = relations(parametertype, ({ many }) => ({
  deviceparametermappings: many(deviceparametermapping),
  alarmrules: many(alarmrule),
  parameterreadings: many(parameterreading),
}));

export const alarmruleRelations = relations(alarmrule, ({ one, many }) => ({
  parametertype: one(parametertype, {
    fields: [alarmrule.parameterTypeId],
    references: [parametertype.parameterTypeId],
  }),
  alarmevents: many(alarmevent),
}));

export const parameterreadingRelations = relations(
  parameterreading,
  ({ one }) => ({
    deviceparametermapping: one(deviceparametermapping, {
      fields: [parameterreading.mappingId],
      references: [deviceparametermapping.mappingId],
    }),
    parametertype: one(parametertype, {
      fields: [parameterreading.parameterTypeId],
      references: [parametertype.parameterTypeId],
    }),
    wellhead: one(wellhead, {
      fields: [parameterreading.wellheadId],
      references: [wellhead.wellheadId],
    }),
  })
);

export const alarmeventRelations = relations(alarmevent, ({ one }) => ({
  alarmrule: one(alarmrule, {
    fields: [alarmevent.alarmRuleId],
    references: [alarmrule.alarmRuleId],
  }),
  wellhead: one(wellhead, {
    fields: [alarmevent.wellheadId],
    references: [wellhead.wellheadId],
  }),
}));
