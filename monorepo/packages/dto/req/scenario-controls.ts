/** Display and persisted-name mapping for the existing five scenario inputs. */
export const SCENARIO_CONTROLS = {
  chokePositionPercent: {
    label: 'Choke opening',
    storedKey: 'choke_position_percent',
    unit: '%',
    numeric: true,
  },
  masterValveOpen: {
    label: 'Master valve',
    storedKey: 'master_valve_open',
    unit: '',
    numeric: false,
  },
  wingValveOpen: {
    label: 'Wing valve',
    storedKey: 'wing_valve_open',
    unit: '',
    numeric: false,
  },
  pumpOn: { label: 'Pump', storedKey: 'pump_on', unit: '', numeric: false },
  emergencyShutdown: {
    label: 'Emergency shutdown',
    storedKey: 'emergency_shutdown',
    unit: '',
    numeric: false,
  },
} as const;
export type ScenarioControlKey = keyof typeof SCENARIO_CONTROLS;
