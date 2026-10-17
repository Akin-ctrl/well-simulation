import { PageHelp } from '../domains/dashboard/components/page-help';
import { useOutletContext } from 'react-router';
import { booleanStateLabel } from '@well-simulation/dto/res/dashboard';
import { SCENARIO_CONTROLS } from '@well-simulation/dto/req/scenario-controls';
import type { WellheadContext } from './_dashboard.wellheads.$wellheadId';
import { ModelPanel } from '../domains/twin/model-status';
import {
  abnormal,
  controlText,
  signalText,
  timeText,
} from '../domains/twin/presentation';
import { TrendChart } from '../domains/dashboard/components/trend-chart';

const GROUPS = [
  {
    name: 'Pressures',
    codes: ['tubing_pressure', 'casing_pressure', 'annulus_pressure'],
  },
  {
    name: 'Production',
    codes: ['flow_rate', 'wellhead_temperature', 'water_cut', 'gas_oil_ratio'],
  },
  {
    name: 'Condition indicators',
    codes: ['sand_detector', 'corrosion_rate', 'h2s_level', 'co2_level', 'vibration'],
  },
  {
    name: 'Equipment readings',
    codes: [
      'choke_valve_position',
      'master_valve_status',
      'wing_valve_status',
      'swab_valve_status',
      'pump_status',
      'emergency_shutdown',
    ],
  },
];

export default function Monitoring() {
  const {
    detail: data,
    fleet,
    controls,
    unverified,
  } = useOutletContext<WellheadContext>();
  return (
    <div className='twin-stack'>
      <PageHelp title='How to read this page'>
        <p>
          Check the data age above before comparing values. These readings come from the
          latest recorded batch. Missing readings say Unavailable; zero remains a value.
        </p>
        <p>
          Out of range compares a value with its configured normal range. An active
          alarm uses its own rule and requires consecutive breaching readings.
        </p>
        <p>
          Forecast keeps the controls unchanged. Scenarios lets you compare proposed
          changes on a model copy.
        </p>
      </PageHelp>
      <div>
        <h2>Current parameters</h2>
        <p className='twin-muted'>
          Latest recorded readings · {timeText(data.latestReadingAt)}
        </p>
      </div>
      {!data.latestReadings.length && (
        <p className='twin-empty'>No readings have arrived for this wellhead.</p>
      )}
      <div className='twin-grid'>
        {GROUPS.map((group) => (
          <section className='twin-panel' key={group.name}>
            <div className='twin-panel-heading'>
              <h2>{group.name}</h2>
            </div>
            <div className='twin-panel-body'>
              <dl className='twin-facts'>
                {group.codes.map((code) => {
                  const reading = data.latestReadings.find(
                    (item) => item.parameterCode === code
                  );
                  return (
                    <div key={code}>
                      <dt>
                        {reading?.parameterDisplayName ?? code.replaceAll('_', ' ')}
                        {reading && abnormal(reading) && (
                          <span className='block text-amber-800'>Out of range</span>
                        )}
                      </dt>
                      <dd>
                        {signalText(code, reading?.rawValue, reading?.canonicalUnit)}
                        {reading &&
                          !booleanStateLabel(code, reading.rawValue) &&
                          reading.normalMin != null &&
                          reading.normalMax != null && (
                            <span className='block twin-caption'>
                              Range {reading.normalMin}–{reading.normalMax}{' '}
                              {reading.canonicalUnit}
                            </span>
                          )}
                      </dd>
                    </div>
                  );
                })}
              </dl>
            </div>
          </section>
        ))}
      </div>
      <section className='twin-panel'>
        <div className='twin-panel-heading'>
          <h2>Active alarms</h2>
          <span className='twin-muted'>{data.activeAlarms.length} shown</span>
        </div>
        <div className='twin-panel-body'>
          {data.activeAlarms.length ? (
            <ul className='space-y-3'>
              {data.activeAlarms.map((alarm) => (
                <li key={alarm.eventId}>
                  <strong>
                    {alarm.severityLevel}: {alarm.parameterDisplayName}
                  </strong>
                  <p className='twin-muted'>
                    {signalText(
                      alarm.parameterCode ?? '',
                      alarm.triggeredValue,
                      alarm.canonicalUnit
                    )}{' '}
                    · {timeText(alarm.triggeredAt)}
                  </p>
                </li>
              ))}
            </ul>
          ) : (
            <p className='twin-muted'>No active alarms for this wellhead.</p>
          )}
        </div>
      </section>
      <div className='twin-grid'>
        <ModelPanel
          fleet={fleet}
          wellheadId={data.wellhead.wellheadId}
          unverified={unverified}
        />
        <section className='twin-panel'>
          <div className='twin-panel-heading'>
            <h2>Installed controls</h2>
            <span className='twin-muted'>Read only</span>
          </div>
          <div className='twin-panel-body'>
            {controls ? (
              <>
                <dl className='twin-facts'>
                  {Object.entries(SCENARIO_CONTROLS).map(([key, info]) => (
                    <div key={key}>
                      <dt>{info.label}</dt>
                      <dd>
                        {controlText(
                          key,
                          controls.running.controls[
                            key as keyof typeof SCENARIO_CONTROLS
                          ]
                        )}
                      </dd>
                    </div>
                  ))}
                  <div>
                    <dt>Swab valve</dt>
                    <dd>
                      {controls.running.controls.swabValveOpen ? 'Open' : 'Closed'}
                    </dd>
                  </div>
                </dl>
                <p className='twin-caption'>
                  Revision {controls.running.revision}
                  {controls.pendingCommandId ? ' · A command is pending' : ''}
                </p>
              </>
            ) : (
              <p className='twin-muted'>Installed controls are unavailable.</p>
            )}
          </div>
        </section>
      </div>
      <div className='twin-grid'>
        {[
          {
            title: 'Pressure trends',
            points: data.pressureTrend,
            codes: ['tubing_pressure', 'casing_pressure', 'annulus_pressure'],
          },
          {
            title: 'Temperature and flow',
            points: data.temperatureFlowTrend,
            codes: ['wellhead_temperature', 'flow_rate'],
          },
          {
            title: 'Water cut and gas–oil ratio',
            points: data.waterCutGorTrend,
            codes: ['water_cut', 'gas_oil_ratio'],
          },
        ].map((chart) => (
          <section className='twin-panel' key={chart.title}>
            <div className='twin-panel-heading'>
              <h2>{chart.title}</h2>
            </div>
            <div className='twin-panel-body'>
              <TrendChart
                points={chart.points}
                metricCodes={chart.codes}
                emptyMessage='No trend data available yet.'
              />
            </div>
          </section>
        ))}
      </div>
    </div>
  );
}
