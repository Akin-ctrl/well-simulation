import { Link } from 'react-router';
import { routes } from '../config/routes';
import { PageHelp } from '../domains/dashboard/components/page-help';

/** A task guide for the available dashboard features, with no automatic writes. */
export default function Guide() {
  return (
    <div className='twin-workspace twin-stack twin-guide'>
      <header className='twin-heading'>
        <div>
          <h1>Guide</h1>
          <p className='twin-muted'>
            Learn the workflow, then return here when a term or result needs an
            explanation.
          </p>
        </div>
        <Link className='twin-button twin-primary' to={routes.dashboard.overview}>
          Choose a well
        </Link>
      </header>
      <section className='twin-panel'>
        <div className='twin-panel-heading'>
          <h2>Start with one well</h2>
        </div>
        <ol className='twin-walkthrough twin-panel-body'>
          <li>
            <h3>Check the current readings</h3>
            <p>
              Open a well from Overview. Check its data age, pressures, production and
              active alarms. Stale readings describe an earlier state.
            </p>
          </li>
          <li>
            <h3>Run a forecast</h3>
            <p>
              Open Forecast. Keep the 30-minute duration and 60-second chart interval,
              then choose Run forecast. The model copies the well state and keeps its
              controls unchanged.
            </p>
          </li>
          <li>
            <h3>Try a scenario</h3>
            <p>
              Open Scenarios. Name the run, keep the 30-minute duration, and set the
              choke to 80% at minute 5. Choose Run scenario. This changes the model
              copy; the running well keeps its controls.
            </p>
            <p>
              To review an earlier state, choose Saved snapshot, search by time, and
              select a compatible snapshot. The scenario clock starts at that time.
              Historical runs use alarm rules active when you run them; chart retention
              follows their simulated dates.
            </p>
          </li>
          <li>
            <h3>Compare the result</h3>
            <p>
              Choose tubing pressure, then flow rate. The dashed baseline keeps the
              original controls. The solid line uses your proposed changes. If the choke
              already starts at 80%, the lines may overlap.
            </p>
          </li>
          <li>
            <h3>Return to a saved run</h3>
            <p>
              Open Saved runs, choose Forecasts or Scenarios, and select a result.
              Expand Original run inputs to check the controls it used. A later draft
              does not edit that result.
            </p>
          </li>
        </ol>
      </section>
      <div className='twin-grid'>
        <section className='twin-panel twin-panel-body'>
          <h2>Choose the right view</h2>
          <dl className='twin-glossary'>
            <dt>Overview</dt>
            <dd>Find a well and compare the latest available readings.</dd>
            <dt>Monitoring</dt>
            <dd>Inspect one well’s readings, alarms, controls and recorded trends.</dd>
            <dt>Forecast</dt>
            <dd>Calculate what may happen with the current controls unchanged.</dd>
            <dt>Scenarios</dt>
            <dd>Compare a proposed control schedule against unchanged controls.</dd>
            <dt>Saved runs</dt>
            <dd>Reopen a previous calculation and its original inputs.</dd>
            <dt>Controls (operators)</dt>
            <dd>
              Save a proposal, review it, then activate a change to the running model.
              Check its status and the running revision.
            </dd>
            <dt>Parameters (administrators)</dt>
            <dd>
              Inspect model inputs and publish overrides. Existing models adopt them
              after a service restart.
            </dd>
            <dt>Alarms</dt>
            <dd>Review active threshold breaches across wells.</dd>
            <dt>Analytics</dt>
            <dd>Compare fleet averages. An average can hide a change at one well.</dd>
          </dl>
        </section>
        <section className='twin-panel twin-panel-body'>
          <h2>Terms in the dashboard</h2>
          <dl className='twin-glossary'>
            <dt>Baseline</dt>
            <dd>
              The copied well with its original controls, calculated over the same
              period as your scenario.
            </dd>
            <dt>Duration</dt>
            <dd>
              How far ahead the model calculates, measured from the copied starting
              state.
            </dd>
            <dt>Chart interval</dt>
            <dd>
              The time between saved result points. Shorter intervals show more detail.
            </dd>
            <dt>Choke position</dt>
            <dd>
              The percentage opening of the flow restriction. A higher value means a
              wider opening.
            </dd>
            <dt>Water cut</dt>
            <dd>
              The fraction of liquid production that is water, expressed as a
              percentage.
            </dd>
            <dt>Gas–oil ratio</dt>
            <dd>
              Gas volume per unit of oil volume, using the unit shown beside the value.
            </dd>
            <dt>Confidence</dt>
            <dd>
              A model quality indicator based on fixed rules. It is not the probability
              that a forecast will come true.
            </dd>
          </dl>
        </section>
      </div>
      <PageHelp title='What to do when data or a run is unavailable'>
        <p>
          Check timestamps first. A failed refresh keeps the last successful readings
          visible and marks them unverified. Use Try again to request fresh data.
        </p>
        <p>
          If a calculation times out, check Saved runs before submitting again. The
          server may have saved the result even though the response did not reach your
          browser.
        </p>
        <p>
          Sign in again after an expired session. Ask an administrator about access
          errors. Older result points may expire under the retention policy.
        </p>
      </PageHelp>
      <PageHelp title='How to interpret model results'>
        <p>
          The model represents a synthetic reference well. It has no field calibration.
          Use its results to explore behaviour and compare assumptions.
        </p>
        <p>
          Projected thresholds cover all returned signals, not only the selected chart.
          A threshold crossing between saved points can go undetected.
        </p>
        <p>
          A missing value breaks the chart line. Zero remains a value. Chart timestamps
          use your browser’s timezone.
        </p>
      </PageHelp>
    </div>
  );
}
