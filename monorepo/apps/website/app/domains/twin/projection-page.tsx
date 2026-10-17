import { PageHelp } from '../dashboard/components/page-help';
import { Link, useLoaderData, useOutletContext } from 'react-router';
import type { WellheadContext } from '../../routes/_dashboard.wellheads.$wellheadId';
import { routes } from '../../config/routes';
import { ProjectionForm } from './projection-form';
import { ProjectionResult } from './projection-result';
import { RequestMessage } from './messages';
import type { ProjectionKind, ResultData } from './projection-route';

export function ProjectionPage({ kind }: { kind: ProjectionKind }) {
  const { detail } = useOutletContext<WellheadContext>();
  const { run, error } = useLoaderData<ResultData>();
  const wellheadId = detail.wellhead.wellheadId;
  return (
    <div className='twin-stack'>
      <div>
        <h2>
          {kind === 'forecast'
            ? 'Forecast with current controls'
            : 'Compare a proposed change'}
        </h2>
        <p className='twin-muted'>
          {kind === 'forecast'
            ? 'See how this well may behave if its current controls stay the same.'
            : 'Try a control schedule on a copy of this well. The running well keeps its controls.'}
        </p>
      </div>
      <PageHelp
        title={kind === 'forecast' ? 'How forecasts work' : 'Try your first scenario'}
      >
        {kind === 'forecast' ? (
          <p>
            The model copies the current well state, holds its controls constant, and
            calculates ahead for your chosen duration. Each run saves a separate result.
          </p>
        ) : (
          <>
            <ol>
              <li>Keep the 30-minute duration and 60-second chart interval.</li>
              <li>Name the scenario and set the choke to 80% at minute 5.</li>
              <li>
                Choose Run scenario, then compare tubing pressure and flow with the
                unchanged baseline.
              </li>
            </ol>
            <p>
              If the choke already starts at 80%, the lines may overlap. Try a different
              target to explore the response.
            </p>
          </>
        )}
        <p>
          A new run starts from the current well state. Editing the draft does not
          change a saved result.
        </p>
      </PageHelp>
      <div className='twin-analysis'>
        <ProjectionForm
          key={`${wellheadId}-${kind}`}
          kind={kind}
          wellheadId={wellheadId}
        />
        <div className='min-w-0'>
          {error ? (
            <RequestMessage error={error} />
          ) : run ? (
            <ProjectionResult key={run.runId} run={run} />
          ) : (
            <section className='twin-panel twin-empty'>
              <h2>Your result will appear here</h2>
              <p className='mt-2'>
                Choose inputs and run the model, or open a previous result from Saved
                runs.
              </p>
              <Link
                className='twin-button mt-5'
                to={`${routes.dashboard.wellhead(wellheadId)}/runs?kind=${kind}`}
              >
                Browse saved runs
              </Link>
            </section>
          )}
        </div>
      </div>
    </div>
  );
}
