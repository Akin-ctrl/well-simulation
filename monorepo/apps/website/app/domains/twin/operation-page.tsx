import { useEffect } from 'react';
import { ProposalEditor } from './operation-editor';
import {
  Link,
  useFetcher,
  useLoaderData,
  useOutletContext,
  useRouteLoaderData,
  useSearchParams,
} from 'react-router';
import { hasCapability, DEFAULT_ROLE } from '@well-simulation/dto/auth/roles';

import {
  SCENARIO_CONTROLS,
  type ScenarioControlKey,
} from '@well-simulation/dto/req/scenario-controls';
import type { ParameterWorkspace } from '@well-simulation/dto/res/controls';
import type { CurrentUserData } from '../../routes/_dashboard';
import type { WellheadContext } from '../../routes/_dashboard.wellheads.$wellheadId';
import { useRetainedRead } from '../../hooks/use-retained-read';
import { useElapsedSeconds } from '../../hooks/use-elapsed-seconds';
import { PageHelp } from '../dashboard/components/page-help';
import { ReadStatus } from '../dashboard/components/read-status';
import { ReadFailure } from './read-failure';
import { controlText, numberText, timeText } from './presentation';
import {
  operationAction,
  operationUrl,
  proposalId,
  type OperationKind,
  type OperationRead,
  type Proposal,
} from './operation-route';

type ActionData = Exclude<
  Awaited<ReturnType<ReturnType<typeof operationAction>>>,
  Response
>;
const keys = Object.keys(SCENARIO_CONTROLS) as ScenarioControlKey[];

export function OperationPage({ kind }: { kind: OperationKind }) {
  const incoming = useLoaderData<OperationRead>();
  const well = useOutletContext<WellheadContext>();
  const [search] = useSearchParams();
  const id = search.get('proposal') ?? '';
  const retained = useRetainedRead(
    incoming.value,
    `${well.detail.wellhead.wellheadId}:${kind}:${id}:${search.get('offset') ?? '0'}`
  );
  if (!retained) return incoming.error ? <ReadFailure error={incoming.error} /> : null;
  return (
    <LoadedOperation
      key={`${kind}:${well.detail.wellhead.wellheadId}:${id}`}
      kind={kind}
      read={retained}
      error={incoming.error}
      well={well}
    />
  );
}

function LoadedOperation({
  kind,
  read,
  error,
  well,
}: {
  kind: OperationKind;
  read: NonNullable<OperationRead['value']>;
  error: OperationRead['error'];
  well: WellheadContext;
}) {
  const user = useRouteLoaderData<CurrentUserData>('routes/_dashboard')?.user;
  const fetcher = useFetcher<ActionData>();
  const busy = fetcher.state !== 'idle';
  const outcome = fetcher.data;
  const elapsed = useElapsedSeconds(read.checkedAt);
  const data = read.data;
  const retainedParameters = useRetainedRead(
    data.parameters?.value ?? null,
    `${well.detail.wellhead.wellheadId}:${kind}`
  );
  const parameterData = retainedParameters?.data ?? null;
  const proposal = data.proposal;
  const selectedId = proposal ? proposalId(proposal) : null;
  const wellId = well.detail.wellhead.wellheadId;
  const permitted = hasCapability(
    user?.role ?? DEFAULT_ROLE,
    kind === 'control' ? 'control:issue' : 'asset:manage'
  );
  const verified =
    !error &&
    !well.unverified &&
    elapsed <= 20 &&
    (kind === 'control'
      ? well.controls?.running.status === 'running'
      : Boolean(parameterData) && !data.parameters?.error);
  const obsolete =
    proposal?.status === 'draft' &&
    ('commandId' in proposal
      ? Boolean(
          well.controls &&
            (proposal.expectedRevision !== well.controls.running.revision ||
              proposal.modelVersion !== well.controls.running.modelVersion)
        )
      : Boolean(
          parameterData &&
            (proposal.modelVersion !== parameterData.modelVersion ||
              proposal.expectedOverrideId !==
                (parameterData.latestOverrides.find(
                  (item) => item.parameterKey === proposal.parameterKey
                )?.parameterOverrideId ?? null))
        ));
  useEffect(() => {
    if (outcome?.error && !busy) document.getElementById('operation-outcome')?.focus();
    else if (outcome && !busy && selectedId) {
      const heading = document.getElementById('proposal-heading');
      heading?.focus({ preventScroll: true });
      heading?.scrollIntoView({ block: 'nearest' });
    }
  }, [outcome, busy, selectedId]);
  if (!permitted)
    return (
      <p className='twin-message'>
        Your account needs {kind === 'control' ? 'operating' : 'asset management'}{' '}
        permission for this view.
      </p>
    );
  const checkOutcome = () =>
    fetcher.submit(
      { intent: 'check', id: selectedId ?? '', key: outcome?.key ?? '' },
      { method: 'post' }
    );
  return (
    <div className='twin-stack'>
      <div>
        <h2>
          {kind === 'control' ? 'Change running controls' : 'Manage model parameters'}
        </h2>
        <p className='twin-muted'>
          {kind === 'control'
            ? 'Propose a change to this running well, then review it before activation.'
            : 'Inspect model inputs and publish a change for the next service restart.'}
        </p>
      </div>
      <PageHelp title='How proposals work'>
        <ol>
          <li>Choose a target and explain why you want to change it.</li>
          <li>Save a proposal and review the well, values and reason.</li>
          <li>
            {kind === 'control'
              ? 'Confirm and activate it. Check the command status and running revision.'
              : 'Confirm and publish it. Existing models adopt it after a service restart.'}
          </li>
        </ol>
        <p>
          Saving a draft does not change the running well. If a response goes missing,
          check Your proposals before trying again.
        </p>
      </PageHelp>
      <ReadStatus checkedAt={read.checkedAt} intervalMs={10000} />
      {error && <ReadFailure error={error} retained />}
      {data.parameters?.error && (
        <ReadFailure error={data.parameters.error} retained={Boolean(parameterData)} />
      )}
      {!verified && (
        <p className='twin-message' role='status'>
          We cannot verify the state for this change. Refresh before submitting. You can
          still read your proposal history.
        </p>
      )}
      {outcome?.error && (
        <section
          id='operation-outcome'
          tabIndex={-1}
          className='twin-message'
          role='alert'
        >
          <p>{outcome.error.message}</p>
          {outcome.uncertain && (
            <>
              <p>
                Check the recorded outcome before another submission. The browser will
                not repeat this write.
              </p>
              <button className='twin-button' disabled={busy} onClick={checkOutcome}>
                Check outcome
              </button>
            </>
          )}
        </section>
      )}
      {kind === 'control' && (
        <section className='twin-panel'>
          <div className='twin-panel-heading'>
            <h2>Installed controls</h2>
          </div>
          <div className='twin-panel-body twin-stack'>
            {well.controls ? (
              <>
                <dl className='twin-facts'>
                  {keys.map((key) => (
                    <div key={key}>
                      <dt>{SCENARIO_CONTROLS[key].label}</dt>
                      <dd>
                        {controlText(
                          SCENARIO_CONTROLS[key].storedKey,
                          well.controls!.running.controls[key]
                        )}
                      </dd>
                    </div>
                  ))}
                </dl>
                <p>
                  Durable revision: {well.controls.active?.revision ?? 'Unavailable'}.
                  Running revision: {well.controls.running.revision}.
                </p>
                <p>
                  Last successful tick: {timeText(well.controls.running.lastTickAt)}.
                </p>
                {well.controls.pendingCommandId && (
                  <p className='twin-message'>A command is queued for this well.</p>
                )}
              </>
            ) : (
              <p>Running controls are unavailable.</p>
            )}
          </div>
        </section>
      )}
      {proposal ? (
        <section className='twin-panel'>
          <div className='twin-panel-heading'>
            <h2 id='proposal-heading' tabIndex={-1}>
              Review proposal
            </h2>
            <span className='twin-badge'>{proposal.status}</span>
          </div>
          <div className='twin-panel-body twin-stack'>
            <ProposalValues proposal={proposal} parameters={parameterData} />
            <p>
              <strong>Reason:</strong> {proposal.reason}
            </p>
            <p>
              Created: {timeText(proposal.createdAt)}. Expires:{' '}
              {timeText(proposal.expiresAt)}.
            </p>
            <p role='status'>{proposalMessage(proposal)}</p>
            {obsolete && (
              <p className='twin-message'>
                This proposal uses an earlier revision. Cancel it and create a new
                proposal after checking the current values.
              </p>
            )}
            {'events' in proposal && proposal.events && (
              <details>
                <summary>Command history</summary>
                <ol>
                  {proposal.events.map((event, index) => (
                    <li key={index}>
                      {timeText(event.occurredAt)} · {event.eventType} ·{' '}
                      {event.reasonCode}
                    </li>
                  ))}
                </ol>
              </details>
            )}
            {(proposal.status === 'draft' ||
              (kind === 'control' && proposal.status === 'queued')) &&
              proposal.createdBy === user?.id && (
                <fetcher.Form method='post' className='twin-stack'>
                  <input type='hidden' name='id' value={proposalId(proposal)} />
                  <input
                    type='hidden'
                    name='revision'
                    value={
                      'expectedRevision' in proposal ? proposal.expectedRevision : ''
                    }
                  />
                  <label className='twin-inline'>
                    <input
                      type='checkbox'
                      name='confirm'
                      value='yes'
                      required
                      disabled={busy}
                    />{' '}
                    I checked this well, target and reason
                  </label>
                  <div className='twin-inline'>
                    {proposal.status === 'draft' ? (
                      <button
                        className='twin-button twin-primary'
                        name='intent'
                        value={kind === 'control' ? 'activate' : 'publish'}
                        disabled={!verified || obsolete || busy || outcome?.uncertain}
                      >
                        {busy
                          ? 'Submitting…'
                          : kind === 'control'
                            ? 'Activate command'
                            : 'Publish parameter'}
                      </button>
                    ) : (
                      <p>
                        Cancellation can lose a race with application. Check the
                        returned status.
                      </p>
                    )}
                    <button
                      className='twin-button'
                      name='intent'
                      value='cancel'
                      disabled={busy || outcome?.uncertain}
                    >
                      Cancel proposal
                    </button>
                  </div>
                </fetcher.Form>
              )}
            <details>
              <summary>Proposal identifier</summary>
              <code className='break-all'>{proposalId(proposal)}</code>
            </details>
            <Link className='twin-button' to={operationUrl(wellId, kind)}>
              New proposal
            </Link>
          </div>
        </section>
      ) : (
        (kind === 'control' || parameterData) && (
          <ProposalEditor
            kind={kind}
            well={well}
            parameters={parameterData}
            disabled={!verified || busy || Boolean(outcome?.uncertain)}
            submit={(payload, key) =>
              fetcher.submit(
                { intent: 'draft', payload: JSON.stringify(payload), key },
                { method: 'post' }
              )
            }
          />
        )
      )}
      <section className='twin-panel'>
        <div className='twin-panel-heading'>
          <h2>Your proposals</h2>
        </div>
        <div className='twin-panel-body twin-stack'>
          {!data.proposals.items.length && <p>You have no proposals on this page.</p>}
          {data.proposals.items.map((item) => (
            <article key={proposalId(item)} className='twin-history-item'>
              <h3>{item.reason}</h3>
              <p>
                {timeText(item.createdAt)} · {item.status}
              </p>
              <Link
                className='twin-button'
                to={operationUrl(wellId, kind, proposalId(item), data.proposals.offset)}
              >
                Open proposal
              </Link>
            </article>
          ))}
          <nav aria-label='Proposal history pages' className='twin-inline'>
            {data.proposals.offset > 0 && (
              <Link
                className='twin-button'
                to={operationUrl(
                  wellId,
                  kind,
                  undefined,
                  Math.max(0, data.proposals.offset - 20)
                )}
              >
                Newer proposals
              </Link>
            )}
            {data.proposals.hasMore && data.proposals.offset < 10000 && (
              <Link
                className='twin-button'
                to={operationUrl(wellId, kind, undefined, data.proposals.offset + 20)}
              >
                Older proposals
              </Link>
            )}
          </nav>
        </div>
      </section>
    </div>
  );
}

function ProposalValues({
  proposal,
  parameters,
}: {
  proposal: Proposal;
  parameters: ParameterWorkspace | null;
}) {
  if ('commandId' in proposal)
    return (
      <>
        <dl className='twin-facts'>
          {keys
            .filter((key) => proposal[key] !== null)
            .map((key) => (
              <div key={key}>
                <dt>{SCENARIO_CONTROLS[key].label}</dt>
                <dd>{controlText(SCENARIO_CONTROLS[key].storedKey, proposal[key]!)}</dd>
              </div>
            ))}
        </dl>
        <p>Expected control revision: {proposal.expectedRevision}.</p>
      </>
    );
  const spec = parameters?.catalogue.parameters.find(
    (item) => item.key === proposal.parameterKey
  );
  const latest = parameters?.latestOverrides.find(
    (item) => item.parameterKey === proposal.parameterKey
  );
  return (
    <>
      <p>
        {spec?.description ?? proposal.parameterKey}:{' '}
        {proposal.value === null
          ? 'Remove the database override'
          : `${numberText(proposal.value)} ${proposal.unit}`}
      </p>
      <p>
        Expected override: {proposal.expectedOverrideId ?? 'None'}. Model version:{' '}
        {proposal.modelVersion}.
      </p>
      <p>
        Running value now: {numberText(spec?.runningValue)} {spec?.unit}. Latest
        published override:{' '}
        {latest?.value == null ? 'None' : `${numberText(latest.value)} ${latest.unit}`}.
      </p>
      <p>
        Existing models need a service restart to adopt published changes. Removing an
        override restores environment/default resolution.
      </p>
    </>
  );
}

function proposalMessage(proposal: Proposal): string {
  if (proposal.status === 'draft') return 'Draft. The running model has not changed.';
  if (proposal.status === 'queued') return 'Queued. Waiting for the model worker.';
  if (proposal.status === 'published')
    return 'Published. Compare the running value after a service restart.';
  if (proposal.status === 'applied' && 'resultRevision' in proposal)
    return `The database committed revision ${proposal.resultRevision}. Check the running revision and tick time above for adoption.`;
  if (proposal.status === 'rejected' && 'resultReason' in proposal)
    return `The worker rejected this command (${proposal.resultReason ?? 'reason unavailable'}). Review the current state before creating another proposal.`;
  return `This proposal is ${proposal.status}. Create a new proposal if you still need the change.`;
}
