import { PageHelp } from '../domains/dashboard/components/page-help';
import { useRetainedRead } from '../hooks/use-retained-read';
import { useState } from 'react';
import {
  Form,
  Link,
  useLoaderData,
  useOutletContext,
  useNavigation,
} from 'react-router';
import {
  runHistoryQuerySchema,
  type RunHistory,
} from '@well-simulation/dto/res/projection-detail';
import { ApiError } from '@well-simulation/dto/error';
import { client, fetchFn } from '../utils/api';
import { routes } from '../config/routes';
import type { WellheadContext } from './_dashboard.wellheads.$wellheadId';
import { errorState, timeText } from '../domains/twin/presentation';
import { RequestMessage } from '../domains/twin/messages';
import {
  projectionUrl,
  savedResultShouldRevalidate,
} from '../domains/twin/projection-route';

export async function clientLoader({
  params,
  request,
}: {
  params: { wellheadId?: string };
  request: Request;
}) {
  const query = new URL(request.url).searchParams;
  const kind = query.get('kind') ?? 'scenario';
  const offset = query.get('offset') ?? '0';
  const parsed = runHistoryQuerySchema.safeParse({ kind, offset });
  try {
    if (!parsed.success)
      throw new ApiError(parsed.error, 'Invalid history filters', 400);
    const input = parsed.data;
    const response = await fetchFn<RunHistory>(
      client.twin.wellheads[':wellheadId'].runs.$get({
        param: { wellheadId: params.wellheadId ?? '' },
        query: {
          kind: input.kind,
          offset: String(input.offset),
          limit: String(input.limit),
        },
      })
    );
    return {
      ...input,
      history: response.data,
      error: null,
      checkedAt: new Date().toISOString(),
    };
  } catch (error) {
    if (error instanceof ApiError && [401, 403].includes(error.statusCode ?? 0))
      throw error;
    return {
      kind,
      offset: parsed.success ? parsed.data.offset : 0,
      limit: 20,
      history: null,
      error: errorState(error),
      checkedAt: null,
    };
  }
}
// History refresh is explicit so a timed status check cannot reset pagination.
export const shouldRevalidate = savedResultShouldRevalidate;
/** A local refresh action causes one loader revalidation without an API write. */
export function clientAction() {
  return { refreshed: true };
}
export default function SavedRuns() {
  const incoming = useLoaderData<typeof clientLoader>();
  const { detail } = useOutletContext<WellheadContext>();
  const retained = useRetainedRead(
    incoming.history ? incoming : null,
    `${detail.wellhead.wellheadId}:${incoming.kind}:${incoming.offset}`
  );
  const data = retained ? { ...retained, error: incoming.error } : incoming;
  const navigation = useNavigation();
  const [search, setSearch] = useState('');
  const base = `${routes.dashboard.wellhead(detail.wellhead.wellheadId)}/runs`;
  const items =
    data.history?.items.filter((run) =>
      `${run.name ?? 'Forecast'} ${run.runId}`
        .toLowerCase()
        .includes(search.toLowerCase())
    ) ?? [];
  return (
    <div className='twin-stack'>
      <div>
        <h2>Saved runs</h2>
        <p className='twin-muted'>
          Open a previous result and inspect the inputs that produced it.
        </p>
      </div>
      <PageHelp title='About saved results'>
        <p>
          Each result keeps its original inputs. Search filters the loaded page. Choose
          a run type, then use Show runs to switch lists.
        </p>
        <p>
          Other dashboard readers with access can open these results. Older result
          points may expire under the retention policy.
        </p>
      </PageHelp>
      <div className='twin-filter'>
        <Form method='get' className='twin-inline'>
          <div>
            <label htmlFor='run-kind'>Run type</label>
            <select id='run-kind' name='kind' defaultValue={data.kind} key={data.kind}>
              <option value='scenario'>Scenarios</option>
              <option value='forecast'>Forecasts</option>
            </select>
          </div>
          <button className='twin-button' type='submit'>
            Show runs
          </button>
        </Form>
        <div>
          <label htmlFor='run-search'>Search this page</label>
          <input
            id='run-search'
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder='Name or run ID'
          />
        </div>
        <Form method='post'>
          <button
            type='submit'
            className='twin-button'
            disabled={navigation.state !== 'idle'}
          >
            Refresh history
          </button>
        </Form>
      </div>
      <p className='twin-caption' role='status'>
        {navigation.state !== 'idle'
          ? 'Loading saved runs…'
          : data.checkedAt
            ? `History checked ${timeText(data.checkedAt)}. Refresh to find newer runs.`
            : ''}
      </p>
      <p className='sr-only' role='status'>
        {items.length} matching runs on this page.
      </p>
      {data.error && <RequestMessage error={data.error} />}
      {data.error && data.history && (
        <p className='twin-message'>
          History refresh failed. The previous list remains below.
        </p>
      )}
      {data.history && (
        <section className='twin-panel'>
          {items.length ? (
            <div
              className='twin-scroll'
              tabIndex={0}
              role='region'
              aria-label='Saved run history'
            >
              <table className='twin-table'>
                <thead>
                  <tr>
                    <th scope='col'>Run</th>
                    <th scope='col'>Created</th>
                    <th scope='col'>Duration</th>
                    <th scope='col'>Sample interval</th>
                    <th scope='col'>Model</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((run) => (
                    <tr key={run.runId}>
                      <td>
                        <Link to={projectionUrl(run.wellheadId, run.kind, run.runId)}>
                          {run.name ?? 'Forecast'}
                        </Link>
                        <p className='twin-caption'>
                          {run.executionOrigin.kind === 'scheduled' && 'Scheduled · '}
                          {run.branchOrigin.kind === 'snapshot'
                            ? 'Saved snapshot'
                            : 'Current state'}{' '}
                          · {timeText(run.baseSimulatedAt)}
                        </p>
                        <details className='twin-caption'>
                          <summary>Run ID</summary>
                          {run.runId}
                        </details>
                      </td>
                      <td>{timeText(run.createdAt)}</td>
                      <td>{run.horizonSeconds / 60} min</td>
                      <td>{run.stepSeconds}s</td>
                      <td>{run.modelVersion}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className='twin-empty'>
              {search
                ? 'No runs match this search on the loaded page.'
                : 'No saved runs on this page yet.'}
            </p>
          )}
          <div className='twin-panel-body twin-inline'>
            {data.offset > 0 && (
              <Link
                className='twin-button'
                to={`${base}?kind=${data.kind}&offset=${Math.max(0, data.offset - data.limit)}`}
              >
                Previous runs
              </Link>
            )}
            <span className='twin-muted'>
              Page {Math.floor(data.offset / data.limit) + 1}
            </span>
            {data.history?.hasMore &&
              runHistoryQuerySchema.safeParse({
                kind: data.kind,
                offset: data.offset + data.limit,
              }).success && (
                <Link
                  className='twin-button'
                  to={`${base}?kind=${data.kind}&offset=${data.offset + data.limit}`}
                >
                  Next runs
                </Link>
              )}
          </div>
        </section>
      )}
    </div>
  );
}
