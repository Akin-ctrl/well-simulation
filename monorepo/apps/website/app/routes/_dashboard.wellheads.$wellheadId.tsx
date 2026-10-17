import { useRetainedRead } from '../hooks/use-retained-read';
import { ReadFailure } from '../domains/twin/read-failure';
import { errorState } from '../domains/twin/presentation';
import {
  Link,
  NavLink,
  Outlet,
  useLoaderData,
  useRevalidator,
  useParams,
} from 'react-router';
import { ApiError } from '@well-simulation/dto/error';
import type { WellheadDetailResponse } from '@well-simulation/dto/res/dashboard';
import type { FleetModelStatus } from '@well-simulation/dto/res/model-status';
import type { RunningControls } from '@well-simulation/dto/res/controls';
import { client, fetchFn } from '../utils/api';
import { routes } from '../config/routes';
import { REFRESH_INTERVALS, useAutoRefresh } from '../hooks/use-auto-refresh';
import { ModelBadge, useModelCheckOverdue } from '../domains/twin/model-status';
import { FreshnessIndicator } from '../domains/dashboard/components/freshness';

export type WellheadContext = {
  unverified?: boolean;
  detail: WellheadDetailResponse;
  fleet: FleetModelStatus | null;
  controls: { running: RunningControls; pendingCommandId: string | null } | null;
};

/** Optional model reads preserve the historian view; session failures still propagate. */
async function optional<T>(read: Promise<T>): Promise<T | null> {
  try {
    return await read;
  } catch (error) {
    if (
      error instanceof ApiError &&
      (error.statusCode === 401 || error.statusCode === 403)
    )
      throw error;
    return null;
  }
}
export async function clientLoader({ params }: { params: { wellheadId?: string } }) {
  try {
    const wellheadId = params.wellheadId ?? '';
    const [detail, fleet, controls] = await Promise.all([
      fetchFn<WellheadDetailResponse>(
        client.dashboard.wellheads[':wellheadId'].$get({ param: { wellheadId } })
      ),
      optional(fetchFn<FleetModelStatus>(client.twin.status.$get())),
      optional(
        fetchFn<NonNullable<WellheadContext['controls']>>(
          client.twin.wellheads[':wellheadId'].controls.$get({ param: { wellheadId } })
        )
      ),
    ]);
    return {
      value: {
        detail: detail.data,
        fleet: fleet?.data ?? null,
        controls: controls?.data ?? null,
      },
      error: null,
    };
  } catch (error) {
    if (
      error instanceof ApiError &&
      (error.statusCode === 401 || error.statusCode === 403)
    )
      throw error;
    return { value: null, error: errorState(error) };
  }
}
export default function WellheadWorkspace() {
  const incoming = useLoaderData<typeof clientLoader>();
  const params = useParams();
  const data = useRetainedRead(incoming.value, params.wellheadId ?? '');
  useAutoRefresh(REFRESH_INTERVALS.wellhead);
  if (!data) return incoming.error ? <ReadFailure error={incoming.error} /> : null;
  return <LoadedWorkspace data={data} error={incoming.error} />;
}
function LoadedWorkspace({
  data,
  error,
}: {
  data: WellheadContext;
  error: ReturnType<typeof errorState> | null;
}) {
  const overdue = useModelCheckOverdue(data.fleet);
  const { wellhead } = data.detail;
  const base = routes.dashboard.wellhead(wellhead.wellheadId);
  const refresh = useRevalidator();
  const status = data.fleet?.wellheads.find(
    (item) => item.wellheadId === wellhead.wellheadId
  );
  return (
    <div className='twin-workspace'>
      {error && <ReadFailure error={error} retained />}
      <Link className='twin-muted hover:underline' to={routes.dashboard.overview}>
        ← Back to overview
      </Link>
      <div className='twin-heading'>
        <div>
          <p className='twin-eyebrow'>Wellhead workspace</p>
          <h1>{wellhead.wellheadName}</h1>
          <p className='twin-muted'>
            {wellhead.fieldName} · {wellhead.locationName} · {wellhead.wellheadType}
          </p>
        </div>
        <ModelBadge
          status={status}
          available={data.fleet?.available ?? false}
          overdue={overdue || Boolean(error)}
          announce
        />
      </div>
      <div className='twin-inline'>
        <FreshnessIndicator
          announce
          ageSeconds={data.detail.latestReadingAgeSeconds}
          latestReadingAt={data.detail.latestReadingAt}
        />
        <button
          className='twin-button'
          onClick={() => refresh.revalidate()}
          disabled={refresh.state !== 'idle'}
        >
          {refresh.state === 'idle' ? 'Refresh status' : 'Refreshing status…'}
        </button>
      </div>
      <nav className='twin-tabs' aria-label='Wellhead views'>
        <NavLink to={base} end>
          Monitoring
        </NavLink>
        <NavLink to={`${base}/forecast`}>Forecast</NavLink>
        <NavLink to={`${base}/scenarios`}>Scenarios</NavLink>
        <NavLink to={`${base}/runs`}>Saved runs</NavLink>
      </nav>
      <Outlet context={{ ...data, unverified: Boolean(error) }} />
    </div>
  );
}
