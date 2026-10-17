import type {
  FleetModelStatus,
  ModelStatus,
} from '@well-simulation/dto/res/model-status';
import { timeText, numberText } from './presentation';
import { REFRESH_INTERVALS } from '../../hooks/use-auto-refresh';
import { useElapsedSeconds } from '../../hooks/use-elapsed-seconds';

/** Two missed polling intervals make the previously checked state unverified. */
export function useModelCheckOverdue(fleet: FleetModelStatus | null): boolean {
  return useElapsedSeconds(fleet) * 1000 > REFRESH_INTERVALS.wellhead * 2;
}

/** Model lifecycle and successful output time are independent of telemetry. */
export function ModelBadge({
  status,
  available,
  overdue = false,
  announce = false,
}: {
  status?: ModelStatus;
  available: boolean;
  overdue?: boolean;
  announce?: boolean;
}) {
  const label = overdue
    ? 'Model check overdue'
    : !available
      ? 'Model unavailable'
      : !status
        ? 'Model not loaded'
        : status.status === 'running' && !status.current
          ? 'Model state stale'
          : `Model ${status.status}`;
  return (
    <span
      role={announce ? 'status' : undefined}
      className={`twin-badge ${!overdue && available && status?.current ? 'twin-ok' : 'twin-attention'}`}
    >
      {label}
    </span>
  );
}
export function ModelPanel({
  fleet,
  wellheadId,
  unverified = false,
}: {
  fleet: FleetModelStatus | null;
  wellheadId: number;
  unverified?: boolean;
}) {
  const model = fleet?.wellheads.find((item) => item.wellheadId === wellheadId);
  const overdue = useModelCheckOverdue(fleet);
  return (
    <details className='twin-panel' aria-label='Model status'>
      <summary className='twin-panel-heading cursor-pointer'>
        <span>Model diagnostics</span>
        <ModelBadge
          status={model}
          available={fleet?.available ?? false}
          overdue={overdue || unverified}
        />
      </summary>
      <div className='twin-panel-body'>
        {model ? (
          <dl className='twin-facts'>
            <div>
              <dt>Version</dt>
              <dd>{model.modelVersion}</dd>
            </div>
            <div>
              <dt>Process calculated at</dt>
              <dd>{timeText(model.simulatedAt)}</dd>
            </div>
            <div>
              <dt>Last scheduler tick</dt>
              <dd>{timeText(model.lastTickAt)}</dd>
            </div>
            <div>
              <dt>Reason</dt>
              <dd>{model.reason.replaceAll('_', ' ')}</dd>
            </div>
            <div>
              <dt>Skipped ticks</dt>
              <dd>{numberText(model.skippedTicks)}</dd>
            </div>
            {model.lastRecoveryEvent && (
              <div>
                <dt>Last recovery</dt>
                <dd>
                  {model.lastRecoveryEvent.type.replaceAll('_', ' ')} ·{' '}
                  {timeText(model.lastRecoveryEvent.time)}
                </dd>
              </div>
            )}
          </dl>
        ) : (
          <p className='twin-muted'>
            The model status request failed. Check the recorded readings above.
          </p>
        )}
        <p className='twin-caption'>
          Checked {timeText(fleet?.checkedAt)}. Times use your browser’s timezone.
        </p>
      </div>
    </details>
  );
}
