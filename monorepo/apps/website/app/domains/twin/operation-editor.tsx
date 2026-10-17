import { useEffect, useState, type FormEvent } from 'react';
import {
  controlDraftSchema,
  parameterDraftSchema,
} from '@well-simulation/dto/req/controls';
import {
  SCENARIO_CONTROLS,
  type ScenarioControlKey,
} from '@well-simulation/dto/req/scenario-controls';
import type { ParameterWorkspace } from '@well-simulation/dto/res/controls';
import type { WellheadContext } from '../../routes/_dashboard.wellheads.$wellheadId';
import type { OperationKind } from './operation-route';
import { controlText, numberText } from './presentation';
const keys = Object.keys(SCENARIO_CONTROLS) as ScenarioControlKey[];

export function ProposalEditor({
  kind,
  well,
  parameters,
  disabled,
  submit,
}: {
  kind: OperationKind;
  well: WellheadContext;
  parameters: ParameterWorkspace | null;
  disabled: boolean;
  submit: (payload: unknown, key: string) => void;
}) {
  const [control, setControl] = useState<ScenarioControlKey>('chokePositionPercent');
  const [parameter, setParameter] = useState(
    parameters?.catalogue.parameters[0]?.key ?? ''
  );
  const [revision, setRevision] = useState(well.controls?.running.revision ?? 0);
  const [overrideId, setOverrideId] = useState(
    parameters?.latestOverrides.find((item) => item.parameterKey === parameter)
      ?.parameterOverrideId ?? null
  );
  const [version] = useState(parameters?.modelVersion);
  const spec = parameters?.catalogue.parameters.find((item) => item.key === parameter);
  const [value, setValue] = useState(
    String(
      kind === 'control'
        ? (well.controls?.running.controls.chokePositionPercent ?? '')
        : (spec?.runningValue ?? spec?.defaultValue ?? '')
    )
  );
  const [reason, setReason] = useState('');
  const [remove, setRemove] = useState(false);
  const [issues, setIssues] = useState<{ field: string; message: string }[]>([]);
  const currentOverride = parameters?.latestOverrides.find(
    (item) => item.parameterKey === parameter
  );
  useEffect(() => {
    if (issues.length) document.getElementById('proposal-errors')?.focus();
  }, [issues]);
  const invalid = (field: string) => issues.some((issue) => issue.field === field);
  const errorText = (field: string) =>
    issues
      .filter((issue) => issue.field === field)
      .map((issue) => issue.message)
      .join(' ');
  function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (disabled) return;
    const target =
      control === 'chokePositionPercent'
        ? value.trim()
          ? Number(value)
          : NaN
        : value === 'true';
    const payload =
      kind === 'control'
        ? {
            wellheadId: well.detail.wellhead.wellheadId,
            expectedRevision: revision,
            reason,
            targets: { [control]: target },
          }
        : {
            wellheadId: well.detail.wellhead.wellheadId,
            modelVersion: version,
            parameterKey: parameter,
            value: remove ? null : value.trim() ? Number(value) : NaN,
            unit: remove ? null : spec?.unit,
            expectedOverrideId: overrideId,
            reason,
          };
    const parsed = (
      kind === 'control' ? controlDraftSchema : parameterDraftSchema
    ).safeParse(payload);
    const errors = parsed.success
      ? []
      : parsed.error.issues.map((issue) => ({
          field: issue.path[0] === 'reason' ? 'reason' : 'value',
          message:
            issue.path[0] === 'reason'
              ? 'Enter a reason from 1 to 500 characters.'
              : 'Enter a valid target using the stated limits.',
        }));
    if (
      kind === 'parameter' &&
      !remove &&
      spec &&
      (Number(value) < spec.minimum || Number(value) > spec.maximum)
    )
      errors.push({
        field: 'value',
        message: `Enter a value from ${spec.minimum} to ${spec.maximum} ${spec.unit}.`,
      });
    setIssues(errors);
    if (parsed.success && !errors.length) submit(parsed.data, crypto.randomUUID());
  }
  return (
    <section className='twin-panel'>
      <div className='twin-panel-heading'>
        <h2>New proposal</h2>
      </div>
      <form className='twin-panel-body twin-stack twin-form' noValidate onSubmit={save}>
        {!!issues.length && (
          <div id='proposal-errors' className='twin-message' tabIndex={-1} role='alert'>
            <h3>Check your inputs</h3>
            <ul>
              {issues.map((issue, index) => (
                <li key={index}>
                  <a
                    href={`#proposal-${issue.field}`}
                    onClick={(event) => {
                      event.preventDefault();
                      document.getElementById(`proposal-${issue.field}`)?.focus();
                    }}
                  >
                    {issue.message}
                  </a>
                </li>
              ))}
            </ul>
          </div>
        )}
        {kind === 'parameter' && version !== parameters?.modelVersion && (
          <p className='twin-message'>
            The model version changed. Reload the page before creating a proposal for
            the new version.
          </p>
        )}
        <fieldset
          disabled={
            disabled || (kind === 'parameter' && version !== parameters?.modelVersion)
          }
          className='twin-stack min-w-0'
        >
          <legend className='sr-only'>Proposal values</legend>
          <div className='twin-field'>
            <label htmlFor='proposal-target'>
              {kind === 'control' ? 'Control' : 'Parameter'}
            </label>
            {kind === 'control' ? (
              <select
                id='proposal-target'
                value={control}
                onChange={(event) => {
                  const key = event.target.value as ScenarioControlKey;
                  setControl(key);
                  setValue(
                    String(
                      well.controls?.running.controls[key] ??
                        (key === 'chokePositionPercent' ? '' : false)
                    )
                  );
                }}
              >
                {keys.map((key) => (
                  <option key={key} value={key}>
                    {SCENARIO_CONTROLS[key].label}
                  </option>
                ))}
              </select>
            ) : (
              <select
                id='proposal-target'
                value={parameter}
                onChange={(event) => {
                  const key = event.target.value;
                  setParameter(key);
                  const next = parameters?.catalogue.parameters.find(
                    (item) => item.key === key
                  );
                  setValue(String(next?.runningValue ?? next?.defaultValue ?? ''));
                  setOverrideId(
                    parameters?.latestOverrides.find(
                      (item) => item.parameterKey === key
                    )?.parameterOverrideId ?? null
                  );
                  setRemove(false);
                }}
              >
                {parameters?.catalogue.parameters.map((item) => (
                  <option key={item.key} value={item.key}>
                    {item.description} ({item.unit})
                  </option>
                ))}
              </select>
            )}
          </div>
          {spec && kind === 'parameter' && (
            <div className='twin-message'>
              <p>
                Running: {numberText(spec.runningValue)} {spec.unit}. Base default:{' '}
                {numberText(spec.defaultValue)} {spec.unit}.
              </p>
              <p>
                Published override:{' '}
                {currentOverride?.value == null
                  ? 'None'
                  : `${numberText(currentOverride.value)} ${currentOverride.unit}`}
                .
              </p>
              <p>
                Permitted values: {numberText(spec.minimum)} to{' '}
                {numberText(spec.maximum)} {spec.unit}.
              </p>
              <p>
                Removing an override restores environment/default resolution, which can
                differ from the base default.
              </p>
            </div>
          )}
          {kind === 'parameter' && (
            <label className='twin-inline'>
              <input
                type='checkbox'
                checked={remove}
                onChange={(event) => setRemove(event.target.checked)}
              />{' '}
              Remove the database override
            </label>
          )}
          <div className='twin-field'>
            <label htmlFor='proposal-value'>
              New value
              {kind === 'parameter'
                ? ` (${spec?.unit ?? ''})`
                : control === 'chokePositionPercent'
                  ? ' (%)'
                  : ''}
            </label>
            {kind === 'parameter' || control === 'chokePositionPercent' ? (
              <input
                id='proposal-value'
                type='number'
                step='any'
                value={value}
                disabled={remove}
                onChange={(event) => setValue(event.target.value)}
                aria-invalid={invalid('value')}
                aria-describedby='proposal-value-help'
              />
            ) : (
              <select
                id='proposal-value'
                value={value}
                onChange={(event) => setValue(event.target.value)}
                aria-describedby='proposal-value-help'
              >
                <option value='false'>
                  {controlText(SCENARIO_CONTROLS[control].storedKey, false)}
                </option>
                <option value='true'>
                  {controlText(SCENARIO_CONTROLS[control].storedKey, true)}
                </option>
              </select>
            )}
            <p id='proposal-value-help' className='twin-muted'>
              {errorText('value') ||
                (kind === 'control' && control === 'chokePositionPercent'
                  ? 'Enter a value from 0 to 100%.'
                  : 'The server validates this target before saving the proposal.')}
            </p>
          </div>
          <div className='twin-field'>
            <label htmlFor='proposal-reason'>Reason</label>
            <textarea
              id='proposal-reason'
              value={reason}
              maxLength={500}
              onChange={(event) => setReason(event.target.value)}
              aria-invalid={invalid('reason')}
              aria-describedby='proposal-reason-help'
            />
            <p id='proposal-reason-help' className='twin-muted'>
              {errorText('reason') ||
                'Explain the change so another person can understand it later.'}
            </p>
          </div>
          <p>
            {kind === 'control'
              ? `Expected control revision: ${revision}.`
              : `Expected override: ${overrideId ?? 'None'}.`}
          </p>
          {kind === 'control' && control === 'emergencyShutdown' && (
            <p className='twin-message'>
              Shutdown and reset each use a separate command. Reset leaves the other
              control targets unchanged and can restore flow if they permit it.
            </p>
          )}
          {kind === 'parameter' &&
            overrideId !== (currentOverride?.parameterOverrideId ?? null) && (
              <p className='twin-message'>
                The published override changed.{' '}
                <button
                  type='button'
                  className='twin-button'
                  onClick={() =>
                    setOverrideId(currentOverride?.parameterOverrideId ?? null)
                  }
                >
                  Use current override
                </button>{' '}
                Check the value again before saving.
              </p>
            )}
          {kind === 'control' &&
            well.controls &&
            revision !== well.controls.running.revision && (
              <p className='twin-message'>
                The running revision changed.{' '}
                <button
                  type='button'
                  className='twin-button'
                  onClick={() => setRevision(well.controls!.running.revision)}
                >
                  Use current revision
                </button>{' '}
                Check the target again before saving.
              </p>
            )}
          <button type='submit' className='twin-button twin-primary'>
            Save proposal
          </button>
        </fieldset>
      </form>
    </section>
  );
}
