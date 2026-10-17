import AxeBuilder from '@axe-core/playwright';
import { randomUUID } from 'node:crypto';
import { expect, test } from '@playwright/test';

test('operating screens review drafts, recover uncertain writes and explain parameter adoption', async ({
  page,
}) => {
  // Presentation faults use intercepted responses. Real server permissions and
  // writes run in the API suite and the isolated operating-screen verification.
  let nextRead = 0;
  await page.route('**/api/**', async (route) => {
    const delay = Math.max(0, nextRead - Date.now());
    nextRead = Date.now() + delay + 900;
    if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
    await route.fallback();
  });
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/login');
  await page.getByLabel('Email or username').fill('demo@example.com');
  await page.getByLabel('Password', { exact: true }).fill('demo-operator-2026');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page).toHaveURL(/\/$/);
  let role = 'OPERATIONS';
  await page.route('**/api/auth/me', (route) =>
    route.fulfill({
      json: { data: { user: { id: 41, role, userName: 'Screen test' } }, status: 200 },
    })
  );
  const base = {
    wellheadId: 1,
    createdBy: 41,
    reason: 'Check the choke response',
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 86400000).toISOString(),
    resultAt: null as string | null,
    modelVersion: '0.3.0',
  };
  let command = {
    ...base,
    commandId: randomUUID(),
    idempotencyKey: randomUUID(),
    status: 'draft',
    expectedRevision: 0,
    chokePositionPercent: 80,
    masterValveOpen: null,
    wingValveOpen: null,
    pumpOn: null,
    emergencyShutdown: null,
    resultRevision: null as number | null,
    resultReason: null,
    events: [],
  };
  let commands: (typeof command)[] = [];
  let writes = 0;
  let loseDraft = false;
  let denyHistory = false;
  await page.route('**/api/twin/wellheads/1/control-commands*', (route) => {
    if (denyHistory) return route.fulfill({ status: 403, json: { msg: 'Forbidden' } });
    const key = new URL(route.request().url()).searchParams.get('idempotencyKey');
    return route.fulfill({
      json: {
        data: {
          items: key
            ? commands.filter((item) => item.idempotencyKey === key)
            : commands,
          offset: 0,
          hasMore: false,
        },
        status: 200,
      },
    });
  });
  await page.route('**/api/twin/control-commands', (route) => {
    writes++;
    const body = route.request().postDataJSON();
    command = {
      ...command,
      ...base,
      commandId: randomUUID(),
      idempotencyKey: route.request().headers()['idempotency-key']!,
      status: 'draft',
      resultRevision: null,
      resultAt: null,
      ...body.targets,
      reason: body.reason,
      expectedRevision: body.expectedRevision,
    };
    commands = [command];
    return route.fulfill({
      status: loseDraft ? 504 : 201,
      json: loseDraft ? { msg: 'Timeout' } : { data: command, status: 201 },
    });
  });
  await page.route('**/api/twin/control-commands/*', (route) =>
    route.fulfill({ json: { data: command, status: 200 } })
  );
  await page.route('**/api/twin/control-commands/*/activate', (route) => {
    writes++;
    expect(route.request().postDataJSON()).toEqual({
      confirm: true,
      expectedRevision: command.expectedRevision,
    });
    command = {
      ...command,
      status: 'applied',
      resultAt: new Date().toISOString(),
      resultRevision: command.expectedRevision + 1,
    };
    commands = [command];
    return route.fulfill({ status: 504, json: { msg: 'Response lost after commit' } });
  });
  await page.route('**/api/twin/control-commands/*/cancel', (route) => {
    writes++;
    command = { ...command, status: 'cancelled', resultAt: new Date().toISOString() };
    commands = [command];
    return route.fulfill({ json: { data: command, status: 200 } });
  });
  await page.goto('/wellheads/1/controls');
  await expect(page.getByRole('link', { name: 'Controls', exact: true })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Parameters', exact: true })).toHaveCount(
    0
  );
  await page.getByRole('button', { name: 'Save proposal', exact: true }).click();
  await expect(page.locator('#proposal-errors')).toBeFocused();
  await page.getByLabel('New value (%)').fill('80');
  await page.getByLabel('Reason', { exact: true }).fill(base.reason);
  await page.getByRole('button', { name: 'Save proposal', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Review proposal' })).toBeFocused();
  expect(writes).toBe(1);
  const controls = (
    await (await page.request.get('/api/twin/wellheads/1/controls')).json()
  ).data;
  await page.route('**/api/twin/wellheads/1/controls', (route) =>
    route.fulfill({
      json: {
        data: {
          ...controls,
          running: { ...controls.running, revision: command.expectedRevision + 1 },
        },
        status: 200,
      },
    })
  );
  await page.getByRole('button', { name: 'Refresh view', exact: true }).click();
  await expect(
    page.getByText('This proposal uses an earlier revision.', { exact: false })
  ).toBeVisible();
  await expect(
    page.getByRole('button', { name: 'Activate command', exact: true })
  ).toBeDisabled();
  await expect(
    page.getByRole('button', { name: 'Cancel proposal', exact: true })
  ).toBeEnabled();
  await page.unroute('**/api/twin/wellheads/1/controls');
  await page.getByRole('button', { name: 'Refresh view', exact: true }).click();
  await expect(
    page.getByRole('button', { name: 'Activate command', exact: true })
  ).toBeEnabled();
  await page.getByLabel('I checked this well, target and reason').check();
  await page.getByRole('button', { name: 'Activate command', exact: true }).click();
  await expect(
    page.getByRole('button', { name: 'Check outcome', exact: true })
  ).toBeVisible();
  await page.getByRole('button', { name: 'Check outcome', exact: true }).click();
  await expect(page.locator('#operation-outcome')).toHaveCount(0);
  await expect(page.getByText(/The database committed revision/)).toBeVisible();
  expect(writes).toBe(2);
  await page.getByRole('link', { name: 'New proposal', exact: true }).click();
  loseDraft = true;
  await page
    .getByLabel('Reason', { exact: true })
    .fill('Recover a missing draft response');
  await page.getByRole('button', { name: 'Save proposal', exact: true }).click();
  await expect(
    page.getByRole('button', { name: 'Check outcome', exact: true })
  ).toBeVisible();
  await expect(
    page.getByRole('button', { name: 'Save proposal', exact: true })
  ).toBeDisabled();
  await page.getByRole('button', { name: 'Check outcome', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Review proposal' })).toBeVisible();
  expect(writes).toBe(3);
  // Another tab may queue the same person's draft before this view refreshes.
  command = { ...command, status: 'queued' };
  commands = [command];
  await page.getByRole('button', { name: 'Refresh view', exact: true }).click();
  await expect(
    page.getByText('Queued. Waiting for the model worker.', { exact: true })
  ).toBeVisible();
  await expect(
    page.getByRole('button', { name: 'Activate command', exact: true })
  ).toHaveCount(0);
  await page.getByLabel('I checked this well, target and reason').check();
  await page.getByRole('button', { name: 'Cancel proposal', exact: true }).click();
  await expect(
    page.getByText('This proposal is cancelled.', { exact: false })
  ).toBeVisible();
  expect(writes).toBe(4);
  await page.setViewportSize({ width: 320, height: 1000 });
  let scan = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(scan.violations).toEqual([]);
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)
  ).toBe(true);
  denyHistory = true;
  await page.getByRole('button', { name: 'Refresh view', exact: true }).click();
  await expect(page.getByText('Access denied.', { exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Your proposals' })).toHaveCount(0);

  role = 'ADMIN';
  let change = {
    ...base,
    changeId: randomUUID(),
    idempotencyKey: randomUUID(),
    parameterKey: 'downstream_pressure',
    value: 260 as number | null,
    unit: 'psi' as string | null,
    expectedOverrideId: null as number | null,
    publishedOverrideId: null as number | null,
    status: 'draft',
  };
  let changes: (typeof change)[] = [];
  let overrides: {
    parameterOverrideId: number;
    parameterKey: string;
    value: number | null;
    unit: string | null;
  }[] = [];
  await page.route('**/api/twin/wellheads/1/parameters', (route) =>
    route.fulfill({
      json: {
        data: {
          wellheadId: 1,
          modelVersion: '0.3.0',
          status: 'running',
          running: null,
          latestOverrides: overrides,
          catalogue: {
            wellheadId: 1,
            modelVersion: '0.3.0',
            parameters: [
              {
                key: 'downstream_pressure',
                description: 'Surface or export pressure',
                unit: 'psi',
                defaultValue: 250,
                minimum: 50,
                maximum: 1000,
                requiresRestart: true,
                runningValue: 250,
              },
            ],
          },
        },
        status: 200,
      },
    })
  );
  await page.route('**/api/twin/wellheads/1/parameter-changes*', (route) =>
    route.fulfill({
      json: { data: { items: changes, offset: 0, hasMore: false }, status: 200 },
    })
  );
  await page.route('**/api/twin/parameter-changes', (route) => {
    writes++;
    const body = route.request().postDataJSON();
    change = {
      ...change,
      ...base,
      ...body,
      changeId: randomUUID(),
      idempotencyKey: route.request().headers()['idempotency-key']!,
      status: 'draft',
    };
    changes = [change];
    return route.fulfill({ status: 201, json: { data: change, status: 201 } });
  });
  await page.route('**/api/twin/parameter-changes/*', (route) =>
    route.fulfill({ json: { data: change, status: 200 } })
  );
  await page.route('**/api/twin/parameter-changes/*/publish', (route) => {
    writes++;
    change = {
      ...change,
      status: 'published',
      publishedOverrideId: 1,
      resultAt: new Date().toISOString(),
    };
    changes = [change];
    overrides = [
      {
        parameterOverrideId: 1,
        parameterKey: change.parameterKey,
        value: change.value,
        unit: change.unit,
      },
    ];
    return route.fulfill({ json: { data: change, status: 200 } });
  });
  await page.goto('/wellheads/1/parameters');
  await expect(page.getByRole('link', { name: 'Controls', exact: true })).toHaveCount(
    0
  );
  await expect(page.getByText('Running: 250 psi.', { exact: false })).toBeVisible();
  await page.getByLabel('New value (psi)').fill('10');
  await page.getByLabel('Reason', { exact: true }).fill('Review parameter publication');
  await page.getByRole('button', { name: 'Save proposal', exact: true }).click();
  await expect(page.locator('#proposal-errors')).toContainText('50 to 1000 psi');
  expect(writes).toBe(4);
  await page.getByLabel('New value (psi)').fill('260');
  await page.getByRole('button', { name: 'Save proposal', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Review proposal' })).toBeVisible();
  await page.getByLabel('I checked this well, target and reason').check();
  await page.getByRole('button', { name: 'Publish parameter', exact: true }).click();
  await expect(
    page.getByText('Published. Compare the running value after a service restart.', {
      exact: true,
    })
  ).toBeVisible();
  await expect(
    page.getByText(/Running value now: 250 psi. Latest published override: 260 psi/)
  ).toBeVisible();
  expect(writes).toBe(6);
  scan = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(scan.violations).toEqual([]);
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)
  ).toBe(true);
  expect(errors).toEqual([]);
});
