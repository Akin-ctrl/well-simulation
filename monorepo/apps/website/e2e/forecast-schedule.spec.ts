import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';
import type { ForecastScheduleView } from '@well-simulation/dto/res/forecast-schedule';

test('schedule readers, reviewed edits, uncertain saves and retained failures', async ({
  page,
}, testInfo) => {
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
  const now = new Date().toISOString();
  const scheduleId = '60000000-0000-4000-8000-000000000001';
  const data: ForecastScheduleView = {
    schedule: {
      scheduleId,
      wellheadId: 1,
      ownerUserId: 1,
      ownerRole: 'ADMIN',
      enabled: false,
      intervalSeconds: 900,
      horizonSeconds: 900,
      stepSeconds: 60,
      revision: 1,
      nextDueAt: null,
      reason: 'Existing schedule',
      updatedAt: now,
    },
    latestJob: null,
    latestCompleted: null,
  };
  let failed = false;
  let forbidden = false;
  let loseWrite = false;
  let writes = 0;
  await page.route('**/api/twin/wellheads/1/forecast-schedule', async (route) => {
    if (forbidden) return route.fulfill({ status: 403, json: { msg: 'Forbidden' } });
    if (route.request().method() === 'PUT') {
      writes++;
      const input = route.request().postDataJSON();
      expect(input.expectedRevision).toBe(data.schedule!.revision);
      data.schedule = {
        ...data.schedule!,
        enabled: input.enabled,
        intervalSeconds: input.intervalMinutes * 60,
        horizonSeconds: input.durationMinutes * 60,
        revision: input.expectedRevision + 1,
        reason: input.reason,
        nextDueAt: input.enabled ? new Date(Date.now() + 900000).toISOString() : null,
      };
      if (loseWrite) return route.abort('failed');
      return route.fulfill({ json: { data: data.schedule, status: 200 } });
    }
    return route.fulfill(
      failed
        ? { status: 503, json: { msg: 'Read failed' } }
        : { json: { data, status: 200 } }
    );
  });
  await page.goto('/wellheads/1/forecast');
  const panel = page.getByRole('region', { name: 'Scheduled forecasts' });
  await expect(
    panel.getByText('Administrators manage this schedule.', { exact: false })
  ).toBeVisible();
  await expect(panel.getByText('Edit forecast schedule')).toHaveCount(0);
  await page.route('**/api/auth/me', (route) =>
    route.fulfill({
      json: {
        data: { user: { id: 1, role: 'ADMIN', userName: 'Schedule test' } },
        status: 200,
      },
    })
  );
  await page.reload();
  await panel.getByText('Edit forecast schedule', { exact: true }).click();
  await panel.getByLabel('Enable scheduled forecasts').check();
  await panel
    .getByLabel('Reason for this schedule change')
    .fill('Keep this reason through a lost response');
  await panel.getByLabel('I checked this well', { exact: false }).check();
  loseWrite = true;
  await panel.getByRole('button', { name: 'Save schedule', exact: true }).click();
  await expect(
    panel.getByRole('button', { name: 'Check saved schedule' })
  ).toBeVisible();
  expect(writes).toBe(1);
  await expect(
    panel.getByRole('button', { name: 'Save schedule', exact: true })
  ).toBeDisabled();
  await panel.getByRole('button', { name: 'Check saved schedule' }).click();
  await expect(
    panel.getByText('Current schedule loaded.', { exact: false })
  ).toBeVisible();
  expect(writes).toBe(1);
  loseWrite = false;
  data.schedule = { ...data.schedule!, revision: 3 };
  await panel.getByRole('button', { name: 'Refresh view', exact: true }).click();
  await expect(
    panel.getByRole('button', { name: 'Use current settings' })
  ).toBeVisible();
  await panel.getByRole('button', { name: 'Use current settings' }).click();
  await panel
    .getByLabel('Reason for this schedule change')
    .fill('Keep this draft after a read failure');
  failed = true;
  await panel.getByRole('button', { name: 'Refresh view', exact: true }).click();
  await expect(panel.getByText('Refresh failed.', { exact: false })).toBeVisible();
  await expect(panel.getByLabel('Reason for this schedule change')).toHaveValue(
    'Keep this draft after a read failure'
  );
  await expect(
    panel.getByRole('button', { name: 'Save schedule', exact: true })
  ).toBeDisabled();
  failed = false;
  await panel.getByRole('button', { name: 'Try again', exact: true }).click();
  await expect(
    panel.getByRole('button', { name: 'Save schedule', exact: true })
  ).toBeEnabled();
  data.latestCompleted = {
    jobId: '60000000-0000-4000-8000-000000000002',
    scheduleId,
    scheduleRevision: 2,
    wellheadId: 1,
    ownerUserId: 1,
    ownerRole: 'ADMIN',
    intervalSeconds: 900,
    horizonSeconds: 900,
    stepSeconds: 60,
    scheduledFor: now,
    startedAt: now,
    leaseExpiresAt: now,
    missedIntervals: 0,
    status: 'complete',
    completedAt: now,
    reasonCode: null,
    runId: '60000000-0000-4000-8000-000000000003',
  };
  data.latestJob = {
    ...data.latestCompleted,
    jobId: '60000000-0000-4000-8000-000000000004',
    status: 'failed',
    reasonCode: 'model_not_current',
    runId: null,
  };
  await panel.getByRole('button', { name: 'Refresh view', exact: true }).click();
  await expect(
    panel.getByRole('heading', { name: 'Latest attempt: failed' })
  ).toBeVisible();
  await expect(
    panel.getByRole('link', { name: 'Open scheduled result' })
  ).toBeVisible();
  await page.setViewportSize({ width: 320, height: 900 });
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)
  ).toBe(true);
  await page.screenshot({
    path: testInfo.outputPath('schedule-phone.png'),
    fullPage: true,
  });
  expect(
    (
      await new AxeBuilder({ page })
        .include('[aria-labelledby="schedule-heading"]')
        .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
        .analyze()
    ).violations
  ).toEqual([]);
  forbidden = true;
  await panel.getByRole('button', { name: 'Refresh view', exact: true }).click();
  await expect(page.getByLabel('Reason for this schedule change')).toHaveCount(0);
  expect(writes).toBe(1);
  expect(errors).toEqual([]);
});
