import AxeBuilder from '@axe-core/playwright';
import { randomUUID } from 'node:crypto';
import { expect, test } from '@playwright/test';
import type { AlarmEvent, AlarmReceipt } from '@well-simulation/dto/res/alarms';

// Faults use intercepted responses; PostgreSQL role and concurrency checks live
// in the API suite. The isolated browser check also exercises committed writes.
test('alarm reviews preserve truth, drafts, permissions and uncertain request identity', async ({
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
  let role = 'USER',
    failRead = false,
    forbidden = false;
  let outcome: 'success' | 'lost' | 'missing' | 'conflict' = 'success';
  const event: AlarmEvent = {
    eventId: '9007199254740993',
    triggeredAt: '2026-10-17T05:00:00.123456Z',
    clearedAt: null,
    severity: 'CRITICAL',
    triggeredValue: 3200,
    target: {
      wellheadId: 1,
      wellheadName: 'Reference Well 1',
      locationName: 'Synthetic reference field',
      ruleId: 1,
      parameterName: 'Tubing pressure',
      unit: 'psi',
      operator: '>',
      threshold: 3000,
      severity: 'CRITICAL',
      triggerAfter: 3,
      clearAfter: 3,
    },
    acknowledgement: null,
    shelf: null,
  };
  const writes: { key: string; input: Record<string, unknown> }[] = [];
  const receipts = new Map<string, AlarmReceipt>();
  await page.route('**/api/auth/me', (route) =>
    route.fulfill({
      json: {
        data: { user: { id: 41, role, userName: 'Alarm screen test' } },
        status: 200,
      },
    })
  );
  await page.route('**/api/alarms**', async (route) => {
    const request = route.request(),
      url = new URL(request.url());
    if (forbidden) return route.fulfill({ status: 403, json: { msg: 'Forbidden' } });
    if (request.method() === 'POST') {
      const key = request.headers()['idempotency-key']!,
        input = request.postDataJSON();
      writes.push({ key, input });
      expect(key).toMatch(/^[0-9a-f-]{36}$/);
      if (outcome === 'conflict')
        return route.fulfill({ status: 409, json: { msg: 'Conflict' } });
      if (outcome === 'missing') return route.abort('failed');
      const receipt: AlarmReceipt = {
        actionId: randomUUID(),
        action: input.action,
        occurredAt: new Date().toISOString(),
        actorUserId: 41,
        actorName: 'Alarm screen test',
        reason: input.reason,
        target: event.target,
        eventId: input.action === 'acknowledge' ? event.eventId : null,
        triggeredAt: input.action === 'acknowledge' ? event.triggeredAt : null,
        revision: input.action === 'acknowledge' ? null : input.expectedRevision + 1,
        shelvedUntil:
          input.action === 'shelve'
            ? new Date(Date.now() + input.durationMinutes * 60000).toISOString()
            : null,
      };
      receipts.set(key, receipt);
      if (input.action === 'acknowledge')
        event.acknowledgement = {
          at: receipt.occurredAt,
          actorName: receipt.actorName,
          reason: receipt.reason,
        };
      else
        event.shelf = {
          revision: receipt.revision!,
          until: receipt.shelvedUntil,
          active: input.action === 'shelve',
          updatedAt: receipt.occurredAt,
          actorName: receipt.actorName,
          reason: receipt.reason,
        };
      return outcome === 'lost'
        ? route.abort('failed')
        : route.fulfill({ json: { data: receipt, status: 200 } });
    }
    if (url.pathname.includes('/actions/')) {
      const receipt = receipts.get(url.pathname.split('/').at(-1)!);
      return route.fulfill(
        receipt
          ? { json: { data: receipt, status: 200 } }
          : { status: 404, json: { msg: 'No receipt' } }
      );
    }
    if (failRead) return route.fulfill({ status: 503, json: { msg: 'Read failed' } });
    const observedAt = new Date().toISOString();
    if (url.pathname.includes('/events/'))
      return route.fulfill({
        json: {
          data: { observedAt, event, actions: [...receipts.values()] },
          status: 200,
        },
      });
    const activeShelf = !!event.shelf?.active,
      attention = !event.acknowledgement && !activeShelf;
    const header = {
      observedAt,
      wells: [{ wellheadId: 1, wellheadName: event.target.wellheadName }],
      summary: {
        active: event.clearedAt ? 0 : 1,
        critical: event.clearedAt ? 0 : 1,
        attention: attention ? 1 : 0,
        shelves: activeShelf ? 1 : 0,
        retained: 1,
      },
      nextCursor: null,
    };
    const view = url.searchParams.get('view') ?? 'attention';
    const data = url.pathname.endsWith('/shelves')
      ? {
          ...header,
          total: activeShelf ? 1 : 0,
          items: activeShelf ? [{ target: event.target, state: event.shelf }] : [],
        }
      : {
          ...header,
          items:
            (view === 'attention' && !attention) ||
            (view === 'active' && event.clearedAt)
              ? []
              : [event],
        };
    return route.fulfill({ json: { data, status: 200 } });
  });
  await page.goto('/alarms');
  await expect(
    page.getByRole('button', { name: 'Review action', exact: true })
  ).toHaveCount(0);
  for (const readerRole of ['USER', 'ADMIN']) {
    role = readerRole;
    await page.reload();
    await expect(
      page.getByText('Your account can inspect', { exact: false })
    ).toBeVisible();
    await expect(
      page.getByRole('button', { name: 'Review action', exact: true })
    ).toHaveCount(0);
  }
  role = 'OPERATIONS';
  await page.reload();
  const editor = page.getByRole('region', {
    name: 'Review action for Reference Well 1',
  });
  const refresh = async () => {
    await page.getByRole('button', { name: 'Refresh view', exact: true }).click();
    await expect(
      page.getByRole('button', { name: 'Refresh view', exact: true })
    ).toBeEnabled();
  };
  const confirm = async (reason: string) => {
    await editor.getByLabel('Reason', { exact: true }).fill(reason);
    await editor.getByLabel('I checked the well', { exact: false }).check();
  };
  await page.getByRole('button', { name: 'Review action', exact: true }).click();
  await expect(editor.getByRole('heading')).toBeFocused();
  await editor.getByRole('button', { name: 'Close review', exact: true }).click();
  await expect(
    page.getByRole('button', { name: 'Review action', exact: true })
  ).toBeFocused();
  await page.getByRole('button', { name: 'Review action', exact: true }).click();
  await confirm('Inspect the pressure instrument');
  failRead = true;
  await refresh();
  await expect(
    editor.getByRole('button', { name: 'Acknowledge event', exact: true })
  ).toBeDisabled();
  await expect(editor.getByLabel('Reason', { exact: true })).toHaveValue(
    'Inspect the pressure instrument'
  );
  failRead = false;
  await refresh();
  event.clearedAt = new Date().toISOString();
  await refresh();
  await expect(
    editor.getByRole('button', { name: 'Review current state' })
  ).toBeVisible();
  await editor.getByRole('button', { name: 'Review current state' }).click();
  await expect(
    editor.getByLabel('I checked the well', { exact: false })
  ).not.toBeChecked();
  await confirm('Instrument inspected after return to normal');
  outcome = 'lost';
  await editor.getByRole('button', { name: 'Acknowledge event', exact: true }).click();
  await expect(editor.getByRole('button', { name: 'Check result' })).toBeVisible();
  expect(writes).toHaveLength(1);
  expect(writes[0]!.input.eventId).toBe(event.eventId);
  expect(writes[0]!.input.triggeredAt).toBe(event.triggeredAt);
  await editor.getByRole('button', { name: 'Check result' }).click();
  await expect(page.locator('#alarm-outcome')).toBeFocused();
  expect(writes).toHaveLength(1);
  await page.getByRole('link', { name: 'History', exact: true }).click();
  await page.getByRole('button', { name: 'Review action', exact: true }).click();
  await expect(editor.getByLabel('Action', { exact: true })).toHaveValue('shelve');
  await confirm('Instrument maintenance for thirty minutes');
  outcome = 'missing';
  await editor.getByRole('button', { name: 'Shelve rule', exact: true }).click();
  await editor.getByRole('button', { name: 'Check result' }).click();
  await expect(
    editor.getByRole('button', { name: 'Retry same request' })
  ).toBeVisible();
  outcome = 'success';
  await editor.getByRole('button', { name: 'Retry same request' }).click();
  await expect(page.locator('#alarm-outcome')).toContainText('shelved the rule');
  expect(writes).toHaveLength(3);
  expect(writes[2]).toEqual(writes[1]);
  await page.getByRole('link', { name: 'Shelved rules', exact: true }).click();
  await page.getByRole('button', { name: 'Review shelf', exact: true }).click();
  await expect(editor.getByLabel('Action', { exact: true })).toHaveValue('unshelve');
  await confirm('Maintenance complete');
  outcome = 'conflict';
  await editor.getByRole('button', { name: 'End shelf now', exact: true }).click();
  await expect(editor.getByRole('status')).toContainText('alarm state changed');
  await expect(editor.getByLabel('Reason', { exact: true })).toHaveValue(
    'Maintenance complete'
  );
  event.shelf!.revision++;
  await refresh();
  await editor.getByRole('button', { name: 'Review current state' }).click();
  for (const width of [1440, 390, 320]) {
    await page.setViewportSize({ width, height: 1000 });
    for (const control of [
      editor.getByLabel('Action', { exact: true }),
      editor.getByLabel('Reason', { exact: true }),
      page.getByLabel('Well', { exact: true }),
    ]) {
      expect((await control.boundingBox())!.height).toBeGreaterThanOrEqual(44);
    }
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)
    ).toBe(true);
    expect(
      (
        await new AxeBuilder({ page })
          .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa'])
          .analyze()
      ).violations
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath(`alarm-review-${width}.png`),
      fullPage: true,
    });
  }
  outcome = 'success';
  await confirm('Maintenance complete');
  await editor.getByRole('button', { name: 'End shelf now', exact: true }).click();
  await expect(page.getByText('No current shelves for this filter.')).toBeVisible();
  await page.getByRole('link', { name: 'History', exact: true }).click();
  await page.getByText('Event details and recent actions', { exact: true }).click();
  await page.getByRole('button', { name: 'Load recent actions' }).click();
  await expect(page.getByText('Latest 20 actions', { exact: false })).toBeVisible();
  await page.getByRole('button', { name: 'Review action', exact: true }).click();
  forbidden = true;
  await page.getByRole('button', { name: 'Refresh view', exact: true }).click();
  await expect(page.getByLabel('Reason', { exact: true })).toHaveCount(0);
  expect(errors).toEqual([]);
});
