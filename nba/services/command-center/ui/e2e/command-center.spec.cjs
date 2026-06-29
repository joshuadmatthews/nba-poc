const { test, expect } = require('@playwright/test');

const go = (page, label) => page.locator('.nav-item', { hasText: label }).click();

test.describe('NBA Command Center', () => {
  test('System Map (centerpiece) renders the topology + live panels', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('.brand')).toContainText('Command Center');
    await expect(page.locator('.sysmap-svg')).toBeVisible();
    await expect(page.locator('.nm-label').first()).toBeVisible();      // topology nodes
    await expect(page.locator('.stats-strip .stat')).toHaveCount(6);    // aggregate stats at every stage
    await expect(page.locator('.scope select')).toBeVisible();          // scope to one action/member
    await expect(page.getByText(/batch orchestrator/)).toBeVisible();   // state-machine node (orchestrator · per-action workflows)
    await expect(page.getByText('Live event stream')).toBeVisible();
  });

  test('Overview: KPIs + funnel + dispositions', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Overview');
    await expect(page.locator('.kpi')).toHaveCount(5);
    await expect(page.locator('.funnel-row').first()).toBeVisible();
    // the Overview dispositions CARD heading (not the nav item of the same name)
    await expect(page.getByRole('heading', { name: 'Dispositions' })).toBeVisible();
  });

  test('Action performance loads from the lake', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Action performance');
    await expect(page.locator('.grid-table thead')).toContainText('Eligible');
    await expect(page.locator('.grid-table tbody tr').first()).toBeVisible();
  });

  test('Channel throttle: per-channel token buckets + daily bars', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Channel throttle');
    await expect(page.locator('.thr-grid')).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.thr-card').first()).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.thr-summary .kpi').first()).toBeVisible({ timeout: 15000 });
  });

  test('Dispositions: per-channel funnels of final outcomes', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Dispositions');
    await expect(page.locator('.thr-grid')).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.disp-funnel').first()).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.disp-row').first()).toBeVisible({ timeout: 15000 });
  });

  test('Validation: operational banner + grouped checks', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Validation');
    await expect(page.locator('.op-banner')).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.op-banner .op-status')).toContainText(/SYSTEM OPERATIONAL|ATTENTION NEEDED/, { timeout: 15000 });
    await expect(page.locator('.chk-row').first()).toBeVisible({ timeout: 15000 });
  });

  test('Throughput: per-layer health metrics', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Throughput');
    await expect(page.locator('.health-grid')).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.hl-card').first()).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.hl-metric').first()).toBeVisible({ timeout: 15000 });
  });

  test('Action performance: operator Suppress/Restore control renders', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Action performance');
    await expect(page.locator('.grid-table tbody tr').first()).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.op-cell button').first()).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.op-cell button').first()).toHaveText(/Suppress|Restore/);
  });

  test('Rule funnel computes member drop-off over gold (not Drools)', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Rule funnel');
    await expect(page.locator('.rf-editor')).toBeVisible();
    await expect(page.locator('.funnel-row').first()).toBeVisible({ timeout: 18000 });
  });

  test('Fact routing shows action -> fact edges', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Fact routing');
    await expect(page.locator('.route').first()).toBeVisible();
    await expect(page.locator('.chip').first()).toBeVisible();
  });

  test('Studio: list actions + open the authoring editor', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-item', { hasText: /^.?Actions$/ }).click();
    await expect(page.locator('.def-row').first()).toBeVisible({ timeout: 12000 });
    await page.getByRole('button', { name: /New action/ }).click();
    await expect(page.locator('.editor')).toBeVisible();
    await expect(page.getByText('Channels')).toBeVisible();
    await expect(page.locator('.cb').first()).toBeVisible();             // condition builder
  });

  test('Studio: content-key variant editor (＋ adds an A/B variant with rules)', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-item', { hasText: /^.?Actions$/ }).click();
    await page.getByRole('button', { name: /New action/ }).click();
    await expect(page.locator('.chan-row').first()).toBeVisible();
    // the ＋ next to a content key reveals a variant block with a % split + a fact-condition builder
    await page.locator('.chan-row button[title*="content variant"]').first().click();
    await expect(page.locator('.variants')).toBeVisible();
    await expect(page.locator('.variant-row').first()).toBeVisible();
    await expect(page.locator('.pct input')).toBeVisible();             // % random gate
    await expect(page.locator('.variant-when .cb')).toBeVisible();      // who (facts) condition builder
  });

  test('Fact library: rule-builder fact input autocompletes from gold', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-item', { hasText: /^.?Actions$/ }).click();
    await page.getByRole('button', { name: /New action/ }).click();
    const incl = page.locator('.block', { hasText: 'Inclusion' });
    await incl.locator('.cb-head button', { hasText: 'condition' }).click();   // add a condition row
    const fi = incl.locator('.fi input');
    await fi.click();
    await fi.fill('operator');
    await expect(page.locator('.fi-drop')).toBeVisible({ timeout: 15000 });      // scrollable dropdown
    await expect(page.locator('.fi-opt').first()).toBeVisible();
    const picked = await page.locator('.fi-opt .fi-key').first().innerText();
    await page.locator('.fi-opt').first().click();                              // picking fills the input
    await expect(fi).toHaveValue(picked);
  });

  test('Variants A/B: per-variant performance view renders', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Variants A/B');
    await expect(page.locator('.vp-intro')).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.vp-grid')).toBeVisible({ timeout: 15000 });   // cards (with data) or empty-state hint
  });

  test('Trace replay: find a member decision and step through it', async ({ page }) => {
    await page.goto('/');
    await go(page, 'Trace replay');
    await expect(page.locator('.trace-input')).toBeVisible();
    await page.locator('.trace-input').fill('iris');
    await page.locator('.trace-bar button.primary').click();
    // a decisions dropdown appears (and the latest decision auto-replays if there is one)
    await expect(page.locator('.replay-ctl, .trace-select')).toBeVisible({ timeout: 15000 });
  });

  test('live toggle pauses/resumes', async ({ page }) => {
    await page.goto('/');
    const pill = page.locator('.pill');
    await expect(pill).toContainText('live');
    await pill.click();
    await expect(pill).toContainText('paused');
  });
});
