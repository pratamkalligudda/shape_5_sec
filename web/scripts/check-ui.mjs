// Headless Edge against the running stack (scripts/run-local.ps1). Usage: node scripts/check-ui.mjs [screenshot.png]
import { chromium } from 'playwright-core';

const browser = await chromium.launch({ channel: 'msedge', headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
const consoleErrors = [];
page.on('console', m => m.type() === 'error' && consoleErrors.push(m.text()));

// The header renders Start until the first GET /live-session answers; decide only after it has.
const firstStatus = page.waitForResponse(r => r.url().endsWith('/live-session') && r.request().method() === 'GET');
await page.goto('http://localhost:5173');
await firstStatus;
await page.waitForTimeout(500);
const start = page.getByRole('button', { name: 'Start' });
if (await start.isVisible({ timeout: 5000 }).catch(() => false)) await start.click();
await page.getByText('● Live').waitFor({ timeout: 15000 });
await page.waitForTimeout(16000);    // three batches

const lines = await page.locator('.recharts-line path').count();
const violations = (await page.getByTestId('violation-count').textContent())?.trim();
const batches = (await page.locator('.facts dd').first().textContent())?.trim();
await page.screenshot({ path: process.argv[2] ?? 'live-mode.png', fullPage: true });
await browser.close();

console.log({ lines, batches, violations, consoleErrors });
if (lines === 0 || violations !== '0' || Number(batches) < 2 || consoleErrors.length > 0) process.exit(1);
