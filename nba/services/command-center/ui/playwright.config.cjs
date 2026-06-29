const { defineConfig } = require('@playwright/test');

// CommonJS (the ui package is type:module, and this host's Node < 18.19 can't load an ESM
// Playwright config). E2E against the running dev server (vite :5173, BFF :4000).
// Software-GL launch args so it runs on headless hosts without a real GPU.
module.exports = defineConfig({
  testDir: './e2e',
  timeout: 30000,
  expect: { timeout: 12000 },
  fullyParallel: false,
  retries: 1,
  reporter: [['list']],
  use: {
    baseURL: process.env.E2E_BASE_URL || 'http://localhost:5173',
    headless: true,
    launchOptions: { args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--use-gl=swiftshader', '--in-process-gpu'] },
  },
  projects: [{ name: 'chromium', use: { browserName: 'chromium' } }],
});
