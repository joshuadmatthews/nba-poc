import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// base './' so assets load regardless of mount path. In prod, nginx proxies /graphql
// to the BFF; in dev, this proxy does the same to localhost:4000.
export default defineConfig({
  plugins: [react()],
  base: './',
  server: { proxy: {
    '/graphql': 'http://127.0.0.1:4000',
    '/topology': 'http://127.0.0.1:4000',
    '/recent': 'http://127.0.0.1:4000',
    '/livestats': 'http://127.0.0.1:4000',
    '/stream': { target: 'http://127.0.0.1:4000', changeOrigin: true },
  } },
});
