/// <reference types="vitest" />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    // sniffer.html: the real dev negotiator, proxied so the browser sees no cross-origin request.
    proxy: {
      '/neg': {
        target: 'https://shapeiotdfunc-socket-vlbtul.azurewebsites.net',
        changeOrigin: true,
        rewrite: path => path.replace(/^\/neg/, ''),
      },
    },
  },
  test: { environment: 'node', include: ['src/**/*.test.ts'] },
});
