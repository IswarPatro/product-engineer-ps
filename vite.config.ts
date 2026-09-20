import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// The client lives in src/client; /api is proxied to the Node service in dev.
export default defineConfig({
  root: 'src/client',
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { '/api': { target: 'http://localhost:8787', changeOrigin: true } },
  },
  build: { outDir: '../../dist/client', emptyOutDir: true },
});
