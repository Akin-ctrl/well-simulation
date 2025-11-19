/// <reference types="vite/client" />
import { reactRouter } from '@react-router/dev/vite';
import tailwindcss from '@tailwindcss/vite';
import { reactRouterDevTools } from 'react-router-devtools';
import { defineConfig } from 'vite';
import svgr from 'vite-plugin-svgr';
import tsconfigPaths from 'vite-tsconfig-paths';
import { SERVER_BASE_URL } from '@corsight/utils/configs';

export default defineConfig({
  build: {
    outDir: 'dist',
  },
  plugins: [
    reactRouterDevTools(),
    reactRouter(),
    tailwindcss(),
    tsconfigPaths(),
    svgr(),
  ],
  server: {
    proxy: {
      '/api': {
        target: SERVER_BASE_URL,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
});
