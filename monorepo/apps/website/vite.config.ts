/// <reference types="vite/client" />
import { reactRouter } from '@react-router/dev/vite';
import tailwindcss from '@tailwindcss/vite';
import { reactRouterDevTools } from 'react-router-devtools';
import { defineConfig } from 'vite';
import svgr from 'vite-plugin-svgr';
import tsconfigPaths from 'vite-tsconfig-paths';

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
});
