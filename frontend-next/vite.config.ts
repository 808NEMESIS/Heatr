import path from 'node:path';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { '@': path.resolve(__dirname, './src') },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8001',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ''),
      },
      // A0 same-origin shell (Sprint 5): serveer Warmr's bestaande app onder
      // dezelfde origin op /warmr/*. Warmr's FastAPI (:8000) mount zijn static
      // frontend op '/', dus strippen we het /warmr-prefix bij het doorsturen.
      // Same-origin ⇒ gedeelde Supabase-sessie in localStorage ⇒ één login.
      // (Prod: één reverse-proxy doet exact dit onder één domein.)
      '/warmr': {
        target: process.env.VITE_WARMR_ORIGIN || 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/warmr/, '') || '/',
      },
    },
  },
});
