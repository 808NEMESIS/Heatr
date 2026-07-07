import path from 'node:path';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // A0 same-origin shell (Sprint 5): Warmr blijft op de origin-root (het gaat
  // uit van '/'), Heatr verhuist onder /heatr/*. Same-origin ⇒ gedeelde
  // Supabase-sessie in localStorage ⇒ één login voor beide.
  base: '/heatr/',
  resolve: {
    alias: { '@': path.resolve(__dirname, './src') },
  },
  server: {
    port: 5173,
    proxy: {
      // Heatr-backend. Origin-absoluut (/api), buiten de /heatr-base — de
      // catch-all hieronder sluit /api expliciet uit.
      '/api': {
        target: 'http://localhost:8001',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ''),
      },
      // Alles wat NIET /heatr (Vite: app + HMR + modules) of /api is → Warmr op
      // de origin-root (:8000). Zo kloppen Warmr's absolute paden (server-307
      // → /index.html, app.js requireAuth/logout → /index.html) vanzelf, want
      // Warmr draait echt op root. (Prod: één reverse-proxy doet exact dit.)
      '^(?!/heatr|/api)': {
        target: process.env.VITE_WARMR_ORIGIN || 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
});
