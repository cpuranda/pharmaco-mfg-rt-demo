import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
// Assets are served by FastAPI from frontend/dist (index.html + /assets mount).
// base: './' keeps asset URLs relative so they resolve under the app origin.
export default defineConfig({
    plugins: [react()],
    base: './',
    build: {
        outDir: 'dist',
        emptyOutDir: true,
    },
    server: {
        port: 5173,
        proxy: {
            '/api': { target: 'http://127.0.0.1:8077', changeOrigin: true },
            '/health': { target: 'http://127.0.0.1:8077', changeOrigin: true },
            '/ready': { target: 'http://127.0.0.1:8077', changeOrigin: true },
        },
    },
});
