import vue from '@vitejs/plugin-vue'
import { defineConfig, loadEnv } from 'vite'

// The API key is bundled into the browser; use this configuration only for local development.
export default defineConfig(({ mode }) => {
  const rootEnv = loadEnv(mode, '..', '')

  return {
    envDir: '..',
    plugins: [vue()],
    define: {
      'import.meta.env.VITE_API_KEY': JSON.stringify(rootEnv.API_KEY || ''),
    },
    server: {
      proxy: {
        '/api/node-a': { target: 'http://127.0.0.1:8000', changeOrigin: true, rewrite: (path) => path.replace(/^\/api\/node-a/, '') },
        '/api/node-b': { target: 'http://127.0.0.1:8001', changeOrigin: true, rewrite: (path) => path.replace(/^\/api\/node-b/, '') },
        '/api/node-c': { target: 'http://127.0.0.1:8002', changeOrigin: true, rewrite: (path) => path.replace(/^\/api\/node-c/, '') },
      },
    },
  }
})
