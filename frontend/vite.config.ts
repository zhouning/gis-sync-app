import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// dev server: 5173；用 proxy 把 /api 和 /ws 转给后端，避免前端代码里写绝对地址。
// 生产构建：npm run build → dist/，交给 nginx（见 docker/nginx.conf）。
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8090',
        changeOrigin: false,
      },
      '/ws': {
        target: 'ws://localhost:8090',
        ws: true,
        changeOrigin: false,
      },
      '/actuator': {
        target: 'http://localhost:8090',
        changeOrigin: false,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
});
