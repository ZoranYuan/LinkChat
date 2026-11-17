import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [vue(), tailwindcss()],
  server: {
    port: 5173,
    watch: {
      usePolling: true,
      interval: 200,
    },
    proxy: {
      "/api": {
        target: "http://localhost:8081",
        changeOrigin: true,
        ws: true,
      },
      "/room-agent": {
        target: "http://dl_container:8000",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/room-agent/, "/api/v1"),
      },
    },
  },
});
