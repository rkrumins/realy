/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath } from "node:url";

// Built assets are served by the FastAPI backend (python/realy/control/static, SPA fallback).
// Docker builds set VITE_OUT_DIR=/ui/dist. `npm run dev` proxies the API + live WebSocket to :8090.
const outDir = process.env.VITE_OUT_DIR || "../python/realy/control/static";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: "/",
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  build: {
    outDir,
    emptyOutDir: true,
    chunkSizeWarningLimit: 1600,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("node_modules/recharts") || id.includes("node_modules/d3-")) return "charts";
          if (id.includes("node_modules/react-markdown") || id.includes("node_modules/remark") ||
              id.includes("node_modules/micromark") || id.includes("node_modules/mdast") ||
              id.includes("node_modules/minisearch")) return "markdown";
          return undefined;
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: { "/api": { target: "http://127.0.0.1:8090", ws: true, changeOrigin: true } },
  },
  test: { environment: "node", include: ["src/**/*.test.ts"] },
});
