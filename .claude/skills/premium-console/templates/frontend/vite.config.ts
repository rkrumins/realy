/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath } from "node:url";

// Built assets are served by the FastAPI backend (backend/server/static, SPA fallback), so the UI and
// the API share one origin: no CORS, cookies just work. Docker builds set VITE_OUT_DIR=/ui/dist.
// `npm run dev` proxies /api (including the live WebSocket) to the backend on :8090.
const outDir = process.env.VITE_OUT_DIR || "../backend/server/static";
const apiTarget = process.env.API_TARGET || "http://127.0.0.1:8090";

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
        // charts are the heaviest dependency: keep them out of the entry chunk
        manualChunks(id) {
          if (id.includes("node_modules/recharts") || id.includes("node_modules/d3-")) return "charts";
          return undefined;
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: { "/api": { target: apiTarget, ws: true, changeOrigin: true } },
  },
  test: { environment: "node", include: ["src/**/*.test.ts"] },
});
