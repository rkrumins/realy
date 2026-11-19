import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Built assets are served by the FastAPI backend (python/realy/control/static).
// `npm run dev` proxies the API (and the live WebSocket) to a console on :8090.
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: { outDir: "../python/realy/control/static", emptyOutDir: true, chunkSizeWarningLimit: 800 },
  server: {
    port: 5173,
    proxy: { "/api": { target: "http://localhost:8090", ws: true, changeOrigin: true } },
  },
});
