// Shared helpers for the Playwright scripts (e2e + screenshots). Chromium is preinstalled; never `playwright install`.
import { createRequire } from "node:module";

// Resolve the preinstalled Playwright the CommonJS way (it is on NODE_PATH, which ESM imports ignore).
const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

export const BASE = process.env.CONSOLE_URL || "http://127.0.0.1:5173";

export async function browser() {
  return chromium.launch();
}

export async function newPage(b, { theme = "dark", width = 1440, height = 900, scale = 1, mobile = false } = {}) {
  const ctx = await b.newContext({
    viewport: { width, height }, deviceScaleFactor: scale, isMobile: mobile, hasTouch: mobile,
    colorScheme: theme,
  });
  await ctx.addInitScript((t) => { try { localStorage.setItem("realy.theme", t); } catch { /* ignore */ } }, theme);
  const page = await ctx.newPage();
  page.on("pageerror", (e) => console.error(`[pageerror] ${e.message}`));
  page.on("console", (m) => { if (m.type() === "error") console.error(`[console] ${m.text()}`); });
  return page;
}

/** Wait until skeletons are gone and the network is quiet (live pages keep a WebSocket open). */
export async function settle(page, ms = 600) {
  await page.waitForLoadState("domcontentloaded");
  try { await page.waitForLoadState("networkidle", { timeout: 6000 }); } catch { /* websocket keeps it busy */ }
  try { await page.waitForFunction(() => document.querySelectorAll(".skeleton").length === 0, null, { timeout: 8000 }); } catch { /* some skeletons are intentional */ }
  await page.waitForTimeout(ms);
}

export async function api(path, init) {
  const r = await fetch(BASE + path, { ...init, headers: { "Content-Type": "application/json", ...(init?.headers || {}) } });
  if (!r.ok) throw new Error(`${path}: ${r.status} ${await r.text()}`);
  return r.json();
}
