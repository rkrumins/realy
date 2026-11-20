#!/usr/bin/env node
/**
 * Screenshots of console pages in dark + light (desktop) and dark at phone width, for review and docs.
 * Uses the preinstalled Playwright/Chromium if available (never runs `playwright install` itself).
 *
 *   CONSOLE_URL=http://127.0.0.1:8090 node scripts/screenshots.mjs                  (backend serving the build)
 *   CONSOLE_URL=http://127.0.0.1:5173 node scripts/screenshots.mjs                  (vite dev, proxying the API)
 *   SHOTS_PAGES="/,/live,/jobs" SHOTS_OUT=./shots SHOTS_SCALE=1 node scripts/screenshots.mjs
 *
 * Env: SHOTS_PAGES (comma-separated paths), SHOTS_OUT (dir, default ./screenshots), SHOTS_SCALE
 * (deviceScaleFactor, default 1), SHOTS_FULL=1 (full-page), THEME_KEY (localStorage key, default
 * "<slug>.theme" read from src/app.config.ts when present).
 */
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
let chromium;
try {
  ({ chromium } = require("playwright")); // resolved via NODE_PATH or a local install
} catch {
  console.error("playwright is not installed: npm i -D playwright@<pinned> && npx playwright install chromium");
  process.exit(2);
}

const here = dirname(fileURLToPath(import.meta.url));
const BASE = (process.env.CONSOLE_URL || "http://127.0.0.1:8090").replace(/\/$/, "");
const OUT = resolve(process.env.SHOTS_OUT || "screenshots");
const SCALE = Number(process.env.SHOTS_SCALE || 1);
const FULL = process.env.SHOTS_FULL === "1";
const PAGES = (process.env.SHOTS_PAGES || "/,/live,/jobs,/environment,/settings").split(",").map((s) => s.trim()).filter(Boolean);

function themeKey() {
  if (process.env.THEME_KEY) return process.env.THEME_KEY;
  const cfg = resolve(here, "../src/app.config.ts");
  const m = existsSync(cfg) ? /slug:\s*"([^"]+)"/.exec(readFileSync(cfg, "utf8")) : null;
  return `${m ? m[1] : "operator-console"}.theme`;
}
const KEY = themeKey();
mkdirSync(OUT, { recursive: true });

const browser = await chromium.launch();
const errors = [];

async function shoot(path, theme, { width = 1440, height = 940, mobile = false, suffix = theme } = {}) {
  const ctx = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: SCALE, isMobile: mobile, hasTouch: mobile, colorScheme: theme });
  await ctx.addInitScript(([k, t]) => { try { localStorage.setItem(k, t); } catch { /* ignore */ } }, [KEY, theme]);
  const page = await ctx.newPage();
  page.on("pageerror", (e) => errors.push(`${path} [${suffix}] pageerror: ${e.message}`));
  page.on("console", (m) => { if (m.type() === "error") errors.push(`${path} [${suffix}] console: ${m.text()}`); });
  await page.goto(BASE + path, { waitUntil: "domcontentloaded" });
  // live pages keep a WebSocket open, so "networkidle" may never come: wait for skeletons instead
  try { await page.waitForLoadState("networkidle", { timeout: 5000 }); } catch { /* websocket keeps it busy */ }
  try { await page.waitForFunction(() => document.querySelectorAll(".skeleton").length === 0, null, { timeout: 8000 }); } catch { /* some are intentional */ }
  await page.waitForTimeout(900);
  // horizontal overflow is a layout bug at any width
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  if (overflow > 1) errors.push(`${path} [${suffix}] horizontal overflow: ${overflow}px`);
  const name = (path === "/" ? "overview" : path.replace(/^\//, "").replace(/[/?=&]+/g, "-")) + `-${suffix}.png`;
  await page.screenshot({ path: `${OUT}/${name}`, fullPage: FULL });
  await ctx.close();
  console.log(`saved ${OUT}/${name}`);
}

for (const path of PAGES) {
  await shoot(path, "dark");
  await shoot(path, "light");
}
for (const path of PAGES.slice(0, 2)) await shoot(path, "dark", { width: 390, height: 844, mobile: true, suffix: "phone" });

await browser.close();
if (errors.length) {
  console.error(`\n${errors.length} problem(s):\n` + errors.map((e) => `  - ${e}`).join("\n"));
  process.exit(1);
}
console.log("\nno page errors, no horizontal overflow");
