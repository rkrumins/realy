#!/usr/bin/env node
/**
 * Screenshots of the topology and infrastructure views (dark, light, phone): Topologies (cards, matrix,
 * detail), the Launch explainer with its preflight, the Environment preflight. Checks for page errors and
 * horizontal overflow on every shot.
 *   CONSOLE_URL=http://127.0.0.1:8090 SHOTS_OUT=/tmp/shots node scripts/shoot-topologies.mjs
 */
import { mkdirSync } from "node:fs";
import { BASE, browser, newPage, settle } from "./shoot-lib.mjs";

const OUT = process.env.SHOTS_OUT || "/tmp/shots";
mkdirSync(OUT, { recursive: true });
const b = await browser();
let problems = 0;

async function shoot(name, theme, path, { width = 1440, height = 940, mobile = false, full = false, prepare } = {}) {
  const page = await newPage(b, { theme, width, height, scale: 1, mobile });
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(BASE + path);
  await settle(page, 900);
  if (prepare) await prepare(page);
  await page.waitForTimeout(500);
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  if (overflow > 1) { problems++; console.error(`[overflow] ${name}: ${overflow}px`); }
  if (errors.length) { problems++; console.error(`[pageerror] ${name}: ${errors.join(" | ")}`); }
  await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: full });
  await page.context().close();
  console.log(`shot ${name}`);
}

await shoot("topologies-dark", "dark", "/topologies?need=multi_message,owned_features_exact", { full: true });
await shoot("topologies-light", "light", "/topologies?need=answers_caller", { full: true });
await shoot("topologies-detail-dark", "dark", "/topologies?t=gateway-kafka", {
  prepare: async (p) => {
    await p.getByLabel("with multi-message transactions").check().catch(() => {});
    await p.locator("#detail").scrollIntoViewIfNeeded();
    await p.getByRole("tab", { name: /Advice from your runs/ }).click().catch(() => {});
    await p.waitForTimeout(400);
  },
});
await shoot("topologies-settings-dark", "dark", "/topologies?t=flink-inline", {
  prepare: async (p) => {
    await p.locator("#detail").scrollIntoViewIfNeeded();
    await p.getByRole("tab", { name: /Settings by component/ }).click().catch(() => {});
  },
});
await shoot("topologies-external-light", "light", "/topologies?t=external", {
  prepare: async (p) => { await p.locator("#detail").scrollIntoViewIfNeeded(); },
});
await shoot("topologies-mobile", "dark", "/topologies?need=multi_message", { width: 390, height: 844, mobile: true });
await shoot("launch-explainer-dark", "dark", "/launch?scenario=flows-smoke", {
  prepare: async (p) => {
    await p.getByRole("button", { name: /Check infrastructure/ }).click().catch(() => {});
    await p.waitForSelector("[data-testid=preflight-summary]", { timeout: 15000 }).catch(() => {});
  },
});
await shoot("environment-preflight-light", "light", "/environment", {
  prepare: async (p) => { await p.waitForSelector("[data-testid=preflight-card] li", { timeout: 15000 }).catch(() => {}); },
});
await shoot("environment-mobile", "dark", "/environment", { width: 390, height: 844, mobile: true });
await b.close();
if (problems) { console.error(`${problems} problem(s)`); process.exit(1); }
