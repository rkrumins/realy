#!/usr/bin/env node
/**
 * Screenshots of every console page (dark + light, 1440 px wide, deviceScaleFactor 2) plus Live and
 * Overview at 390 px, written to docs/img/. Launches one short smoke run so the Live page has data.
 *
 *   CONSOLE_URL=http://127.0.0.1:5173 node scripts/screenshots.mjs
 *   SHOTS_NO_RUN=1 ...            skip the live run (Live shows its idle state)
 *   SHOTS_REPORT=<run id> ...     run used for the report/explorer/compare shots (default: newest verified PASS)
 */
import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { api, BASE, browser, newPage, settle } from "./shoot-lib.mjs";

const OUT = process.env.SHOTS_OUT || resolve(dirname(fileURLToPath(import.meta.url)), "../../docs/img");
mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const W = 1440, H = 940;

const runs = (await api("/api/runs?limit=200")).history.filter((r) => r.status === "done");
const pick = (pred) => runs.find(pred)?.run_id;
const reportRun = process.env.SHOTS_REPORT || pick((r) => r.verdict === "PASS" && r.verify_passed) || pick((r) => r.verdict === "PASS") || runs[0]?.run_id;
const compareB = pick((r) => r.run_id !== reportRun && r.verdict === "PASS" && r.topology !== runs.find((x) => x.run_id === reportRun)?.topology) || runs[1]?.run_id;
console.log(`report run: ${reportRun} · compare with: ${compareB}`);

const b = await browser();
const shots = [];

async function shoot(name, theme, path, { prepare, width = W, height = H, mobile = false, full = false } = {}) {
  const page = await newPage(b, { theme, width, height, scale: 2, mobile });
  await page.goto(BASE + path);
  await settle(page, 900);
  if (prepare) await prepare(page);
  await page.waitForTimeout(400);
  const file = `${OUT}/${name}.png`;
  await page.screenshot({ path: file, fullPage: full });
  await page.context().close();
  shots.push(file);
  console.log(`📸 ${name}.png`);
}

// ---------------------------------------------------------------- live (needs a running pipeline)
if (!process.env.SHOTS_NO_RUN) {
  const st = await api("/api/status");
  if (!st.current) {
    await api("/api/runs", { method: "POST", body: JSON.stringify({ scenario: "smoke", overrides: { traffic: { duration_s: 40 } } }) });
    console.log("launched smoke (40 s) for the Live screenshots");
  }
  for (let i = 0; i < 90; i++) {
    const s = await api("/api/status");
    if (s.current?.phase === "MEASURE" || s.current?.phase === "measure") break;
    await sleep(1000);
  }
  await sleep(20_000); // let the charts and the decision feed fill
  await shoot("live-dark", "dark", "/live");
  await shoot("live-light", "light", "/live");
  await shoot("live-mobile", "dark", "/live", { width: 390, height: 844, mobile: true });
  for (let i = 0; i < 150; i++) {
    if (!(await api("/api/status")).current) break;
    await sleep(2000);
  }
  await sleep(3000);
} else {
  await shoot("live-dark", "dark", "/live");
  await shoot("live-light", "light", "/live");
  await shoot("live-mobile", "dark", "/live", { width: 390, height: 844, mobile: true });
}

// ---------------------------------------------------------------- everything else
// the explorer shot uses the newest run (full spans with clock domains); report/compare the verified PASS run
const latest = (await api("/api/runs?limit=5")).history.find((r) => r.status === "done")?.run_id || reportRun;
const pages = [
  ["overview", "/"],
  ["launch", "/launch?scenario=mixed-realistic", async (p) => {
    await p.getByRole("button", { name: "Budget check" }).click();
    await p.getByText("Monte Carlo").first().waitFor({ timeout: 30_000 }).catch(() => {});
  }],
  ["runs", "/runs"],
  ["report", `/runs/${reportRun}`],
  ["explorer", `/explorer?run=${latest}`, async (p) => {
    await p.getByTestId("search-row").first().click({ timeout: 30_000 });
    await p.getByTestId("decision-detail").waitFor({ timeout: 30_000 });
    await p.waitForTimeout(900);
  }],
  ["compare", `/compare?a=${reportRun}&b=${compareB}`],
  ["scenarios", "/scenarios?name=step-ramp"],
  ["environment", "/environment"],
  ["docs", "/docs?path=architecture.md"],
];
for (const theme of ["dark", "light"]) {
  for (const [name, path, prepare] of pages) await shoot(`${name}-${theme}`, theme, path, { prepare });
}
await shoot("overview-mobile", "dark", "/", { width: 390, height: 844, mobile: true });

await b.close();
console.log(`\n${shots.length} screenshots in ${OUT}`);
