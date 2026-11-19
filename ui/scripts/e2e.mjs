#!/usr/bin/env node
/**
 * End-to-end check of the console against a real backend + pipeline:
 *   Overview → launch a 40 s smoke run from Launch → Live updates (topology + decision feed) → change TPS
 *   once → finished report → open a decision in Explorer → Environment, Docs, Scenarios builder.
 *
 *   CONSOLE_URL=http://127.0.0.1:5173 node scripts/e2e.mjs        (vite preview/dev proxying :8090)
 *   CONSOLE_URL=http://127.0.0.1:8090 node scripts/e2e.mjs        (console serving the built UI)
 */
import { api, BASE, browser, newPage, settle } from "./shoot-lib.mjs";

const DURATION_S = Number(process.env.E2E_DURATION_S || 40);
const steps = [];
const t0 = Date.now();
function ok(name, detail = "") {
  steps.push({ name, ok: true, detail });
  console.log(`✔ ${name}${detail ? ` — ${detail}` : ""} (${((Date.now() - t0) / 1000).toFixed(0)}s)`);
}
async function step(name, fn) {
  try {
    const d = await fn();
    ok(name, typeof d === "string" ? d : "");
  } catch (e) {
    steps.push({ name, ok: false, detail: e.message });
    console.error(`✘ ${name}: ${e.message}`);
    throw e;
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const b = await browser();
const page = await newPage(b, { theme: "dark", width: 1440, height: 900 });
let runId = null;
let failed = false;

try {
  const st = await api("/api/status");
  if (st.current) throw new Error(`a run is already active (${st.current.scenario}); wait for it to finish`);

  await step("Overview renders", async () => {
    await page.goto(BASE + "/");
    await page.getByRole("heading", { name: "Fraud pipeline benchmark" }).waitFor();
    await settle(page);
    return `${await page.locator("tbody tr").count()} recent runs listed`;
  });

  await step(`Launch smoke (${DURATION_S}s) from the Launch page`, async () => {
    await page.goto(BASE + "/launch?scenario=smoke");
    await page.getByTestId("scenario-smoke").waitFor();
    await page.getByLabel("Duration").fill(String(DURATION_S));
    await page.getByTestId("launch-button").click();
    await page.waitForURL(/\/live$/, { timeout: 15_000 });
    for (let i = 0; i < 60 && !runId; i++) {
      const s = await api("/api/status");
      runId = s.current?.run_id ?? null;
      if (!runId) await sleep(1000);
    }
    if (!runId) throw new Error("the run did not start within 60 s");
    return runId;
  });

  await step("Live: KPIs, topology and charts update", async () => {
    await page.getByText("Pipeline topology").waitFor();
    // emitted TPS KPI becomes numeric once the emitter publishes
    await page.waitForFunction(() => {
      const tiles = [...document.querySelectorAll("div")].filter((d) => d.textContent?.trim().startsWith("Emitted"));
      return tiles.some((t) => /\d/.test(t.querySelector(".num")?.textContent || ""));
    }, null, { timeout: 90_000 });
    // a topology edge or node shows a live rate ("…/s")
    await page.waitForFunction(() => [...document.querySelectorAll("svg[role=img] text")].some((t) => /\d.*\/s/.test(t.textContent || "")), null, { timeout: 60_000 });
    return "emitted TPS + topology rates live";
  });

  await step("Live: decision feed streams decisions", async () => {
    await page.getByTestId("decision-feed").locator("li").first().waitFor({ timeout: 60_000 });
    return `${await page.getByTestId("decision-feed").locator("li").count()} decisions visible`;
  });

  await step("Live: change the target rate once", async () => {
    await page.getByTestId("open-controls").click();
    const field = page.getByLabel("Exact TPS");
    await field.fill("650");
    await page.getByTestId("apply-rate").click();
    await page.getByText("Target rate → 650 TPS").waitFor({ timeout: 10_000 });
    await page.keyboard.press("Escape");
    return "650 TPS published";
  });

  await step("Run finishes and the report is written", async () => {
    const deadline = Date.now() + (DURATION_S + 150) * 1000;
    for (;;) {
      const s = await api("/api/status");
      if (!s.current) break;
      if (Date.now() > deadline) throw new Error(`still ${s.current.phase} after the deadline`);
      await sleep(3000);
    }
    let detail = null;
    for (let i = 0; i < 20; i++) {
      detail = await api(`/api/runs/${runId}`);
      if (detail.report) break;
      await sleep(2000);
    }
    if (!detail?.report) throw new Error("no report.json");
    return `verdict ${detail.report.summary.verdict}, p99 ${detail.report.summary.e2e_p99_ms?.toFixed(1)} ms, ${detail.report.summary.decided} decided`;
  });

  await step("Report page shows the verdict, step waterfall and timeline", async () => {
    await page.goto(BASE + "/runs");
    await page.locator("tbody tr").first().click();
    await page.waitForURL(new RegExp(`/runs/${runId}`));
    await page.getByText("Latency by step").waitFor();
    await page.getByText("Throughput funnel").waitFor();
    const verdict = await page.locator("h1").first().textContent();
    return `report for ${verdict}`;
  });

  await step("Explorer opens a decision with spans and features", async () => {
    await page.goto(`${BASE}/explorer?run=${runId}`);
    await page.getByTestId("search-row").first().click({ timeout: 30_000 });
    const detail = page.getByTestId("decision-detail");
    await detail.waitFor({ timeout: 30_000 });
    await detail.getByText("Span waterfall").waitFor();
    await detail.getByText(/^Features \(\d+\)/).waitFor();
    return (await detail.locator("h2").first().textContent()) ?? "";
  });

  await step("Environment lists healthy components", async () => {
    await page.goto(BASE + "/environment");
    await page.getByTestId("env-kafka").waitFor({ timeout: 20_000 });
    await page.waitForTimeout(500);
    const up = await page.evaluate(() => [...document.querySelectorAll('[data-testid^="env-"]')]
      .filter((c) => [...c.querySelectorAll("span")].some((x) => x.textContent?.trim() === "up")).length);
    if (!up) throw new Error("no component reports up");
    return `${up} components up`;
  });

  await step("Docs render and search works", async () => {
    await page.goto(BASE + "/docs");
    await page.getByTestId("doc-content").waitFor({ timeout: 20_000 });
    await page.getByTestId("docs-search").fill("latency budget");
    await page.getByText("Latency budget", { exact: false }).first().waitFor({ timeout: 20_000 });
    await page.getByRole("button", { name: "Scenario schema (live)" }).click();
    await page.getByRole("heading", { name: "Scenario schema" }).waitFor();
    return "markdown + search + generated schema";
  });

  await step("Scenario builder syncs form → YAML", async () => {
    await page.goto(BASE + "/scenarios?name=smoke");
    await page.getByText("Rate profile").waitFor({ timeout: 20_000 });
    await page.getByLabel("rate").first().fill("750");
    await page.getByRole("tab", { name: "YAML" }).click();
    const yaml = await page.getByTestId("yaml-editor").inputValue();
    if (!/tps: 750/.test(yaml)) throw new Error("YAML did not pick up the form change");
    await page.getByRole("tab", { name: "Model" }).click();
    await page.getByText("estimated capacity").waitFor();
    return "tps: 750 in YAML (not saved)";
  });
} catch {
  failed = true;
} finally {
  await b.close();
}

const passed = steps.filter((s) => s.ok).length;
console.log(`\n${passed}/${steps.length} steps passed${runId ? ` · run ${runId}` : ""}`);
process.exit(failed ? 1 : 0);
