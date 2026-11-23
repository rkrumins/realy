// Screenshots of how a run's times show: Live (lifecycle bar), the runs list and the report.
//   CONSOLE_URL=http://127.0.0.1:8090 node scripts/shoot-timing.mjs <out-dir> <tag> [run-id]
// Each page in dark and light at desktop width, and Live and the report at phone width. Fails on page
// errors or horizontal overflow at phone width.
import { mkdirSync } from "node:fs";
import { BASE, browser, newPage, settle } from "./shoot-lib.mjs";

const [out = "shots", tag = "t", runId] = process.argv.slice(2);
mkdirSync(out, { recursive: true });
const pages = [["live", "/live"], ["runs", "/runs"]];
if (runId) pages.push(["report", `/runs/${encodeURIComponent(runId)}`]);

const b = await browser();
let errors = 0;
for (const theme of ["dark", "light"]) {
  for (const [name, path] of pages) {
    const page = await newPage(b, { theme });
    page.on("pageerror", () => { errors += 1; });
    await page.goto(BASE + path);
    await settle(page, 1200);
    await page.screenshot({ path: `${out}/${tag}-${name}-${theme}.png`, fullPage: name !== "live" });
    await page.context().close();
  }
}
for (const [name, path] of pages.filter(([n]) => n !== "runs")) {
  const page = await newPage(b, { theme: "dark", width: 390, height: 844, mobile: true });
  page.on("pageerror", () => { errors += 1; });
  await page.goto(BASE + path);
  await settle(page, 1200);
  const over = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  if (over > 1) { console.error(`${name}: ${over}px horizontal overflow at phone width`); errors += 1; }
  await page.screenshot({ path: `${out}/${tag}-${name}-phone.png` });
  await page.context().close();
}
await b.close();
console.log(errors ? `FAIL: ${errors} problem(s)` : "ok");
process.exit(errors ? 1 : 0);
