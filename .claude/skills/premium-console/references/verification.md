# Verification

Automated gates first, then look at the result. A console can pass every test and still show a green tile
next to an over-budget number, labels that wrap, or "unknown" filler; only screenshots catch those.

## 1. Automated gates

From the project root (layout `frontend/` + `backend/`):

```bash
<skill-dir>/scripts/verify.sh .                 # all gates, pass/fail table, exit 1 on failure
PYTHON=.venv/bin/python <skill-dir>/scripts/verify.sh .
```

or one by one:

```bash
cd frontend
npx tsc --noEmit          # strict types, unused locals/params are errors
npx vitest run            # unit tests in src/**/*.test.ts (node environment)
npm run build             # tsc + vite build into ../backend/server/static (or npx vite build)
cd ../backend
python -m pytest -q       # API + state tests; no network, no threads
```

What the unit tests should cover (template examples in `frontend/src/test/state.test.ts`,
`backend/tests/`): formatting (units, "—" for missing), hysteresis (escalate after n agreeing samples,
de-escalate at once, backfilled series), progress wording (identical on both sides), state precedence and
sticky windows, insight ranking, every API error path, auth, SPA fallback, WebSocket first message.

Without network access, reuse an existing `node_modules` by symlinking it into the project
(`ln -s /path/to/other/node_modules frontend/node_modules`) as long as it contains the pinned packages.

## 2. Run the real thing

```bash
cd frontend && npm run build                       # UI into backend/server/static
cd ../backend && APP_DATA_DIR=/tmp/console-data python -m server --port 8090
curl -s localhost:8090/api/health                  # {"ok": true, ...}
curl -s -X POST localhost:8090/api/jobs -H 'content-type: application/json' \
     -d '{"kind": "demo", "params": {"items": 9000, "rate": 120}}'   # ~75 s of live data
```

For hot reload during development: `python -m server --reload` and `npm run dev` (Vite on :5173 proxies
`/api` and the WebSocket to :8090).

Wait on a condition rather than a fixed sleep, e.g. until the job is 60% through:

```bash
until curl -s localhost:8090/api/status | python3 -c "import json,sys; c=json.load(sys.stdin)['current']; \
  s=[x for x in (c or {}).get('steps',[]) if x['id']=='process']; p=(s[0].get('progress') or {}) if s else {}; \
  sys.exit(0 if p and p['done']/p['total']>0.6 else 1)"; do sleep 2; done
```

## 3. Screenshots (Playwright)

`frontend/scripts/screenshots.mjs` captures each page in dark and light at 1440 x 940, plus the first two
pages at 390 x 844 (phone, dark). It seeds the theme in localStorage before load, waits for skeletons to
disappear (not `networkidle`: the WebSocket keeps the network busy), and fails on page errors, console
errors or horizontal overflow.

```bash
cd frontend
CONSOLE_URL=http://127.0.0.1:8090 node scripts/screenshots.mjs
CONSOLE_URL=http://127.0.0.1:8090 SHOTS_PAGES="/,/live,/jobs,/jobs/<id>,/jobs?new=1" \
  SHOTS_OUT=./screenshots SHOTS_SCALE=1 SHOTS_FULL=1 node scripts/screenshots.mjs
```

It resolves Playwright with `createRequire` (so a globally installed copy on `NODE_PATH` works) and never
runs `playwright install`; if Playwright is missing it prints how to add it. Use `deviceScaleFactor` 1 for
review (smaller files), 2 for documentation images.

Take the Live screenshots while a job runs (start one, wait for progress), and the report screenshots from
a finished job with interesting data (failures, a slow spell), so every state is exercised.

## 4. Look at them

Open every image and check, at minimum:

- Header: eyebrow, title, one-line subtitle, actions aligned right and wrapping on phone.
- Numbers: units everywhere, tabular, "—" for missing, no `NaN`, no wrapped axis ticks.
- Colour: tones match meaning (a value over budget is never green), legends present, both themes balanced.
- States: skeletons gone, empty states have an action, freshness badge on live pages, reasons in words.
- Phone: nothing cut off, tables scroll inside cards, KPI tiles in two columns, compact status pill.
- Then walk the full rubric in `quality-checklist.md`.

Fix, rebuild, re-shoot. Repeat until nothing on the list is open.

## 5. End to end (optional)

The reference console also had a Playwright end-to-end script (`ui/scripts/e2e.mjs`): open Overview, start a
short run from the create page, wait for the live page to update (map + feed), change a live control once,
open the finished report, open one record in the explorer, then visit the remaining pages, printing a
checkmark per step with timings and failing fast. Use `data-testid` on the elements such a script clicks
(`start-job`, `abort-job`, rows), and `getByRole` / `getByLabel` everywhere else.
