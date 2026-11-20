# Page patterns

Seven page shapes cover almost every operator console. Each lists its layout, the data it needs, and the
states it must handle. Templates implement Overview, Live, list + detail (Jobs / JobDetail, the detail being
a report page), Environment (health) and Settings; Explorer and Compare are described from the reference
console so you can build them with the same kit.

## Contents
1. Overview (dashboard)
2. Live monitoring
3. List + detail
4. Report page with section anchors
5. Explorer / search
6. Settings (and Environment / health)
7. Compare
8. Launch / create forms

Common anatomy: `PageHeader` (eyebrow, title, subtitle that says what the page is for, actions) → content.
Routes are lazy (`React.lazy` per page) with a skeleton fallback shaped like a page.

## 1. Overview (dashboard) — `pages/Overview.tsx`

Question it answers: "Is anything running, how did recent work go, is everything healthy?"

1. Header actions: secondary "Live view", gradient primary "New …".
2. **Running-now hero** (only while something runs): accent-tinted gradient card, live dot + phase, entity
   name, active step line, two live numbers with units, a sparkline, "Open live view →". The whole card is a link.
3. **KPI row** (5 tiles): totals and rates with denominators in `sub` ("2 done · 0 failed", "12 finished",
   "budget 120 ms"). Tone only on judged values. Skeleton tiles while loading.
4. Two-column grid (`xl:grid-cols-[minmax(0,1.65fr)_minmax(0,1fr)]`):
   - left: **Recent items table** (status badge, mono name + id, key numbers right-aligned, relative time),
     row click → detail, empty state with a "start your first …" action;
   - right: **Environment summary** (dot, name, version, probe latency or state per dependency, "Details →")
     and a **trend chart** (bars per recent item coloured by verdict, log scale for latency, a legend).
5. Refresh: summary every 10 s; live parts from the WebSocket.

Backend: one `/api/summary` endpoint returning totals + last N (light rows) + current, so the page needs
one request. The reference also fell back to computing the summary from the list endpoint on a 404.

## 2. Live monitoring — `pages/Live.tsx`

Question: "What is the running thing doing right now, and is it healthy?"

1. Header: eyebrow `● Live · <phase>` (pulsing), mono title = what runs, subtitle = id + freshness badge
   ("live", "reconnecting · polling every 3 s", "live · last sample 12 s ago"). Actions: time window
   `Segmented` (1m / 5m / 10m), Controls drawer or Abort (`danger`), "Report" when idle, primary "New …".
2. **Progress bar** under the header: elapsed vs planned, gradient fill, `role="progressbar"`. The reference
   also tinted the warm-up share of the bar.
3. Banners: abort requested (warn), resource held by someone else (info, with an action).
4. **KPI strip** (5-6 tiles): throughput (mean of last 5 s), p99 vs budget (tone with hysteresis), p50 as a
   share of budget, errors per second, backlog / queue. Every `sub` names the window and unit.
5. **StepChecklist** while preparing (auto-collapses once the main work runs).
6. **State map**: component tiles (or a topology diagram) with explicit states and a legend. The reference
   drew an SVG pipeline map: nodes on a column/row grid, edges animated at a speed proportional to rate,
   node colour = explicit state if not ready, else share of the latency budget, with hysteresis.
7. **Charts** (2 columns, shared `syncId` crosshair): throughput funnel (stages as lines), latency
   percentiles with budget line + good band + phase markers, stacked per-stage latency, small charts for
   secondary signals (backlog, lag).
8. **Feed + log** (2 columns): sampled live events (filter chips, pause, click → detail) and the log console.
9. Idle: keep the page useful: show the last telemetry seen ("Nothing is running. Showing the last
   telemetry seen (<id>)"), a report button, and an EmptyState with the primary action when there is none.
10. When the running item finishes: invalidate list queries and toast "Finished" with an "Open report" action.

Controls drawer (reference): right-side `Dialog` with tabs per subsystem; sliders commit on release;
quick-pick buttons (0.5× 1× 2×); each Apply is a mutation with a toast naming what changed and where it
was published; a red-tinted "Abort this run" block at the bottom explaining what abort does.

## 3. List + detail — `pages/Jobs.tsx`, `pages/JobDetail.tsx`

List: header with the primary "New …" action; `Tabs` bound to `?tab=` (History · Queue, with counts);
toolbar: search input (icon inside), `Select` filters, "Clear filters", a result count on the right; a card
with a wide table (`min-w-[860px]` inside `overflow-x-auto`); two different empty states ("No jobs yet" with
an action vs "No jobs match" with a clear-filters hint). Queue tab: running + queued rows with
Abort / Cancel; selection checkboxes and bulk actions when needed (the reference had "Abort selected",
"Cancel all queued", and selecting two rows to compare). Poll every 5 s.

Create: a dialog opened by `?new=1` (so Overview can deep-link to it), form generated from a backend
registry of kinds and parameters (labels, units, min/max, hints); validation errors come back as 400 with a
message and show in a toast.

Detail: see the report pattern below. While the item is queued or running, poll every 2 s and show "Watch
live" / "Abort"; once final, stop polling and show "Run again" + downloads.

## 4. Report page with section anchors — `pages/JobDetail.tsx`

Order matters: verdict before detail.

1. `BackLink` → **Hero** (status badge + verdict badge, mono title, meta line, reasons/error panel, actions,
   gradient tinted by verdict).
2. Jump bar of section chips (recommended addition; the reference used anchors from insight cards only).
3. **Summary**: KPI grid (4-8 tiles) with judged tones and explanations.
4. **Insights**: ranked findings, each linking to the section with its evidence (`#timeline`, `#steps`, `#log`).
5. **Over time**: charts from the stored per-second series (same `TimeChart`s as the live page, `secs`
   x-axis, phase markers).
6. Domain sections, each a `SectionTitle id=… ` + cards: breakdown tables with `MiniBar`s, per-category cards,
   load curves with the knee marked, verification panels with parity numbers, honesty cards (in-sample vs
   out-of-sample, assumptions editable inline and recomputed live).
7. **Steps** (timings), **Parameters** (labels and units from the registry), **Log** (pre block, max height).
8. "No result yet" EmptyState while it runs; "Not verified" EmptyState that says how to get verification.

All sections get `scroll-mt-20` so anchors land below the sticky top bar.

## 5. Explorer / search (reference: `pages/Explorer.tsx`)

Question: "Show me this one event / entity and why it came out that way."

1. Toolbar: a `Select` for the dataset (run), a search form with a typed query syntax in the placeholder
   (`id`, `owner:123`, `status:failed`), a Search button. All of it lives in the URL (`run`, `q`, `item`,
   `owner`, `tab`) via a small `useParam` hook, so every view is a shareable link.
2. Results table with a sticky header (`sticky top-0`), shrinking its max height when a detail is open;
   cells that are entities are buttons opening that entity's panel.
3. Detail panel below the table: a header card (badges, mono title, the headline number large on the right,
   close button) split into two columns of `Stat` rows; then explanation cards ("why this result": rules
   with the values they saw, attribution bars diverging from a centre line, a timeline/waterfall).
4. Entity panel (an owner, a host...): scatter of its events coloured by outcome with a legend, click a point to
   open it.
5. Tabs for other indexes (groups of related events), each a table + timeline.
6. States: no dataset → EmptyState; endpoint missing → `Unavailable`; no match → EmptyState with syntax help.

## 6. Settings (and Environment / health)

Settings (`pages/Settings.tsx`): 2-column card grid. Appearance (theme `Segmented`, "Stored in this browser
only."), Session (auth required? signed in? sign-out), Endpoints (API origin, live channel URL with its
connection state, OpenAPI link, external tool links from the backend).

Environment (`pages/Environment.tsx`): one card per dependency: tinted icon tile, name, kind · version,
status badge with a pulsing dot when not up, up to five unit-formatted detail rows (`*_bytes`, `*_pct`,
`*_ms`), footer with URL and probe latency; checked-at time, refresh interval and a legend under the grid;
a Refresh button. The reference added detail cards per system (consumer lag bars, job vertices with
backpressure, table sizes) and external links (dashboards) in the header.

## 7. Compare (reference: `pages/Compare.tsx`)

1. A card with two pickers: A (baseline) and B (candidate), each with its series colour dot, and a swap
   button between them. Both live in the URL (`?a=&b=`). Entry points: select two rows in the list.
2. Metric table: Metric · A · B · Δ, where Δ is `Delta` (relative %, green when B is better given
   `better: "lower" | "higher"` per metric). Verdict and category rows use badges.
3. Grouped horizontal bars per stage/category (A and B in `--chart-1` / `--chart-2`) with a legend.
4. Overlaid curves when both have the same x (load curves).
5. EmptyState "Pick two runs. Deltas are B relative to A; green means B is better."

## 8. Launch / create forms (reference: `pages/Launch.tsx`)

Two columns: left a searchable grid of selectable cards grouped by kind (group title with icon and hint);
right a sticky card (`xl:sticky xl:top-20`) with a thin gradient strip on top, the selected item, override
fields (`NumberField`s in a 2-column grid, a clear-overrides icon button), an inline feasibility check whose
result shows a verdict badge + three numbers + advice, and two actions (secondary "Check", gradient
"Launch"). A "When" segmented control (now / at a time / recurring) and the current queue below it.
