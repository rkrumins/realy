# Quality checklist

Use this as the review rubric for every page and every PR. Each line says what to check and, where it is
not obvious, why. Review with the app running and screenshots open in dark, light and phone.

## Visual

- [ ] No hex colours in components; every colour is a token from `index.css`, so both themes work.
- [ ] Both themes look designed: contrast of `text-subtle` on `surface` readable, borders visible, tinted
      panels not muddy in light mode.
- [ ] One primary (gradient) action per view; everything else secondary / outline / ghost; destructive is
      `danger` (tinted), never solid red.
- [ ] Type scale respected (page 26, card 14, KPI 24, table 13, labels 11-11.5 uppercase); no ad-hoc sizes.
- [ ] Spacing rhythm: 20-24 px between blocks, card padding 20 px, consistent grid gaps.
- [ ] Alignment: numbers right-aligned and tabular; columns line up across rows; truncation has a tooltip.
- [ ] Icons from one family (lucide), sized by context (16 / 14 / 12 px), always with a label or `aria-label`.
- [ ] Charts: hairline grid, no axis lines, readable ticks that do not wrap (size `YAxis width`), units in
      ticks or subtitle, legend when two or more series, no animation on live data.

## Content honesty

- [ ] Every number has a unit (`ms`, `/s`, `%`, `items`) and a sub-line or subtitle saying what it is and
      over which window ("mean of the last 5 s", "seconds whose p99 met the budget").
- [ ] Missing data shows "—", never `NaN`, `0` or an empty cell; "no data yet" is said in words.
- [ ] Judged numbers show what they are judged against ("budget 120 ms", "need 99.0%").
- [ ] Status (finished?) and verdict (good?) are separate and both visible.
- [ ] Rates and ratios name their denominator ("100.00% of emitted", "3 pass · 2 fail").
- [ ] Every state has a reason in words ("warming: Load inputs 71% · ETA 17 s", "only 0.6% free"); no
      "unknown version" filler, no unexplained red.
- [ ] Copy is specific and plain: what the page is for in one subtitle line; no marketing words; units and
      sample sizes where a reader would ask "out of what?".
- [ ] Insights carry evidence and a next step, ranked most severe first, linking to the evidence.
- [ ] Projections or estimates are labelled as such, with the formula or assumption visible (the reference
      printed the projection formula and its inputs in a caption under the projected table).

## States

- [ ] Loading: skeletons shaped like the content (KPI tiles, table rows, chart height); no layout jump.
- [ ] Empty: icon, title, one sentence of why, and the next action (button). "No data" vs "no match for
      your filters" are different states with different help.
- [ ] Error: `ErrorState` with the API's message; 404 on an optional endpoint → `Unavailable`, not red.
- [ ] Live: freshness visible (live / polling / last sample N s ago); last data kept when idle and labelled;
      hysteresis on anything coloured by a live value; explicit starting / warming states beat metric colour.
- [ ] Long-running actions: progress with done / total, rate and ETA; abort available and explained.
- [ ] Mutations: button disabled while pending, toast on success (with a follow-up action when useful) and
      on error (with the reason); lists invalidated.
- [ ] Destructive or bulk actions confirm with a description of exactly what will happen.

## Accessibility and keyboard

- [ ] Visible focus ring on every interactive element (`:focus-visible` outline in accent).
- [ ] Icon-only buttons have `aria-label`; collapsed nav items have tooltips and labels.
- [ ] Segmented controls are radio groups (`role="radiogroup"`, `aria-checked`); filter chips use
      `aria-pressed`; progress bars `role="progressbar"` with values; errors `role="alert"`.
- [ ] Dialogs, menus, selects, tabs and tooltips come from Radix (focus trap, Esc, arrow keys).
- [ ] ⌘K / Ctrl K palette reaches every page and the main actions; clickable table rows are focusable and
      open on Enter.
- [ ] Colour is never the only signal: badges carry words, icons differ per state, charts have legends.
- [ ] Reduced motion respected (template addition in `index.css`).

## Responsive

- [ ] 390 px wide: no page-level horizontal scroll (the screenshot script fails on it), grids collapse to
      1-2 columns, wide tables scroll inside their card, header actions wrap, top bar shows the compact pill
      and a menu button opening the drawer.
- [ ] 768-1280 px: sidebar visible from `md`, card grids switch to two columns at `lg`/`xl`.
- [ ] Very wide screens: content capped at 1480 px.

## Performance

- [ ] Pages are lazy routes; heavy libraries in their own chunk (charts) via `manualChunks`.
- [ ] List endpoints return light rows (no per-second series); details carry the heavy fields.
- [ ] Live updates throttled (one React update per 200 ms); derived series memoised per store version.
- [ ] Polling intervals proportional to how fast the data changes; detail polling stops when final.
- [ ] Backend: blocking work off the event loop (`asyncio.to_thread`), probes in parallel with timeouts,
      short caches for expensive lookups polled every second.

## Tests and verification

- [ ] `npx tsc --noEmit`, `npx vitest run`, `npm run build` (or `npx vite build`), `python -m pytest -q` pass.
- [ ] Pure logic has unit tests: formatters, hysteresis, progress wording, state derivation, insight ranking.
- [ ] API tests run without threads or network (app factory with fakes) and cover error paths and auth.
- [ ] Playwright screenshots taken in dark, light and phone with no page errors, and actually looked at.
