---
name: premium-console
description: Build premium, information-rich operator UIs (consoles, dashboards, admin panels, internal tools, status and monitoring pages) and the FastAPI backend services behind them. React 19 + TypeScript + Vite + Tailwind v4 with a token-based light/dark design system, a component kit (KPI tiles, cards, badges, segmented controls, tables, step checklists with progress and ETA, ranked insight cards, live feeds, charts), a typed API client, a WebSocket live channel with polling fallback, and a backend with an app factory, sqlite job store, background worker, WebSocket hub, dependency health probes, optional token auth and SPA serving. Use this whenever the user wants to create, redesign or extend an operator console, admin UI, dashboard, control panel, back-office or internal tool, or a backend service that exposes jobs, live status or health to such a UI, even if they only say "a UI for this service", "a dashboard for X" or "make it look professional". Ships copyable templates, a scaffold script, a quality rubric and verification steps (tsc, vitest, build, pytest, Playwright screenshots in light, dark and phone).
---

# Premium operator consoles

This skill packages the patterns of a production operator console: a dense, calm, dark-by-default UI where
every number has a unit and an explanation, every async view has loading / empty / error states, live data
streams over one WebSocket, and a small FastAPI service runs jobs, probes dependencies and serves the built
UI from the same origin. Start from the templates; they are runnable and already pass their tests.

## When to use

- A new console, dashboard, admin page or internal tool (any domain: replace "jobs" with your nouns).
- Adding pages to an existing React/Tailwind app in this style, or restyling one to this quality bar.
- A backend that runs work in the background and shows live progress, health and history to operators.

Not a fit for marketing sites or document-style pages; for a single static chart, a plain artifact is enough.

## Stack and why

| Layer | Choice | Why |
|---|---|---|
| UI | React 19, TypeScript strict, Vite 8 | fast dev loop, strict types catch API drift |
| Styling | Tailwind v4 (`@tailwindcss/vite`, no config file) + CSS variables | tokens on `:root` / `.dark`, exposed as utilities via `@theme inline` |
| Primitives | Radix (dialog, dropdown, select, tabs, tooltip, switch, slider, checkbox) + `cva` | accessible behaviour, our look |
| Data | TanStack Query 5 + a `useSyncExternalStore` live store | caching/polling for REST, throttled renders for streams |
| Charts | Recharts 3 wrapped in `charts.tsx` | consistent axes, tooltips, legends, empty states |
| Icons, toasts, palette | lucide-react, sonner, cmdk | one icon family; feedback for every mutation; ⌘K |
| Fonts | Inter Variable + JetBrains Mono via `@fontsource` (bundled, no CDN) | tabular figures, works offline |
| Backend | FastAPI + sqlite + threads + asyncio WebSocket | one process, one port, no broker needed |

Exact pinned versions are in `templates/frontend/package.json` and `templates/backend/pyproject.toml`.

## Workflow

1. **Scaffold.** `python <skill-dir>/scripts/scaffold.py <target> --name "Fleet Console" --tagline "fleet ops"`
   copies `templates/` (frontend, backend, Dockerfile, compose, Makefile) and renames it. For an existing
   project, copy only what you need: `src/index.css` (tokens), `src/components/ui/*`, `common.tsx`,
   `charts.tsx`, `lib/{api,live,hooks,format,theme,cn,state}.ts`, then adapt.
2. **Model the domain first, backend side.** Decide the nouns (items, jobs, runs...), their states, and the
   few numbers an operator acts on. Write the store schema, the endpoints and the job function contract
   (see `references/backend-service.md`), then mirror the payloads in `src/lib/types.ts` and add one typed
   method per endpoint to `api.ts`.
3. **Identity and tokens.** Set `src/app.config.ts`; adjust brand colours and gradients only in
   `src/index.css` (`--accent`, `--gradient-*`, both themes). Components never hard-code hex values.
   Read `references/design-system.md` before changing tokens, type sizes or state colours.
4. **Shell.** Edit `components/layout/nav.ts` (groups are verbs: Operate, Analyse, Configure, Help; each
   item has a hint for the command palette). Keep the top bar's status pill, health dot and theme toggle.
5. **Pages.** Pick a pattern per page from `references/page-patterns.md` (overview, live, list + detail,
   report with anchors, explorer, settings, compare) and build it from the kit in
   `references/components.md`. Register the lazy route in `App.tsx`.
6. **Live data.** Push one `status` message per second plus domain messages over `/api/live`; backfill from
   `/api/live/history` on connect; poll `/api/status` while the socket is down. Use hysteresis for anything
   coloured by a live value. Details: `references/data-and-live.md`.
7. **Backend.** Keep `create_app(...)` injectable (data dir, `run_fn`, probes, `background`, `static_dir`)
   so tests run without threads or network. Explicit component states come from a pure function. Every
   failure path returns words the UI can show.
8. **Verify, then look.** Run `scripts/verify.sh <project>` (tsc, vitest, build, pytest), start the backend
   serving the build, take screenshots with `frontend/scripts/screenshots.mjs` (dark, light, phone), open
   them, and go through `references/quality-checklist.md`. Fix what looks off; screenshots catch what tests
   cannot (wrapping axis labels, a green tile next to an over-budget number, "unknown" filler text).
   Details: `references/verification.md`.

## The premium bar (short form)

Hold every page to this; the full rubric with reasons is `references/quality-checklist.md`.

- [ ] Every colour is a token; both themes checked; state colours mean the same thing everywhere.
- [ ] Every number: tabular (`num`), unit-formatted (`fmtMs`, `fmtPct`, `fmtCompact`...), "—" when missing,
      and a sub-line saying what it is ("items/s · last 5 s", "budget 120 ms").
- [ ] Coloured by meaning, not decoration; anything coloured by state has a legend.
- [ ] Every query renders loading (skeleton shaped like the content), empty (icon, title, why, next action)
      and error (`ErrorState`, or `Unavailable` for a 404 from an older backend) states.
- [ ] Live views: show freshness (live / polling / last sample N s ago), never paint red on one bad sample
      (hysteresis), and say what is happening in words ("warming: Load inputs 71% · ETA 17 s").
- [ ] Verdict before detail: hero first (status + verdict + actions), then KPIs, insights, charts, tables.
- [ ] Every mutation gives toast feedback with the outcome; destructive ones confirm or are reversible.
- [ ] Keyboard: focus rings visible, ⌘K palette, icon-only buttons have `aria-label`, dialogs from Radix.
- [ ] Responsive to 390 px: grids collapse, wide tables scroll inside their card, no page-level horizontal
      scroll, top bar switches to compact pill.
- [ ] Copy is honest and specific: units, time windows, sample sizes; no "unknown" filler; no marketing.
- [ ] `tsc --noEmit`, `vitest run`, `vite build`, `pytest` pass; screenshots reviewed in dark, light, phone.

## Conventions worth keeping

- Page anatomy: `PageHeader` (accent eyebrow, 26 px title, one-line subtitle, actions right) → optional hero
  card → KPI grid (`grid-cols-2 md:grid-cols-3 xl:grid-cols-5/6`) → cards in 2-column grids with
  `minmax(0,1.6fr)_minmax(0,1fr)` → `SectionTitle` groups (uppercase 13 px) for long pages.
- URL holds view state that should survive reload or be shareable (tab, filters, selected id, `?new=1`).
- Lists stay light (drop heavy fields such as per-second series in list endpoints); details carry them.
- Status (did it finish?) is separate from verdict (was it good?). Show both.
- State overrides metric colouring: a component that is starting or warming is never shown as slow.
- The backend ranks insights; the UI only renders them, most severe first, each with an anchor to evidence.

## Files

| Path | Read it when |
|---|---|
| `references/design-system.md` | changing tokens, typography, spacing, state colours, motion, density, dark mode |
| `references/components.md` | building any page: every primitive and composite with its API and when to use it |
| `references/page-patterns.md` | deciding the layout of a page (overview, live, list/detail, report, explorer, settings, compare) |
| `references/data-and-live.md` | wiring the API client, polling, the WebSocket store, hysteresis, stale data |
| `references/backend-service.md` | writing or extending the FastAPI service, worker, store, hub, probes, auth, tests, Docker |
| `references/quality-checklist.md` | reviewing a page or a PR against the premium bar |
| `references/verification.md` | running the gates and the Playwright screenshot review |
| `templates/frontend/` | React app: shell, kit, pages (Overview, Live, Jobs, JobDetail, Environment, Settings, SignIn) |
| `templates/backend/` | FastAPI service `server/` (app, store, worker, tasks, hub, health, state, auth) + pytest |
| `templates/Dockerfile`, `docker-compose.yml`, `Makefile` | multi-stage image (UI built into `server/static`), local run |
| `scripts/scaffold.py` | new project from the templates with a name |
| `scripts/verify.sh` | all automated gates with a pass/fail table |

## Gotchas

- Tailwind v4 has no `tailwind.config.js`: tokens live in `index.css` (`@theme inline`), dark mode is the
  `@custom-variant dark` on `.dark`. Arbitrary gradients from tokens: `bg-[image:var(--gradient-brand)]`.
- `index.html` applies the stored theme before first paint; keep its localStorage key equal to
  `${APP.slug}.theme` (scaffold.py keeps them in sync).
- Radix Select cannot use `""` as a value; the template's `Select` maps it to a sentinel.
- Recharts: set `isAnimationActive={false}` on live series, give `ResponsiveContainer` a sized parent,
  and size `YAxis width` for the longest formatted tick (or labels wrap).
- Playwright `networkidle` never settles while a WebSocket is open: wait for `.skeleton` to disappear.
- Hysteresis computed per render double-counts under StrictMode; compute it from the (backfilled) series
  (`steadyLevelOf`) or once per sample, never per render.
- `vite build` empties `outDir` (`../backend/server/static`); never put hand-written files there.
- The backend marks jobs that were running at startup as `interrupted` instead of pretending they finished.
