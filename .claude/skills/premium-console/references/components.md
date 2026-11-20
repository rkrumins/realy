# Component kit

Everything here exists in `templates/frontend/src/components/`. Primitives (`ui/`) are generic and copied
from the reference console almost verbatim; composites are generalised from it. Import with `@/components/...`.

## Contents
1. Primitives (`ui/`): Button, Badge + Dot, Card, Input / Label / NumberField / Textarea, Select, Tabs,
   Segmented, Switch / Checkbox / Slider, Dialog (centre + side drawer), Dropdown, Tooltip, Skeleton, Kbd
2. Page blocks (`common.tsx`): PageHeader, Kpi, StatusBadge, EmptyState, ErrorState, Unavailable, Stat,
   MiniBar, SectionTitle, Delta, LegendDots, BackLink
3. Charts (`charts.tsx`): TimeChart, Sparkline, HBars, Legend, TooltipBox
4. Live and progress: StepChecklist / ActiveStep (`steps.tsx`), LogConsole / EventFeed (`feeds.tsx`),
   StateBadge / StateLegend / ComponentTiles (`state.tsx`)
5. Findings: Insights (`insights.tsx`)
6. Shell: AppShell, StatusPill, CommandPalette, nav (`layout/`)
7. Recipes: tables, hero cards, selectable cards, confirm dialogs, mutation feedback

## 1. Primitives

**Button** (`cva`, Radix `Slot` for `asChild`)
`variant`: `default` (solid accent) · `gradient` (the one primary CTA per view) · `secondary` · `outline` ·
`ghost` (toolbar, "All runs →") · `danger` (tinted red) · `link`. `size`: `sm` 32 px · `default` 36 px ·
`lg` · `icon` · `icon-sm`. Icons as children get 16 px automatically. Use `asChild` to render a `Link`/`a`.
```tsx
<Button variant="gradient" onClick={start}><Play />Start</Button>
<Button asChild variant="secondary"><Link to="/">Back to overview</Link></Button>
<Button size="icon-sm" variant="ghost" aria-label="Delete schedule"><Trash2 /></Button>
```

**Badge** `tone`: `neutral | accent | pass | fail | warn | info | solidPass | solidFail`; 11.5 px pill,
icons 12 px. **Dot** `tone`, `pulse` (live). Use badges for status words, counts per severity, tags
(`className="font-mono"` for technical values). Never use a badge as a button.

**Card / CardHeader / CardBody** — `CardHeader` props: `title`, `subtitle`, `icon`, `actions` (right side:
a ghost "Details →" button, a badge, a segmented control). Tables go directly in the `Card` (edge to edge)
after the header; use `className="overflow-hidden"` on the card so the table's header row is clipped.
```tsx
<Card>
  <CardHeader title="Recent jobs" subtitle="Newest first. Click a job for its full report."
    actions={<Button variant="ghost" size="sm" onClick={() => nav("/jobs")}>All jobs<ArrowRight /></Button>} />
  <div className="overflow-x-auto"><table className="w-full min-w-[640px] text-[13px]">…</table></div>
</Card>
```

**Input, Textarea, Label, NumberField** — `Label` wraps its control and takes a `hint` line. `NumberField`
is labelled, maps empty to `undefined`, shows a `unit` inside the field and hides native spinners:
`<NumberField label="Target rate" unit="/s" value={v} onChange={setV} min={1} hint="items per second" />`.
Search inputs: an absolutely positioned `Search` icon + `className="pl-9"`.

**Select** (Radix) `value`, `onChange`, `options: {value, label, hint?}[]`, `placeholder`, `size`,
`ariaLabel`. Options can carry a second line (`hint`). `""` is supported (mapped to a sentinel internally).

**Tabs / TabsList / TabsTrigger / TabsContent** — pill tabs; put counts in triggers
(`History <span className="text-subtle">{n}</span>`); bind `value` to a URL param so tabs survive reload.

**Segmented** — 2-5 mutually exclusive options (time window 1m/5m/10m, theme, lookback 30/60/90 d).
`role="radiogroup"`, each option `role="radio"` with `aria-checked`. Prefer it over a Select for <= 5 options.

**Switch / Checkbox / Slider** — labelled wrappers on Radix. Slider: `onValueChange` while dragging,
`onValueCommit` to send (avoid one request per pixel).

**Dialog / DialogContent** `title`, `description`, `side="center" | "right"`. Use the right-side drawer for
control panels next to live data (the page stays visible behind the overlay), centre for forms and confirms.
Close button, focus trap, Esc and a hidden title for screen readers are built in.

**Dropdown** (`DropdownContent`, `DropdownItem`, `DropdownLabel`, `DropdownSeparator`) for secondary
actions behind a `…` or "Downloads" button.

**Tooltip** `content`, `side`. For icon buttons, collapsed nav items, truncated values, chart-adjacent
explanations. Renders children unchanged when `content` is empty.

**Skeleton** `className` sets the shape (`h-24 rounded-2xl` for a KPI tile). **Kbd** for shortcuts (⌘K).

## 2. Page blocks (`common.tsx`)

| Component | Props | Use |
|---|---|---|
| `PageHeader` | `eyebrow`, `title`, `subtitle`, `actions`, `children` | top of every page; eyebrow = section name or a live state ("● Live · run") |
| `Kpi` | `label`, `value`, `sub`, `tone` (`pass/fail/warn/accent`), `icon`, `spark`, `title` | headline numbers; `sub` explains (unit, window, budget, denominator) |
| `StatusBadge` | `status`, `phase?` | job lifecycle; spinner while running; phase appended |
| `EmptyState` | `icon`, `title`, `children` (why), `action` | no data yet / no match; always offer the next step |
| `ErrorState` | `error`, `title` | failed query; `role="alert"`, message from the API |
| `Unavailable` | `what` | endpoint 404/405 on an older backend: explain, do not alarm |
| `Stat` | `label`, `value`, `mono` | definition rows (label left, tabular value right) |
| `MiniBar` | `value`, `max`, `tone` | ratios inside tables and lists (precision, coverage, share) |
| `SectionTitle` | `children`, `actions`, `id` | groups on long report pages; `id` + `scroll-mt-20` for anchors |
| `Delta` | `a`, `b`, `better: "lower" | "higher"`, `pct` | compare views: signed, coloured by improvement |
| `LegendDots` | `items: {label, className}[]` | colour key for state-coloured marks |
| `BackLink` | `to`, `children` | "← All jobs" above a detail page |

KPI tone rules: tone only when the number is judged (against a budget, a threshold, zero errors). A plain
count stays neutral. Pair tone with the reason in `sub` ("budget 120 ms", "need 99.0%").

## 3. Charts (`charts.tsx`)

**TimeChart** — the workhorse. Props: `data`, `x` (default `"t"`, epoch seconds or seconds since start),
`series: {key, label, color?, kind?: "line"|"area", dashed?, stack?, width?, hidden?}[]`, `height`,
`yFormat`, `xFormat` (`clock` or `secs`), `budget` + `budgetLabel`, `band {from,to}`, `markers {x,label}[]`,
`yDomain`, `legend`, `syncId` (shared crosshair across charts of one page), `empty`.
```tsx
<TimeChart data={series} syncId="live" budget={budget} band={{ from: 0, to: budget }} yFormat={(v) => fmtMs(v)}
  series={[{ key: "p50_ms", label: "p50" }, { key: "p99_ms", label: "p99" }, { key: "p999_ms", label: "p99.9", dashed: true }]} />
```
**Sparkline** `data: (number|null)[]`, `color`, `height` — inside KPI tiles (`spark`) and hero cards.
**HBars** `data: {name, value}[]`, `format`, `highlight` — ranked categories (share per stage, hits per rule).
**Legend** — line swatches; rendered automatically by TimeChart for >= 2 series.
For one-off charts (bar per run coloured by verdict, scatter by outcome), use Recharts directly with the
same tokens: `CartesianGrid stroke="var(--grid)"`, ticks `fill: var(--fg-subtle) fontSize 11`, custom
tooltip in the `TooltipBox` style, and a `LegendDots` below.

## 4. Live and progress

**StepChecklist** `steps: Step[]`, `title`, `defaultOpen`, `className`. A step is
`{id, index, label, status: pending|running|done|skipped|warn|failed, detail?, duration_s?, progress?: {done,
total, unit?, rate?, eta_s?}, component?}`. Running steps show a progress bar and
`fmtProgress()` → "71% (412,330/583,063 items) · 9,800/s · ETA 17 s"; finished steps show their duration;
pending ones are dimmed. Two columns on desktop. Header subtitle summarises the active step, or "4 of 4 done
in 77 s"; a warn badge counts steps needing attention. Auto-opens when a step starts, collapses when done.
**ActiveStep** — the same active step as one line for a hero card.

**LogConsole** `lines: {line, ts?}[]`, `height` — monospace, coloured by content (error/warn/success/
`[console]` lines), filter box, follow mode that stops when you scroll up and resumes at the bottom.

**EventFeed<T>** `items`, `filters: {value, label, test}[]`, `render`, `itemKey`, `empty`, `max` — newest
first, filter chips (`aria-pressed`), pause/resume that freezes the list so a row can be read or clicked.
Rows are links to the explorer/detail of that event. Rate-limit the server side (the reference sampled
~20 events/s).

**ComponentTiles / StateBadge / StateLegend** — explicit states `down | starting | warming | degraded |
ready` from the backend, with the reason in words and progress while warming. Always place `StateLegend`
under the tiles (or any map coloured by state).

## 5. Findings (`insights.tsx`)

**Insights** `items: {severity: critical|warning|info|good, title, detail?, action?, area?, anchor?}[]`,
`limit` (6), `subtitle`. Renders a card with severity counts as badges, a 2-column grid of tinted finding
cards (icon, title, evidence, "Next: …", area caption), linking to `#anchor` when given, and "Show all N".
Write findings as: what happened (with the number), the evidence, and what to do next. The backend ranks
them (most severe first); the UI never re-ranks.

## 6. Shell (`layout/`)

- **AppShell**: sticky blurred sidebar (232 px, collapsible to 68 px, persisted in localStorage), mobile
  drawer under `md`, top bar with search button (opens the palette), `StatusPill`, environment health dot,
  theme toggle, sign-out (only when auth is on), skip link, `<main id="main">` with the page `Outlet`.
- **nav.ts**: groups of `{to, label, icon, hint}`; active item = tinted background + 3 px accent bar on the
  left edge; a pulsing dot on "Live" while something runs; tooltips when collapsed.
- **StatusPill**: idle → neutral pill linking to the list ("Idle · 2 queued"); running → accent pill
  "phase · kind | 120 /s | p99 260 ms / 120 · 48s", red when over budget; compact variant on phones.
- **CommandPalette** (cmdk in a Dialog, ⌘K / Ctrl K): "Running now" actions (watch, abort), "Go to" pages
  with hints, recent entities, theme toggle. Every page reachable without the mouse.

## 7. Recipes

**Table** (from the reference): header row `border-y border-border bg-surface-2/40 text-left text-[11px]
uppercase tracking-wider text-subtle`, cells `px-3 py-2.5` (first/last `px-5`), rows `border-b
border-border last:border-0 hover:bg-surface-2/60 cursor-pointer` navigating on click, numeric cells
`text-right num`, secondary numbers `text-muted`, timestamps relative (`fmtAgo`) with the absolute time in
`title`. Selected rows `bg-accent/6`. Checkbox cells stop propagation. Keyboard: `tabIndex={0}` + Enter.

**Hero card** (detail pages): status + verdict badges, mono title, meta line (id · started · took), reasons
or error as a tinted panel, actions on the right (secondary actions, a `…` dropdown with downloads / copy
id). Background gradient tinted by verdict (design-system.md §5).

**Selectable cards** (pick a template or preset): `button` cards in a responsive grid, selected =
`border-accent/60 bg-accent/8 ring-1 ring-accent/30`, mono name, two-line description (`line-clamp-2`),
metadata icons at the bottom.

**Confirm dialog** for destructive bulk actions (the reference "Stop all"): title as a question, description
of exactly what will happen, an option checkbox, `ghost` Cancel + `danger` confirm.

**Mutation feedback**: `useMutation` with `onSuccess` → `toast.success(title, {description, action})`
(e.g. "Job queued" with a "Watch live" action) and `invalidateQueries` for affected lists; `onError` →
`toast.error(title, {description: error.message})`. Disable the trigger while `isPending`.
