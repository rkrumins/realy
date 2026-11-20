# Design system

The look comes from a small set of rules applied everywhere: tokens for every colour, one type scale,
hairline borders on layered surfaces, colour only where it carries meaning, and tabular numbers. All values
below are what the templates use (`templates/frontend/src/index.css` and the components).

## Contents
1. Tokens and surfaces
2. Typography
3. Spacing, radii, sizes
4. Colour roles and state colours
5. Tints, gradients, shadows
6. Charts
7. Dark mode
8. Motion
9. Density and layout rules

## 1. Tokens and surfaces

Defined as CSS variables on `:root` (light) and `.dark`, then exposed to Tailwind with `@theme inline`, so
`bg-surface`, `text-muted`, `border-border`, `text-pass`, `bg-chart-3` all switch with the theme.

| Token (CSS) | Utility | Use |
|---|---|---|
| `--bg` | `bg-bg` | page background (plus a soft radial `--bg-glow` at the top right) |
| `--card` | `bg-surface` | cards, dialogs, dropdowns, sidebar (70% + blur) |
| `--card-2` | `bg-surface-2` | inputs (60%), table header rows (40%), hover rows (60%), chips |
| `--card-3` | `bg-surface-3` | progress tracks, bar backgrounds, tooltips, skeleton highlight |
| `--border` / `--border-strong` | `border-border` / `border-border-strong` | hairlines (7.5-9% alpha) / hover, axes, dividers in pills |
| `--fg` / `--fg-muted` / `--fg-subtle` | `text-fg` / `text-muted` / `text-subtle` | primary text / secondary / labels, hints, axes |
| `--accent`, `--accent-2`, `--accent-fg` | `text-accent`, `bg-accent` ... | brand, active nav, primary buttons, "running" |
| `--pass` `--fail` `--warn` `--info` | `text-pass` ... | state roles (section 4) |
| `--chart-1..8` | `bg-chart-1` ... / `var(--chart-1)` | categorical series |
| `--grid` | `var(--grid)` | chart gridlines, `.grid-bg` |
| `--shadow` | `shadow-card` | cards and KPI tiles |
| `--gradient-brand`, `--gradient-logo`, `--gradient-progress` | `bg-[image:var(--gradient-brand)]` | primary CTA, logo, progress bars |

Rule: components never contain hex colours. A rebrand or a new theme is an edit to `index.css` only. (The
reference console hard-coded its brand gradient in four places; the templates tokenise it.)

Layering: page `bg` → card `surface` → inset panel `surface-2/40` with `border-border` → track `surface-3`.
Each layer is one step lighter (dark) or darker (light); never put a card on a card of the same colour
without a border.

## 2. Typography

Inter Variable for UI (with `font-feature-settings: "cv11", "ss01", "ss03"`), JetBrains Mono for ids, code,
log lines and small numeric captions. Both bundled with `@fontsource`, no CDN.

| Role | Classes |
|---|---|
| Page title | `text-[26px] font-semibold leading-tight tracking-tight` |
| Hero title (an entity id or name) | `font-mono text-[22px] md:text-[26px] font-semibold tracking-tight` |
| Eyebrow above a title | `text-[11.5px] font-semibold uppercase tracking-[0.12em] text-accent` |
| Section title (long pages) | `text-[13px] font-semibold uppercase tracking-[0.1em] text-subtle` |
| Card title / subtitle | `text-[14px] font-semibold tracking-tight` / `text-[12.5px] text-subtle` |
| KPI label / value / sub | `text-[11.5px] font-medium uppercase tracking-wider text-subtle` / `text-[24px] font-semibold leading-none tracking-tight num` / `text-[12px] text-muted num` |
| Table header / body | `text-[11px] font-medium uppercase tracking-wider text-subtle` / `text-[13px]` (dense: 12.5) |
| Body / helper / micro | 13-14 px / 12-12.5 px `text-muted` / 10.5-11.5 px `text-subtle` (legends, captions) |
| Sidebar group label | `text-[10.5px] font-semibold uppercase tracking-[0.14em] text-subtle` |
| Ids in tables | `font-mono text-[12.5px] text-fg` with a second line `font-mono text-[11px] text-subtle` |

Numbers: every number that changes or sits in a column gets `.num` (`font-variant-numeric: tabular-nums`),
so digits do not jitter on live updates and columns align. Right-align numeric columns.

## 3. Spacing, radii, sizes

| Thing | Value |
|---|---|
| Content width / padding | `max-w-[1480px]`, `px-4 py-6 md:px-8 md:py-8` |
| Card padding | header `px-5 pt-4 pb-3`, body `px-5 pb-5` (tables run edge to edge inside the card) |
| Grid gaps | 12 px (KPI grids on phone), 16 px (KPI grids), 20 px (card grids) |
| Vertical rhythm | 20-24 px between blocks (`mt-5`/`mt-6`); `SectionTitle` adds 32 px above |
| Radii | cards and KPI tiles `rounded-2xl`; inset panels, feeds, dialogs' inner blocks `rounded-xl`; buttons, inputs `rounded-lg`; badges, pills, chips `rounded-full` |
| Control heights | buttons and inputs 36 px (`h-9`), small 32 px (`h-8`), segmented 28/24 px |
| Shell | top bar 56 px (`h-14`), sidebar 232 px or 68 px collapsed, nav item 36 px |
| Icons | 16 px in buttons (`[&_svg]:size-4`), 14 px in KPI labels and tabs, 12 px in badges |

## 4. Colour roles and state colours

Colour means state, never decoration. The same meaning always uses the same token:

| Role | Token | Examples |
|---|---|---|
| good / healthy / within budget | `pass` | done, up, ready, verdict pass, "good" insight |
| bad / down / over budget | `fail` | failed, down, critical insight, budget line |
| needs attention | `warn` | aborted, interrupted, degraded, warning insight, non-zero errors |
| informative / transitional | `info` | queued, starting, info insight |
| active / ours / in progress | `accent` | running, warming, live dot, active nav, selected row (`bg-accent/6-8`) |
| neutral | `neutral` badge (`surface-2` + `muted`) | cancelled, idle, unknown, pending |

Mappings used by the templates:

- Job status → tone: done pass, running accent (spinner), queued info, aborted warn, failed fail,
  cancelled neutral, interrupted warn (`StatusBadge`).
- Component state → colour: down fail, starting info, warming accent, degraded warn, ready pass
  (`components/state.tsx`, with a legend).
- Insight severity: critical fail, warning warn, info info, good pass (`insights.tsx`).
- Health: up pass, degraded warn, down fail, unknown neutral (top bar dot, Environment page).
- Live level vs budget: idle (no tone), good pass, warn (>= 80% of budget), hot fail (>= 100%)
  (`lib/state.ts`; thresholds are parameters).

Every view that colours marks by state shows a legend (`LegendDots`, `StateLegend`, chart `Legend`).

## 5. Tints, gradients, shadows

- Tinted chip / badge: `border-{tone}/25 bg-{tone}/10 text-{tone}`. Tinted panel: `border-{tone}/30
  bg-{tone}/[0.05-0.07]`. Selected: `border-accent/60 bg-accent/8 ring-1 ring-accent/30`.
- Verdict hero: `bg-[linear-gradient(115deg,color-mix(in_oklab,var(--pass)_13%,var(--card))_0%,var(--card)_55%)]`
  with `border-pass/25` (swap `--fail`, `--accent` for other verdicts). "Running now" card: the same with
  `--accent` at 14% and 110deg.
- Primary call to action per page uses the `gradient` button variant; at most one per view. Secondary
  actions use `secondary` / `outline` / `ghost`; destructive ones `danger` (tinted, not solid red).
- Shadows are soft and mostly invisible in dark mode (`--shadow` is an inset highlight there). Floating
  layers (dropdowns, dialogs, tooltips, toasts) use `shadow-xl` / `shadow-2xl`.
- Translucent chrome: sidebar `bg-surface/70 backdrop-blur-xl`, top bar `bg-bg/75 backdrop-blur-xl`.

## 6. Charts

- Series colours `var(--chart-1..8)` in order (validated for colour-vision deficiency; the legend and the
  tooltip carry the labels, so low-contrast slots stay readable). State-coloured bars use `--pass`/`--fail`.
- Hairline horizontal grid (`--grid`), no axis lines, 11 px `--fg-subtle` ticks, `minTickGap` 48.
- Lines 2 px, `type="monotone"`, no dots, active dot r=4 with a `--card` stroke; areas use a vertical
  gradient fill from 28% to 0 opacity (stacked: 55% to 25%).
- Budget: dashed `--fail` reference line labelled "budget 120 ms"; the "good" zone as a `--pass` band at 6%.
- Markers (phase changes) as dashed `--border-strong` verticals with a 10.5 px label.
- Tooltip: `rounded-xl border bg-surface/95 backdrop-blur`, label in `text-muted num`, rows with a colour dot,
  name and right-aligned tabular value. Legend shown only for two or more series.
- No chart animation (`isAnimationActive={false}`): live charts must not wobble on every update.
- Empty: an overlay "Waiting for data…" (or a specific sentence) instead of empty axes.
- Log-scale axes for latencies that span orders of magnitude; say "(log scale)" in the subtitle.

## 7. Dark mode

- Class strategy: `<html class="dark">` by default; `index.html` removes it before first paint when the
  stored choice is light (`localStorage["<slug>.theme"]`), so there is no flash.
- `lib/theme.ts` exposes `useTheme()` (via `useSyncExternalStore`), `setTheme`, `toggleTheme`; the top bar
  has a sun/moon button and Settings a segmented Dark / Light control.
- `color-scheme` is set on `html` so native scrollbars and inputs match; scrollbars are restyled thin.
- Both themes are designed, not inverted: accents are lighter in dark (`#818cf8` vs `#4f46e5`), state
  colours brighter (`#34d399` vs `#059669`), borders white-alpha instead of slate-alpha.
- Recommendation (not in the reference console): when nothing is stored, follow
  `prefers-color-scheme` instead of defaulting to dark.

## 8. Motion

Motion signals liveness and state change only.

- Live dot: `Dot pulse` (ping at 2 s) next to "running", on the Live nav item and in the status pill.
- Running: `Loader2 animate-spin` inside the badge or step icon.
- Progress bars: `transition-[width] duration-700` (steps) / `duration-1000` (job), so 1 s updates glide.
- Skeletons: shimmer gradient (`.skeleton`), shaped like the content they replace.
- Hover: colour/background transitions only; arrows nudge `group-hover:translate-x-0.5`.
- A flow diagram can animate a dashed stroke whose speed is proportional to the rate it shows
  (the reference topology map does: `stroke-dasharray` + `stroke-dashoffset` keyframes).
- Templates add `prefers-reduced-motion` handling in `index.css` (an addition to the reference).

## 9. Density and layout rules

- Information-rich, not cramped: 13 px data text, 8-10 px row padding, hairline separators
  (`divide-y divide-border`), labels left in muted, values right in fg and tabular (`Stat`).
- Every flex child that holds text gets `min-w-0` + `truncate` (with a `title` for the full value).
- Wide tables keep a `min-w-[640-980px]` inside an `overflow-x-auto` wrapper inside the card, so the page
  never scrolls sideways on a phone.
- KPI grids: `grid-cols-2` on phone, 3 on tablet, 5-6 on desktop; an odd last tile spans 2 on phone.
- Two-column card grids use fractional minmax tracks (`xl:grid-cols-[minmax(0,1.65fr)_minmax(0,1fr)]`),
  collapsing to one column below `xl`/`lg`.
- Sticky side panels (`xl:sticky xl:top-20 xl:self-start`) for forms next to long lists.
