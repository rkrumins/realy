# Data and live updates

Two channels: REST (typed client + TanStack Query) for everything addressable, one WebSocket for what
changes every second. The UI and API share one origin (the backend serves the build; Vite proxies `/api`
in dev), so there is no CORS and cookies just work.

## Contents
1. Typed API client (`lib/api.ts`)
2. Query conventions (TanStack Query)
3. Live store (`lib/live.ts`): WebSocket, backfill, reconnect, throttling
4. Hooks (`lib/hooks.ts`): status with polling fallback, series, env, timers
5. Message contract
6. Hysteresis (`lib/state.ts`)
7. Stale data and freshness
8. Auth flow across REST and WebSocket

## 1. Typed API client

- `request<T>(path, {method, json, ...})` sets `Accept`/`Content-Type`, sends `credentials: "same-origin"`,
  parses JSON (or text), and throws `ApiError(status, message, detail)` on non-2xx. The message comes from
  FastAPI's `{"detail": ...}`: a string as is, or Pydantic `{errors: [{loc, msg}]}` joined as
  `"params.items: must be >= 10"`, so toasts are readable.
- A 401 on any non-auth path notifies `onUnauthorized` listeners (the app flips to the sign-in screen).
- `isNotFound(e)` (404/405) marks an endpoint an older backend does not have: render `Unavailable` or fall
  back, never a red error. The reference used this to keep a newer UI working against an older API.
- One method per endpoint in an `api` object, ids always `encodeURIComponent`-ed:
  `job: (id) => request<Job>(\`/api/jobs/${enc(id)}\`)`. Types live in `lib/types.ts`, mirrored by hand from
  the backend payloads; update both sides in the same change.

## 2. Query conventions

- Global defaults (`main.tsx`): `refetchOnWindowFocus: false`, `staleTime: 5 s`, retry at most twice and
  never on 4xx (it will not get better and it delays the error state).
- Query keys are arrays named after the resource: `["jobs"]`, `["job", id]`, `["summary"]`, `["env"]`.
- Poll with `refetchInterval`, chosen per page: lists 5 s, summary 10 s, environment 15 s, a detail every
  2 s only while it is not final: `refetchInterval: (q) => (isFinal(q.state.data) ? false : 2000)`.
- `enabled` for dependent queries (`enabled: Boolean(runId)`) and for the palette (only when open).
- `placeholderData: (prev) => prev` when a parameter changes (e.g. a lookback toggle) to avoid flashing.
- Mutations: `useMutation` → toast on success/error → `invalidateQueries` for the lists they affect.
- Optional endpoints: wrap the call, return `null` on `isNotFound`, and let the page show "not available".

## 3. Live store

`live` is a singleton class read through `useSyncExternalStore` (`useLiveVersion()`):

- **Connects lazily** when the first component subscribes; URL `ws(s)://<host>/api/live`.
- **Reconnects** with exponential backoff 1 s → 10 s; `reconnect()` after sign-in.
- **Backfills** on every open from `/api/live/history?seconds=…` (metrics, logs, current status), merging
  and de-duplicating (`job:second` keys for metrics, `ts|line` for logs), so a page opened mid-job has full
  charts at once and a reconnect leaves no gap.
- **Bounded memory**: keep ~15 min of per-second samples and ~3000 log lines.
- **Throttled notifications**: at most one React update per 200 ms, however many frames arrive (the
  reference received 1 s snapshots from many instances at once).
- **Close code 4401** → auth listeners (sign-in screen), no reconnect loop.
- Derived data is computed by pure functions over the store (`buildSeries(windowS, jobId)`,
  `logsFor(jobId)`, `focusJobId()` = the running job, else the last one seen) and memoised per version.

## 4. Hooks

- `useStatus()`: the WebSocket's latest `status` message; while the socket is down, polls `/api/status`
  every 3 s (`refetchInterval: live.connected ? false : 3000`). Pages never care which channel delivered it.
- `useLiveSeries(windowS, jobId)`: memoised `buildSeries` for the window.
- `useEnv()` + `envHealth()`: dependency health every 15 s, reduced to up / degraded / down / unknown for the
  top bar dot.
- `useNow(ms)`, `useElapsed(startedAt)`: ticking clocks for "48s elapsed", "last sample 12 s ago".

## 5. Message contract

Every frame is JSON with a `type`:

| type | sent | payload |
|---|---|---|
| `status` | on connect, then every 1 s (backend ticker) | `{current, queue, components, server_time}`; `current` carries id, kind, phase, started_at, planned_s, budget, abort_reason, steps |
| `metrics` | when the job produces a sample (about 1/s) | `{job_id, ts, throughput, p50_ms, p99_ms, errors}` (domain fields) |
| `log` | per log line | `{job_id, line, ts}` |
| domain events | rate-limited samples (reference: ~20/s) | e.g. `{type: "event", ...}` for a live feed |

Rules: send the first `status` immediately on connect (no blank second); keep frames small (lists of
steps yes, per-second series no: those come from history); every frame carries the id it belongs to so the
UI can filter to the focused job.

## 6. Hysteresis

A live value judged against a budget must not flash red on one bad second.

- `level(value, budget, warnAt = 0.8, hotAt = 1)` → `idle | good | warn | hot`.
- `steadyLevel(history, next, n = 3)` (from the reference topology map): escalate to warn/hot only when the
  last `n` samples all agree, de-escalate at once; `history` is a mutable ring per tile, fed once per sample.
- `steadyLevelOf(values, budget, n = 3)`: the same rule computed over a series. Prefer it when the series is
  backfilled: a page opened mid-incident is red immediately instead of green for three seconds (found while
  validating the templates: the streaming version showed a green p99 tile next to "260 ms / budget 120").
- Backend-side hysteresis for states: `server/state.py` keeps a component `degraded` for `STICKY_S` after
  the last bad probe ("recovering: degraded 12 s ago"); the reference held a failed checkpoint for 30 s.
- State beats metric: an explicit `starting` / `warming` state overrides latency colouring, because
  startup traffic is not representative (the reference only judged latency once the run was measuring).

## 7. Stale data and freshness

- Show the channel: `live` badge, `reconnecting · polling every 3 s` while the socket is down, and
  `live · last sample N s ago` when samples stop arriving (threshold 5 s in the template).
- When idle, keep the last data on screen and say so ("Showing the last telemetry seen (<id>)"); summarise
  only the seconds that carried data rather than averaging in zeros.
- Filter by age where it matters: the reference only used per-instance snapshots younger than 4-5 s for the
  live map, so a dead instance stops contributing instead of freezing its last value.
- Settings shows the live channel URL and whether it is connected.
- Cache short-lived expensive lookups on the backend (the reference cached a lease lookup for 2 s because
  the status endpoint is polled every second) and set the timestamp before the slow call so concurrent
  requests reuse the old value.

## 8. Auth flow

- `GET /api/auth/me` → `{auth_required, authenticated}`; a 404 means "no auth on this backend".
- The app gate shows `SignIn` when auth is required and missing, or after any 401 / WebSocket 4401.
- Login posts the token once; the server sets an HttpOnly, SameSite=Strict cookie holding an HMAC of a fixed
  label keyed by the token (never the token itself). REST and WebSocket both accept the cookie, a Bearer
  header, and (WebSocket only) `?token=`.
- After sign-in: refetch `me` and `live.reconnect()`.
