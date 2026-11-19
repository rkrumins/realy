import { useQuery } from "@tanstack/react-query";
import { BookOpen, Check, Copy, FileJson, Menu, Search, Webhook } from "lucide-react";
import MiniSearch from "minisearch";
import * as React from "react";
import ReactMarkdown, { type Components } from "react-markdown";
import { Link, useSearchParams } from "react-router";
import remarkGfm from "remark-gfm";
import { EmptyState, ErrorState, Unavailable } from "@/components/common";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { Input } from "@/components/ui/input";
import { api, isNotFound, request } from "@/lib/api";
import { cn } from "@/lib/cn";
import { useTheme } from "@/lib/theme";
import type { DocPage, DocsIndex } from "@/lib/types";

const SCHEMA = "@reference/scenario-schema";
const HTTP = "@reference/http-api";
const REFERENCE = { title: "Generated reference", items: [{ path: SCHEMA, title: "Scenario schema (live)" }, { path: HTTP, title: "HTTP API (live)" }] };

const slug = (s: string) => s.toLowerCase().replace(/<[^>]+>/g, "").replace(/[^\w\s-]/g, "").trim().replace(/\s+/g, "-");
const textOf = (n: React.ReactNode): string => (typeof n === "string" || typeof n === "number" ? String(n)
  : Array.isArray(n) ? n.map(textOf).join("") : React.isValidElement(n) ? textOf((n.props as { children?: React.ReactNode }).children) : "");

function resolve(base: string, rel: string): string {
  const dir = base === "../README.md" ? [] : base.split("/").slice(0, -1);
  for (const part of rel.split("/")) {
    if (part === "." || part === "") continue;
    if (part === "..") dir.pop();
    else dir.push(part);
  }
  return dir.join("/");
}

// ------------------------------------------------------------------ mermaid (lazy)
let mermaidSeq = 0;
function Mermaid({ code }: { code: string }) {
  const theme = useTheme();
  const [svg, setSvg] = React.useState<string | null>(null);
  const [err, setErr] = React.useState<string | null>(null);
  React.useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const m = (await import("mermaid")).default;
        m.initialize({ startOnLoad: false, theme: theme === "dark" ? "dark" : "default", securityLevel: "strict", fontFamily: "Inter Variable, sans-serif" });
        const { svg: out } = await m.render(`mmd-${++mermaidSeq}`, code);
        if (!cancelled) setSvg(out);
      } catch (e) {
        if (!cancelled) setErr((e as Error).message);
      }
    })();
    return () => { cancelled = true; };
  }, [code, theme]);
  if (err) return <pre className="text-fail">{err}</pre>;
  if (!svg) return <Skeleton className="my-4 h-40" />;
  return <div className="my-5 flex justify-center overflow-x-auto rounded-xl border border-border bg-surface-2/40 p-4 [&_svg]:max-w-full" dangerouslySetInnerHTML={{ __html: svg }} />;
}

function CodeBlock({ lang, code }: { lang: string; code: string }) {
  const [copied, setCopied] = React.useState(false);
  return (
    <div className="group relative my-4">
      {lang && <span className="absolute left-4 top-2 text-[10.5px] uppercase tracking-wider text-subtle">{lang}</span>}
      <button aria-label="Copy code" onClick={() => { navigator.clipboard?.writeText(code); setCopied(true); setTimeout(() => setCopied(false), 1500); }}
        className="absolute right-2 top-2 rounded-md border border-border bg-surface px-1.5 py-1 text-subtle opacity-0 transition group-hover:opacity-100 hover:text-fg cursor-pointer">
        {copied ? <Check className="size-3.5 text-pass" /> : <Copy className="size-3.5" />}
      </button>
      <pre className={cn("!my-0", lang && "!pt-8")}><code>{code}</code></pre>
    </div>
  );
}

function Markdown({ page }: { page: DocPage }) {
  const comps: Components = {
    h1: ({ children }) => <h1 id={slug(textOf(children))}>{children}</h1>,
    h2: ({ children }) => <h2 id={slug(textOf(children))} className="scroll-mt-20">{children}</h2>,
    h3: ({ children }) => <h3 id={slug(textOf(children))} className="scroll-mt-20">{children}</h3>,
    pre: ({ children }) => <>{children}</>,
    code: ({ className, children }) => {
      const m = /language-(\w+)/.exec(className || "");
      const code = String(children ?? "").replace(/\n$/, "");
      if (!m && !code.includes("\n")) return <code>{children}</code>;
      if (m?.[1] === "mermaid") return <Mermaid code={code} />;
      return <CodeBlock lang={m?.[1] ?? ""} code={code} />;
    },
    a: ({ href = "", children }) => {
      if (/^(https?:|mailto:)/.test(href)) return <a href={href} target="_blank" rel="noreferrer">{children}</a>;
      if (href.startsWith("#")) return <a href={href}>{children}</a>;
      const [p, hash] = href.split("#");
      if (p.endsWith(".md")) return <Link to={`/docs?path=${encodeURIComponent(resolve(page.path, p))}${hash ? `#${hash}` : ""}`}>{children}</Link>;
      return <a href={`/api/docs/raw/${resolve(page.path, p)}`} target="_blank" rel="noreferrer">{children}</a>;
    },
    img: ({ src = "", alt }) => {
      const s = typeof src === "string" ? src : "";
      return <img src={/^(https?:|data:)/.test(s) ? s : `/api/docs/raw/${resolve(page.path, s)}`} alt={alt ?? ""} loading="lazy" />;
    },
    table: ({ children }) => <table>{children}</table>,
  };
  return <ReactMarkdown remarkPlugins={[remarkGfm]} components={comps}>{page.markdown}</ReactMarkdown>;
}

function Toc({ markdown }: { markdown: string }) {
  const heads = [...markdown.matchAll(/^(##|###)\s+(.+)$/gm)].map((m) => ({ level: m[1].length, text: m[2].replace(/[`*_]/g, "") }));
  if (heads.length < 3) return null;
  return (
    <nav className="sticky top-20 hidden max-h-[calc(100dvh-6rem)] overflow-y-auto 2xl:block">
      <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.12em] text-subtle">On this page</div>
      <ul className="space-y-1 border-l border-border">
        {heads.map((h, i) => (
          <li key={i}><a href={`#${slug(h.text)}`} className={cn("-ml-px block border-l border-transparent py-0.5 text-[12.5px] text-muted hover:border-accent hover:text-fg", h.level === 3 ? "pl-6" : "pl-3")}>{h.text}</a></li>
        ))}
      </ul>
    </nav>
  );
}

// ------------------------------------------------------------------ generated reference pages
type JsonSchema = { type?: string | string[]; description?: string; default?: unknown; enum?: unknown[]; const?: unknown; $ref?: string; anyOf?: JsonSchema[];
  items?: JsonSchema; properties?: Record<string, JsonSchema>; additionalProperties?: JsonSchema | boolean; title?: string; prefixItems?: JsonSchema[] };

function typeLabel(s: JsonSchema): string {
  if (s.$ref) return s.$ref.split("/").pop()!;
  if (s.enum) return s.enum.map((e) => JSON.stringify(e)).join(" | ");
  if (s.const !== undefined) return JSON.stringify(s.const);
  if (s.anyOf) return s.anyOf.map(typeLabel).join(" | ");
  if (s.type === "array") return s.prefixItems ? `[${s.prefixItems.map(typeLabel).join(", ")}]` : `${s.items ? typeLabel(s.items) : "any"}[]`;
  if (s.type === "object" && s.additionalProperties && typeof s.additionalProperties === "object") return `map<string, ${typeLabel(s.additionalProperties)}>`;
  return Array.isArray(s.type) ? s.type.join(" | ") : s.type ?? "any";
}

function SchemaPage() {
  const q = useQuery({ queryKey: ["schema"], queryFn: api.scenarioSchema, retry: 0 });
  if (q.isLoading) return <Skeleton className="h-96" />;
  if (q.error) return isNotFound(q.error) ? <Unavailable what="Scenario schema" /> : <ErrorState error={q.error} />;
  const root = q.data as JsonSchema & { $defs?: Record<string, JsonSchema> };
  const defs = Object.entries(root.$defs ?? {});
  const Table = ({ s }: { s: JsonSchema }) => (
    <table>
      <thead><tr><th>Field</th><th>Type</th><th>Default</th></tr></thead>
      <tbody>{Object.entries(s.properties ?? {}).map(([k, p]) => (
        <tr key={k}>
          <td><code>{k}</code>{p.description && <div className="mt-1 text-[13px] text-muted">{p.description}</div>}</td>
          <td className="font-mono text-[12.5px]">{p.$ref || p.anyOf?.some((x) => x.$ref) ? (
            <a href={`#${slug(typeLabel(p))}`}>{typeLabel(p)}</a>) : typeLabel(p)}</td>
          <td className="font-mono text-[12.5px] text-muted">{p.default === undefined ? "—" : JSON.stringify(p.default)}</td>
        </tr>
      ))}</tbody>
    </table>
  );
  return (
    <div className="prose-doc">
      <h1>Scenario schema</h1>
      <p>Generated live from the <code>Scenario</code> pydantic model (<code>python/realy/scenario.py</code>) via <code>/api/reference/scenario-schema</code>. Every scenario YAML under <code>scenarios/</code> validates against it.</p>
      <h2 id="scenario">Scenario</h2>
      <Table s={root} />
      {defs.map(([name, s]) => (
        <React.Fragment key={name}>
          <h3 id={slug(name)}>{name}</h3>
          {s.description && <p>{s.description}</p>}
          {s.properties ? <Table s={s} /> : <p className="font-mono text-[13px]">{typeLabel(s)}</p>}
        </React.Fragment>
      ))}
    </div>
  );
}

const METHOD_TONE: Record<string, "pass" | "accent" | "warn" | "fail" | "info"> = { get: "info", post: "pass", put: "warn", delete: "fail", websocket: "accent" };

function HttpPage() {
  const q = useQuery({ queryKey: ["openapi"], queryFn: api.openapi, retry: 0 });
  if (q.isLoading) return <Skeleton className="h-96" />;
  if (q.error) return isNotFound(q.error) ? <Unavailable what="OpenAPI document" /> : <ErrorState error={q.error} />;
  const spec = q.data as { info?: { title?: string; description?: string; version?: string }; paths?: Record<string, Record<string, { summary?: string; tags?: string[]; description?: string; parameters?: { name: string; in: string; required?: boolean }[]; requestBody?: unknown }>> };
  const groups = new Map<string, { method: string; path: string; summary: string; params: string[]; body: boolean }[]>();
  for (const [path, ops] of Object.entries(spec.paths ?? {})) {
    for (const [method, op] of Object.entries(ops)) {
      const tag = op.tags?.[0] ?? (path.split("/")[2] || "console");
      if (!groups.has(tag)) groups.set(tag, []);
      groups.get(tag)!.push({ method, path, summary: op.summary ?? "", params: (op.parameters ?? []).map((p) => `${p.name}${p.required ? "" : "?"} (${p.in})`), body: Boolean(op.requestBody) });
    }
  }
  groups.set("live (WebSocket)", [{ method: "websocket", path: "/api/live", summary: "Telemetry stream: metrics | log | status | decision messages (close 4401 = sign in)", params: [], body: false }]);
  return (
    <div className="prose-doc">
      <h1>HTTP API</h1>
      <p>{spec.info?.description} Generated from <code>/api/openapi.json</code>; interactive docs at <a href="/api/swagger" target="_blank" rel="noreferrer">/api/swagger</a>.</p>
      {[...groups.entries()].map(([tag, ops]) => (
        <React.Fragment key={tag}>
          <h2 id={slug(tag)}>{tag}</h2>
          <div className="not-prose space-y-2">
            {ops.map((o) => (
              <div key={o.method + o.path} className="rounded-xl border border-border bg-surface-2/40 px-4 py-2.5">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge tone={METHOD_TONE[o.method] ?? "neutral"} className="w-[74px] justify-center font-mono uppercase">{o.method}</Badge>
                  <code className="!border-0 !bg-transparent !p-0 font-mono text-[13px] text-fg">{o.path}</code>
                  <span className="text-[13px] text-muted">{o.summary}</span>
                </div>
                {(o.params.length > 0 || o.body) && <div className="mt-1 pl-[82px] font-mono text-[11.5px] text-subtle">{[...o.params, ...(o.body ? ["JSON body"] : [])].join(" · ")}</div>}
              </div>
            ))}
          </div>
        </React.Fragment>
      ))}
    </div>
  );
}

// ------------------------------------------------------------------ search
function useDocSearch(index: DocsIndex | undefined, enabled: boolean) {
  return useQuery({
    queryKey: ["docs-search", index?.sections.length],
    enabled: enabled && Boolean(index),
    staleTime: Infinity,
    queryFn: async () => {
      const items = index!.sections.flatMap((s) => s.items);
      const pages = await Promise.all(items.map((it) => request<DocPage>(`/api/docs/page?path=${encodeURIComponent(it.path)}`).catch(() => null)));
      const ms = new MiniSearch<{ id: string; title: string; body: string }>({ fields: ["title", "body"], storeFields: ["title", "body"], searchOptions: { boost: { title: 3 }, prefix: true, fuzzy: 0.15 } });
      ms.addAll(pages.filter(Boolean).map((p) => ({ id: p!.path, title: p!.title, body: p!.markdown })));
      return ms;
    },
  });
}

function snippet(body: string, terms: string[]) {
  const low = body.toLowerCase();
  const i = Math.max(0, ...terms.map((t) => low.indexOf(t.toLowerCase())).filter((x) => x >= 0).slice(0, 1));
  return body.slice(Math.max(0, i - 50), i + 110).replace(/[#*`>|]/g, "").replace(/\s+/g, " ");
}

export default function Docs() {
  const [params, setParams] = useSearchParams();
  const index = useQuery({ queryKey: ["docs"], queryFn: api.docs, retry: 0, staleTime: 300_000 });
  const [q, setQ] = React.useState("");
  const [navOpen, setNavOpen] = React.useState(false);
  const search = useDocSearch(index.data, q.length > 1);
  const first = index.data?.sections[0]?.items[0]?.path;
  const path = params.get("path") || first || "";
  const generated = path.startsWith("@reference/");
  const page = useQuery({ queryKey: ["doc", path], queryFn: () => api.docPage(path), enabled: Boolean(path) && !generated, retry: 0 });
  const sections = [...(index.data?.sections ?? []), REFERENCE];
  const results = q.length > 1 && search.data ? search.data.search(q).slice(0, 8) : [];

  React.useEffect(() => {
    const h = location.hash.slice(1);
    if (h && page.data) setTimeout(() => document.getElementById(h)?.scrollIntoView(), 50);
    else window.scrollTo({ top: 0 });
  }, [page.data, path]);

  const go = (p: string) => { setParams({ path: p }); setQ(""); setNavOpen(false); };
  const nav = (
    <nav className="space-y-5">
      {sections.map((s) => (
        <div key={s.title}>
          <div className="mb-1.5 px-2.5 text-[10.5px] font-semibold uppercase tracking-[0.14em] text-subtle">{s.title}</div>
          {s.items.map((it) => (
            <button key={it.path} onClick={() => go(it.path)}
              className={cn("flex w-full items-center gap-2 rounded-lg px-2.5 py-1.5 text-left text-[13px] cursor-pointer", it.path === path ? "bg-accent/12 font-medium text-fg" : "text-muted hover:bg-surface-2 hover:text-fg")}>
              {it.path === SCHEMA ? <FileJson className="size-3.5 shrink-0" /> : it.path === HTTP ? <Webhook className="size-3.5 shrink-0" /> : null}
              <span className="truncate">{it.title}</span>
            </button>
          ))}
        </div>
      ))}
    </nav>
  );

  return (
    <div className="grid gap-8 lg:grid-cols-[240px_minmax(0,1fr)] 2xl:grid-cols-[240px_minmax(0,1fr)_200px]">
      <aside className="lg:sticky lg:top-20 lg:max-h-[calc(100dvh-6rem)] lg:self-start lg:overflow-y-auto">
        <div className="relative mb-4">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-subtle" />
          <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search docs…" className="pl-9" data-testid="docs-search" />
          {q.length > 1 && (
            <Card className="absolute inset-x-0 top-11 z-30 max-h-96 overflow-y-auto p-1.5 shadow-2xl">
              {search.isLoading ? <div className="p-3 text-[12.5px] text-subtle">Indexing…</div> : results.length === 0 ? <div className="p-3 text-[12.5px] text-subtle">No results.</div> : results.map((r) => (
                <button key={r.id} onClick={() => go(r.id)} className="block w-full rounded-lg px-3 py-2 text-left hover:bg-surface-2 cursor-pointer">
                  <div className="text-[13px] font-medium text-fg">{r.title as string}</div>
                  <div className="mt-0.5 line-clamp-2 text-[11.5px] text-muted">{snippet(r.body as string, r.terms)}</div>
                </button>
              ))}
            </Card>
          )}
        </div>
        <button onClick={() => setNavOpen(!navOpen)} className="mb-3 flex items-center gap-2 text-[13px] text-muted lg:hidden cursor-pointer"><Menu className="size-4" />Contents</button>
        <div className={cn(!navOpen && "hidden lg:block")}>
          {index.isLoading ? <div className="space-y-2">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-7" />)}</div> : nav}
        </div>
      </aside>
      <article className="min-w-0">
        {path === SCHEMA ? <SchemaPage /> : path === HTTP ? <HttpPage /> : index.error && isNotFound(index.error) ? <Unavailable what="Docs" />
          : !path ? <EmptyState icon={<BookOpen />} title="No documentation found">The docs/ directory is empty.</EmptyState>
            : page.isLoading ? <div className="space-y-3"><Skeleton className="h-9 w-2/3" /><Skeleton className="h-4" /><Skeleton className="h-4 w-5/6" /><Skeleton className="h-64" /></div>
              : page.error ? <ErrorState error={page.error} title="Page not found" />
                : page.data && <div className="prose-doc max-w-[860px]" data-testid="doc-content"><Markdown page={page.data} /></div>}
      </article>
      {page.data && !generated && <Toc markdown={page.data.markdown} />}
    </div>
  );
}
