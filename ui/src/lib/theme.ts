import { useSyncExternalStore } from "react";

export type Theme = "dark" | "light";
const KEY = "realy.theme";
const listeners = new Set<() => void>();

function read(): Theme {
  return document.documentElement.classList.contains("dark") ? "dark" : "light";
}

export function setTheme(t: Theme) {
  document.documentElement.classList.toggle("dark", t === "dark");
  document.documentElement.style.colorScheme = t;
  try {
    localStorage.setItem(KEY, t);
  } catch {
    /* storage unavailable (private mode): theme still applies for this page */
  }
  listeners.forEach((f) => f());
}

export function toggleTheme() {
  setTheme(read() === "dark" ? "light" : "dark");
}

export function useTheme(): Theme {
  return useSyncExternalStore(
    (f) => {
      listeners.add(f);
      return () => listeners.delete(f);
    },
    read,
    () => "dark",
  );
}

/** Small persisted UI preferences (sidebar collapsed, last tab...). Never critical state. */
export function pref<T>(key: string, fallback: T): T {
  try {
    const v = localStorage.getItem(`realy.${key}`);
    return v === null ? fallback : (JSON.parse(v) as T);
  } catch {
    return fallback;
  }
}
export function setPref(key: string, v: unknown) {
  try {
    localStorage.setItem(`realy.${key}`, JSON.stringify(v));
  } catch {
    /* ignore */
  }
}
