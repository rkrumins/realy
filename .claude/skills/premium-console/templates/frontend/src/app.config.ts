/** Product identity: the one place to rename the console (scaffold.py rewrites these defaults). */
export const APP = {
  name: "Operator Console",
  /** small caps line under the name in the sidebar */
  tagline: "operations",
  /** prefix for localStorage keys (theme, sidebar state); keep in sync with index.html */
  slug: "operator-console",
} as const;
