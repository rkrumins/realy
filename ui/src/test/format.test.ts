import { describe, expect, it } from "vitest";
import { fmtFeature, fmtScore } from "@/lib/format";

describe("fmtScore", () => {
  it("keeps the magnitude of tiny probabilities instead of a flat 0.00", () => {
    expect(fmtScore(2e-5)).toBe("2e-5");
    expect(fmtScore(0.004)).toBe("0.004");
    expect(fmtScore(0.97)).toBe("0.97");
    expect(fmtScore(0)).toBe("0");
  });
  it("shows a missing score as a dash, never as zero", () => {
    expect(fmtScore(null)).toBe("—");
    expect(fmtScore(undefined)).toBe("—");
  });
});

describe("fmtFeature", () => {
  it("reads null as n/a: the entity was absent or the feature was defaulted", () => {
    expect(fmtFeature(null, "usd")).toBe("n/a");
    expect(fmtFeature(undefined)).toBe("n/a");
  });
  it("formats money aggregates held in cents", () => {
    expect(fmtFeature(409132, "usd_minor")).toBe("$4,091");
    expect(fmtFeature(4091, "usd")).toBe("$4,091");
  });
  it("formats flags, ratios and enum names", () => {
    expect(fmtFeature(1, "flag")).toBe("yes");
    expect(fmtFeature(0, "flag")).toBe("no");
    expect(fmtFeature(3.25, "ratio")).toBe("3.3×");
    expect(fmtFeature("ECOM_3DS")).toBe("3ds");
    expect(fmtFeature("CONTACTLESS")).toBe("contactless");
    expect(fmtFeature(true)).toBe("yes");
  });
});

describe("doc anchors", () => {
  it("match GitHub's slugs", async () => {
    const { slug } = await import("@/pages/Docs");
    expect(slug("Lost / stolen card")).toBe("lost--stolen-card");
    expect(slug("Startup: what a run does before it measures")).toBe("startup-what-a-run-does-before-it-measures");
    expect(slug("What's new?")).toBe("whats-new");
  });
});
