import { describe, expect, it } from "vitest";
import { SimStore, contentEtag } from "./store";

describe("SimStore S3 semantics", () => {
  it("putIfAbsent creates once; second attempt sees exists", () => {
    const s = new SimStore();
    expect(s.putIfAbsent("k", "a").kind).toBe("created");
    expect(s.putIfAbsent("k", "b").kind).toBe("exists");
    const got = s.get("k");
    expect(got.kind === "found" && got.body).toBe("a");
  });

  it("delete is idempotent and frees the key for putIfAbsent", () => {
    const s = new SimStore();
    s.putIfAbsent("k", "a");
    s.delete("k");
    s.delete("k"); // idempotent
    // The versioned-bucket delete-marker behavior the freed-key hazard needs.
    expect(s.putIfAbsent("k", "b").kind).toBe("created");
  });

  it("putIfMatch CASes on the current etag", () => {
    const s = new SimStore();
    const { etag } = s.put("k", "v1");
    expect(s.putIfMatch("k", "v2", '"bogus"').kind).toBe("precondition-failed");
    const updated = s.putIfMatch("k", "v2", etag);
    expect(updated.kind).toBe("updated");
    expect(s.putIfMatch("k", "v3", etag).kind).toBe("precondition-failed");
    expect(s.putIfMatch("missing", "v", etag).kind).toBe("precondition-failed");
  });

  it("pinned get returns precondition-failed after replacement", () => {
    const s = new SimStore();
    const { etag } = s.put("k", "v1");
    expect(s.get("k", { ifMatch: etag }).kind).toBe("found");
    s.put("k", "v2");
    expect(s.get("k", { ifMatch: etag }).kind).toBe("precondition-failed");
    expect(s.get("gone", { ifMatch: etag }).kind).toBe("not-found");
  });

  it("etags are content hashes: identical bodies share an etag", () => {
    // The weakest real-S3 guarantee — the pinned-GET rules must not depend
    // on etags distinguishing byte-identical bodies (SIMULATOR_PLAN.md).
    const s = new SimStore();
    const a = s.put("a", "same");
    s.delete("a");
    const b = s.put("a", "same");
    expect(a.etag).toBe(b.etag);
    expect(contentEtag("x")).not.toBe(contentEtag("y"));
  });

  it("list pages lexicographically with resume-strictly-after tokens", () => {
    const s = new SimStore();
    for (const k of ["p/03", "p/01", "p/05", "p/04", "p/02", "q/01"]) s.put(k, k);
    const page1 = s.list("p/", { maxKeys: 2 });
    expect(page1.keys.map((k) => k.key)).toEqual(["p/01", "p/02"]);
    expect(page1.nextStartAfter).toBe("p/02");
    const page2 = s.list("p/", { maxKeys: 2, startAfter: page1.nextStartAfter! });
    expect(page2.keys.map((k) => k.key)).toEqual(["p/03", "p/04"]);
    const page3 = s.list("p/", { maxKeys: 2, startAfter: page2.nextStartAfter! });
    expect(page3.keys.map((k) => k.key)).toEqual(["p/05"]);
    expect(page3.nextStartAfter).toBeUndefined();
    expect(page1.keys[0]!.etag).toBe(contentEtag("p/01"));
  });

  it("startAfter skips keys <= the marker", () => {
    const s = new SimStore();
    s.put("p/1", "a");
    s.put("p/2", "b");
    expect(s.list("p/", { startAfter: "p/1" }).keys.map((k) => k.key)).toEqual(["p/2"]);
    expect(s.list("p/", { startAfter: "p/2" }).keys).toEqual([]);
  });
});
