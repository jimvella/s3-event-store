/**
 * Factored driver-conformance suite (SIMULATOR_PLAN.md): the semantics
 * DESIGN.md assumes of any backend, expressed once against the
 * `StorageDriver` interface. SimStore (via the direct driver) is the
 * executable reference; every real driver — and eventually every real
 * backend — must pass the identical suite. A backend quirk is a divergence
 * from this file, captured here as a test, never as a scattered `if`.
 */

import { beforeAll, describe, expect, it } from "vitest";
import type { StorageDriver } from "../src/driver.js";

export interface ConformanceTarget {
  driver: StorageDriver;
  /** Key namespace for this run — lets real buckets isolate/expire runs. */
  ns: string;
}

export function conformanceSuite(
  name: string,
  make: () => Promise<ConformanceTarget> | ConformanceTarget,
): void {
  describe(`driver conformance: ${name}`, () => {
    let driver: StorageDriver;
    let ns: string;
    beforeAll(async () => {
      ({ driver, ns } = await make());
    });
    const k = (suffix: string) => `${ns}/${suffix}`;

    it("putIfAbsent is create-only", async () => {
      const created = await driver.putIfAbsent(k("cas/a"), "one");
      expect(created.kind).toBe("created");
      expect(created.kind === "created" && created.etag).toBeTruthy();
      expect((await driver.putIfAbsent(k("cas/a"), "two")).kind).toBe("exists");
      const got = await driver.get(k("cas/a"));
      expect(got.kind === "found" && got.body).toBe("one");
    });

    it("delete is idempotent and frees the key for putIfAbsent", async () => {
      await driver.putIfAbsent(k("free/a"), "one");
      await driver.delete(k("free/a"));
      await driver.delete(k("free/a")); // idempotent
      expect((await driver.get(k("free/a"))).kind).toBe("not-found");
      // The versioned-bucket delete-marker behavior the freed-key hazard
      // depends on: create-only PUT succeeds after DELETE.
      expect((await driver.putIfAbsent(k("free/a"), "two")).kind).toBe("created");
    });

    it("putIfMatch CASes on the current etag; missing key is a precondition failure", async () => {
      const { etag } = await driver.put(k("cas/b"), "v1");
      expect((await driver.putIfMatch(k("cas/b"), "v2", '"bogus"')).kind).toBe(
        "precondition-failed",
      );
      const updated = await driver.putIfMatch(k("cas/b"), "v2", etag);
      expect(updated.kind).toBe("updated");
      expect((await driver.putIfMatch(k("cas/b"), "v3", etag)).kind).toBe("precondition-failed");
      expect((await driver.putIfMatch(k("cas/missing"), "v", etag)).kind).toBe(
        "precondition-failed",
      );
    });

    it("pinned get: matches its etag, 412s after replacement, 404s when absent", async () => {
      const { etag } = await driver.put(k("pin/a"), "v1");
      const pinned = await driver.get(k("pin/a"), { ifMatch: etag });
      expect(pinned.kind === "found" && pinned.body).toBe("v1");
      await driver.put(k("pin/a"), "v2");
      expect((await driver.get(k("pin/a"), { ifMatch: etag })).kind).toBe("precondition-failed");
      expect((await driver.get(k("pin/missing"), { ifMatch: etag })).kind).toBe("not-found");
    });

    it("put returns the etag a subsequent get and list report", async () => {
      const { etag } = await driver.put(k("etag/a"), "body");
      const got = await driver.get(k("etag/a"));
      expect(got.kind === "found" && got.etag).toBe(etag);
      const page = await driver.list(k("etag/"));
      expect(page.keys).toEqual([{ key: k("etag/a"), etag }]);
    });

    it("list pages lexicographically and resumes strictly after the token", async () => {
      for (const s of ["03", "01", "05", "04", "02"]) await driver.put(k(`page/${s}`), s);
      await driver.put(k("pagz/off-prefix"), "x"); // must not appear
      const collected: string[] = [];
      let startAfter: string | undefined;
      let pages = 0;
      for (;;) {
        const page =
          startAfter !== undefined
            ? await driver.list(k("page/"), { startAfter, maxKeys: 2 })
            : await driver.list(k("page/"), { maxKeys: 2 });
        pages++;
        collected.push(...page.keys.map((x) => x.key));
        if (page.nextStartAfter === undefined) break;
        startAfter = page.nextStartAfter;
      }
      expect(collected).toEqual(["01", "02", "03", "04", "05"].map((s) => k(`page/${s}`)));
      expect(pages).toBeGreaterThanOrEqual(3);
      // startAfter is strictly-after
      const after = await driver.list(k("page/"), { startAfter: k("page/03") });
      expect(after.keys.map((x) => x.key)).toEqual([k("page/04"), k("page/05")]);
    });

    it("deleteMany removes every named key", async () => {
      for (const s of ["a", "b", "c"]) await driver.put(k(`many/${s}`), s);
      await driver.deleteMany([k("many/a"), k("many/c")]);
      const page = await driver.list(k("many/"));
      expect(page.keys.map((x) => x.key)).toEqual([k("many/b")]);
      await driver.deleteMany([]); // no-op, must not throw
    });
  });
}
