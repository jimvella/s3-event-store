/**
 * SimStore: the in-memory S3 semantic model (SIMULATOR_PLAN.md).
 *
 * Exactly the semantics DESIGN.md assumes of a backend, no more:
 * strong consistency, conditional PUT/GET, lexicographic paged LIST with
 * etags, idempotent DELETE that frees the key for `putIfAbsent`
 * (versioned-bucket delete-marker behavior).
 *
 * ETags are content hashes — deliberately, matching real S3's
 * MD5-for-simple-PUT: two identical bodies share an etag. The pinned-GET
 * rules must hold under this, the weakest guarantee (commit bodies always
 * differ by commitId; the simulator must not strengthen the primitive with
 * random-per-write etags).
 *
 * All operations are synchronous (atomic): scheduling and faults live in
 * the scheduler, not here.
 */

import type {
  GetResult,
  ListPage,
  PutIfAbsentResult,
  PutIfMatchResult,
} from "../src/driver.js";

/** FNV-1a 64-bit over the body, hex — stands in for S3's content MD5. */
export function contentEtag(body: string): string {
  let h = 0xcbf29ce484222325n;
  for (let i = 0; i < body.length; i++) {
    h ^= BigInt(body.charCodeAt(i));
    h = (h * 0x100000001b3n) & 0xffffffffffffffffn;
  }
  return `"${h.toString(16).padStart(16, "0")}"`;
}

export class SimStore {
  private objects = new Map<string, { body: string; etag: string }>();

  get(key: string, opts?: { ifMatch?: string }): GetResult {
    const obj = this.objects.get(key);
    if (!obj) return { kind: "not-found" };
    if (opts?.ifMatch !== undefined && opts.ifMatch !== obj.etag) {
      return { kind: "precondition-failed" };
    }
    return { kind: "found", body: obj.body, etag: obj.etag };
  }

  put(key: string, body: string): { etag: string } {
    const etag = contentEtag(body);
    this.objects.set(key, { body, etag });
    return { etag };
  }

  putIfAbsent(key: string, body: string): PutIfAbsentResult {
    if (this.objects.has(key)) return { kind: "exists" };
    return { kind: "created", ...this.put(key, body) };
  }

  putIfMatch(key: string, body: string, etag: string): PutIfMatchResult {
    const obj = this.objects.get(key);
    if (!obj || obj.etag !== etag) return { kind: "precondition-failed" };
    return { kind: "updated", ...this.put(key, body) };
  }

  /**
   * One lexicographic page. `nextStartAfter` is the last key consumed,
   * present iff truncated — resume strictly after it (the s2 rule).
   */
  list(prefix: string, opts?: { startAfter?: string; maxKeys?: number }): ListPage {
    const maxKeys = opts?.maxKeys ?? 1000;
    const startAfter = opts?.startAfter;
    const all = [...this.objects.keys()]
      .filter((k) => k.startsWith(prefix) && (startAfter === undefined || k > startAfter))
      .sort();
    const page = all.slice(0, maxKeys);
    const keys = page.map((key) => ({ key, etag: this.objects.get(key)!.etag }));
    if (all.length > maxKeys && page.length > 0) {
      return { keys, nextStartAfter: page[page.length - 1]! };
    }
    return { keys };
  }

  /** Idempotent; frees the key — a later putIfAbsent succeeds. */
  delete(key: string): void {
    this.objects.delete(key);
  }

  deleteMany(keys: string[]): void {
    for (const key of keys) this.objects.delete(key);
  }

  /** Snapshot for oracle/invariant checks (read-only). */
  dump(): ReadonlyMap<string, { body: string; etag: string }> {
    return this.objects;
  }
}
