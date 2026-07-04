/**
 * Storage-driver contract (DESIGN.md, "Storage drivers").
 *
 * The seam between the protocol and any S3-compatible backend, and the seam
 * the simulator injects at. Semantics every implementation must honor
 * (conformance-tested; SimStore is the executable reference):
 *
 * - Strong read-after-write consistency.
 * - `putIfAbsent` = PUT `If-None-Match: *` (create-only).
 * - `putIfMatch`  = PUT `If-Match: <etag>` (compare-and-swap).
 * - `get` takes an optional `ifMatch` etag (pinned GET).
 * - `list` returns one lexicographically ordered page per call, each key
 *   with its etag; `nextStartAfter` resumes strictly after it.
 * - `delete` is idempotent and frees the key: a subsequent `putIfAbsent`
 *   succeeds (versioned-bucket delete-marker behavior).
 *
 * Outcomes the protocol branches on (404/412) are result values; transport
 * failures (timeout, 5xx) are thrown as `TransientStoreError`.
 */

export type GetResult =
  | { kind: "found"; body: string; etag: string }
  | { kind: "not-found" }
  | { kind: "precondition-failed" };

export type PutIfAbsentResult =
  | { kind: "created"; etag: string }
  | { kind: "exists" };

export type PutIfMatchResult =
  | { kind: "updated"; etag: string }
  | { kind: "precondition-failed" };

export interface ListedKey {
  key: string;
  etag: string;
}

export interface ListPage {
  keys: ListedKey[];
  /** Present iff the listing is truncated; pass as `startAfter` to resume. */
  nextStartAfter?: string;
}

export interface StorageDriver {
  get(key: string, opts?: { ifMatch?: string }): Promise<GetResult>;
  /** Plain last-writer-wins PUT (head.json hint only — never commits). */
  put(key: string, body: string): Promise<{ etag: string }>;
  putIfAbsent(key: string, body: string): Promise<PutIfAbsentResult>;
  putIfMatch(key: string, body: string, etag: string): Promise<PutIfMatchResult>;
  list(prefix: string, opts?: { startAfter?: string; maxKeys?: number }): Promise<ListPage>;
  delete(key: string): Promise<void>;
  deleteMany(keys: string[]): Promise<void>;
}
