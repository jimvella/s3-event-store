/**
 * The event store: append protocol steps 1-4 and the chunk-aware read path
 * (DESIGN.md, "Core mechanism"). Phase-1 scope: cold-path head resolution
 * only (head.json hints and the in-process head cache are roadmap phase 2;
 * nothing here writes head.json yet).
 */

import type { StorageDriver, ListedKey } from "./driver.js";
import { ConcurrencyError, CorruptionError, TransientStoreError } from "./errors.js";
import {
  baseFromKey,
  bucketBase,
  chunkKey,
  chunkPrefix,
  commitKey,
  commitPrefix,
  validateStreamId,
} from "./keys.js";
import type {
  AppendResult,
  ChunkObject,
  CommitObject,
  EventEnvelope,
  EventInput,
  ExpectedVersion,
} from "./types.js";

export interface EventStoreConfig {
  driver: StorageDriver;
  prefix: string;
  /**
   * N: chunk bucket width and per-commit event cap. Store-level constant —
   * chunk keys derive from it; changing it under existing data means full
   * recompaction.
   */
  chunkSize?: number;
  /** Injectable id source (commitId, event ids) — deterministic under sim. */
  ids?: () => string;
  /** Injectable clock — deterministic under sim. */
  clock?: () => string;
  /** Bounded retries for transient driver failures and `"any"` conflicts. */
  maxRetries?: number;
}

export interface ReadOptions {
  fromVersion?: number;
}

export interface AppendOptions {
  expectedVersion: ExpectedVersion;
}

export interface EventStore {
  append(streamId: string, events: EventInput[], opts: AppendOptions): Promise<AppendResult>;
  read(streamId: string, opts?: ReadOptions): AsyncIterable<EventEnvelope>;
  /** Resolve the current head version, or "noStream". Exposed for tests. */
  resolveHead(streamId: string): Promise<HeadResolution>;
}

export type HeadResolution =
  | { kind: "noStream" }
  | { kind: "head"; version: number; lastCommitKey: string; lastCommitEtag: string };

export function createEventStore(config: EventStoreConfig): EventStore {
  const driver = config.driver;
  const prefix = config.prefix;
  const chunkSize = config.chunkSize ?? 500;
  const ids = config.ids ?? (() => globalThis.crypto.randomUUID());
  const clock = config.clock ?? (() => new Date().toISOString());
  const maxRetries = config.maxRetries ?? 5;

  /** Drain a paginated LIST; each page is a separate driver call. */
  async function listAll(pfx: string, startAfter?: string): Promise<ListedKey[]> {
    const keys: ListedKey[] = [];
    let after = startAfter;
    for (;;) {
      const page = after !== undefined ? await driver.list(pfx, { startAfter: after }) : await driver.list(pfx);
      keys.push(...page.keys);
      if (page.nextStartAfter === undefined) return keys;
      after = page.nextStartAfter;
    }
  }

  function parseCommit(body: string): CommitObject {
    return JSON.parse(body) as CommitObject;
  }

  function parseChunk(body: string): ChunkObject {
    return JSON.parse(body) as ChunkObject;
  }

  /**
   * Cold-path head resolution (DESIGN.md, "Head discovery"). Every anchor
   * GET is pinned to the ETag its LIST reported; a 412/404 means the anchor
   * was replaced or compacted after the LIST — re-resolve.
   */
  async function resolveHead(streamId: string): Promise<HeadResolution> {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const chunks = await listAll(chunkPrefix(prefix, streamId));
      let startAfter: string | undefined;
      if (chunks.length > 0) {
        const last = chunks[chunks.length - 1]!;
        const got = await driver.get(last.key, { ifMatch: last.etag });
        if (got.kind !== "found") continue; // chunk replaced under us; re-resolve
        startAfter = parseChunk(got.body).lastCommitKey;
      }
      const tail = await listAll(commitPrefix(prefix, streamId), startAfter);
      if (tail.length === 0) {
        if (chunks.length === 0) return { kind: "noStream" };
        // A non-empty stream always has commits under e/ — the highest
        // occupied bucket can never seal (DESIGN.md, Head discovery).
        throw new CorruptionError(`stream ${streamId}: chunks exist but e/ tail is empty`);
      }
      const anchor = tail[tail.length - 1]!;
      const got = await driver.get(anchor.key, { ifMatch: anchor.etag });
      if (got.kind !== "found") continue; // anchor compacted/replaced; re-resolve
      const commit = parseCommit(got.body);
      // Version math from the body, never key arithmetic.
      return {
        kind: "head",
        version: commit.baseVersion + commit.events.length - 1,
        lastCommitKey: anchor.key,
        lastCommitEtag: got.etag,
      };
    }
    throw new TransientStoreError(`head resolution for ${streamId} kept losing races; giving up`);
  }

  /** Conditional PUT with bounded transient retry; the commitId self-check
   * (append step 3) disambiguates a retry colliding with its own write. */
  async function putCommit(key: string, body: string) {
    let lastErr: unknown;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        return await driver.putIfAbsent(key, body);
      } catch (err) {
        if (!(err instanceof TransientStoreError)) throw err;
        lastErr = err;
      }
    }
    throw lastErr;
  }

  /** Does this bucket's chunk exist, and does it contain our commit? */
  async function chunkVerdict(
    streamId: string,
    base: number,
    commitId: string,
  ): Promise<"no-chunk" | "ours" | "foreign"> {
    const key = chunkKey(prefix, streamId, bucketBase(base, chunkSize));
    const got = await driver.get(key);
    if (got.kind !== "found") return "no-chunk";
    const chunk = parseChunk(got.body);
    return chunk.commits.some((c) => c.commitId === commitId) ? "ours" : "foreign";
  }

  async function appendAt(
    streamId: string,
    baseVersion: number,
    events: EventInput[],
  ): Promise<AppendResult> {
    const committedAt = clock();
    const commit: CommitObject = {
      commitId: ids(),
      streamId,
      baseVersion,
      events: events.map((e, i) => ({
        id: e.id ?? ids(),
        type: e.type,
        version: baseVersion + i,
        data: e.data,
        meta: { ...e.meta, ts: e.meta?.ts ?? committedAt },
      })),
      committedAt,
    };
    const key = commitKey(prefix, streamId, baseVersion);
    const result = await putCommit(key, JSON.stringify(commit));
    const ok: AppendResult = {
      streamId,
      nextExpectedVersion: baseVersion + events.length - 1,
      committedAt,
    };

    if (result.kind === "created") {
      // Step 4: verify the target bucket has no chunk. Keyed on the
      // *baseVersion* just written (a boundary-straddling commit's last
      // event selects the next bucket, which can never be chunked first).
      switch (await chunkVerdict(streamId, baseVersion, commit.commitId)) {
        case "no-chunk":
          return ok; // the overwhelmingly common case
        case "ours":
          return ok; // lost response earlier; our commit already compacted
        case "foreign":
          // The PUT recreated a freed key: unreadable orphan, sweep garbage.
          throw new ConcurrencyError(
            streamId,
            `append at version ${baseVersion} landed in an already-compacted bucket`,
          );
      }
    }

    // Step 3: 412. GET the key we just targeted and compare commitId.
    const got = await driver.get(key);
    if (got.kind === "found") {
      if (parseCommit(got.body).commitId === commit.commitId) return ok; // we already won
      // A foreign commitId at the key is not yet proof we lost: our own
      // commit may have been compacted and its freed key recreated.
      if ((await chunkVerdict(streamId, baseVersion, commit.commitId)) === "ours") return ok;
    } else {
      // Key compacted since the 412 — check the bucket's chunk instead.
      if ((await chunkVerdict(streamId, baseVersion, commit.commitId)) === "ours") return ok;
    }
    throw new ConcurrencyError(streamId, `version ${baseVersion} was won by another writer`);
  }

  async function append(
    streamId: string,
    events: EventInput[],
    opts: AppendOptions,
  ): Promise<AppendResult> {
    validateStreamId(streamId);
    if (events.length === 0) throw new RangeError("append requires at least one event");
    if (events.length > chunkSize) {
      throw new RangeError(`a commit holds at most ${chunkSize} events (got ${events.length})`);
    }
    const expected = opts.expectedVersion;
    if (typeof expected === "number" && (!Number.isInteger(expected) || expected < 0)) {
      // -1 is rejected rather than aliased: first append is "noStream".
      throw new RangeError(`expectedVersion must be >= 0, "any", or "noStream" (got ${expected})`);
    }

    const attempts = expected === "any" ? maxRetries : 1;
    let lastConflict: ConcurrencyError | undefined;
    for (let i = 0; i < attempts; i++) {
      // Step 1: head resolution — mandatory, authoritative for rejection.
      const head = await resolveHead(streamId);
      if (expected === "noStream" && head.kind !== "noStream") {
        throw new ConcurrencyError(streamId, "expected no stream, but the stream exists");
      }
      if (typeof expected === "number") {
        if (head.kind === "noStream" || head.version !== expected) {
          const actual = head.kind === "noStream" ? "no stream" : `version ${head.version}`;
          throw new ConcurrencyError(streamId, `expected version ${expected}, found ${actual}`);
        }
      }
      const base = head.kind === "noStream" ? 0 : head.version + 1;
      try {
        return await appendAt(streamId, base, events);
      } catch (err) {
        if (err instanceof ConcurrencyError && expected === "any") {
          lastConflict = err;
          continue;
        }
        throw err;
      }
    }
    throw lastConflict ?? new ConcurrencyError(streamId, "append retries exhausted");
  }

  /**
   * Chunk-aware read (DESIGN.md, "Reader path with chunks"): LIST c/ → GET
   * chunks → LIST e/ tail → pinned GETs, ignoring commits in chunked
   * buckets, verifying contiguity. A 404/412/discontinuity means "compacted
   * since our LIST" — re-LIST c/ and fill the gap from the new chunk.
   */
  async function* read(streamId: string, opts?: ReadOptions): AsyncGenerator<EventEnvelope> {
    validateStreamId(streamId);
    const fromVersion = opts?.fromVersion ?? 0;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const yielded: EventEnvelope[] = [];
      const ok = await readOnce(streamId, fromVersion, yielded);
      // Buffer-then-yield per attempt keeps recovery retries invisible to
      // the consumer (never yield, fail, and re-yield). Streaming with
      // mid-iteration recovery is a later optimization.
      if (ok) {
        yield* yielded;
        return;
      }
    }
    throw new CorruptionError(`read of ${streamId} could not reach a consistent listing`);
  }

  /** One read attempt; false = lost a race with a compactor, retry. */
  async function readOnce(
    streamId: string,
    fromVersion: number,
    out: EventEnvelope[],
  ): Promise<boolean> {
    const chunks = await listAll(chunkPrefix(prefix, streamId));
    const chunkedBuckets = new Set(chunks.map((c) => baseFromKey(c.key)));

    let expectedBase = 0;
    let startAfter: string | undefined;
    for (const listed of chunks) {
      const got = await driver.get(listed.key, { ifMatch: listed.etag });
      if (got.kind !== "found") return false; // chunk replaced; re-list
      const chunk = parseChunk(got.body);
      for (const commit of chunk.commits) {
        if (commit.baseVersion !== expectedBase) {
          throw new CorruptionError(
            `stream ${streamId}: chunk ${chunk.chunkBase} discontiguous at base ${commit.baseVersion}`,
          );
        }
        expectedBase = commit.baseVersion + commit.events.length;
        emit(commit, fromVersion, out);
      }
      startAfter = chunk.lastCommitKey;
    }

    // Tail: buffered listing (simplification noted in SIMULATOR_PLAN.md);
    // pages remain separate LIST calls, so LIST-time races still occur.
    const tail = await listAll(commitPrefix(prefix, streamId), startAfter);

    // Sealed-bucket check, iterated (DESIGN.md, Compaction failure modes):
    // never yield tail commits from a sealed bucket without confirming it
    // has no chunk. 404 clears every sealed bucket (chunks are dense); a
    // hit means our c/ listing is stale — re-list and re-anchor.
    const maxListedBase = tail.length > 0 ? baseFromKey(tail[tail.length - 1]!.key) : -1;
    for (const listed of tail) {
      const bucket = bucketBase(baseFromKey(listed.key), chunkSize);
      if (chunkedBuckets.has(bucket)) continue; // already known chunked
      const sealed = maxListedBase >= bucket + chunkSize;
      if (!sealed) break; // buckets are ascending; the rest are hot tail
      const got = await driver.get(chunkKey(prefix, streamId, bucket));
      if (got.kind === "found") return false; // stale listing; re-list c/
      break; // 404: chunks are dense, so no later bucket has one either
    }

    for (const listed of tail) {
      const base = baseFromKey(listed.key);
      if (chunkedBuckets.has(bucketBase(base, chunkSize))) continue; // chunk is authoritative
      const got = await driver.get(listed.key, { ifMatch: listed.etag });
      if (got.kind !== "found") return false; // compacted or replaced post-LIST
      const commit = parseCommit(got.body);
      if (commit.baseVersion !== expectedBase) return false; // gap: compacted between pages
      expectedBase = commit.baseVersion + commit.events.length;
      emit(commit, fromVersion, out);
    }
    return true;
  }

  function emit(commit: CommitObject, fromVersion: number, out: EventEnvelope[]): void {
    for (const event of commit.events) {
      if (event.version >= fromVersion) out.push(event);
    }
  }

  return { append, read, resolveHead };
}
