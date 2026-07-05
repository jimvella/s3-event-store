/**
 * The event store: the append protocol (steps 1-5), head discovery (fast
 * path via the head.json hint, cold path via chunk-seeded LISTs, in-process
 * head cache), the chunk-aware read path, and compaction
 * (DESIGN.md, "Core mechanism" / "Compaction protocol").
 */

import type { StorageDriver, ListedKey } from "./driver.js";
import { ConcurrencyError, CorruptionError, TransientStoreError } from "./errors.js";
import { jsonSerializer, type PayloadSerializer } from "./serializer.js";
import {
  baseFromKey,
  bucketBase,
  chunkKey,
  chunkPrefix,
  commitKey,
  commitPrefix,
  headKey,
  validateStreamId,
} from "./keys.js";
import type {
  AppendResult,
  ChunkObject,
  CommitObject,
  EventEnvelope,
  EventInput,
  ExpectedVersion,
  HeadHint,
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
  /** Payload serializer (the encryption seam). Default: plain JSON. */
  serializer?: PayloadSerializer;
  /**
   * Permit `$`-prefixed stream IDs — library-internal writers only
   * ($system.key-audit). The external surface must keep rejecting them.
   */
  allowReservedStreams?: boolean;
}

export interface ReadOptions {
  fromVersion?: number;
  /** Skip deserialization: yield stored payloads (ciphertext) verbatim —
   * the model-B egress path. Default false (plaintext out). */
  raw?: boolean;
}

export interface AppendOptions {
  expectedVersion: ExpectedVersion;
}

export interface EventStore {
  /**
   * The chunk/bucket width this store compacts to — the store-level constant N.
   * Exposed so HTTP egress helpers can align page boundaries to chunk
   * boundaries without a caller re-declaring N (DESIGN.md, "Page boundaries are
   * a deterministic function of the version, aligned to chunk size N").
   */
  readonly chunkSize: number;
  append(streamId: string, events: EventInput[], opts: AppendOptions): Promise<AppendResult>;
  read(streamId: string, opts?: ReadOptions): AsyncIterable<EventEnvelope>;
  /** Resolve the current head version, or "noStream". Exposed for tests. */
  resolveHead(streamId: string): Promise<HeadResolution>;
  /**
   * Compact the lowest sealed uncompacted bucket, if any (at most one per
   * invocation — DESIGN.md, Compaction). Safe under sloppy triggering:
   * duplicate and racing invocations are harmless by construction.
   */
  compactStream(streamId: string): Promise<CompactionResult>;
  /**
   * Delete sub-watermark garbage commits (crash leftovers, freed-key
   * recreations). Hygiene, not correctness; scans from the stream start.
   */
  sweepStream(streamId: string): Promise<{ deleted: number }>;
}

export type CompactionResult =
  /** This invocation created the chunk and deleted the sources. */
  | { status: "compacted"; chunkBase: number }
  /** A racing compactor won this bucket; state is already correct. */
  | { status: "stood-down"; chunkBase: number }
  /** No sealed uncompacted bucket exists behind the head. */
  | { status: "nothing-to-do" };

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
  const serializer = config.serializer ?? jsonSerializer();
  const idOpts = { allowReserved: config.allowReservedStreams === true };

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
   * In-process head cache (DESIGN.md, Cost & performance): a hint that
   * skips resolution, sound only because of the step-4 chunk check. A 412
   * or a step-4 ConcurrencyError invalidates it; stale-high cannot arise
   * within the protocol (the process only caches heads it observed).
   */
  interface CachedHead {
    version: number;
    lastCommitKey: string;
    lastCommitEtag: string;
    /** Last-known compaction watermark — feeds the trigger arithmetic. */
    compactedTo: number;
  }
  const headCache = new Map<string, CachedHead>();

  interface ResolvedState {
    head: HeadResolution;
    watermark: number;
  }

  /** Full resolution: fast path via the hint, cold path as fallback. */
  async function resolveHeadInternal(streamId: string): Promise<ResolvedState> {
    const hintGot = await driver.get(headKey(prefix, streamId));
    if (hintGot.kind === "found") {
      const hint = JSON.parse(hintGot.body) as HeadHint;
      if (hint.lastCommitKey !== null && hint.lastCommitEtag !== null) {
        const fast = await resolveFast(streamId, hint.lastCommitKey, hint.lastCommitEtag);
        if (fast !== null) {
          const state = { head: fast, watermark: hint.compactedTo ?? 0 };
          cacheState(streamId, state);
          return state;
        }
        // Every hint corruption falls safe to the cold path, never corrupt.
      }
    }
    const state = await resolveCold(streamId);
    cacheState(streamId, state);
    return state;
  }

  function cacheState(streamId: string, state: ResolvedState): void {
    if (state.head.kind === "head") {
      headCache.set(streamId, {
        version: state.head.version,
        lastCommitKey: state.head.lastCommitKey,
        lastCommitEtag: state.head.lastCommitEtag,
        compactedTo: state.watermark,
      });
    } else {
      headCache.delete(streamId);
    }
  }

  /**
   * Fast path (DESIGN.md, Head discovery): LIST strictly after the hint's
   * key — the key, not a version the hint asserts. If the LIST returns
   * keys, the newest anchors the head (pinned to its listed ETag). If it
   * returns nothing, the hint is the only evidence: GET `lastCommitKey`
   * pinned to `lastCommitEtag` and derive the head from the body —
   * existence alone is not corroboration. Returns null to fall cold.
   */
  async function resolveFast(
    streamId: string,
    lastCommitKey: string,
    lastCommitEtag: string,
  ): Promise<HeadResolution | null> {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const tail = await listAll(commitPrefix(prefix, streamId), lastCommitKey);
      if (tail.length > 0) {
        const anchor = tail[tail.length - 1]!;
        const got = await driver.get(anchor.key, { ifMatch: anchor.etag });
        if (got.kind !== "found") continue; // anchor replaced/compacted; re-list
        const commit = parseCommit(got.body);
        return {
          kind: "head",
          version: commit.baseVersion + commit.events.length - 1,
          lastCommitKey: anchor.key,
          lastCommitEtag: got.etag,
        };
      }
      const got = await driver.get(lastCommitKey, { ifMatch: lastCommitEtag });
      if (got.kind !== "found") return null; // hint invalidated → cold path
      const commit = parseCommit(got.body);
      return {
        kind: "head",
        version: commit.baseVersion + commit.events.length - 1,
        lastCommitKey,
        lastCommitEtag: got.etag,
      };
    }
    return null;
  }

  /**
   * Cold path (DESIGN.md, "Head discovery"): the last chunk's recorded
   * anchor seeds the e/ LIST. Every anchor GET is pinned to the ETag its
   * LIST reported; a 412/404 means the anchor was replaced or compacted
   * after the LIST — re-resolve.
   */
  async function resolveCold(streamId: string): Promise<ResolvedState> {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const chunks = await listAll(chunkPrefix(prefix, streamId));
      let startAfter: string | undefined;
      let watermark = 0;
      if (chunks.length > 0) {
        const last = chunks[chunks.length - 1]!;
        watermark = baseFromKey(last.key) + chunkSize;
        const got = await driver.get(last.key, { ifMatch: last.etag });
        if (got.kind !== "found") continue; // chunk replaced under us; re-resolve
        startAfter = parseChunk(got.body).lastCommitKey;
      }
      const tail = await listAll(commitPrefix(prefix, streamId), startAfter);
      if (tail.length === 0) {
        if (chunks.length === 0) return { head: { kind: "noStream" }, watermark: 0 };
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
        head: {
          kind: "head",
          version: commit.baseVersion + commit.events.length - 1,
          lastCommitKey: anchor.key,
          lastCommitEtag: got.etag,
        },
        watermark,
      };
    }
    throw new TransientStoreError(`head resolution for ${streamId} kept losing races; giving up`);
  }

  async function resolveHead(streamId: string): Promise<HeadResolution> {
    validateStreamId(streamId, idOpts);
    return (await resolveHeadInternal(streamId)).head;
  }

  /**
   * Step 5: best-effort head.json hint (plain LWW PUT). Off the critical
   * path — transient failures are swallowed; regression is benign.
   */
  async function writeHint(streamId: string, hint: HeadHint): Promise<void> {
    try {
      await driver.put(headKey(prefix, streamId), JSON.stringify(hint));
    } catch (err) {
      if (!(err instanceof TransientStoreError)) throw err;
    }
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

  /** EventInput with its payload already through the serializer. */
  interface PreparedEvent extends EventInput {
    keyId?: string;
  }

  async function appendAt(
    streamId: string,
    baseVersion: number,
    events: PreparedEvent[],
    watermark: number,
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
        ...(e.keyId !== undefined ? { keyId: e.keyId } : {}),
        meta: { ...e.meta, ts: e.meta?.ts ?? committedAt },
      })),
      committedAt,
    };
    const key = commitKey(prefix, streamId, baseVersion);
    const newHead = baseVersion + events.length - 1;
    const ok: AppendResult = {
      streamId,
      nextExpectedVersion: newHead,
      committedAt,
      // Trigger arithmetic (DESIGN.md, Scheduling): a base at least one full
      // bucket past the watermark proves a sealed uncompacted bucket exists.
      compactionSuggested: baseVersion >= watermark + chunkSize,
    };
    /** Success bookkeeping: refresh the cache and the head.json hint. */
    const won = async (etag: string | null): Promise<AppendResult> => {
      if (etag !== null) {
        headCache.set(streamId, { version: newHead, lastCommitKey: key, lastCommitEtag: etag, compactedTo: watermark });
        await writeHint(streamId, {
          headVersion: newHead,
          lastCommitKey: key,
          lastCommitEtag: etag,
          compactedTo: watermark,
        });
      } else {
        // Success proven via the chunk: no live key to point the hint at.
        headCache.delete(streamId);
      }
      return ok;
    };
    const result = await putCommit(key, JSON.stringify(commit));

    if (result.kind === "created") {
      // Step 4: verify the target bucket has no chunk. Keyed on the
      // *baseVersion* just written (a boundary-straddling commit's last
      // event selects the next bucket, which can never be chunked first).
      switch (await chunkVerdict(streamId, baseVersion, commit.commitId)) {
        case "no-chunk":
          return won(result.etag); // the overwhelmingly common case
        case "ours":
          return won(null); // lost response earlier; our commit already compacted
        case "foreign":
          // The PUT recreated a freed key: unreadable orphan, sweep garbage.
          headCache.delete(streamId);
          throw new ConcurrencyError(
            streamId,
            `append at version ${baseVersion} landed in an already-compacted bucket`,
          );
      }
    }

    // Step 3: 412. GET the key we just targeted and compare commitId.
    const got = await driver.get(key);
    if (got.kind === "found") {
      if (parseCommit(got.body).commitId === commit.commitId) {
        // OUR commitId at the key is NOT yet proof we won: with the first
        // PUT's response lost (nothing written — another writer held the
        // key), a later retry can itself be the freed-key recreation after
        // compaction. Same chunk verdict as step 4 disambiguates.
        // (Found by the simulator's storage invariant, seed 641.)
        switch (await chunkVerdict(streamId, baseVersion, commit.commitId)) {
          case "no-chunk":
            return won(got.etag); // live commit in an unchunked bucket: we won
          case "ours":
            return won(null); // compacted; the object at the key may be our orphan
          case "foreign":
            break; // our object at the key is a freed-key orphan: we lost
        }
      } else if ((await chunkVerdict(streamId, baseVersion, commit.commitId)) === "ours") {
        // A foreign commitId at the key is not yet proof we lost: our own
        // commit may have been compacted and its freed key recreated.
        return won(null);
      }
    } else {
      // Key compacted since the 412 — check the bucket's chunk instead.
      if ((await chunkVerdict(streamId, baseVersion, commit.commitId)) === "ours") return won(null);
    }
    headCache.delete(streamId);
    throw new ConcurrencyError(streamId, `version ${baseVersion} was won by another writer`);
  }

  async function append(
    streamId: string,
    events: EventInput[],
    opts: AppendOptions,
  ): Promise<AppendResult> {
    validateStreamId(streamId, idOpts);
    if (events.length === 0) throw new RangeError("append requires at least one event");
    if (events.length > chunkSize) {
      throw new RangeError(`a commit holds at most ${chunkSize} events (got ${events.length})`);
    }
    const expected = opts.expectedVersion;
    if (typeof expected === "number" && (!Number.isInteger(expected) || expected < 0)) {
      // -1 is rejected rather than aliased: first append is "noStream".
      throw new RangeError(`expectedVersion must be >= 0, "any", or "noStream" (got ${expected})`);
    }

    // Serialize once, before any retry loop: a retried conditional PUT
    // must carry byte-identical content (the commitId self-check depends
    // on it), and the encrypting serializer's tombstone consult fails a
    // doomed append here, before any PUT (SubjectErasedError).
    const prepared: PreparedEvent[] = [];
    for (const e of events) {
      const s = await serializer.serialize(streamId, { type: e.type, data: e.data });
      prepared.push({ ...e, data: s.data, ...(s.keyId !== undefined ? { keyId: s.keyId } : {}) });
    }

    const attempts = expected === "any" ? maxRetries : 1;
    let lastConflict: ConcurrencyError | undefined;
    for (let i = 0; i < attempts; i++) {
      // Step 1: head resolution — mandatory, authoritative for rejection.
      // The in-process cache may stand in for resolution only when it
      // *agrees* with the caller's intent (a disagreement could be a stale
      // cache, so it must never mint a rejection on its own — except for
      // "noStream", where any observed head proves the stream exists).
      const cached = headCache.get(streamId);
      let state: ResolvedState;
      if (cached && expected === "noStream") {
        throw new ConcurrencyError(streamId, "expected no stream, but the stream exists");
      } else if (
        cached &&
        (expected === "any" || (typeof expected === "number" && cached.version === expected))
      ) {
        state = {
          head: {
            kind: "head",
            version: cached.version,
            lastCommitKey: cached.lastCommitKey,
            lastCommitEtag: cached.lastCommitEtag,
          },
          watermark: cached.compactedTo,
        };
      } else {
        state = await resolveHeadInternal(streamId);
      }
      const head = state.head;
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
        return await appendAt(streamId, base, prepared, state.watermark);
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
    validateStreamId(streamId, idOpts);
    const fromVersion = opts?.fromVersion ?? 0;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const yielded: EventEnvelope[] = [];
      const ok = await readOnce(streamId, fromVersion, yielded);
      // Buffer-then-yield per attempt keeps recovery retries invisible to
      // the consumer (never yield, fail, and re-yield). Streaming with
      // mid-iteration recovery is a later optimization.
      if (ok) {
        if (opts?.raw === true) {
          yield* yielded;
        } else {
          for (const envelope of yielded) {
            yield { ...envelope, data: await serializer.deserialize(streamId, envelope) };
          }
        }
        return;
      }
    }
    // Every retry means a compaction pass moved the stream under us —
    // legal contention, not an impossible state. Retryable by the caller.
    throw new TransientStoreError(
      `read of ${streamId} kept losing compaction races; retry`,
    );
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

  /**
   * Compactor steps 1-4 (DESIGN.md, "Compactor steps"). Lock-free: the
   * bucket is deterministic (lowest sealed uncompacted), the chunk key is
   * deterministic, and `If-None-Match: *` picks one winner among racers.
   */
  async function compactStream(streamId: string): Promise<CompactionResult> {
    validateStreamId(streamId, idOpts);

    // Step 1: the last chunk's recorded anchor seeds the e/ LIST, so the
    // walk covers only the uncompacted tail (and skips sub-watermark
    // garbage, which is the sweep's business, not ours).
    const chunks = await listAll(chunkPrefix(prefix, streamId));
    let bucket = 0;
    let startAfter: string | undefined;
    if (chunks.length > 0) {
      const last = chunks[chunks.length - 1]!;
      bucket = baseFromKey(last.key) + chunkSize; // chunks are dense
      const got = await driver.get(last.key);
      if (got.kind !== "found") {
        throw new CorruptionError(`stream ${streamId}: chunk ${last.key} vanished (chunks are immutable)`);
      }
      startAfter = parseChunk(got.body).lastCommitKey;
    }
    const tail = await listAll(commitPrefix(prefix, streamId), startAfter);

    // Step 2: select bucket k only once bucket k+1 has started — the lag is
    // structural, not clock-based; the bucket can never gain a commit.
    const sealed = tail.some((k) => baseFromKey(k.key) >= bucket + chunkSize);
    if (!sealed) return { status: "nothing-to-do" };
    const members = tail.filter((k) => {
      const base = baseFromKey(k.key);
      return base >= bucket && base < bucket + chunkSize;
    });
    if (members.length === 0) {
      // A racing winner can compact this bucket between our c/ LIST and our
      // e/ LIST, emptying it from the listing. Its chunk proves that; only
      // sealed-and-empty with *no* chunk is impossible (bases are dense).
      const chunk = await driver.get(chunkKey(prefix, streamId, bucket));
      if (chunk.kind === "found") return { status: "stood-down", chunkBase: bucket };
      throw new CorruptionError(`stream ${streamId}: sealed bucket ${bucket} has no commits`);
    }

    // Step 3: assemble. GETs are pinned to the listing; a 404 or 412 on a
    // source means a racing winner is already deleting (its chunk strictly
    // precedes its deletes) — confirm the chunk and stand down.
    const commits: CommitObject[] = [];
    for (const member of members) {
      const got = await driver.get(member.key, { ifMatch: member.etag });
      if (got.kind !== "found") {
        const chunk = await driver.get(chunkKey(prefix, streamId, bucket));
        if (chunk.kind === "found") return { status: "stood-down", chunkBase: bucket };
        throw new CorruptionError(`stream ${streamId}: source ${member.key} gone with no chunk`);
      }
      commits.push(parseCommit(got.body));
    }
    const chunk: ChunkObject = {
      streamId,
      chunkBase: bucket,
      commits,
      lastCommitKey: members[members.length - 1]!.key,
    };
    const put = await driver.putIfAbsent(chunkKey(prefix, streamId, bucket), JSON.stringify(chunk));

    // Step 4: deletes strictly after the chunk exists. The 412 loser also
    // proceeds — membership is deterministic, so the keys are identical and
    // deletes are idempotent.
    await driver.deleteMany(members.map((m) => m.key));
    if (put.kind === "created") {
      await advanceWatermark(streamId, bucket + chunkSize);
      return { status: "compacted", chunkBase: bucket };
    }
    return { status: "stood-down", chunkBase: bucket };
  }

  /**
   * Best-effort watermark bump on head.json (LWW read-modify-write,
   * preserving the appenders' hint fields). Regression by a concurrent
   * stale writer is benign: consumers treat head.json as a hint, and a
   * regressed watermark costs at most a redundant, idempotent compaction
   * pass (DESIGN.md, Scheduling).
   */
  async function advanceWatermark(streamId: string, watermark: number): Promise<void> {
    try {
      const got = await driver.get(headKey(prefix, streamId));
      const prev = got.kind === "found" ? (JSON.parse(got.body) as HeadHint) : null;
      if (prev !== null && prev.compactedTo >= watermark) return;
      await driver.put(
        headKey(prefix, streamId),
        JSON.stringify({
          headVersion: prev?.headVersion ?? null,
          lastCommitKey: prev?.lastCommitKey ?? null,
          lastCommitEtag: prev?.lastCommitEtag ?? null,
          compactedTo: watermark,
        } satisfies HeadHint),
      );
    } catch (err) {
      if (!(err instanceof TransientStoreError)) throw err;
    }
  }

  /**
   * Step 5, as its own entry point: every e/ key below the watermark is
   * garbage by definition (chunks are dense up to it). Scans from the
   * stream start — an arbitrarily stalled writer can recreate a freed key
   * in a bucket the watermark passed long ago.
   */
  async function sweepStream(streamId: string): Promise<{ deleted: number }> {
    validateStreamId(streamId, idOpts);
    const chunks = await listAll(chunkPrefix(prefix, streamId));
    if (chunks.length === 0) return { deleted: 0 };
    const watermark = baseFromKey(chunks[chunks.length - 1]!.key) + chunkSize;
    const all = await listAll(commitPrefix(prefix, streamId));
    const garbage = all.map((k) => k.key).filter((key) => baseFromKey(key) < watermark);
    if (garbage.length > 0) await driver.deleteMany(garbage);
    return { deleted: garbage.length };
  }

  return { chunkSize, append, read, resolveHead, compactStream, sweepStream };
}
