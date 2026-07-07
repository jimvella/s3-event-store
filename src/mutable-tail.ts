/**
 * The MutableTail strategy (DESIGN.md, "Core mechanism: the mutable tail").
 *
 * The last chunk under `c/` is the live tail: an append CAS-updates it in
 * place, or — when the tail as read is full — mints a new chunk keyed by the
 * incoming commit's base. Nothing is ever deleted, so there is no freed-key
 * hazard: the ETag chain *is* the version check. The head is a number (the
 * current version); the tail is the physical last chunk object that holds it.
 */

import type { ListedKey, StorageDriver } from "./driver.js";
import { ConcurrencyError, TransientStoreError } from "./errors.js";
import { baseFromKey, chunkKey, chunkPrefix, validateStreamId } from "./keys.js";
import { jsonSerializer, type PayloadSerializer } from "./serializer.js";
import type {
  AppendResult,
  CommitObject,
  EventEnvelope,
  EventInput,
  ExpectedVersion,
  TailChunk,
} from "./types.js";
import type {
  AppendOptions,
  CompactionResult,
  EventStore,
  EventStoreConfig,
  HeadResolution,
  ReadOptions,
} from "./store.js";

export const DEFAULT_BYTE_CAP = 256 * 1024;

/** Resolved per-stream roll policy. */
interface RollPolicy {
  n: number;
  byteCap: number;
}

export interface MutableTailParams {
  chunkSize: number;
  byteCap: number;
  /** Per-stream override, consulted only when a chunk is minted. */
  policyFor?: (streamId: string) => { chunkSize?: number; byteCap?: number } | undefined;
}

/** EventInput with its payload already through the serializer. */
interface PreparedEvent extends EventInput {
  keyId?: string;
}

type TailState =
  | { kind: "empty" }
  | { kind: "tail"; key: string; etag: string; chunk: TailChunk; bytes: number };

export function createMutableTailStore(
  config: EventStoreConfig,
  params: MutableTailParams,
): EventStore {
  const driver = config.driver;
  const prefix = config.prefix;
  const ids = config.ids ?? (() => globalThis.crypto.randomUUID());
  const clock = config.clock ?? (() => new Date().toISOString());
  const maxRetries = config.maxRetries ?? 5;
  const serializer = config.serializer ?? jsonSerializer();
  const idOpts = { allowReserved: config.allowReservedStreams === true };
  const storeDefault: RollPolicy = { n: params.chunkSize, byteCap: params.byteCap };

  /** In-process tail cache: a hint that skips resolution. Sound with no
   * freed-key caveat — a stale entry simply 412s on the CAS and re-resolves. */
  const tailCache = new Map<string, TailState & { kind: "tail" }>();

  async function listAll(pfx: string, startAfter?: string): Promise<ListedKey[]> {
    const keys: ListedKey[] = [];
    let after = startAfter;
    for (;;) {
      const page =
        after !== undefined ? await driver.list(pfx, { startAfter: after }) : await driver.list(pfx);
      keys.push(...page.keys);
      if (page.nextStartAfter === undefined) return keys;
      after = page.nextStartAfter;
    }
  }

  const parseChunk = (body: string): TailChunk => JSON.parse(body) as TailChunk;

  /** Head version a tail chunk currently derives: last commit's base + count − 1. */
  function headOf(chunk: TailChunk): number {
    const last = chunk.commits[chunk.commits.length - 1]!;
    return last.baseVersion + last.events.length - 1;
  }

  /** Fullness judged on the tail *as read* (DESIGN.md, Append protocol step 3). */
  function isFull(chunk: TailChunk, bytes: number): boolean {
    return chunk.commits.length >= chunk.n || bytes >= chunk.byteCap;
  }

  /**
   * Resolve the tail: LIST `c/` and GET the greatest-base chunk (the live
   * tail). Missing a chunk created concurrently after the LIST is a legal
   * linearization — the CAS/create precondition catches any real move.
   */
  async function resolveTail(streamId: string): Promise<TailState> {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const chunks = await listAll(chunkPrefix(prefix, streamId));
      if (chunks.length === 0) return { kind: "empty" };
      const last = chunks[chunks.length - 1]!;
      const got = await driver.get(last.key);
      if (got.kind !== "found") continue; // never deleted; a vanished listing → re-list
      return { kind: "tail", key: last.key, etag: got.etag, chunk: parseChunk(got.body), bytes: got.body.length };
    }
    throw new TransientStoreError(`tail resolution for ${streamId} kept failing; retry`);
  }

  function policyFromResolver(streamId: string): Partial<RollPolicy> | undefined {
    const p = params.policyFor?.(streamId);
    if (!p) return undefined;
    const out: Partial<RollPolicy> = {};
    if (p.chunkSize !== undefined) out.n = p.chunkSize;
    if (p.byteCap !== undefined) out.byteCap = p.byteCap;
    return out;
  }

  /**
   * Policy stamped into a chunk being minted. Precedence (DESIGN.md,
   * Configuring N): creation-time override → policyFor → propagate the tail's
   * policy on a roll, or the store default at stream creation.
   */
  function mintPolicy(streamId: string, tail: TailState, opts: AppendOptions): RollPolicy {
    const base: RollPolicy = tail.kind === "tail" ? { n: tail.chunk.n, byteCap: tail.chunk.byteCap } : storeDefault;
    const resolved = policyFromResolver(streamId);
    const creation = tail.kind === "empty" ? opts : undefined; // overrides only at stream creation
    return {
      n: creation?.chunkSize ?? resolved?.n ?? base.n,
      byteCap: creation?.byteCap ?? resolved?.byteCap ?? base.byteCap,
    };
  }

  function buildCommit(streamId: string, base: number, events: PreparedEvent[]): { commit: CommitObject; committedAt: string; newHead: number } {
    const committedAt = clock();
    const commit: CommitObject = {
      commitId: ids(),
      streamId,
      baseVersion: base,
      events: events.map((e, i) => ({
        id: e.id ?? ids(),
        type: e.type,
        version: base + i,
        data: e.data,
        ...(e.keyId !== undefined ? { keyId: e.keyId } : {}),
        meta: { ...e.meta, ts: e.meta?.ts ?? committedAt },
      })),
      committedAt,
    };
    return { commit, committedAt, newHead: base + events.length - 1 };
  }

  function won(streamId: string, key: string, etag: string, chunk: TailChunk, newHead: number, committedAt: string): AppendResult {
    const bytes = JSON.stringify(chunk).length;
    tailCache.set(streamId, { kind: "tail", key, etag, chunk, bytes });
    return { streamId, nextExpectedVersion: newHead, committedAt, compactionSuggested: false };
  }

  /**
   * A conditional write lost. Re-resolve and look for our own `commitId` in the
   * chunk covering our base: a lost-response retry finds it there and reports
   * success; otherwise another writer won the version.
   */
  async function onConflict(streamId: string, base: number, commitId: string, newHead: number, committedAt: string): Promise<AppendResult> {
    tailCache.delete(streamId);
    const chunks = await listAll(chunkPrefix(prefix, streamId));
    let covering: ListedKey | undefined;
    for (const k of chunks) {
      if (baseFromKey(k.key) <= base) covering = k;
      else break;
    }
    if (covering) {
      const got = await driver.get(covering.key);
      if (got.kind === "found" && parseChunk(got.body).commits.some((c) => c.commitId === commitId)) {
        return { streamId, nextExpectedVersion: newHead, committedAt, compactionSuggested: false };
      }
    }
    throw new ConcurrencyError(streamId, `version ${base} was won by another writer`);
  }

  async function putIfMatch(key: string, body: string, etag: string) {
    let lastErr: unknown;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        return await driver.putIfMatch(key, body, etag);
      } catch (err) {
        if (!(err instanceof TransientStoreError)) throw err;
        lastErr = err;
      }
    }
    throw lastErr;
  }

  async function putIfAbsent(key: string, body: string) {
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

  /** Append or roll against a resolved tail. */
  async function tryAppend(streamId: string, tail: TailState, base: number, events: PreparedEvent[], opts: AppendOptions): Promise<AppendResult> {
    if (tail.kind === "tail" && !isFull(tail.chunk, tail.bytes)) {
      if (events.length > tail.chunk.n) {
        throw new RangeError(`a commit holds at most ${tail.chunk.n} events (got ${events.length})`);
      }
      const { commit, committedAt, newHead } = buildCommit(streamId, base, events);
      const next: TailChunk = { ...tail.chunk, commits: [...tail.chunk.commits, commit] };
      const res = await putIfMatch(tail.key, JSON.stringify(next), tail.etag);
      if (res.kind === "updated") return won(streamId, tail.key, res.etag, next, newHead, committedAt);
      return onConflict(streamId, base, commit.commitId, newHead, committedAt);
    }
    // Mint a new chunk keyed by the incoming base (stream creation, or a roll).
    const policy = mintPolicy(streamId, tail, opts);
    if (events.length > policy.n) {
      throw new RangeError(`a commit holds at most ${policy.n} events (got ${events.length})`);
    }
    const { commit, committedAt, newHead } = buildCommit(streamId, base, events);
    const chunk: TailChunk = { streamId, chunkBase: base, n: policy.n, byteCap: policy.byteCap, commits: [commit] };
    const key = chunkKey(prefix, streamId, base);
    const res = await putIfAbsent(key, JSON.stringify(chunk));
    if (res.kind === "created") return won(streamId, key, res.etag, chunk, newHead, committedAt);
    return onConflict(streamId, base, commit.commitId, newHead, committedAt);
  }

  async function append(streamId: string, events: EventInput[], opts: AppendOptions): Promise<AppendResult> {
    validateStreamId(streamId, idOpts);
    if (events.length === 0) throw new RangeError("append requires at least one event");
    const expected = opts.expectedVersion;
    if (typeof expected === "number" && (!Number.isInteger(expected) || expected < 0)) {
      throw new RangeError(`expectedVersion must be >= 0, "any", or "noStream" (got ${expected})`);
    }

    // Serialize once, before any retry: a retried write must carry
    // byte-identical content, and the encrypting serializer's tombstone
    // consult must fail a doomed append here, before any PUT.
    const prepared: PreparedEvent[] = [];
    for (const e of events) {
      const s = await serializer.serialize(streamId, { type: e.type, data: e.data });
      prepared.push({ ...e, data: s.data, ...(s.keyId !== undefined ? { keyId: s.keyId } : {}) });
    }

    const attempts = expected === "any" ? maxRetries : 1;
    let lastConflict: ConcurrencyError | undefined;
    for (let i = 0; i < attempts; i++) {
      // The cache stands in for resolution only when it agrees with intent —
      // a disagreement could be a stale cache, so it never mints a rejection
      // on its own (except "noStream": any observed tail proves existence).
      const cached = tailCache.get(streamId);
      let tail: TailState;
      if (cached && expected === "noStream") {
        throw new ConcurrencyError(streamId, "expected no stream, but the stream exists");
      } else if (cached && (expected === "any" || (typeof expected === "number" && headOf(cached.chunk) === expected))) {
        tail = cached;
      } else {
        tail = await resolveTail(streamId);
      }

      const head = tail.kind === "empty" ? -1 : headOf(tail.chunk);
      if (expected === "noStream" && head !== -1) {
        throw new ConcurrencyError(streamId, "expected no stream, but the stream exists");
      }
      if (typeof expected === "number" && head !== expected) {
        const actual = head === -1 ? "no stream" : `version ${head}`;
        throw new ConcurrencyError(streamId, `expected version ${expected}, found ${actual}`);
      }

      try {
        return await tryAppend(streamId, tail, head + 1, prepared, opts);
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

  async function resolveHead(streamId: string): Promise<HeadResolution> {
    validateStreamId(streamId, idOpts);
    const tail = await resolveTail(streamId);
    if (tail.kind === "empty") return { kind: "noStream" };
    return { kind: "head", version: headOf(tail.chunk), lastCommitKey: tail.key, lastCommitEtag: tail.etag };
  }

  /**
   * Read (DESIGN.md, Read protocol): LIST `c/`, GET the chunks covering the
   * range, yield. Chunks are dense and (once sealed) immutable — no dedupe, no
   * contiguity check, no freed-key phantom. The only race is reading the live
   * tail as it grows, which yields a benign prefix.
   */
  async function* read(streamId: string, opts?: ReadOptions): AsyncGenerator<EventEnvelope> {
    validateStreamId(streamId, idOpts);
    const fromVersion = opts?.fromVersion ?? 0;
    const chunks = await listAll(chunkPrefix(prefix, streamId));
    let startIdx = 0;
    for (let i = 0; i < chunks.length; i++) {
      if (baseFromKey(chunks[i]!.key) <= fromVersion) startIdx = i;
      else break;
    }
    const out: EventEnvelope[] = [];
    for (let i = startIdx; i < chunks.length; i++) {
      const got = await driver.get(chunks[i]!.key);
      if (got.kind !== "found") continue; // never deleted; skip a vanished listing
      for (const commit of parseChunk(got.body).commits) {
        for (const ev of commit.events) if (ev.version >= fromVersion) out.push(ev);
      }
    }
    if (opts?.raw === true) {
      yield* out;
    } else {
      for (const envelope of out) {
        yield { ...envelope, data: await serializer.deserialize(streamId, envelope) };
      }
    }
  }

  // Compaction and sweep are ImmutableChunk concerns; the mutable tail has no
  // background work — every stream is fully packed by construction.
  async function compactStream(_streamId: string): Promise<CompactionResult> {
    return { status: "nothing-to-do" };
  }
  async function sweepStream(_streamId: string): Promise<{ deleted: number }> {
    return { deleted: 0 };
  }

  return { chunkSize: params.chunkSize, append, read, resolveHead, compactStream, sweepStream };
}
