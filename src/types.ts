/** Envelope and commit-object shapes (DESIGN.md, "Commit object body"). */

export interface EventMeta {
  correlationId?: string;
  causationId?: string;
  /** Writer wall clock — data, not a coordinate; no monotonicity guarantee. */
  ts: string;
  [key: string]: unknown;
}

export interface EventInput {
  type: string;
  data: unknown;
  /** Idempotency/dedupe key; library-generated unless supplied. */
  id?: string;
  meta?: Partial<EventMeta>;
}

export interface EventEnvelope {
  id: string;
  type: string;
  version: number;
  data: unknown;
  /**
   * Reserved field (DESIGN.md, Encryption & erasure contract): opaque key
   * generation identifier — plaintext, outside the encryption boundary,
   * inside the envelope compaction copies verbatim. Absent on unencrypted
   * events.
   */
  keyId?: string;
  meta: EventMeta;
}

export interface CommitObject {
  /** Writer-generated; disambiguates retried conditional PUTs. */
  commitId: string;
  streamId: string;
  baseVersion: number;
  events: EventEnvelope[];
  committedAt: string;
}

/** Chunk object: one compacted bucket of commits (DESIGN_IMMUTABLE_CHUNK.md). */
export interface ChunkObject {
  streamId: string;
  chunkBase: number;
  commits: CommitObject[];
  /** Key of the last commit in the chunk — seeds tail LISTs. */
  lastCommitKey: string;
}

/**
 * Mutable-tail chunk body (DESIGN.md, "Core mechanism: the mutable tail").
 * The last chunk under `c/` is the live tail, updated in place by CAS; earlier
 * chunks are sealed. The roll policy (`n`, `byteCap`) travels in the body so
 * every appender reads the same caps the CAS is conditioned on and agrees on
 * the boundary (DESIGN.md, "Per-stream (and evolving) N").
 */
export interface TailChunk {
  streamId: string;
  /** Base version of the first commit; equals this object's key. */
  chunkBase: number;
  /** Roll policy: max commits per chunk (also the per-commit event cap). */
  n: number;
  /** Roll policy: max serialized bytes before a new chunk is minted. */
  byteCap: number;
  commits: CommitObject[];
}

export type ExpectedVersion = number | "any" | "noStream";

export interface AppendResult {
  streamId: string;
  /** Version of the last event just committed = the stream's new head. */
  nextExpectedVersion: number;
  committedAt: string;
  /**
   * The write-driven compaction trigger (DESIGN.md, Scheduling): true when
   * this append's base implies a sealed uncompacted bucket behind the head
   * (pure arithmetic against the last-known watermark; a stale watermark
   * over-fires, which is benign — `compactStream` no-ops). Deployments fire
   * `ctx.waitUntil(store.compactStream(id))` on it.
   */
  compactionSuggested: boolean;
}

/**
 * `head.json` body — a non-authoritative hint (DESIGN.md, Head discovery).
 * Written last-writer-wins by two populations: appenders bump the hint
 * fields, compactors advance `compactedTo`. Any field can regress; every
 * consumer treats it as a hint. `headVersion` is a human-readable
 * diagnostic only — never load-bearing for resolution.
 */
export interface HeadHint {
  headVersion: number | null;
  lastCommitKey: string | null;
  /** Written by the same PUT as lastCommitKey: the pair can regress together but never split. */
  lastCommitEtag: string | null;
  /** Compaction watermark: chunks are dense below it. */
  compactedTo: number;
}
