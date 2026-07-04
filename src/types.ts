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

/** Chunk object: one compacted bucket of commits (DESIGN.md, Compaction). */
export interface ChunkObject {
  streamId: string;
  chunkBase: number;
  commits: CommitObject[];
  /** Key of the last commit in the chunk — seeds tail LISTs. */
  lastCommitKey: string;
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
