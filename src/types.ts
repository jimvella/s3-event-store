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
}
