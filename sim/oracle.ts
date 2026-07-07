/**
 * Oracle: three-valued append outcomes and invariant checks
 * (SIMULATOR_PLAN.md, "Oracle and invariants").
 *
 * Attempts are tracked by caller-supplied event ids (the public API's
 * idempotency ids), so the oracle observes only what a real client could.
 */

import type { EventStore, HeadResolution } from "../src/store.js";
import type { ChunkObject, CommitObject, EventEnvelope, TailChunk } from "../src/types.js";
import type { SimStore } from "./store.js";

export type Outcome = "committed" | "rejected" | "indefinite";

export interface Attempt {
  streamId: string;
  eventIds: string[];
  outcome: Outcome;
  /** Head version the committed append reported (nextExpectedVersion). */
  version?: number;
}

export class Oracle {
  private attempts: Attempt[] = [];
  private pending = new Map<number, Attempt>();
  private next = 0;

  /** Register an attempt before issuing it; resolve it with the outcome. */
  begin(streamId: string, eventIds: string[]): number {
    const token = this.next++;
    this.pending.set(token, { streamId, eventIds, outcome: "indefinite" });
    return token;
  }

  resolve(token: number, outcome: Outcome, version?: number): void {
    const attempt = this.pending.get(token);
    if (!attempt) throw new Error(`unknown attempt token ${token}`);
    attempt.outcome = outcome;
    if (version !== undefined) attempt.version = version;
    this.pending.delete(token);
    this.attempts.push(attempt);
  }

  /** Any attempt never resolved (actor crashed mid-append) is indefinite. */
  finalize(): void {
    for (const attempt of this.pending.values()) this.attempts.push(attempt);
    this.pending.clear();
  }

  get committedCount(): number {
    return this.attempts.filter((a) => a.outcome === "committed").length;
  }

  get rejectedCount(): number {
    return this.attempts.filter((a) => a.outcome === "rejected").length;
  }

  get indefiniteCount(): number {
    return this.attempts.filter((a) => a.outcome === "indefinite").length;
  }

  /** Highest version any committed append reported for the stream; -1 if none. */
  maxCommittedVersion(streamId: string): number {
    let max = -1;
    for (const a of this.attempts) {
      if (a.streamId === streamId && a.outcome === "committed" && a.version !== undefined) {
        max = Math.max(max, a.version);
      }
    }
    return max;
  }

  /**
   * Events that *may* occupy versions beyond maxCommittedVersion: in-flight
   * attempts plus resolved-indefinite ones (applied-but-unacknowledged).
   */
  potentialExtraEvents(streamId: string): number {
    let extra = 0;
    for (const a of this.pending.values()) {
      if (a.streamId === streamId) extra += a.eventIds.length;
    }
    for (const a of this.attempts) {
      if (a.streamId === streamId && a.outcome === "indefinite") extra += a.eventIds.length;
    }
    return extra;
  }

  /** All event ids of committed attempts, across streams. */
  committedEventIds(): Set<string> {
    const ids = new Set<string>();
    for (const a of this.attempts) {
      if (a.outcome === "committed") for (const id of a.eventIds) ids.add(id);
    }
    return ids;
  }

  /**
   * Verify a quiescent full replay of one stream against the recorded
   * attempts. Throws with a descriptive message on any violation.
   */
  verifyStream(streamId: string, replay: EventEnvelope[]): void {
    // Invariant: contiguous dense versions from 0.
    replay.forEach((e, i) => {
      if (e.version !== i) {
        throw new Error(`${streamId}: version gap — event ${i} has version ${e.version}`);
      }
    });

    const seen = new Map<string, number>();
    replay.forEach((e, i) => {
      if (seen.has(e.id)) {
        throw new Error(`${streamId}: duplicate event id ${e.id} at ${seen.get(e.id)} and ${i}`);
      }
      seen.set(e.id, i);
    });

    const known = new Set<string>();
    for (const attempt of this.attempts) {
      if (attempt.streamId !== streamId) continue;
      attempt.eventIds.forEach((id) => known.add(id));
      const present = attempt.eventIds.filter((id) => seen.has(id)).length;
      switch (attempt.outcome) {
        case "committed":
          if (present !== attempt.eventIds.length) {
            throw new Error(
              `${streamId}: committed attempt lost events (${present}/${attempt.eventIds.length} present: ${attempt.eventIds.join(",")})`,
            );
          }
          break;
        case "rejected":
          if (present !== 0) {
            // The phantom-read invariant: a rejected writer's events are
            // never readable.
            throw new Error(
              `${streamId}: phantom read — rejected attempt's events present (${attempt.eventIds.join(",")})`,
            );
          }
          break;
        case "indefinite":
          if (present !== 0 && present !== attempt.eventIds.length) {
            throw new Error(
              `${streamId}: indefinite attempt partially applied (${present}/${attempt.eventIds.length}) — commit atomicity violated`,
            );
          }
          break;
      }
    }

    // No event from outside any recorded attempt.
    for (const e of replay) {
      if (!known.has(e.id)) {
        throw new Error(`${streamId}: replay contains unknown event id ${e.id}`);
      }
    }
  }
}

export async function collect(iter: AsyncIterable<EventEnvelope>): Promise<EventEnvelope[]> {
  const out: EventEnvelope[] = [];
  for await (const e of iter) out.push(e);
  return out;
}

/**
 * Invariant 4 — no forged heads (SIMULATOR_PLAN.md): a resolution must land
 * in [max version committed before the call, max committed after the call +
 * events that may be applied but unacknowledged]. Anything below is a lost
 * head; anything above is a head derived from a substituted anchor.
 */
export async function resolveHeadChecked(
  oracle: Oracle,
  store: EventStore,
  streamId: string,
): Promise<HeadResolution> {
  const lower = oracle.maxCommittedVersion(streamId);
  const head = await store.resolveHead(streamId);
  const upper = oracle.maxCommittedVersion(streamId) + oracle.potentialExtraEvents(streamId);
  if (head.kind === "head") {
    if (head.version < lower || head.version > upper) {
      throw new Error(
        `forged head: ${streamId} resolved to ${head.version}, outside [${lower}, ${upper}]`,
      );
    }
  } else if (lower >= 0) {
    throw new Error(`forged head: ${streamId} resolved to noStream but version ${lower} committed`);
  }
  return head;
}

/**
 * Invariant 5 — the compaction invariant, checked at every instant
 * (SIMULATOR_PLAN.md): every committed event is *readable* — present in a
 * chunk, or in an e/ commit whose bucket has no chunk (readers ignore
 * commits in chunked buckets, so those don't count). Wire via
 * `ctx.afterEveryOp(storageInvariant(ctx.simStore, oracle, chunkSize))`.
 */
export function storageInvariant(simStore: SimStore, oracle: Oracle, chunkSize: number): () => void {
  return () => {
    const readable = new Set<string>();
    const chunkedBuckets = new Set<string>();
    const tailCommits: { root: string; base: number; commit: CommitObject }[] = [];
    for (const [key, obj] of simStore.dump()) {
      const chunk = /^(.*)\/c\/(\d{12})\.json$/.exec(key);
      if (chunk) {
        chunkedBuckets.add(`${chunk[1]}#${Number(chunk[2])}`);
        for (const c of (JSON.parse(obj.body) as ChunkObject).commits) {
          for (const e of c.events) readable.add(e.id);
        }
        continue;
      }
      const commit = /^(.*)\/e\/(\d{12})\.json$/.exec(key);
      if (commit) {
        tailCommits.push({
          root: commit[1]!,
          base: Number(commit[2]),
          commit: JSON.parse(obj.body) as CommitObject,
        });
      }
    }
    for (const t of tailCommits) {
      const bucket = Math.floor(t.base / chunkSize) * chunkSize;
      if (!chunkedBuckets.has(`${t.root}#${bucket}`)) {
        for (const e of t.commit.events) readable.add(e.id);
      }
    }
    for (const id of oracle.committedEventIds()) {
      if (!readable.has(id)) {
        throw new Error(`storage invariant: committed event ${id} is unreadable`);
      }
    }
  };
}

/**
 * The MutableTail storage invariant (DESIGN.md, Core mechanism): all history
 * lives in chunk objects under `c/` and nothing is ever deleted, so every
 * committed event must be present in some chunk at every instant. There is no
 * `e/` tail and no freed-key class to reason about.
 */
export function mutableTailStorageInvariant(simStore: SimStore, oracle: Oracle): () => void {
  return () => {
    const readable = new Set<string>();
    for (const [key, obj] of simStore.dump()) {
      if (!/\/c\/\d{12}\.json$/.test(key)) continue;
      for (const c of (JSON.parse(obj.body) as TailChunk).commits) {
        for (const e of c.events) readable.add(e.id);
      }
    }
    for (const id of oracle.committedEventIds()) {
      if (!readable.has(id)) {
        throw new Error(`mutable-tail storage invariant: committed event ${id} is unreadable`);
      }
    }
  };
}
