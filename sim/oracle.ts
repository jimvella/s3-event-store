/**
 * Oracle: three-valued append outcomes and invariant checks
 * (SIMULATOR_PLAN.md, "Oracle and invariants").
 *
 * Attempts are tracked by caller-supplied event ids (the public API's
 * idempotency ids), so the oracle observes only what a real client could.
 */

import type { EventEnvelope } from "../src/types.js";

export type Outcome = "committed" | "rejected" | "indefinite";

export interface Attempt {
  streamId: string;
  eventIds: string[];
  outcome: Outcome;
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

  resolve(token: number, outcome: Outcome): void {
    const attempt = this.pending.get(token);
    if (!attempt) throw new Error(`unknown attempt token ${token}`);
    attempt.outcome = outcome;
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
