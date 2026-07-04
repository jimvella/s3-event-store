/**
 * Cooperative scheduler (SIMULATOR_PLAN.md, "Scheduler").
 *
 * Actors are async functions written against the ordinary driver interface;
 * every driver call suspends the actor and registers a pending op. The loop
 * picks the next schedulable item with the seeded PRNG, applies it
 * atomically, resolves the actor's promise, and drains microtasks so the
 * actor runs exactly to its next driver call before the next pick. With no
 * timers and no I/O, the pick sequence fully determines the run.
 *
 * Two-phase ops: apply (mutate, produce response) and deliver (resolve the
 * actor's promise) are separable — a lost response applies without
 * delivering, then fails the actor's promise at a later, scheduler-chosen
 * step. Both phases are schedulable items, so arbitrarily many other ops
 * can land in between (the GC-pause / SDK-backoff window).
 */

import type { Rng } from "./rng.js";
import { TransientStoreError } from "../src/errors.js";

export type FaultDecision =
  | { kind: "normal" }
  /** Apply the mutation, but the actor's response is lost (timeout). */
  | { kind: "lose-response" }
  /** Do not apply; the actor sees a transient failure. */
  | { kind: "fail" };

export interface FaultPlan {
  /** Consulted once per op, before apply. Defaults to "normal". */
  decide?(op: { actor: string; label: string; step: number }, rng: Rng): FaultDecision;
}

export interface TraceEntry {
  step: number;
  actor: string;
  label: string;
  outcome: string;
}

interface Item {
  actor: string;
  label: string;
  run: () => void;
}

export class Scheduler {
  private queue: Item[] = [];
  private runningActors = 0;
  private actorFailures: { actor: string; error: unknown }[] = [];
  readonly trace: TraceEntry[] = [];
  private step = 0;
  private opRng: Rng;
  private afterOp: (() => void) | undefined;

  /** Hook run after every applied op — continuous invariant checking. */
  setAfterOp(fn: () => void): void {
    this.afterOp = fn;
  }

  constructor(
    private rng: Rng,
    private faults: FaultPlan = {},
  ) {
    this.opRng = rng.fork();
  }

  /** Simulated clock: derived from the step counter, never wall time. */
  now(): string {
    return new Date(Date.UTC(2026, 0, 1) + this.step * 7).toISOString();
  }

  spawn(name: string, fn: () => Promise<void>): void {
    this.runningActors++;
    // Defer the actor's first synchronous segment into the run loop's
    // microtask drain, so spawn order doesn't execute user code eagerly.
    Promise.resolve()
      .then(fn)
      .catch((error) => this.actorFailures.push({ actor: name, error }))
      .finally(() => this.runningActors--);
  }

  /**
   * Register a driver op for `actor`; returns a promise resolved when the
   * scheduler picks and delivers it. `apply` must be synchronous (atomic).
   */
  op<T>(actor: string, label: string, apply: () => T): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.queue.push({
        actor,
        label,
        run: () => {
          const decision: FaultDecision =
            this.faults.decide?.({ actor, label, step: this.step }, this.opRng) ?? { kind: "normal" };
          switch (decision.kind) {
            case "normal": {
              let result: T;
              try {
                result = apply();
              } catch (error) {
                this.record(actor, label, `throw ${String(error)}`);
                reject(error);
                return;
              }
              this.record(actor, label, summarize(result));
              resolve(result);
              return;
            }
            case "lose-response": {
              try {
                apply();
              } catch {
                // An op that would have failed anyway: still just a timeout
                // from the actor's point of view.
              }
              this.record(actor, label, "applied, response lost");
              // Delivery of the failure is itself a schedulable item.
              this.queue.push({
                actor,
                label: `${label} (timeout delivery)`,
                run: () => {
                  this.record(actor, label, "timeout delivered");
                  reject(new TransientStoreError(`response lost: ${label}`));
                },
              });
              return;
            }
            case "fail": {
              this.record(actor, label, "injected failure");
              reject(new TransientStoreError(`injected failure: ${label}`));
              return;
            }
          }
        },
      });
    });
  }

  /** Run until all actors complete. Throws on deadlock or actor failure. */
  async run(): Promise<void> {
    for (;;) {
      await drainMicrotasks();
      if (this.queue.length === 0) {
        if (this.runningActors > 0) {
          throw new Error(
            `deadlock: ${this.runningActors} actor(s) blocked with no pending ops`,
          );
        }
        break;
      }
      const item = this.queue.splice(this.rng.int(this.queue.length), 1)[0]!;
      this.step++;
      item.run();
      if (this.afterOp) {
        try {
          this.afterOp();
        } catch (error) {
          throw new Error(
            `invariant violated after step ${this.step} (${item.actor}: ${item.label}): ${String(error)}`,
            { cause: error },
          );
        }
      }
    }
    if (this.actorFailures.length > 0) {
      const first = this.actorFailures[0]!;
      throw new Error(`actor ${first.actor} failed: ${String(first.error)}`, {
        cause: first.error,
      });
    }
  }

  private record(actor: string, label: string, outcome: string): void {
    this.trace.push({ step: this.step, actor, label, outcome });
  }

  traceText(): string {
    return this.trace
      .map((t) => `s${t.step} ${t.actor} ${t.label} -> ${t.outcome}`)
      .join("\n");
  }
}

function summarize(result: unknown): string {
  if (result === undefined) return "ok";
  if (result !== null && typeof result === "object" && "kind" in result) {
    return String((result as { kind: unknown }).kind);
  }
  return "ok";
}

/** Let every resolved promise continuation run before the next pick. */
function drainMicrotasks(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}
