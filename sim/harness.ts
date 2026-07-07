/**
 * Simulation harness: wires seed → rng → scheduler → SimStore → per-actor
 * SimDrivers → event stores, runs a scenario to quiescence, and returns the
 * trace (SIMULATOR_PLAN.md, "harness").
 */

import type { StorageDriver } from "../src/driver.js";
import { createEventStore, immutableChunk, type EventStore, type StrategyConfig } from "../src/store.js";
import { createSimDriver } from "./driver.js";
import { createRng, type Rng } from "./rng.js";
import { Scheduler, type FaultPlan, type TraceEntry } from "./scheduler.js";
import { SimStore } from "./store.js";

export interface SimOptions {
  seed: number;
  chunkSize?: number;
  listPageSize?: number;
  faults?: FaultPlan;
  /** Storage strategy under test. Defaults to `immutableChunk()`. */
  strategy?: StrategyConfig;
}

export interface SimContext {
  rng: Rng;
  simStore: SimStore;
  scheduler: Scheduler;
  /** Spawn an actor with its own driver-bound event store. */
  spawn(name: string, fn: (store: EventStore, driver: StorageDriver) => Promise<void>): void;
  /** Run a check after every applied op (the continuous storage invariant). */
  afterEveryOp(fn: () => void): void;
}

export interface SimResult {
  trace: TraceEntry[];
  traceText: string;
  simStore: SimStore;
  /** Event store over a direct (unscheduled) driver, for quiescent checks. */
  quiescent: EventStore;
}

export const SIM_PREFIX = "sim";

export async function runSim(
  opts: SimOptions,
  scenario: (ctx: SimContext) => void,
): Promise<SimResult> {
  const rng = createRng(opts.seed);
  const scheduler = new Scheduler(rng.fork(), opts.faults ?? {});
  const simStore = new SimStore();
  const chunkSize = opts.chunkSize ?? 4;

  const ctx: SimContext = {
    rng: rng.fork(),
    simStore,
    scheduler,
    spawn(name, fn) {
      const driver = createSimDriver(scheduler, simStore, name, {
        listPageSize: opts.listPageSize ?? 3,
      });
      let idCounter = 0;
      const store = createEventStore({
        driver,
        prefix: SIM_PREFIX,
        chunkSize,
        strategy: opts.strategy ?? immutableChunk(),
        ids: () => `${name}#${idCounter++}`,
        clock: () => scheduler.now(),
      });
      scheduler.spawn(name, () => fn(store, driver));
    },
    afterEveryOp(fn) {
      scheduler.setAfterOp(fn);
    },
  };

  // Runtime tripwire: sim and library code must draw randomness from the
  // seeded rng only. (Date.now is left alone — test infrastructure reads it
  // between our awaits; the library's clock is injected instead.)
  const realRandom = Math.random;
  Math.random = () => {
    throw new Error("Math.random called during simulation — determinism violation");
  };
  try {
    scenario(ctx);
    await scheduler.run();
  } finally {
    Math.random = realRandom;
  }

  return {
    trace: scheduler.trace,
    traceText: scheduler.traceText(),
    simStore,
    quiescent: quiescentStore(simStore, chunkSize, opts.strategy),
  };
}

/** A driver that executes immediately — for setup and post-run checks. */
export function directDriver(store: SimStore): StorageDriver {
  return {
    get: async (key, o) => store.get(key, o),
    put: async (key, body) => store.put(key, body),
    putIfAbsent: async (key, body) => store.putIfAbsent(key, body),
    putIfMatch: async (key, body, etag) => store.putIfMatch(key, body, etag),
    list: async (prefix, o) => store.list(prefix, o),
    delete: async (key) => store.delete(key),
    deleteMany: async (keys) => store.deleteMany(keys),
  };
}

export function quiescentStore(
  simStore: SimStore,
  chunkSize: number,
  strategy?: StrategyConfig,
): EventStore {
  let idCounter = 0;
  return createEventStore({
    driver: directDriver(simStore),
    prefix: SIM_PREFIX,
    chunkSize,
    strategy: strategy ?? immutableChunk(),
    ids: () => `quiescent#${idCounter++}`,
    clock: () => new Date(0).toISOString(),
  });
}

/**
 * Wrap a driver so chosen calls block until released — hand-scripted
 * interleavings for the named-race regression tests (a stalled writer's
 * GC pause, a reader's delayed GET).
 */
export function gatedDriver(
  inner: StorageDriver,
  gate: (op: { method: string; key: string }) => Promise<void> | null,
): StorageDriver {
  const held = async <T>(method: string, key: string, run: () => Promise<T>): Promise<T> => {
    const hold = gate({ method, key });
    if (hold) await hold;
    return run();
  };
  return {
    get: (key, o) => held("get", key, () => inner.get(key, o)),
    put: (key, body) => held("put", key, () => inner.put(key, body)),
    putIfAbsent: (key, body) => held("putIfAbsent", key, () => inner.putIfAbsent(key, body)),
    putIfMatch: (key, body, etag) => held("putIfMatch", key, () => inner.putIfMatch(key, body, etag)),
    list: (prefix, o) => held("list", prefix, () => inner.list(prefix, o)),
    delete: (key) => held("delete", key, () => inner.delete(key)),
    deleteMany: (keys) => held("deleteMany", keys.join(","), () => inner.deleteMany(keys)),
  };
}
