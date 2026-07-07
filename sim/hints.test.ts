/**
 * head.json hint, in-process head cache, and the compaction trigger
 * (DESIGN.md, Head discovery / Scheduling; SIMULATOR_PLAN.md's hint-damage
 * faults). The hint is never load-bearing: every corruption must fail safe
 * to the cold path, never mint a forged head.
 */

import { describe, expect, it } from "vitest";
import type { StorageDriver } from "../src/driver";
import { headKey } from "../src/keys";
import { createEventStore, immutableChunk, type EventStore } from "../src/store";
import type { HeadHint } from "../src/types";
import { SIM_PREFIX, directDriver } from "./harness";
import { collect } from "./oracle";
import { SimStore } from "./store";

const STREAM = "s";

function makeStore(driver: StorageDriver, name: string, chunkSize = 500): EventStore {
  let n = 0;
  return createEventStore({
    driver,
    prefix: SIM_PREFIX,
    chunkSize,
    strategy: immutableChunk(),
    ids: () => `${name}#${n++}`,
    clock: () => "1970-01-01T00:00:00.000Z",
  });
}

function countingDriver(inner: StorageDriver): { driver: StorageDriver; calls: string[] } {
  const calls: string[] = [];
  const wrap = <A extends unknown[], R>(method: string, fn: (...args: A) => Promise<R>) => {
    return (...args: A): Promise<R> => {
      calls.push(`${method} ${String(args[0])}`);
      return fn(...args);
    };
  };
  return {
    calls,
    driver: {
      get: wrap("get", inner.get.bind(inner)),
      put: wrap("put", inner.put.bind(inner)),
      putIfAbsent: wrap("putIfAbsent", inner.putIfAbsent.bind(inner)),
      putIfMatch: wrap("putIfMatch", inner.putIfMatch.bind(inner)),
      list: wrap("list", inner.list.bind(inner)),
      delete: wrap("delete", inner.delete.bind(inner)),
      deleteMany: wrap("deleteMany", inner.deleteMany.bind(inner)),
    },
  };
}

async function readHint(sim: SimStore): Promise<HeadHint> {
  const got = sim.get(headKey(SIM_PREFIX, STREAM));
  if (got.kind !== "found") throw new Error("no hint written");
  return JSON.parse(got.body) as HeadHint;
}

describe("head.json hint", () => {
  it("appends write the hint; a fresh store resolves via the fast path", async () => {
    const sim = new SimStore();
    const writer = makeStore(directDriver(sim), "W");
    await writer.append(STREAM, [{ type: "E", data: 0, id: "e0" }], { expectedVersion: "noStream" });
    await writer.append(STREAM, [{ type: "E", data: 1, id: "e1" }], { expectedVersion: 0 });

    const hint = await readHint(sim);
    expect(hint.headVersion).toBe(1);
    expect(hint.lastCommitKey).toContain("/e/000000000001");
    expect(hint.lastCommitEtag).toBeTruthy();

    // Fresh store (cold process): hint GET + tail LIST (empty) + pinned
    // corroboration GET — no chunk LIST, no full pagination.
    const { driver, calls } = countingDriver(directDriver(sim));
    const head = await makeStore(driver, "R").resolveHead(STREAM);
    expect(head).toMatchObject({ kind: "head", version: 1 });
    expect(calls).toEqual([
      `get ${headKey(SIM_PREFIX, STREAM)}`,
      `list ${SIM_PREFIX}/streams/${STREAM}/e/`,
      `get ${SIM_PREFIX}/streams/${STREAM}/e/000000000001.json`,
    ]);
  });

  it("hot appends with a warm cache cost 1 PUT + 1 chunk GET + 1 hint PUT", async () => {
    const sim = new SimStore();
    const { driver, calls } = countingDriver(directDriver(sim));
    const store = makeStore(driver, "W");
    await store.append(STREAM, [{ type: "E", data: 0, id: "e0" }], { expectedVersion: "noStream" });
    await store.append(STREAM, [{ type: "E", data: 1, id: "e1" }], { expectedVersion: 0 });
    calls.length = 0;
    await store.append(STREAM, [{ type: "E", data: 2, id: "e2" }], { expectedVersion: 1 });
    expect(calls).toEqual([
      `putIfAbsent ${SIM_PREFIX}/streams/${STREAM}/e/000000000002.json`,
      `get ${SIM_PREFIX}/streams/${STREAM}/c/000000000000.json`, // step-4 backstop
      `put ${headKey(SIM_PREFIX, STREAM)}`,
    ]);
  });

  it("a fabricated stale-high hint fails safe to the cold path (no forged head)", async () => {
    const sim = new SimStore();
    const writer = makeStore(directDriver(sim), "W");
    await writer.append(STREAM, [{ type: "E", data: 0, id: "e0" }], { expectedVersion: "noStream" });
    await writer.append(STREAM, [{ type: "E", data: 1, id: "e1" }], { expectedVersion: 0 });

    // External interference: a hint asserting a head far past the real one,
    // pointing at a key that does not exist (the stale-high orphan vector).
    sim.put(
      headKey(SIM_PREFIX, STREAM),
      JSON.stringify({
        headVersion: 99,
        lastCommitKey: `${SIM_PREFIX}/streams/${STREAM}/e/000000000099.json`,
        lastCommitEtag: '"fabricated"',
        compactedTo: 0,
      } satisfies HeadHint),
    );

    const head = await makeStore(directDriver(sim), "R").resolveHead(STREAM);
    expect(head).toMatchObject({ kind: "head", version: 1 }); // the truth, not 99
  });

  it("a hint whose etag no longer matches its key fails safe to the cold path", async () => {
    const sim = new SimStore();
    const writer = makeStore(directDriver(sim), "W");
    await writer.append(STREAM, [{ type: "E", data: 0, id: "e0" }], { expectedVersion: "noStream" });
    await writer.append(STREAM, [{ type: "E", data: 1, id: "e1" }], { expectedVersion: 0 });

    // Same key, wrong evidence: existence alone must not corroborate.
    const hint = await readHint(sim);
    sim.put(
      headKey(SIM_PREFIX, STREAM),
      JSON.stringify({ ...hint, lastCommitEtag: '"not-the-real-etag"' } satisfies HeadHint),
    );

    const head = await makeStore(directDriver(sim), "R").resolveHead(STREAM);
    expect(head).toMatchObject({ kind: "head", version: 1 });
  });

  it("a deleted hint falls back to the cold path", async () => {
    const sim = new SimStore();
    const writer = makeStore(directDriver(sim), "W");
    await writer.append(STREAM, [{ type: "E", data: 0, id: "e0" }], { expectedVersion: "noStream" });
    sim.delete(headKey(SIM_PREFIX, STREAM));
    const head = await makeStore(directDriver(sim), "R").resolveHead(STREAM);
    expect(head).toMatchObject({ kind: "head", version: 0 });
    const replay = await collect(makeStore(directDriver(sim), "R2").read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["e0"]);
  });
});

describe("compaction trigger (compactionSuggested)", () => {
  it("fires when the append's base implies a sealed bucket behind the watermark", async () => {
    const sim = new SimStore();
    const store = makeStore(directDriver(sim), "W", 2);
    const r0 = await store.append(STREAM, [{ type: "E", data: 0, id: "e0" }], { expectedVersion: "noStream" });
    const r1 = await store.append(STREAM, [{ type: "E", data: 1, id: "e1" }], { expectedVersion: 0 });
    expect(r0.compactionSuggested).toBe(false); // base 0 < watermark 0 + N
    expect(r1.compactionSuggested).toBe(false); // base 1 < 2
    const r2 = await store.append(STREAM, [{ type: "E", data: 2, id: "e2" }], { expectedVersion: 1 });
    expect(r2.compactionSuggested).toBe(true); // base 2 seals bucket 0

    // The compactor advances the watermark on head.json.
    const c = await store.compactStream(STREAM);
    expect(c).toEqual({ status: "compacted", chunkBase: 0 });
    expect((await readHint(sim)).compactedTo).toBe(2);

    // A fresh store learns the new watermark from the hint: base 3 no
    // longer suggests (bucket 2 is not sealed), base 4 does.
    const fresh = makeStore(directDriver(sim), "W2", 2);
    const r3 = await fresh.append(STREAM, [{ type: "E", data: 3, id: "e3" }], { expectedVersion: 2 });
    expect(r3.compactionSuggested).toBe(false); // base 3 < 2 + 2 ... wait: 3 >= 4 is false
    const r4 = await fresh.append(STREAM, [{ type: "E", data: 4, id: "e4" }], { expectedVersion: 3 });
    expect(r4.compactionSuggested).toBe(true); // base 4 >= 2 + 2

    // End-to-end sanity after hint-driven compaction.
    const r = await store.compactStream(STREAM);
    expect(r).toEqual({ status: "compacted", chunkBase: 2 });
    const replay = await collect(makeStore(directDriver(sim), "R", 2).read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["e0", "e1", "e2", "e3", "e4"]);
  });
});
