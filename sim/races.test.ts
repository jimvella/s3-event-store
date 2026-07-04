/**
 * Named-race regression scenarios (SIMULATOR_PLAN.md, "Scenarios").
 *
 * Each test hand-scripts one of DESIGN.md's analyzed schedules using a gated
 * driver (holds a chosen call until released — the stalled writer's GC
 * pause, the reader's delayed GET) and the real compactor
 * (`compactStream`). Races internal to compaction itself live in
 * compaction.test.ts.
 */

import { describe, expect, it } from "vitest";
import type { StorageDriver } from "../src/driver";
import { ConcurrencyError, TransientStoreError } from "../src/errors";
import { chunkKey, commitKey, commitPrefix } from "../src/keys";
import { createEventStore, type EventStore } from "../src/store";
import type { CommitObject } from "../src/types";
import { SIM_PREFIX, directDriver, gatedDriver } from "./harness";
import { collect } from "./oracle";
import { SimStore } from "./store";

const CHUNK_SIZE = 2;
const STREAM = "s";

function makeStore(driver: StorageDriver, name: string): EventStore {
  let n = 0;
  return createEventStore({
    driver,
    prefix: SIM_PREFIX,
    chunkSize: CHUNK_SIZE,
    ids: () => `${name}#${n++}`,
    clock: () => "1970-01-01T00:00:00.000Z",
  });
}

/** Run the real compactor once; assert it compacted the expected bucket. */
async function compact(sim: SimStore, expectedBucket: number): Promise<void> {
  const result = await makeStore(directDriver(sim), "compactor").compactStream(STREAM);
  expect(result).toEqual({ status: "compacted", chunkBase: expectedBucket });
}

/** One-shot gate: holds the first matching op until release() is called. */
function makeGate(match: (op: { method: string; key: string }) => boolean) {
  let reachedResolve!: () => void;
  let releaseResolve!: () => void;
  const reached = new Promise<void>((r) => (reachedResolve = r));
  const released = new Promise<void>((r) => (releaseResolve = r));
  let used = false;
  return {
    reached,
    release: () => releaseResolve(),
    fn: (op: { method: string; key: string }): Promise<void> | null => {
      if (used || !match(op)) return null;
      used = true;
      reachedResolve();
      return released;
    },
  };
}

describe("freed-key orphan (append step 4)", () => {
  it("a stalled writer whose target bucket was compacted gets ConcurrencyError, and its events are never readable", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "b0" }], { expectedVersion: "noStream" });
    await setup.append(STREAM, [{ type: "E", data: 1, id: "b1" }], { expectedVersion: 0 });

    // Writer A resolves head=1, targets e/2, then stalls at the PUT.
    const gate = makeGate((op) => op.method === "putIfAbsent" && op.key.includes("/e/"));
    const stalled = makeStore(gatedDriver(directDriver(sim), gate.fn), "A");
    const attempt = stalled.append(STREAM, [{ type: "E", data: "orphan", id: "A-ev" }], {
      expectedVersion: 1,
    });
    attempt.catch(() => {}); // outcome asserted below; avoid unhandled-rejection noise
    await gate.reached;

    // During the stall: a full bucket of later commits lands and a
    // compaction cycle runs (the design's unbounded client-side window).
    await setup.append(STREAM, [{ type: "E", data: 2, id: "b2" }], { expectedVersion: 1 });
    await setup.append(STREAM, [{ type: "E", data: 3, id: "b3" }], { expectedVersion: 2 });
    await setup.append(STREAM, [{ type: "E", data: 4, id: "b4" }], { expectedVersion: 3 });
    await compact(sim, 0);
    await compact(sim, 2);

    // A's create-only PUT now "succeeds" at the freed key — step 4 catches it.
    gate.release();
    await expect(attempt).rejects.toThrow(ConcurrencyError);

    // The rejected writer's events are unreadable (no phantom), the stream
    // is intact, and head resolution is unpoisoned by the orphan.
    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["b0", "b1", "b2", "b3", "b4"]);
    const head = await makeStore(directDriver(sim), "R2").resolveHead(STREAM);
    expect(head).toMatchObject({ kind: "head", version: 4 });

    // The orphan object persists at the freed key until the sweep runs.
    const orphanKey = commitKey(SIM_PREFIX, STREAM, 2);
    expect(sim.dump().has(orphanKey)).toBe(true);
    const swept = await makeStore(directDriver(sim), "S").sweepStream(STREAM);
    expect(swept.deleted).toBe(1);
    expect(sim.dump().has(orphanKey)).toBe(false);
    const after = await collect(makeStore(directDriver(sim), "R3").read(STREAM));
    expect(after.map((e) => e.id)).toEqual(["b0", "b1", "b2", "b3", "b4"]);
  });
});

describe("lost response resolved through compaction (append step 3/4)", () => {
  it("a commit that was applied, compacted, and retried reports success exactly once", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "b0" }], { expectedVersion: "noStream" });
    await setup.append(STREAM, [{ type: "E", data: 1, id: "b1" }], { expectedVersion: 0 });

    // A's first PUT applies but the response is lost; its retry is stalled.
    const gate = makeGate((op) => op.method === "putIfAbsent" && op.key.includes("/e/"));
    let calls = 0;
    const inner = directDriver(sim);
    const lossy: StorageDriver = {
      ...gatedDriver(inner, gate.fn),
      putIfAbsent: async (key, body) => {
        calls++;
        if (calls === 1) {
          await inner.putIfAbsent(key, body); // applied...
          throw new TransientStoreError("response lost"); // ...but undelivered
        }
        const hold = gate.fn({ method: "putIfAbsent", key });
        if (hold) await hold;
        return inner.putIfAbsent(key, body);
      },
    };
    const writerA = makeStore(lossy, "A");
    const attempt = writerA.append(STREAM, [{ type: "E", data: 2, id: "A-ev" }], {
      expectedVersion: 1,
    });
    await gate.reached;

    // A's commit (base 2) is live; others build on it and it gets compacted.
    await setup.append(STREAM, [{ type: "E", data: 3, id: "b3" }], { expectedVersion: 2 });
    await setup.append(STREAM, [{ type: "E", data: 4, id: "b4" }], { expectedVersion: 3 });
    await compact(sim, 0);
    await compact(sim, 2);

    // The retry recreates the freed key, and step 4 finds our commitId in
    // the chunk: success, not a false ConcurrencyError.
    gate.release();
    const result = await attempt;
    expect(result.nextExpectedVersion).toBe(2);

    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["b0", "b1", "A-ev", "b3", "b4"]);
  });
});

describe("post-LIST substitution (pinned GET)", () => {
  it("a listed key compacted, freed, and recreated before its GET is caught by the etag pin", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "r0" }], { expectedVersion: "noStream" });
    for (let v = 0; v < 4; v++) {
      await setup.append(STREAM, [{ type: "E", data: v + 1, id: `r${v + 1}` }], { expectedVersion: v });
    }

    // Reader lists the tail, then stalls before GETting e/2.
    const gate = makeGate((op) => op.method === "get" && op.key.includes("/e/000000000002"));
    const reader = makeStore(gatedDriver(directDriver(sim), gate.fn), "R");
    const reading = collect(reader.read(STREAM));
    await gate.reached;

    // Between its LIST and its GET: the bucket seals' chunk lands, the key
    // is freed, and a stalled foreign writer recreates it.
    await compact(sim, 0);
    await compact(sim, 2);
    const forged: CommitObject = {
      commitId: "forged",
      streamId: STREAM,
      baseVersion: 2,
      events: [{ id: "forged-ev", type: "X", version: 2, data: null, meta: { ts: "0" } }],
      committedAt: "0",
    };
    const put = await directDriver(sim).putIfAbsent(
      `${commitPrefix(SIM_PREFIX, STREAM)}000000000002.json`,
      JSON.stringify(forged),
    );
    expect(put.kind).toBe("created"); // the key really was freed

    gate.release();
    const replay = await reading; // pin 412s -> re-list -> chunks authoritative
    expect(replay.map((e) => e.id)).toEqual(["r0", "r1", "r2", "r3", "r4"]);
  });
});

describe("LIST-time orphan (sealed-bucket check)", () => {
  it("an orphan already present in the tail listing is rejected by the chunk check", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "r0" }], { expectedVersion: "noStream" });
    for (let v = 0; v < 4; v++) {
      await setup.append(STREAM, [{ type: "E", data: v + 1, id: `r${v + 1}` }], { expectedVersion: v });
    }

    // Reader completes its c/ LIST (no chunks yet), stalls before its e/ LIST.
    const gate = makeGate((op) => op.method === "list" && op.key === commitPrefix(SIM_PREFIX, STREAM));
    const calls: string[] = [];
    const inner = directDriver(sim);
    const spy: StorageDriver = {
      ...inner,
      get: async (key, o) => {
        calls.push(`get ${key}`);
        return inner.get(key, o);
      },
    };
    const reader = makeStore(gatedDriver(spy, gate.fn), "R");
    const reading = collect(reader.read(STREAM));
    await gate.reached;

    // Compaction runs and an orphan lands at a freed key — all *before* the
    // reader's e/ LIST, so the orphan is listed with a current etag and the
    // pinned GET alone cannot catch it.
    await compact(sim, 0);
    await compact(sim, 2);
    const orphan: CommitObject = {
      commitId: "orphan",
      streamId: STREAM,
      baseVersion: 2,
      events: [{ id: "orphan-ev", type: "X", version: 2, data: null, meta: { ts: "0" } }],
      committedAt: "0",
    };
    await directDriver(sim).putIfAbsent(
      `${commitPrefix(SIM_PREFIX, STREAM)}000000000002.json`,
      JSON.stringify(orphan),
    );

    gate.release();
    const replay = await reading;
    expect(replay.map((e) => e.id)).toEqual(["r0", "r1", "r2", "r3", "r4"]);
    // The sealed-bucket check fired: the reader consulted the bucket's chunk
    // before yielding tail commits from a sealed bucket.
    expect(calls).toContain(`get ${chunkKey(SIM_PREFIX, STREAM, 2)}`);
  });
});

describe("compacted-stream expectedVersion semantics", () => {
  it('"noStream" and stale-low versions are rejected by head resolution, not by a blind PUT at a freed key', async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "r0" }], { expectedVersion: "noStream" });
    for (let v = 0; v < 4; v++) {
      await setup.append(STREAM, [{ type: "E", data: v + 1, id: `r${v + 1}` }], { expectedVersion: v });
    }
    await compact(sim, 0);
    await compact(sim, 2);

    const store = makeStore(directDriver(sim), "A");
    // e/0 is a freed key: a blind create-only PUT would "succeed" and write
    // unreadable garbage. Head resolution rejects both intents first.
    await expect(
      store.append(STREAM, [{ type: "E", data: "x" }], { expectedVersion: "noStream" }),
    ).rejects.toThrow(ConcurrencyError);
    await expect(
      store.append(STREAM, [{ type: "E", data: "x" }], { expectedVersion: 0 }),
    ).rejects.toThrow(ConcurrencyError);
    // And the correct intent still works.
    await store.append(STREAM, [{ type: "E", data: 5, id: "r5" }], { expectedVersion: 4 });
    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["r0", "r1", "r2", "r3", "r4", "r5"]);
  });
});

describe("boundary-straddling commits", () => {
  it("multi-event commits compact by base, replay contiguously, and trim mid-commit fromVersion", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "m0" }], { expectedVersion: "noStream" });
    // Base 1, two events (versions 1-2): straddles the bucket boundary;
    // belongs wholly to bucket 0, next base lands in bucket 2.
    await setup.append(
      STREAM,
      [
        { type: "E", data: 1, id: "m1" },
        { type: "E", data: 2, id: "m2" },
      ],
      { expectedVersion: 0 },
    );
    await setup.append(STREAM, [{ type: "E", data: 3, id: "m3" }], { expectedVersion: 2 });
    await setup.append(STREAM, [{ type: "E", data: 4, id: "m4" }], { expectedVersion: 3 });
    await compact(sim, 0); // commits at bases 0 and 1
    await compact(sim, 2); // the single commit at base 3

    const store = makeStore(directDriver(sim), "R");
    const replay = await collect(store.read(STREAM));
    expect(replay.map((e) => [e.id, e.version])).toEqual([
      ["m0", 0],
      ["m1", 1],
      ["m2", 2],
      ["m3", 3],
      ["m4", 4],
    ]);
    // fromVersion falls mid-commit: keys encode bases, so the read starts at
    // a commit boundary and trims locally.
    const tail = await collect(store.read(STREAM, { fromVersion: 2 }));
    expect(tail.map((e) => e.id)).toEqual(["m2", "m3", "m4"]);
    const head = await store.resolveHead(STREAM);
    expect(head).toMatchObject({ kind: "head", version: 4 });
  });
});
