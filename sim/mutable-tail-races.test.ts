/**
 * Named-race regressions for the MutableTail strategy (SIMULATOR_PLAN.md,
 * "Scenarios"). Each hand-scripts one schedule using a gated/lossy driver.
 *
 * The mutable tail deletes nothing, so its hazard surface is small: the roll
 * verdict and the next-chunk key must be a deterministic function of the tail
 * *as read*, so concurrent CAS-appends and rolls resolve to one winner and a
 * lost response is recognized via the writer's own commitId. There is no
 * freed-key class — the elaborate pinned-anchor / sealed-bucket machinery the
 * ImmutableChunk races cover (races.test.ts) has no analogue here.
 */

import { describe, expect, it } from "vitest";
import type { StorageDriver } from "../src/driver";
import { ConcurrencyError, TransientStoreError } from "../src/errors";
import { baseFromKey, chunkPrefix } from "../src/keys";
import { createEventStore, mutableTail, type EventStore } from "../src/store";
import { SIM_PREFIX, directDriver, gatedDriver } from "./harness";
import { collect } from "./oracle";
import { SimStore } from "./store";

const STREAM = "s";

function makeStore(driver: StorageDriver, name: string, chunkSize = 2): EventStore {
  let n = 0;
  return createEventStore({
    driver,
    prefix: SIM_PREFIX,
    strategy: mutableTail({ chunkSize }),
    ids: () => `${name}#${n++}`,
    clock: () => "1970-01-01T00:00:00.000Z",
  });
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

/** Sorted chunk bases present for the stream. */
function chunkBases(sim: SimStore): number[] {
  return [...sim.dump().keys()]
    .filter((k) => k.startsWith(chunkPrefix(SIM_PREFIX, STREAM)))
    .map((k) => baseFromKey(k))
    .sort((a, b) => a - b);
}

describe("concurrent roll (create-only next chunk)", () => {
  it("two writers that resolved the same full tail target the same next key — one wins", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "b0" }], { expectedVersion: "noStream" });
    await setup.append(STREAM, [{ type: "E", data: 1, id: "b1" }], { expectedVersion: 0 });
    // c/0 is now full (n=2); the next append must roll to c/2.

    // Writer A resolves head=1, decides to roll, then stalls at the create.
    const gate = makeGate((op) => op.method === "putIfAbsent" && op.key.includes("/c/000000000002"));
    const stalled = makeStore(gatedDriver(directDriver(sim), gate.fn), "A");
    const attempt = stalled.append(STREAM, [{ type: "E", data: "A", id: "A-ev" }], {
      expectedVersion: 1,
    });
    attempt.catch(() => {});
    await gate.reached;

    // Writer B rolls first, creating c/2 with its own commit.
    await setup.append(STREAM, [{ type: "E", data: 2, id: "b2" }], { expectedVersion: 1 });

    // A's create now finds the key taken: conflict, not a fork.
    gate.release();
    await expect(attempt).rejects.toThrow(ConcurrencyError);

    expect(chunkBases(sim)).toEqual([0, 2]); // exactly one chunk at base 2
    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["b0", "b1", "b2"]);
    const head = await makeStore(directDriver(sim), "R2").resolveHead(STREAM);
    expect(head).toMatchObject({ kind: "head", version: 2 });
  });
});

describe("concurrent CAS-append (compare-and-swap on the tail)", () => {
  it("two writers on the same tail etag — one updates, the other 412s to a conflict", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "b0" }], { expectedVersion: "noStream" });
    // c/0 holds one commit (n=2, not full); the next append CAS-appends.

    const gate = makeGate((op) => op.method === "putIfMatch" && op.key.includes("/c/000000000000"));
    const stalled = makeStore(gatedDriver(directDriver(sim), gate.fn), "A");
    const attempt = stalled.append(STREAM, [{ type: "E", data: "A", id: "A-ev" }], {
      expectedVersion: 0,
    });
    attempt.catch(() => {});
    await gate.reached;

    // Writer B CAS-appends first, changing the tail etag out from under A.
    await setup.append(STREAM, [{ type: "E", data: 1, id: "b1" }], { expectedVersion: 0 });

    gate.release();
    await expect(attempt).rejects.toThrow(ConcurrencyError);

    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => [e.id, e.version])).toEqual([
      ["b0", 0],
      ["b1", 1],
    ]);
  });
});

describe("lost response on a CAS-append", () => {
  it("the retry 412s on its own applied write and recognizes its commitId — success, once", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "b0" }], { expectedVersion: "noStream" });

    // A's putIfMatch applies (tail grows, etag changes) but the response is lost.
    let calls = 0;
    const inner = directDriver(sim);
    const lossy: StorageDriver = {
      ...inner,
      putIfMatch: async (key, body, etag) => {
        calls++;
        if (calls === 1) {
          await inner.putIfMatch(key, body, etag); // applied...
          throw new TransientStoreError("response lost"); // ...but undelivered
        }
        return inner.putIfMatch(key, body, etag); // retry: 412 on the stale etag
      },
    };
    const writerA = makeStore(lossy, "A");
    const result = await writerA.append(STREAM, [{ type: "E", data: 1, id: "A-ev" }], {
      expectedVersion: 0,
    });
    expect(result.nextExpectedVersion).toBe(1);

    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["b0", "A-ev"]); // exactly once, no duplicate
  });
});

describe("lost response on a roll", () => {
  it("the retry gets `exists` on its own created chunk and recognizes its commitId — success, once", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "b0" }], { expectedVersion: "noStream" });
    await setup.append(STREAM, [{ type: "E", data: 1, id: "b1" }], { expectedVersion: 0 });
    // c/0 full; A's append must roll to c/2.

    let calls = 0;
    const inner = directDriver(sim);
    const lossy: StorageDriver = {
      ...inner,
      putIfAbsent: async (key, body) => {
        calls++;
        if (calls === 1) {
          await inner.putIfAbsent(key, body); // chunk created...
          throw new TransientStoreError("response lost"); // ...but undelivered
        }
        return inner.putIfAbsent(key, body); // retry: exists (our own chunk)
      },
    };
    const writerA = makeStore(lossy, "A");
    const result = await writerA.append(STREAM, [{ type: "E", data: 2, id: "A-ev" }], {
      expectedVersion: 1,
    });
    expect(result.nextExpectedVersion).toBe(2);

    expect(chunkBases(sim)).toEqual([0, 2]);
    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["b0", "b1", "A-ev"]);
  });
});

describe("mismatched-config writers cannot fork", () => {
  it("the roll verdict comes from the tail body, not the store's N — so N is stable per stream", async () => {
    const sim = new SimStore();
    // Two deployments on the same stream with *different* configured N.
    const storeA = makeStore(directDriver(sim), "A", 2); // N=2
    const storeB = makeStore(directDriver(sim), "B", 4); // N=4

    // A creates the stream: c/0 is stamped with n=2 in its body.
    await storeA.append(STREAM, [{ type: "E", data: 0, id: "e0" }], { expectedVersion: "noStream" });

    // B (configured N=4) continues. It must honor the tail body's n=2, not its
    // own config — so it rolls after the 2nd commit, never carrying to 4.
    await storeB.append(STREAM, [{ type: "E", data: 1, id: "e1" }], { expectedVersion: 0 });
    await storeB.append(STREAM, [{ type: "E", data: 2, id: "e2" }], { expectedVersion: 1 });
    await storeB.append(STREAM, [{ type: "E", data: 3, id: "e3" }], { expectedVersion: 2 });

    // Boundaries follow the creator's N=2 (c/0, c/2), not B's N=4 — no fork.
    expect(chunkBases(sim)).toEqual([0, 2]);
    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => [e.id, e.version])).toEqual([
      ["e0", 0],
      ["e1", 1],
      ["e2", 2],
      ["e3", 3],
    ]);
  });
});

describe("reader racing a roll", () => {
  it("a read that listed the tail before a roll yields a benign prefix, never an error", async () => {
    const sim = new SimStore();
    const setup = makeStore(directDriver(sim), "W");
    await setup.append(STREAM, [{ type: "E", data: 0, id: "b0" }], { expectedVersion: "noStream" });
    await setup.append(STREAM, [{ type: "E", data: 1, id: "b1" }], { expectedVersion: 0 });
    // c/0 = [b0, b1], full. The reader will list [c/0], then stall before GET.

    const gate = makeGate((op) => op.method === "get" && op.key.includes("/c/000000000000"));
    const reader = makeStore(gatedDriver(directDriver(sim), gate.fn), "R");
    const reading = collect(reader.read(STREAM));
    await gate.reached;

    // A roll lands during the stall: c/2 is created with b2 — invisible to the
    // reader, whose listing predates it.
    await setup.append(STREAM, [{ type: "E", data: 2, id: "b2" }], { expectedVersion: 1 });

    gate.release();
    const replay = await reading;
    // A consistent prefix of the stream — b2 is simply not yet visible.
    expect(replay.map((e) => e.id)).toEqual(["b0", "b1"]);

    // A fresh read sees the whole stream.
    const full = await collect(makeStore(directDriver(sim), "R2").read(STREAM));
    expect(full.map((e) => e.id)).toEqual(["b0", "b1", "b2"]);
  });
});
