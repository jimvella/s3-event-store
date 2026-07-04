/**
 * Races internal to compaction itself (DESIGN.md, "Failure modes, all
 * harmless"): racing compactors, crash between chunk PUT and source
 * deletes, mid-GET stand-down — plus the randomized write-triggered sweep
 * where appenders, readers, compactors, and the sweeper all interleave.
 */

import { describe, expect, it } from "vitest";
import type { StorageDriver } from "../src/driver";
import { ConcurrencyError, TransientStoreError } from "../src/errors";
import { chunkKey, chunkPrefix, commitKey } from "../src/keys";
import { createEventStore, type EventStore } from "../src/store";
import { SIM_PREFIX, directDriver, gatedDriver, runSim } from "./harness";
import { Oracle, collect, resolveHeadChecked, storageInvariant } from "./oracle";
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

async function seedStream(sim: SimStore, count: number): Promise<void> {
  const setup = makeStore(directDriver(sim), "W");
  await setup.append(STREAM, [{ type: "E", data: 0, id: "r0" }], { expectedVersion: "noStream" });
  for (let v = 0; v < count - 1; v++) {
    await setup.append(STREAM, [{ type: "E", data: v + 1, id: `r${v + 1}` }], { expectedVersion: v });
  }
}

describe("crash between chunk PUT and source deletes", () => {
  it("leaves duplication readers never see; the next pass moves on; the sweep cleans up", async () => {
    const sim = new SimStore();
    await seedStream(sim, 5); // bases 0-4: buckets 0 and 2 sealed

    // The compactor dies after its chunk PUT, before any delete.
    const inner = directDriver(sim);
    const crashing: StorageDriver = {
      ...inner,
      deleteMany: async () => {
        throw new TransientStoreError("compactor crashed before deletes");
      },
    };
    await expect(makeStore(crashing, "C1").compactStream(STREAM)).rejects.toThrow(
      TransientStoreError,
    );

    // Duplication, not corruption: chunk and source commits both exist...
    expect(sim.dump().has(chunkKey(SIM_PREFIX, STREAM, 0))).toBe(true);
    expect(sim.dump().has(commitKey(SIM_PREFIX, STREAM, 0))).toBe(true);
    expect(sim.dump().has(commitKey(SIM_PREFIX, STREAM, 1))).toBe(true);
    // ...but readers never see it (tail LISTs seed from the chunk anchor).
    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["r0", "r1", "r2", "r3", "r4"]);

    // The next invocation is not wedged by the leftovers: it selects the
    // next bucket (the watermark advanced when the chunk landed).
    const next = await makeStore(directDriver(sim), "C2").compactStream(STREAM);
    expect(next).toEqual({ status: "compacted", chunkBase: 2 });

    // The sweep deletes the crash leftovers (bases 0 and 1 < watermark 4).
    const swept = await makeStore(directDriver(sim), "S").sweepStream(STREAM);
    expect(swept.deleted).toBe(2);
    const after = await collect(makeStore(directDriver(sim), "R2").read(STREAM));
    expect(after.map((e) => e.id)).toEqual(["r0", "r1", "r2", "r3", "r4"]);
  });
});

describe("mid-GET stand-down (compactor step 3's 404 rule)", () => {
  it("a compactor that loses the assembly race confirms the winner's chunk and stands down", async () => {
    const sim = new SimStore();
    await seedStream(sim, 5);

    // B lists the tail, then stalls before GETting its first source commit.
    const gate = (() => {
      let reachedResolve!: () => void;
      let releaseResolve!: () => void;
      const reached = new Promise<void>((r) => (reachedResolve = r));
      const released = new Promise<void>((r) => (releaseResolve = r));
      let used = false;
      return {
        reached,
        release: () => releaseResolve(),
        fn: (op: { method: string; key: string }): Promise<void> | null => {
          if (used || op.method !== "get" || !op.key.includes("/e/")) return null;
          used = true;
          reachedResolve();
          return released;
        },
      };
    })();
    const loser = makeStore(gatedDriver(directDriver(sim), gate.fn), "B");
    const attempt = loser.compactStream(STREAM);
    await gate.reached;

    // A wins the whole bucket: chunk PUT and deletes both complete.
    const winner = await makeStore(directDriver(sim), "A").compactStream(STREAM);
    expect(winner).toEqual({ status: "compacted", chunkBase: 0 });

    // B's pinned source GET finds the key gone, confirms the chunk, stands
    // down without deleting anything or raising.
    gate.release();
    expect(await attempt).toEqual({ status: "stood-down", chunkBase: 0 });
    const replay = await collect(makeStore(directDriver(sim), "R").read(STREAM));
    expect(replay.map((e) => e.id)).toEqual(["r0", "r1", "r2", "r3", "r4"]);
  });
});

describe("racing compactors under random schedules", () => {
  it("deterministic keys + If-None-Match pick one winner per bucket; state converges", async () => {
    let stoodDown = 0;
    for (let seed = 700; seed < 712; seed++) {
      const result = await runSim({ seed, chunkSize: CHUNK_SIZE }, (ctx) => {
        ctx.spawn("writer", async (store) => {
          await store.append(STREAM, [{ type: "E", data: 0, id: "r0" }], {
            expectedVersion: "noStream",
          });
          for (let v = 0; v < 7; v++) {
            await store.append(STREAM, [{ type: "E", data: v + 1, id: `r${v + 1}` }], {
              expectedVersion: v,
            });
          }
        });
        for (const name of ["compactorA", "compactorB"]) {
          ctx.spawn(name, async (store) => {
            for (let i = 0; i < 6; i++) {
              const r = await store.compactStream(STREAM);
              if (r.status === "stood-down") stoodDown++;
            }
          });
        }
      });

      // Converge: drain remaining sealed buckets, then verify.
      const quiescent = result.quiescent;
      for (;;) {
        const r = await quiescent.compactStream(STREAM);
        if (r.status === "nothing-to-do") break;
      }
      const replay = await collect(quiescent.read(STREAM));
      expect(replay.map((e) => e.id)).toEqual([...Array(8)].map((_, i) => `r${i}`));
      // Bases 0-7: buckets 0, 2, 4 sealed and chunked exactly once; bucket 6
      // holds the head and can never seal.
      const chunkKeys = [...result.simStore.dump().keys()].filter((k) =>
        k.startsWith(chunkPrefix(SIM_PREFIX, STREAM)),
      );
      expect(chunkKeys.sort()).toEqual([
        chunkKey(SIM_PREFIX, STREAM, 0),
        chunkKey(SIM_PREFIX, STREAM, 2),
        chunkKey(SIM_PREFIX, STREAM, 4),
      ]);
      expect((await quiescent.sweepStream(STREAM)).deleted).toBe(0); // no leftovers without crashes
    }
    // The race must actually occur across the seed batch.
    expect(stoodDown).toBeGreaterThan(0);
  });
});

describe("randomized write-triggered compaction sweep", () => {
  it("appenders, readers, compactors, and the sweeper interleave without violating invariants", async () => {
    let totalChunks = 0;
    for (let seed = 800; seed < 815; seed++) {
      const oracle = new Oracle();
      const result = await runSim(
        {
          seed,
          chunkSize: CHUNK_SIZE,
          faults: {
            decide: (op, rng) =>
              op.label.startsWith("putIfAbsent") && op.label.includes("/e/") && rng.float() < 0.08
                ? { kind: "lose-response" }
                : { kind: "normal" },
          },
        },
        (ctx) => {
          ctx.afterEveryOp(storageInvariant(ctx.simStore, oracle, CHUNK_SIZE));
          for (let a = 0; a < 2; a++) {
            const rng = ctx.rng.fork();
            ctx.spawn(`appender${a}`, async (store) => {
              for (let i = 0; i < 5; i++) {
                const id = `a${a}-op${i}`;
                const token = oracle.begin(STREAM, [id]);
                try {
                  let expected: number | "any" | "noStream" = "any";
                  if (rng.int(2) === 0) {
                    const head = await resolveHeadChecked(oracle, store, STREAM);
                    expected = head.kind === "noStream" ? "noStream" : head.version;
                  }
                  const r = await store.append(STREAM, [{ type: "E", data: i, id }], {
                    expectedVersion: expected,
                  });
                  oracle.resolve(token, "committed", r.nextExpectedVersion);
                  // Write-triggered compaction, fired from the write path.
                  if (r.compactionSuggested) await store.compactStream(STREAM);
                } catch (err) {
                  if (err instanceof ConcurrencyError) oracle.resolve(token, "rejected");
                  else if (err instanceof TransientStoreError) oracle.resolve(token, "indefinite");
                  else throw err;
                }
              }
            });
          }
          ctx.spawn("reader", async (store) => {
            await collect(store.read(STREAM));
            await collect(store.read(STREAM));
          });
          ctx.spawn("sweeper", async (store) => {
            await store.sweepStream(STREAM);
            await store.sweepStream(STREAM);
          });
        },
      );

      oracle.finalize();
      const replay = await collect(result.quiescent.read(STREAM));
      try {
        oracle.verifyStream(STREAM, replay);
      } catch (err) {
        throw new Error(`seed ${seed}: ${String(err)}`, { cause: err });
      }
      totalChunks += [...result.simStore.dump().keys()].filter((k) =>
        k.startsWith(chunkPrefix(SIM_PREFIX, STREAM)),
      ).length;
    }
    // Compaction must actually have run somewhere in the batch.
    expect(totalChunks).toBeGreaterThan(0);
  });
});
