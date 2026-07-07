/**
 * MutableTail strategy under the deterministic simulator: the same
 * strategy-agnostic invariants the ImmutableChunk suite checks (no lost
 * events, no duplicate versions, no phantom reads, no forged heads, contiguous
 * prefix), plus per-stream N. The mutable tail deletes nothing, so the schedule
 * space is small — concurrent CAS PUTs and rolls must never fork or drop a
 * commit.
 */

import { describe, expect, it } from "vitest";
import { ConcurrencyError, TransientStoreError } from "../src/errors";
import { createEventStore, mutableTail } from "../src/store";
import { SIM_PREFIX, directDriver, runSim, type SimContext } from "./harness";
import { Oracle, collect, mutableTailStorageInvariant, resolveHeadChecked } from "./oracle";
import type { FaultPlan } from "./scheduler";
import { SimStore } from "./store";

describe("mutable tail — smoke", () => {
  it("append/read/head derive the head from the tail chunk", async () => {
    const result = await runSim({ seed: 1, strategy: mutableTail() }, (ctx) => {
      ctx.spawn("writer", async (store) => {
        await store.append("s", [{ type: "A", data: 1, id: "e0" }], { expectedVersion: "noStream" });
        await store.append(
          "s",
          [
            { type: "B", data: 2, id: "e1" },
            { type: "C", data: 3, id: "e2" },
          ],
          { expectedVersion: 0 },
        );
        await store.append("s", [{ type: "D", data: 4, id: "e3" }], { expectedVersion: 2 });
        const events = await collect(store.read("s"));
        if (events.map((e) => e.id).join(",") !== "e0,e1,e2,e3") {
          throw new Error(`unexpected replay: ${events.map((e) => e.id).join(",")}`);
        }
      });
    });
    const replay = await collect(result.quiescent.read("s"));
    expect(replay.map((e) => [e.id, e.version])).toEqual([
      ["e0", 0],
      ["e1", 1],
      ["e2", 2],
      ["e3", 3],
    ]);
    const partial = await collect(result.quiescent.read("s", { fromVersion: 2 }));
    expect(partial.map((e) => e.id)).toEqual(["e2", "e3"]);
  });

  it("expectedVersion mismatches raise ConcurrencyError before any write", async () => {
    await runSim({ seed: 2, strategy: mutableTail() }, (ctx) => {
      ctx.spawn("writer", async (store) => {
        await store.append("s", [{ type: "A", data: 0, id: "e0" }], { expectedVersion: "noStream" });
        await expect(
          store.append("s", [{ type: "A", data: 0 }], { expectedVersion: "noStream" }),
        ).rejects.toThrow(ConcurrencyError);
        await expect(
          store.append("s", [{ type: "A", data: 0 }], { expectedVersion: 5 }),
        ).rejects.toThrow(ConcurrencyError);
      });
    });
  });
});

/** Randomized concurrent workload; oracle-verified, storage invariant live. */
function mtScenario(oracle: Oracle, streams: string[], appenders: number, opsEach: number) {
  return (ctx: SimContext) => {
    ctx.afterEveryOp(mutableTailStorageInvariant(ctx.simStore, oracle));
    for (let a = 0; a < appenders; a++) {
      const rng = ctx.rng.fork();
      const name = `appender${a}`;
      ctx.spawn(name, async (store) => {
        for (let i = 0; i < opsEach; i++) {
          const streamId = streams[rng.int(streams.length)]!;
          const ids = [`${name}-op${i}a`, ...(rng.int(3) === 0 ? [`${name}-op${i}b`] : [])];
          const events = ids.map((id) => ({ type: "E", data: { by: name, i }, id }));
          const token = oracle.begin(streamId, ids);
          try {
            let expected: number | "any" | "noStream" = "any";
            if (rng.int(2) === 0) {
              const head = await resolveHeadChecked(oracle, store, streamId);
              expected = head.kind === "noStream" ? "noStream" : head.version;
            }
            const r = await store.append(streamId, events, { expectedVersion: expected });
            oracle.resolve(token, "committed", r.nextExpectedVersion);
          } catch (err) {
            if (err instanceof ConcurrencyError) oracle.resolve(token, "rejected");
            else if (err instanceof TransientStoreError) oracle.resolve(token, "indefinite");
            else throw err;
          }
        }
      });
    }
    ctx.spawn("reader", async (store) => {
      for (const streamId of streams) {
        await resolveHeadChecked(oracle, store, streamId);
        await collect(store.read(streamId));
      }
    });
  };
}

describe("mutable tail — randomized concurrency sweep", () => {
  it("invariants hold across seeds (no faults)", async () => {
    for (let seed = 300; seed < 320; seed++) {
      const oracle = new Oracle();
      const result = await runSim(
        { seed, chunkSize: 4, strategy: mutableTail() },
        mtScenario(oracle, ["s0", "s1"], 4, 4),
      );
      oracle.finalize();
      for (const streamId of ["s0", "s1"]) {
        const replay = await collect(result.quiescent.read(streamId));
        try {
          oracle.verifyStream(streamId, replay);
        } catch (err) {
          throw new Error(`seed ${seed}: ${String(err)}`, { cause: err });
        }
      }
      expect(oracle.committedCount).toBeGreaterThan(0);
    }
  });

  it("invariants hold across seeds (lost responses and injected failures)", async () => {
    for (let seed = 400; seed < 420; seed++) {
      const oracle = new Oracle();
      const faults: FaultPlan = {
        decide: (op, rng) => {
          // MutableTail commits are putIfMatch (CAS-append) and putIfAbsent
          // (roll / stream creation); never fault reads.
          if (!op.label.startsWith("putIfMatch") && !op.label.startsWith("putIfAbsent")) {
            return { kind: "normal" };
          }
          const roll = rng.float();
          if (roll < 0.1) return { kind: "lose-response" };
          if (roll < 0.15) return { kind: "fail" };
          return { kind: "normal" };
        },
      };
      const result = await runSim(
        { seed, chunkSize: 4, faults, strategy: mutableTail() },
        mtScenario(oracle, ["s0"], 3, 3),
      );
      oracle.finalize();
      const replay = await collect(result.quiescent.read("s0"));
      try {
        oracle.verifyStream("s0", replay);
      } catch (err) {
        throw new Error(`seed ${seed}: ${String(err)}`, { cause: err });
      }
    }
  });
});

describe("mutable tail — per-stream N", () => {
  it("policyFor and the creation-time override pin N per stream; rolls at the boundary", async () => {
    const sim = new SimStore();
    let n = 0;
    const store = createEventStore({
      driver: directDriver(sim),
      prefix: SIM_PREFIX,
      strategy: mutableTail({
        chunkSize: 4, // store default N
        policyFor: (id) => (id.startsWith("big-") ? { chunkSize: 3 } : undefined),
      }),
      ids: () => `id#${n++}`,
      clock: () => "1970-01-01T00:00:00.000Z",
    });

    const fill = async (streamId: string, count: number, firstOpts?: { chunkSize?: number }) => {
      for (let v = 0; v < count; v++) {
        await store.append(streamId, [{ type: "E", data: v, id: `${streamId}#${v}` }], {
          expectedVersion: v === 0 ? "noStream" : v - 1,
          ...(v === 0 && firstOpts ? firstOpts : {}),
        });
      }
    };

    const bases = (streamId: string) =>
      [...sim.dump().keys()]
        .map((k) => new RegExp(`^${SIM_PREFIX}/streams/${streamId}/c/(\\d{12})\\.json$`).exec(k))
        .filter((m): m is RegExpExecArray => m !== null)
        .map((m) => Number(m[1]))
        .sort((a, b) => a - b);

    await fill("s", 6); // default N=4  → c/0 (0-3), c/4 (4-5)
    await fill("big-1", 5); // policyFor N=3 → c/0 (0-2), c/3 (3-4)
    await fill("audit", 5, { chunkSize: 2 }); // creation override N=2 → c/0, c/2, c/4

    expect(bases("s")).toEqual([0, 4]);
    expect(bases("big-1")).toEqual([0, 3]);
    expect(bases("audit")).toEqual([0, 2, 4]);

    // Reads are N-agnostic and still yield the full contiguous stream.
    expect((await collect(store.read("s"))).map((e) => e.version)).toEqual([0, 1, 2, 3, 4, 5]);
    expect((await collect(store.read("big-1"))).map((e) => e.id)).toEqual([
      "big-1#0",
      "big-1#1",
      "big-1#2",
      "big-1#3",
      "big-1#4",
    ]);
  });
});
