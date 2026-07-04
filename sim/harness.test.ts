import { describe, expect, it } from "vitest";
import { ConcurrencyError, TransientStoreError } from "../src/errors";
import { runSim, type SimContext } from "./harness";
import { Oracle, collect, resolveHeadChecked, storageInvariant } from "./oracle";
import type { FaultPlan } from "./scheduler";

describe("smoke", () => {
  it("one appender, one reader, no faults", async () => {
    const result = await runSim({ seed: 1 }, (ctx) => {
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
    await runSim({ seed: 2 }, (ctx) => {
      ctx.spawn("writer", async (store) => {
        await store.append("s", [{ type: "A", data: 0, id: "e0" }], { expectedVersion: "noStream" });
        await expect(
          store.append("s", [{ type: "A", data: 0 }], { expectedVersion: "noStream" }),
        ).rejects.toThrow(ConcurrencyError);
        await expect(
          store.append("s", [{ type: "A", data: 0 }], { expectedVersion: 5 }),
        ).rejects.toThrow(ConcurrencyError);
        await expect(
          store.append("x", [{ type: "A", data: 0 }], { expectedVersion: 0 }),
        ).rejects.toThrow(ConcurrencyError);
      });
    });
  });
});

/** Randomized concurrent workload; oracle-verified, both invariants live. */
function mixScenario(
  oracle: Oracle,
  streams: string[],
  appenders: number,
  opsEach: number,
  chunkSize = 4, // must match runSim's default
) {
  return (ctx: SimContext) => {
    // Invariant 5: every committed event readable, after every mutation.
    ctx.afterEveryOp(storageInvariant(ctx.simStore, oracle, chunkSize));
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
            const useAny = rng.int(2) === 0;
            let expected: number | "any" | "noStream" = "any";
            if (!useAny) {
              // Invariant 4: every resolution lands inside the oracle bounds.
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
      // Concurrent reads: store.read verifies contiguity internally; any
      // violation fails the actor and thus the simulation. Each read is
      // preceded by a bounds-checked head resolution (invariant 4).
      for (const streamId of streams) {
        await resolveHeadChecked(oracle, store, streamId);
        await collect(store.read(streamId));
      }
    });
  };
}

describe("randomized concurrency sweep", () => {
  it("invariants hold across seeds (no faults)", async () => {
    for (let seed = 100; seed < 120; seed++) {
      const oracle = new Oracle();
      const result = await runSim({ seed }, mixScenario(oracle, ["s0", "s1"], 4, 4));
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
    for (let seed = 200; seed < 220; seed++) {
      const oracle = new Oracle();
      const faults: FaultPlan = {
        decide: (op, rng) => {
          // Never fault reads: fault injection models the write-side races;
          // a lost GET response only exercises caller retries.
          if (!op.label.startsWith("putIfAbsent")) return { kind: "normal" };
          const roll = rng.float();
          if (roll < 0.1) return { kind: "lose-response" };
          if (roll < 0.15) return { kind: "fail" };
          return { kind: "normal" };
        },
      };
      const oracleStreams = ["s0"];
      const result = await runSim({ seed, faults }, mixScenario(oracle, oracleStreams, 3, 3));
      oracle.finalize();
      for (const streamId of oracleStreams) {
        const replay = await collect(result.quiescent.read(streamId));
        try {
          oracle.verifyStream(streamId, replay);
        } catch (err) {
          throw new Error(`seed ${seed}: ${String(err)}`, { cause: err });
        }
      }
    }
  });
});

describe("determinism meta test", () => {
  it("same seed twice produces byte-identical traces", async () => {
    const run = async () => {
      const oracle = new Oracle();
      const result = await runSim(
        {
          seed: 42,
          faults: {
            decide: (op, rng) =>
              op.label.startsWith("putIfAbsent") && rng.float() < 0.1
                ? { kind: "lose-response" }
                : { kind: "normal" },
          },
        },
        mixScenario(oracle, ["s0", "s1"], 3, 3),
      );
      return result.traceText;
    };
    const a = await run();
    const b = await run();
    expect(a.length).toBeGreaterThan(0);
    expect(b).toBe(a);
  });

  it("different seeds explore different schedules", async () => {
    const run = async (seed: number) => {
      const oracle = new Oracle();
      return (await runSim({ seed }, mixScenario(oracle, ["s0"], 3, 3))).traceText;
    };
    expect(await run(7)).not.toBe(await run(8));
  });

  it("Math.random is a determinism violation inside a simulation", async () => {
    await expect(
      runSim({ seed: 3 }, (ctx) => {
        ctx.spawn("bad", async () => {
          Math.random();
        });
      }),
    ).rejects.toThrow(/Math.random called during simulation/);
  });
});

describe("lost responses (two-phase ops)", () => {
  it("a lost PUT response resolves via the commitId self-check, exactly once", async () => {
    // Lose the response to the very first conditional commit PUT: the store
    // retries, gets `exists` (412), GETs the key, recognizes its own
    // commitId, and reports success — no duplicate, no false conflict.
    let lost = false;
    const faults: FaultPlan = {
      decide: (op) => {
        if (!lost && op.label.includes("putIfAbsent") && op.label.includes("/e/")) {
          lost = true;
          return { kind: "lose-response" };
        }
        return { kind: "normal" };
      },
    };
    const result = await runSim({ seed: 5, faults }, (ctx) => {
      ctx.spawn("writer", async (store) => {
        const r = await store.append("s", [{ type: "A", data: 1, id: "e0" }], {
          expectedVersion: "noStream",
        });
        if (r.nextExpectedVersion !== 0) throw new Error("wrong version");
      });
    });
    const replay = await collect(result.quiescent.read("s"));
    expect(replay.map((e) => e.id)).toEqual(["e0"]);
    expect(result.traceText).toContain("applied, response lost");
  });
});
