/**
 * Manual deep-exploration sweep for the MutableTail strategy — the local
 * stand-in for nightly CI (SIMULATOR_PLAN.md). Skipped unless SWEEP_SEEDS is
 * set; run via `npm run sweep` (see scripts/sweep.mjs), which sweeps both
 * strategies.
 *
 * The mutable tail deletes nothing, so the schedule space is small: the
 * invariants reduce to "concurrent CAS PUTs and rolls never fork or drop a
 * commit." Any failure names the seed; rerun it alone with
 * `npm run sweep -- 1 <seed>`, then distill it into mutable-tail-races.test.ts.
 */

import { expect, it } from "vitest";
import { ConcurrencyError, TransientStoreError } from "../src/errors";
import { chunkPrefix } from "../src/keys";
import { mutableTail } from "../src/store";
import { SIM_PREFIX, runSim } from "./harness";
import { Oracle, collect, mutableTailStorageInvariant, resolveHeadChecked } from "./oracle";

const SEEDS = Number(process.env.SWEEP_SEEDS ?? 0);
const START = Number(process.env.SWEEP_START ?? 0);
const CHUNK_SIZE = 2;

it.skipIf(SEEDS === 0)(
  `mutable-tail sweep: ${SEEDS} seeds from ${START}`,
  async () => {
    const stats = { committed: 0, rejected: 0, indefinite: 0, chunks: 0 };
    for (let seed = START; seed < START + SEEDS; seed++) {
      const oracle = new Oracle();
      // Vary the fault mix by seed: a third fault-free, a third losing
      // responses, a third also injecting outright failures.
      const mode = seed % 3;
      let result;
      try {
        result = await runSim(
          {
            seed,
            chunkSize: CHUNK_SIZE,
            strategy: mutableTail(),
            faults: {
              decide: (op, rng) => {
                const isWrite =
                  op.label.startsWith("putIfMatch") || op.label.startsWith("putIfAbsent");
                if (mode === 0 || !isWrite) return { kind: "normal" };
                const roll = rng.float();
                if (roll < 0.1) return { kind: "lose-response" };
                if (mode === 2 && roll < 0.15) return { kind: "fail" };
                return { kind: "normal" };
              },
            },
          },
          (ctx) => {
            ctx.afterEveryOp(mutableTailStorageInvariant(ctx.simStore, oracle));
            for (let a = 0; a < 3; a++) {
              const rng = ctx.rng.fork();
              ctx.spawn(`appender${a}`, async (store) => {
                for (let i = 0; i < 4; i++) {
                  const id = `a${a}-op${i}`;
                  const token = oracle.begin("s", [id]);
                  try {
                    let expected: number | "any" | "noStream" = "any";
                    if (rng.int(2) === 0) {
                      const head = await resolveHeadChecked(oracle, store, "s");
                      expected = head.kind === "noStream" ? "noStream" : head.version;
                    }
                    const r = await store.append("s", [{ type: "E", data: i, id }], {
                      expectedVersion: expected,
                    });
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
              await resolveHeadChecked(oracle, store, "s");
              try {
                await collect(store.read("s"));
                await collect(store.read("s", { fromVersion: 3 }));
              } catch (err) {
                if (!(err instanceof TransientStoreError)) throw err;
              }
            });
          },
        );
      } catch (err) {
        throw new Error(`SEED ${seed} FAILED: ${String(err)}`, { cause: err });
      }
      oracle.finalize();
      const replay = await collect(result.quiescent.read("s"));
      try {
        oracle.verifyStream("s", replay);
      } catch (err) {
        throw new Error(`SEED ${seed} FAILED: ${String(err)}`, { cause: err });
      }
      stats.committed += oracle.committedCount;
      stats.rejected += oracle.rejectedCount;
      stats.indefinite += oracle.indefiniteCount;
      stats.chunks += [...result.simStore.dump().keys()].filter((k) =>
        k.startsWith(chunkPrefix(SIM_PREFIX, "s")),
      ).length;
      if ((seed - START + 1) % 1000 === 0) {
        console.log(`…${seed - START + 1}/${SEEDS} seeds clean`);
      }
    }
    console.log(`mutable-tail sweep clean: seeds ${START}..${START + SEEDS - 1}`, stats);
    expect(stats.committed).toBeGreaterThan(0);
  },
  { timeout: 1_800_000 },
);
