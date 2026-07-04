// Manual seed sweep launcher: `npm run sweep [-- <count> [<startSeed>]]`.
// Cross-platform env plumbing for sim/sweep.test.ts; a random start seed is
// chosen (and printed) unless given, so a failing sweep is reproducible.
import { spawnSync } from "node:child_process";

const count = process.argv[2] ?? "2000";
const start = process.argv[3] ?? String(Math.floor(Math.random() * 2 ** 31));

console.log(`sweeping ${count} seeds starting at ${start}`);
console.log(`reproduce with: npm run sweep -- ${count} ${start}`);

const result = spawnSync("npx", ["vitest", "run", "sim/sweep.test.ts"], {
  stdio: "inherit",
  shell: true,
  env: { ...process.env, SWEEP_SEEDS: count, SWEEP_START: start },
});
process.exit(result.status ?? 1);
