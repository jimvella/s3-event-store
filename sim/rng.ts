/**
 * Seeded PRNG — the only source of randomness in a simulation.
 * splitmix32: tiny, fast, good-enough statistical quality for schedule
 * exploration; not cryptographic (doesn't need to be).
 */

export interface Rng {
  /** Uniform float in [0, 1). */
  float(): number;
  /** Uniform integer in [0, n). */
  int(n: number): number;
  /** Fork an independent, deterministic child stream. */
  fork(): Rng;
}

export function createRng(seed: number): Rng {
  let state = seed >>> 0;
  const nextU32 = (): number => {
    state = (state + 0x9e3779b9) >>> 0;
    let z = state;
    z ^= z >>> 16;
    z = Math.imul(z, 0x21f0aaad);
    z ^= z >>> 15;
    z = Math.imul(z, 0x735a2d97);
    z ^= z >>> 15;
    return z >>> 0;
  };
  return {
    float: () => nextU32() / 0x1_0000_0000,
    int: (n: number) => {
      if (!Number.isInteger(n) || n <= 0) throw new RangeError(`int(${n})`);
      return nextU32() % n;
    },
    fork: () => createRng(nextU32()),
  };
}
