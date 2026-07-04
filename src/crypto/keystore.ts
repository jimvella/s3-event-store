/**
 * KeyStore interface + the default S3-bucket implementation
 * (KEYS_DESIGN.md, "Key store as a separate S3 bucket").
 *
 * Layout (two disjoint prefixes; the split is load-bearing — the tombstone
 * must live outside the generations' listed prefix):
 *
 *   keys/{subjectId}/{gen:06d}.json    wrapped data key, one per generation
 *   tombstones/{subjectId}.json        shred state machine
 *
 * The key bucket needs the INVERTED configuration from the event bucket:
 * versioning never enabled, no Object Lock, no replication, keys wrapped.
 * Verify at startup with `verifyKeyBucketConfig` (drivers/aws-sdk).
 *
 * Tombstone-authoritative reads: `pending`/`committing` are soft-deleted —
 * keyring and key delivery return empty, `currentKey` (the append path)
 * throws SubjectErasedError. Read-path tombstone staleness shares the
 * key-cache TTL; the write path re-checks on the shorter negative-cache
 * TTL; minting uses direct, uncached GETs.
 */

import type { StorageDriver } from "../driver.js";
import { SubjectErasedError, TransientStoreError } from "../errors.js";
import { base64ToBytes, bytesToBase64, cryptoRandom, type RandomFn } from "./bytes.js";
import type { MasterKey } from "./master-key.js";

export interface KeyringEntry {
  keyId: string;
  key: Uint8Array;
  /** Authorization window, not disclosure control (KEYS_DESIGN.md). */
  expiresAt: string;
}

export interface KeyStore {
  /** Newest generation for encryption; lazily mints generation 0. Fails
   * closed (SubjectErasedError) for a soft-deleted subject. */
  currentKey(subjectId: string): Promise<{ keyId: string; key: Uint8Array }>;
  /** Unwrapped key for decryption; null when shredded or unknown (fail closed). */
  keyById(subjectId: string, keyId: string): Promise<Uint8Array | null>;
  /** Every generation the subject has; empty when soft-deleted. */
  keyring(subjectId: string): Promise<KeyringEntry[]>;
  /** Mint the next generation (tombstone-guarded). */
  rotate(subjectId: string): Promise<{ keyId: string }>;
}

export type TombstoneState = "pending" | "committing" | "cancelled";

export interface Tombstone {
  subjectId: string;
  state: TombstoneState;
  /** Waiting-period start (ms epoch); restarted by a cancelled→pending reopen. */
  requestedAt: number;
  /** Audit-stream position of the newest intent stamped on this tombstone. */
  intent: number;
}

const GEN_PAD = 6;

export function generationKey(subjectId: string, gen: number): string {
  return `keys/${subjectId}/${String(gen).padStart(GEN_PAD, "0")}.json`;
}

export function tombstoneKey(subjectId: string): string {
  return `tombstones/${subjectId}.json`;
}

export function keyIdOf(gen: number): string {
  return String(gen).padStart(GEN_PAD, "0");
}

interface WrappedKeyObject {
  subjectId: string;
  keyId: string;
  wrappedKey: string; // base64
  createdAt: string;
}

export interface S3KeyStoreConfig {
  /** Driver over the KEY bucket — never the event bucket. */
  driver: StorageDriver;
  masterKey: MasterKey;
  /** ms epoch; injectable for deterministic tests. */
  clock?: () => number;
  random?: RandomFn;
  /** Read-path staleness bound: unwrapped-key and tombstone caches. */
  keyCacheTtlMs?: number;
  /** Write-path (append) tombstone freshness — minutes, not hours. */
  tombstoneTtlMs?: number;
  /** TTL stamped on delivered keyring entries. */
  keyringTtlMs?: number;
  /** Audit hook: KeyCreated / KeyRotated appends to $system.key-audit. */
  audit?: (type: "KeyCreated" | "KeyRotated", data: Record<string, unknown>) => Promise<void>;
}

export function createS3KeyStore(config: S3KeyStoreConfig): KeyStore {
  const driver = config.driver;
  const masterKey = config.masterKey;
  const clock = config.clock ?? (() => Date.now());
  const random = config.random ?? cryptoRandom;
  const keyCacheTtl = config.keyCacheTtlMs ?? 3_600_000;
  const tombstoneTtl = config.tombstoneTtlMs ?? 60_000;
  const keyringTtl = config.keyringTtlMs ?? 3_600_000;

  const keyCache = new Map<string, { key: Uint8Array; at: number }>(); // subject/keyId
  const currentCache = new Map<string, { keyId: string; key: Uint8Array; at: number }>();
  const tombstoneCache = new Map<string, { state: TombstoneState | "none"; at: number }>();

  async function readTombstoneDirect(subjectId: string): Promise<Tombstone | null> {
    const got = await driver.get(tombstoneKey(subjectId));
    const t = got.kind === "found" ? (JSON.parse(got.body) as Tombstone) : null;
    tombstoneCache.set(subjectId, { state: t?.state ?? "none", at: clock() });
    return t;
  }

  async function tombstoneState(subjectId: string, maxAgeMs: number): Promise<TombstoneState | "none"> {
    const cached = tombstoneCache.get(subjectId);
    if (cached && clock() - cached.at < maxAgeMs) return cached.state;
    const t = await readTombstoneDirect(subjectId);
    return t?.state ?? "none";
  }

  function softDeleted(state: TombstoneState | "none"): boolean {
    return state === "pending" || state === "committing";
  }

  async function listGenerations(subjectId: string) {
    const keys = [];
    let after: string | undefined;
    const prefix = `keys/${subjectId}/`;
    for (;;) {
      const page =
        after !== undefined ? await driver.list(prefix, { startAfter: after }) : await driver.list(prefix);
      keys.push(...page.keys);
      if (page.nextStartAfter === undefined) return keys;
      after = page.nextStartAfter;
    }
  }

  function genOf(key: string): number {
    const m = /(\d{6})\.json$/.exec(key);
    if (!m) throw new TransientStoreError(`foreign object in key prefix: ${key}`);
    return Number(m[1]);
  }

  async function unwrapObject(body: string): Promise<{ keyId: string; key: Uint8Array }> {
    const obj = JSON.parse(body) as WrappedKeyObject;
    return { keyId: obj.keyId, key: await masterKey.unwrap(base64ToBytes(obj.wrappedKey)) };
  }

  /**
   * Mint generation `gen`: check → write → re-check (the same shape as
   * append step 4). Both tombstone reads are direct, uncached GETs.
   * Deliberately no delete step — minters hold no DeleteObject; a failed
   * mint's stray is inert (never delivered, never used to encrypt).
   */
  async function mint(
    subjectId: string,
    gen: number,
  ): Promise<{ keyId: string; key: Uint8Array } | "lost-race"> {
    const before = await readTombstoneDirect(subjectId);
    if (before && softDeleted(before.state)) throw new SubjectErasedError(subjectId);

    const raw = random(32);
    const keyId = keyIdOf(gen);
    const object: WrappedKeyObject = {
      subjectId,
      keyId,
      wrappedKey: bytesToBase64(await masterKey.wrap(raw)),
      createdAt: new Date(clock()).toISOString(),
    };
    const put = await driver.putIfAbsent(generationKey(subjectId, gen), JSON.stringify(object));
    if (put.kind === "exists") return "lost-race"; // racing mint won this generation

    const after = await readTombstoneDirect(subjectId);
    if (after && softDeleted(after.state)) {
      // The re-check precedes any encryption: no ciphertext ever exists
      // under the stray; keyring reads never deliver it; the shred's
      // re-list or a later sweep removes it.
      throw new SubjectErasedError(subjectId);
    }
    await config.audit?.(gen === 0 ? "KeyCreated" : "KeyRotated", { subjectId, keyId });
    return { keyId, key: raw };
  }

  async function resolveCurrent(subjectId: string): Promise<{ keyId: string; key: Uint8Array }> {
    for (let attempt = 0; attempt < 5; attempt++) {
      const gens = await listGenerations(subjectId);
      if (gens.length === 0) {
        const minted = await mint(subjectId, 0);
        if (minted !== "lost-race") return minted;
        continue; // re-list and read the winner's generation
      }
      const newest = gens[gens.length - 1]!;
      const got = await driver.get(newest.key, { ifMatch: newest.etag });
      if (got.kind !== "found") continue; // shredded or changed under us; re-list
      return unwrapObject(got.body);
    }
    throw new TransientStoreError(`current-key resolution for ${subjectId} kept losing races`);
  }

  return {
    async currentKey(subjectId) {
      // The append path's tombstone consult: short negative-cache TTL — a
      // cached key must never keep minting ciphertext under a shredded
      // generation for the full read-side TTL.
      if (softDeleted(await tombstoneState(subjectId, tombstoneTtl))) {
        throw new SubjectErasedError(subjectId);
      }
      const cached = currentCache.get(subjectId);
      if (cached && clock() - cached.at < keyCacheTtl) {
        return { keyId: cached.keyId, key: cached.key };
      }
      const current = await resolveCurrent(subjectId);
      currentCache.set(subjectId, { ...current, at: clock() });
      keyCache.set(`${subjectId}/${current.keyId}`, { key: current.key, at: clock() });
      return current;
    },

    async keyById(subjectId, keyId) {
      // Read path: tombstone staleness shares the key-cache TTL.
      if (softDeleted(await tombstoneState(subjectId, keyCacheTtl))) return null;
      const cacheKey = `${subjectId}/${keyId}`;
      const cached = keyCache.get(cacheKey);
      if (cached && clock() - cached.at < keyCacheTtl) return cached.key;
      const got = await driver.get(generationKey(subjectId, Number(keyId)));
      if (got.kind !== "found") return null; // shredded: fail closed
      const { key } = await unwrapObject(got.body);
      keyCache.set(cacheKey, { key, at: clock() });
      return key;
    },

    async keyring(subjectId) {
      // Empty for a soft-deleted subject, whatever a crashed minter left.
      if (softDeleted(await tombstoneState(subjectId, keyCacheTtl))) return [];
      const gens = await listGenerations(subjectId);
      const entries: KeyringEntry[] = [];
      const expiresAt = new Date(clock() + keyringTtl).toISOString();
      for (const g of gens) {
        const got = await driver.get(g.key, { ifMatch: g.etag });
        if (got.kind !== "found") continue; // shredded mid-listing
        const { keyId, key } = await unwrapObject(got.body);
        entries.push({ keyId, key, expiresAt });
      }
      return entries;
    },

    async rotate(subjectId) {
      for (let attempt = 0; attempt < 5; attempt++) {
        const gens = await listGenerations(subjectId);
        const next = gens.length === 0 ? 0 : genOf(gens[gens.length - 1]!.key) + 1;
        const minted = await mint(subjectId, next);
        if (minted === "lost-race") continue;
        currentCache.set(subjectId, { ...minted, at: clock() });
        keyCache.set(`${subjectId}/${minted.keyId}`, { key: minted.key, at: clock() });
        return { keyId: minted.keyId };
      }
      throw new TransientStoreError(`rotation for ${subjectId} kept losing races`);
    },
  };
}
