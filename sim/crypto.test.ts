/**
 * Encryption layer per KEYS_DESIGN.md: encrypting serializer, S3 key
 * store, tombstone state machine, shred workflow + sweeper. Deterministic:
 * direct drivers (no scheduler), manual clock, real WebCrypto.
 */

import { describe, expect, it } from "vitest";
import { ShreddedDataError, SubjectErasedError } from "../src/errors";
import { cryptoRandom } from "../src/crypto/bytes";
import { aesMasterKey } from "../src/crypto/master-key";
import { decryptPayload, encryptPayload, payloadAad } from "../src/crypto/payload";
import {
  createS3KeyStore,
  generationKey,
  tombstoneKey,
  type Tombstone,
} from "../src/crypto/keystore";
import { rewrapKeys } from "../src/crypto/rewrap";
import { encryptingSerializer } from "../src/crypto/serializer";
import {
  AUDIT_STREAM,
  cancelShred,
  requestShred,
  sweepShreds,
  type ShredContext,
} from "../src/crypto/shred";
import { verifyKeyBucketConfig } from "../src/drivers/aws-sdk";
import { createEventStore } from "../src/store";
import { SIM_PREFIX, directDriver } from "./harness";
import { collect } from "./oracle";
import { SimStore } from "./store";

const DAY = 24 * 3600 * 1000;
const SECRET = new Uint8Array(32).fill(7);

function setup(opts?: { keyCacheTtlMs?: number; tombstoneTtlMs?: number; waitingPeriodMs?: number }) {
  const eventSim = new SimStore();
  const keySim = new SimStore();
  const clockRef = { now: 1_000_000_000 };
  const clock = () => clockRef.now;
  const isoClock = () => new Date(clockRef.now).toISOString();
  let n = 0;
  const ids = () => `id#${n++}`;

  const auditStore = createEventStore({
    driver: directDriver(eventSim),
    prefix: SIM_PREFIX,
    chunkSize: 500,
    ids,
    clock: isoClock,
    allowReservedStreams: true,
  });
  const keyDriver = directDriver(keySim);
  const keys = createS3KeyStore({
    driver: keyDriver,
    masterKey: aesMasterKey(SECRET),
    clock,
    keyCacheTtlMs: opts?.keyCacheTtlMs ?? 0, // 0 = always fresh (no caching)
    tombstoneTtlMs: opts?.tombstoneTtlMs ?? 0,
    keyringTtlMs: 3_600_000,
    audit: async (type, data) => {
      await auditStore.append(AUDIT_STREAM, [{ type, data }], { expectedVersion: "any" });
    },
  });
  const serializer = encryptingSerializer({
    keys,
    subjectFor: (streamId) => (streamId.startsWith("user-") ? `subject:${streamId}` : null),
  });
  const store = createEventStore({
    driver: directDriver(eventSim),
    prefix: SIM_PREFIX,
    chunkSize: 2,
    ids,
    clock: isoClock,
    serializer,
  });
  const shredCtx: ShredContext = {
    auditStore,
    keyDriver,
    waitingPeriodMs: opts?.waitingPeriodMs ?? 14 * DAY,
    clock,
  };
  return { eventSim, keySim, clockRef, auditStore, keyDriver, keys, store, shredCtx };
}

const SUBJECT = "subject:user-1";

describe("encrypting serializer end to end", () => {
  it("round-trips through the store; ciphertext at rest; keyId on the envelope", async () => {
    const { store, eventSim } = setup();
    await store.append("user-1", [{ type: "T", data: { secretText: "alpha-plain" }, id: "e0" }], {
      expectedVersion: "noStream",
    });
    await store.append("user-1", [{ type: "T", data: { secretText: "beta-plain" }, id: "e1" }], {
      expectedVersion: 0,
    });

    const replay = await collect(store.read("user-1"));
    expect(replay.map((e) => e.data)).toEqual([
      { secretText: "alpha-plain" },
      { secretText: "beta-plain" },
    ]);
    expect(replay.every((e) => typeof e.keyId === "string")).toBe(true);

    // No plaintext at rest, anywhere in the event bucket.
    for (const [, obj] of eventSim.dump()) {
      expect(obj.body).not.toContain("alpha-plain");
      expect(obj.body).not.toContain("secretText");
    }
    // Raw read (model-B egress): base64 ciphertext verbatim.
    const raw = await collect(store.read("user-1", { raw: true }));
    expect(typeof raw[0]!.data).toBe("string");
    expect(raw[0]!.data).not.toContain("alpha");

    // Unmapped streams stay plaintext, no keyId.
    await store.append("order-9", [{ type: "T", data: { po: 1 }, id: "e2" }], {
      expectedVersion: "noStream",
    });
    const plain = await collect(store.read("order-9"));
    expect(plain[0]!.keyId).toBeUndefined();
    expect(plain[0]!.data).toEqual({ po: 1 });
  });

  it("mints lazily, rotates generationally, and old events keep decrypting", async () => {
    const { store, keys, auditStore } = setup();
    await store.append("user-1", [{ type: "T", data: "gen0-data", id: "e0" }], {
      expectedVersion: "noStream",
    });
    const rotated = await keys.rotate(SUBJECT);
    expect(rotated.keyId).toBe("000001");
    await store.append("user-1", [{ type: "T", data: "gen1-data", id: "e1" }], {
      expectedVersion: 0,
    });

    const replay = await collect(store.read("user-1"));
    expect(replay.map((e) => [e.data, e.keyId])).toEqual([
      ["gen0-data", "000000"],
      ["gen1-data", "000001"],
    ]);
    const ring = await keys.keyring(SUBJECT);
    expect(ring.map((k) => k.keyId)).toEqual(["000000", "000001"]);

    const audit = await collect(auditStore.read(AUDIT_STREAM));
    expect(audit.map((e) => e.type)).toEqual(["KeyCreated", "KeyRotated"]);
    // Audit payloads are plaintext (no key material, opaque IDs only).
    expect(audit[0]!.keyId).toBeUndefined();
    expect(audit[0]!.data).toEqual({ subjectId: SUBJECT, keyId: "000000" });
  });

  it("compaction copies ciphertext verbatim — encrypted streams decrypt after compaction", async () => {
    const { store } = setup();
    await store.append("user-1", [{ type: "T", data: "v0", id: "e0" }], { expectedVersion: "noStream" });
    for (let v = 0; v < 4; v++) {
      await store.append("user-1", [{ type: "T", data: `v${v + 1}`, id: `e${v + 1}` }], {
        expectedVersion: v,
      });
    }
    expect(await store.compactStream("user-1")).toMatchObject({ status: "compacted", chunkBase: 0 });
    expect(await store.compactStream("user-1")).toMatchObject({ status: "compacted", chunkBase: 2 });
    const replay = await collect(store.read("user-1"));
    expect(replay.map((e) => e.data)).toEqual(["v0", "v1", "v2", "v3", "v4"]);
  });
});

describe("shred lifecycle", () => {
  it("soft delete fails writes and reads closed; the sweeper hard-deletes after the waiting period", async () => {
    const { store, keys, keySim, auditStore, shredCtx, clockRef } = setup();
    await store.append("user-1", [{ type: "T", data: "personal", id: "e0" }], {
      expectedVersion: "noStream",
    });

    const { intentPosition } = await requestShred(shredCtx, SUBJECT);
    expect(intentPosition).toBeGreaterThanOrEqual(0);

    // The tombstone is the soft delete: everything fails closed at once.
    await expect(
      store.append("user-1", [{ type: "T", data: "more" }], { expectedVersion: 0 }),
    ).rejects.toThrow(SubjectErasedError);
    expect(await keys.keyring(SUBJECT)).toEqual([]);
    await expect(collect(store.read("user-1"))).rejects.toThrow(ShreddedDataError);
    // ...but the key objects are still physically present (recoverable).
    expect(keySim.dump().has(generationKey(SUBJECT, 0))).toBe(true);

    // Before the waiting period: the sweeper must NOT hard-delete.
    let report = await sweepShreds(shredCtx);
    expect(report.hardDeleted).toEqual([]);
    expect(report.openSubjects).toEqual([SUBJECT]);
    expect(keySim.dump().has(generationKey(SUBJECT, 0))).toBe(true);

    // After it: commit point, hard delete, re-list, ShredCompleted.
    clockRef.now += 15 * DAY;
    report = await sweepShreds(shredCtx);
    expect(report.hardDeleted).toEqual([SUBJECT]);
    expect(keySim.dump().has(generationKey(SUBJECT, 0))).toBe(false);
    const tomb = JSON.parse(keySim.dump().get(tombstoneKey(SUBJECT))!.body) as Tombstone;
    expect(tomb.state).toBe("committing"); // terminal never-re-key signal

    const audit = await collect(auditStore.read(AUDIT_STREAM));
    expect(audit.map((e) => e.type)).toEqual(["KeyCreated", "ShredRequested", "ShredCompleted"]);

    // Idempotent under resume: another sweep changes nothing.
    report = await sweepShreds(shredCtx);
    expect(report.hardDeleted).toEqual([]);

    // Re-keying a committed subject fails forever.
    await expect(keys.currentKey(SUBJECT)).rejects.toThrow(SubjectErasedError);
  });

  it("read-side budget: reads may outlive the tombstone up to the key-cache TTL, never past it", async () => {
    const { store, shredCtx, clockRef } = setup({
      keyCacheTtlMs: 3_600_000, // 1 h read-side budget
      tombstoneTtlMs: 60_000,
    });
    await store.append("user-1", [{ type: "T", data: "d0", id: "e0" }], { expectedVersion: "noStream" });
    await collect(store.read("user-1")); // warm caches: key + live tombstone verdict

    await requestShred(shredCtx, SUBJECT);
    // Within the budget the stale live verdict is accepted...
    expect((await collect(store.read("user-1"))).length).toBe(1);
    // ...and never past it: unreadability has propagated.
    clockRef.now += 3_600_001;
    await expect(collect(store.read("user-1"))).rejects.toThrow(ShreddedDataError);
  });

  it("write-side leash: appends fail closed within the shorter negative-cache TTL", async () => {
    const { store, shredCtx, clockRef } = setup({
      keyCacheTtlMs: 3_600_000,
      tombstoneTtlMs: 60_000, // minutes, not hours
    });
    await store.append("user-1", [{ type: "T", data: "d0", id: "e0" }], { expectedVersion: "noStream" });
    await requestShred(shredCtx, SUBJECT);

    // Within the leash a cached live verdict may still mint ciphertext —
    // the accepted doomed-writer window, erased by the shred like
    // everything before it.
    await store.append("user-1", [{ type: "T", data: "doomed", id: "e1" }], { expectedVersion: 0 });

    // Past the leash: fail closed, before any PUT. (The re-read also
    // propagates the verdict to the read path — fresher knowledge is
    // always allowed; TTLs are maximum delays, not grace windows.)
    clockRef.now += 60_001;
    await expect(
      store.append("user-1", [{ type: "T", data: "d2" }], { expectedVersion: 1 }),
    ).rejects.toThrow(SubjectErasedError);
    await expect(collect(store.read("user-1"))).rejects.toThrow(ShreddedDataError);
  });

  it("cancellation before the deadline recovers the subject, audited", async () => {
    const { store, keys, shredCtx, auditStore, clockRef } = setup();
    await store.append("user-1", [{ type: "T", data: "d0", id: "e0" }], { expectedVersion: "noStream" });
    const { intentPosition } = await requestShred(shredCtx, SUBJECT);

    expect(await cancelShred(shredCtx, SUBJECT, intentPosition)).toBe("cancelled");
    // Live again: reads, appends, keyring all recover.
    expect((await collect(store.read("user-1"))).length).toBe(1);
    await store.append("user-1", [{ type: "T", data: "d1", id: "e1" }], { expectedVersion: 0 });
    expect((await keys.keyring(SUBJECT)).length).toBe(1);

    // The sweeper has nothing to do — the intent is closed.
    clockRef.now += 30 * DAY;
    const report = await sweepShreds(shredCtx);
    expect(report.hardDeleted).toEqual([]);
    const audit = await collect(auditStore.read(AUDIT_STREAM));
    expect(audit.map((e) => e.type)).toContain("ShredCancelled");
  });

  it("a re-shred after cancellation restarts the waiting period (full-body reopen)", async () => {
    const { keySim, store, shredCtx, clockRef } = setup();
    await store.append("user-1", [{ type: "T", data: "d0", id: "e0" }], { expectedVersion: "noStream" });
    const first = await requestShred(shredCtx, SUBJECT);
    await cancelShred(shredCtx, SUBJECT, first.intentPosition);

    clockRef.now += 20 * DAY; // well past the first intent's would-be deadline
    await requestShred(shredCtx, SUBJECT);
    const tomb = JSON.parse(keySim.dump().get(tombstoneKey(SUBJECT))!.body) as Tombstone;
    expect(tomb.state).toBe("pending");
    expect(tomb.requestedAt).toBe(clockRef.now); // fresh clock, not the inherited one

    // An immediate sweep must NOT hard-delete: the period restarted.
    const report = await sweepShreds(shredCtx);
    expect(report.hardDeleted).toEqual([]);
    expect(report.openSubjects).toEqual([SUBJECT]);
  });

  it("a cancellation that loses the commit-point CAS observes committing", async () => {
    const { store, shredCtx, clockRef } = setup();
    await store.append("user-1", [{ type: "T", data: "d0", id: "e0" }], { expectedVersion: "noStream" });
    const { intentPosition } = await requestShred(shredCtx, SUBJECT);
    clockRef.now += 15 * DAY;
    await sweepShreds(shredCtx); // commit point passed; hard delete done
    expect(await cancelShred(shredCtx, SUBJECT, intentPosition)).toBe("lost-to-commit");
  });

  it("a crashed cancellation (audit appended, CAS never ran) is reconciled by the sweeper", async () => {
    const { auditStore, keySim, store, shredCtx } = setup();
    await store.append("user-1", [{ type: "T", data: "d0", id: "e0" }], { expectedVersion: "noStream" });
    const { intentPosition } = await requestShred(shredCtx, SUBJECT);
    // Crash simulation: the ShredCancelled append landed, the tombstone CAS didn't.
    await auditStore.append(
      AUDIT_STREAM,
      [{ type: "ShredCancelled", data: { subjectId: SUBJECT, intent: intentPosition } }],
      { expectedVersion: "any" },
    );
    const report = await sweepShreds(shredCtx);
    expect(report.reconciledCancellations).toEqual([SUBJECT]);
    const tomb = JSON.parse(keySim.dump().get(tombstoneKey(SUBJECT))!.body) as Tombstone;
    expect(tomb.state).toBe("cancelled");
  });

  it("a mint racing the tombstone fails at the re-check; its stray is inert and swept", async () => {
    const base = setup();
    const { keySim, shredCtx, clockRef } = base;

    // A minter stalled between its generation PUT and its tombstone re-check.
    let reach!: () => void;
    let release!: () => void;
    const reached = new Promise<void>((r) => (reach = r));
    const released = new Promise<void>((r) => (release = r));
    let tombstoneGets = 0;
    const inner = directDriver(keySim);
    const gated = {
      ...inner,
      get: async (key: string, o?: { ifMatch?: string }) => {
        if (key.startsWith("tombstones/")) {
          tombstoneGets++;
          // #1 = currentKey's consult, #2 = mint pre-check, #3 = mint
          // re-check (after the generation PUT) — stall there.
          if (tombstoneGets === 3) {
            reach();
            await released;
          }
        }
        return inner.get(key, o);
      },
    };
    const stalledKeys = createS3KeyStore({
      driver: gated,
      masterKey: aesMasterKey(SECRET),
      clock: () => clockRef.now,
      keyCacheTtlMs: 0,
      tombstoneTtlMs: 0,
    });
    const minting = stalledKeys.currentKey(SUBJECT); // lazy gen-0 mint
    minting.catch(() => {});
    await reached;

    // The shred lands while the minter is stalled.
    await requestShred(shredCtx, SUBJECT);
    release();
    await expect(minting).rejects.toThrow(SubjectErasedError);

    // The stray generation exists but is inert: keyring reads never deliver it.
    expect(keySim.dump().has(generationKey(SUBJECT, 0))).toBe(true);
    expect(await base.keys.keyring(SUBJECT)).toEqual([]);

    // The sweep's enumeration + re-list clears it.
    clockRef.now += 15 * DAY;
    const report = await sweepShreds(shredCtx);
    expect(report.hardDeleted).toEqual([SUBJECT]);
    expect(keySim.dump().has(generationKey(SUBJECT, 0))).toBe(false);
  });
});

describe("master-key re-wrap", () => {
  const NEW_SECRET = new Uint8Array(32).fill(9);

  it("rewraps every generation; ciphertext and caches never notice; idempotent under resume", async () => {
    const { store, keys, keyDriver, clockRef } = setup();
    await store.append("user-1", [{ type: "T", data: "d0", id: "e0" }], { expectedVersion: "noStream" });
    await keys.rotate(SUBJECT);
    await store.append("user-2", [{ type: "T", data: "d1", id: "e1" }], { expectedVersion: "noStream" });

    const from = aesMasterKey(SECRET);
    const to = aesMasterKey(NEW_SECRET);
    const report = await rewrapKeys({ driver: keyDriver, from, to });
    expect(report).toEqual({ rewrapped: 3, alreadyCurrent: 0, skipped: 0, failed: [] });

    // A key store under the NEW master decrypts everything — including
    // ciphertext written before the rewrap (ciphertext never noticed).
    const rewrappedKeys = createS3KeyStore({
      driver: keyDriver,
      masterKey: to,
      clock: () => clockRef.now,
      keyCacheTtlMs: 0,
      tombstoneTtlMs: 0,
    });
    expect(await rewrappedKeys.keyById(SUBJECT, "000000")).not.toBeNull();
    expect(await rewrappedKeys.keyById(SUBJECT, "000001")).not.toBeNull();
    expect((await rewrappedKeys.keyring("subject:user-2")).length).toBe(1);

    // Resume is idempotent: nothing left to do, nothing double-wrapped.
    const again = await rewrapKeys({ driver: keyDriver, from, to });
    expect(again).toEqual({ rewrapped: 0, alreadyCurrent: 3, skipped: 0, failed: [] });
  });

  it("a rewrap racing a shred skips on the CAS and never resurrects the deleted key", async () => {
    const { store, keySim, keyDriver } = setup();
    await store.append("user-1", [{ type: "T", data: "d0", id: "e0" }], { expectedVersion: "noStream" });

    // Hold the rewrap between its GET and its CAS write-back; the shred's
    // hard delete lands in the window.
    let reach!: () => void;
    let release!: () => void;
    const reached = new Promise<void>((r) => (reach = r));
    const released = new Promise<void>((r) => (release = r));
    const gated = {
      ...keyDriver,
      putIfMatch: async (key: string, body: string, etag: string) => {
        reach();
        await released;
        return keyDriver.putIfMatch(key, body, etag);
      },
    };
    const running = rewrapKeys({
      driver: gated,
      from: aesMasterKey(SECRET),
      to: aesMasterKey(NEW_SECRET),
    });
    await reached;
    keySim.delete(generationKey(SUBJECT, 0)); // the hard delete
    release();

    const report = await running;
    expect(report).toEqual({ rewrapped: 0, alreadyCurrent: 0, skipped: 1, failed: [] });
    // The resurrection guard: the shredded key stays deleted.
    expect(keySim.dump().has(generationKey(SUBJECT, 0))).toBe(false);
  });
});

describe("context binding (AAD)", () => {
  it("rejects ciphertext transplanted to another stream or generation, even under the same key", async () => {
    const key = cryptoRandom(32);
    const aad = payloadAad("user-1", "000000");
    const ct = await encryptPayload(
      key,
      { secretText: "alpha" },
      { compress: true, random: cryptoRandom, aad },
    );
    expect(await decryptPayload(key, ct, aad)).toEqual({ secretText: "alpha" });
    // Same key, foreign stream / foreign generation: fails authentication
    // instead of decrypting cleanly in the wrong context.
    await expect(decryptPayload(key, ct, payloadAad("user-2", "000000"))).rejects.toThrow(
      ShreddedDataError,
    );
    await expect(decryptPayload(key, ct, payloadAad("user-1", "000001"))).rejects.toThrow(
      ShreddedDataError,
    );
  });

  it("a wrapped key grafted into another subject's prefix fails to unwrap — key delivery never hands over a foreign key", async () => {
    const { keys, keyDriver } = setup();
    await keys.currentKey(SUBJECT); // mint subject:user-1 generation 0
    const stolen = await keyDriver.get(generationKey(SUBJECT, 0));
    if (stolen.kind !== "found") throw new Error("expected the wrapped key object");

    // Naive graft — copy the object verbatim: the body/path cross-check fires.
    await keyDriver.putIfAbsent(generationKey("subject:user-2", 0), stolen.body);
    await expect(keys.keyById("subject:user-2", "000000")).rejects.toThrow(/grafted or corrupt/);

    // Doctored graft — body rewritten to claim the target location: the
    // wrap context (bound at mint, derived from the key path) fails the
    // unwrap; neither keyById nor keyring ever delivers the foreign key.
    const doctored = JSON.stringify({
      ...(JSON.parse(stolen.body) as Record<string, unknown>),
      subjectId: "subject:user-3",
    });
    await keyDriver.putIfAbsent(generationKey("subject:user-3", 0), doctored);
    await expect(keys.keyById("subject:user-3", "000000")).rejects.toThrow();
    await expect(keys.keyring("subject:user-3")).rejects.toThrow();

    // The legitimate owner is unaffected.
    expect(await keys.keyById(SUBJECT, "000000")).not.toBeNull();
  });
});

describe("sweeper config re-verification", () => {
  it("re-verifies the key bucket immediately before the first hard delete; a failure aborts before anything is destroyed", async () => {
    const { keys, keySim, clockRef, shredCtx } = setup();
    await keys.currentKey(SUBJECT); // mint generation 0
    await requestShred(shredCtx, SUBJECT);
    clockRef.now += 15 * DAY;

    let checks = 0;
    await expect(
      sweepShreds({
        ...shredCtx,
        verifyKeyBucketConfig: async () => {
          checks++;
          throw new Error("key bucket versioning is Enabled");
        },
      }),
    ).rejects.toThrow(/versioning is Enabled/);
    expect(checks).toBe(1);
    // Config drift blocked the destruction: the generation survives.
    expect(keySim.dump().has(generationKey(SUBJECT, 0))).toBe(true);

    // A later run against a fixed bucket resumes idempotently and completes.
    const report = await sweepShreds({
      ...shredCtx,
      verifyKeyBucketConfig: async () => {
        checks++;
      },
    });
    expect(report.hardDeleted).toEqual([SUBJECT]);
    expect(checks).toBe(2);
    expect(keySim.dump().has(generationKey(SUBJECT, 0))).toBe(false);
  });
});

describe("key bucket startup verification", () => {
  function fakeConfigClient(state: { versioning?: string; replication?: boolean; lock?: boolean }) {
    return {
      async send(command: unknown): Promise<unknown> {
        const name = (command as object).constructor.name;
        if (name === "GetBucketVersioningCommand") {
          return state.versioning !== undefined ? { Status: state.versioning } : {};
        }
        if (name === "GetBucketReplicationCommand") {
          if (state.replication) return { ReplicationConfiguration: {} };
          throw Object.assign(new Error("no replication"), {
            name: "ReplicationConfigurationNotFoundError",
            $metadata: { httpStatusCode: 404 },
          });
        }
        if (name === "GetObjectLockConfigurationCommand") {
          if (state.lock) return { ObjectLockConfiguration: {} };
          throw Object.assign(new Error("no lock"), {
            name: "ObjectLockConfigurationNotFoundError",
            $metadata: { httpStatusCode: 404 },
          });
        }
        throw new Error(`unexpected command ${name}`);
      },
    };
  }

  it("passes on the inverted configuration and fails fast on each violation", async () => {
    await verifyKeyBucketConfig(fakeConfigClient({}), "k");
    await expect(verifyKeyBucketConfig(fakeConfigClient({ versioning: "Enabled" }), "k")).rejects.toThrow(
      /versioning is Enabled/,
    );
    // Suspended fails too: old versions remain readable forever.
    await expect(
      verifyKeyBucketConfig(fakeConfigClient({ versioning: "Suspended" }), "k"),
    ).rejects.toThrow(/versioning is Suspended/);
    await expect(verifyKeyBucketConfig(fakeConfigClient({ replication: true }), "k")).rejects.toThrow(
      /replication/,
    );
    await expect(verifyKeyBucketConfig(fakeConfigClient({ lock: true }), "k")).rejects.toThrow(
      /Object Lock/,
    );
  });
});
