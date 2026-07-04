/**
 * Shred protocol (KEYS_DESIGN.md): intent-first, tombstone-guarded
 * crypto-shredding with a soft-delete waiting period.
 *
 * The tombstone is a state machine, never a bare marker: `pending` →
 * `committing` (sweeper) or `pending` → `cancelled` (cancellation), every
 * transition a CAS PUT with If-Match; a tombstone, once created, is never
 * deleted. `pending`/`committing` are soft-deleted; `cancelled` (or no
 * tombstone) means live.
 *
 * The initiating request performs only steps 1–2 (intent + instant
 * unreadability); the sweeper owns everything after. A crash anywhere
 * leaves a visible dangling intent, never a silent shred.
 */

import type { StorageDriver } from "../driver.js";
import { TransientStoreError } from "../errors.js";
import type { EventStore } from "../store.js";
import type { Tombstone } from "./keystore.js";
import { tombstoneKey } from "./keystore.js";

export const AUDIT_STREAM = "$system.key-audit";

export interface ShredContext {
  /** Event store over the EVENT bucket, created with allowReservedStreams. */
  auditStore: EventStore;
  /** Driver over the KEY bucket. The sweeper's principal alone holds delete. */
  keyDriver: StorageDriver;
  /** Soft-delete waiting period before the commit point (e.g. 14 days). */
  waitingPeriodMs: number;
  /** ms epoch; injectable for deterministic tests. */
  clock?: () => number;
}

interface OpenIntent {
  subjectId: string;
  /** The ShredRequested event's version — the intent's identity. */
  position: number;
}

interface SweepCheckpoint {
  fromVersion: number;
  openIntents: OpenIntent[];
}

const CHECKPOINT_KEY = "sweep/checkpoint.json";

function now(ctx: ShredContext): number {
  return (ctx.clock ?? (() => Date.now()))();
}

async function readTombstone(
  driver: StorageDriver,
  subjectId: string,
): Promise<{ body: Tombstone; etag: string } | null> {
  const got = await driver.get(tombstoneKey(subjectId));
  if (got.kind !== "found") return null;
  return { body: JSON.parse(got.body) as Tombstone, etag: got.etag };
}

/**
 * Step 2: write or take over the tombstone on behalf of an intent.
 * Never adopt as found — take over by state; the intent stamp is
 * forward-only (a stale repair must never regress it past a newer one).
 */
export async function ensureTombstone(
  ctx: ShredContext,
  subjectId: string,
  intentPosition: number,
): Promise<Tombstone["state"]> {
  const driver = ctx.keyDriver;
  for (let attempt = 0; attempt < 5; attempt++) {
    const existing = await readTombstone(driver, subjectId);
    if (existing === null) {
      const fresh: Tombstone = {
        subjectId,
        state: "pending",
        requestedAt: now(ctx),
        intent: intentPosition,
      };
      const put = await driver.putIfAbsent(tombstoneKey(subjectId), JSON.stringify(fresh));
      if (put.kind === "created") return "pending";
      continue; // lost the create race; take over whatever won
    }
    const { body, etag } = existing;
    switch (body.state) {
      case "pending": {
        if (intentPosition <= body.intent) return "pending"; // forward-only
        // Refresh the stamp, keeping the existing timestamp: the subject is
        // already soft-deleted; the earlier clock only brings hard delete sooner.
        const cas = await driver.putIfMatch(
          tombstoneKey(subjectId),
          JSON.stringify({ ...body, intent: intentPosition } satisfies Tombstone),
          etag,
        );
        if (cas.kind === "updated") return "pending";
        continue;
      }
      case "committing":
        return "committing"; // terminal; remaining steps are idempotent
      case "cancelled": {
        // Reopen with a FULL body rewrite: fresh timestamp — the subject
        // was live until now, so the waiting period restarts.
        const reopened: Tombstone = {
          subjectId,
          state: "pending",
          requestedAt: now(ctx),
          intent: intentPosition,
        };
        const cas = await driver.putIfMatch(tombstoneKey(subjectId), JSON.stringify(reopened), etag);
        if (cas.kind === "updated") return "pending";
        continue;
      }
    }
  }
  throw new TransientStoreError(`tombstone for ${subjectId} kept changing; giving up`);
}

/** Steps 1–2: append the intent, then soft-delete. Returns the intent position. */
export async function requestShred(
  ctx: ShredContext,
  subjectId: string,
): Promise<{ intentPosition: number }> {
  const result = await ctx.auditStore.append(
    AUDIT_STREAM,
    [{ type: "ShredRequested", data: { subjectId } }],
    { expectedVersion: "any" },
  );
  const intentPosition = result.nextExpectedVersion;
  await ensureTombstone(ctx, subjectId, intentPosition);
  return { intentPosition };
}

export type CancelOutcome =
  | "cancelled" // this intent closed and the tombstone flipped to cancelled
  | "superseded" // a newer intent holds the tombstone; the subject stays soft-deleted
  | "lost-to-commit" // the sweeper won the commit-point CAS; the shred happened
  | "already-cancelled";

/**
 * Cancellation: audit first, CAS second (a crash between the two leaves a
 * visible cancelled-but-pending subject the sweeper reconciles — never an
 * unaudited recovery). Per-intent: CAS only when the stamp names this intent.
 */
export async function cancelShred(
  ctx: ShredContext,
  subjectId: string,
  intentPosition: number,
): Promise<CancelOutcome> {
  await ctx.auditStore.append(
    AUDIT_STREAM,
    [{ type: "ShredCancelled", data: { subjectId, intent: intentPosition } }],
    { expectedVersion: "any" },
  );
  for (let attempt = 0; attempt < 5; attempt++) {
    const existing = await readTombstone(ctx.keyDriver, subjectId);
    if (existing === null) return "already-cancelled"; // nothing soft-deleted
    const { body, etag } = existing;
    if (body.state === "committing") return "lost-to-commit";
    if (body.state === "cancelled") return "already-cancelled";
    if (body.intent !== intentPosition) return "superseded"; // a newer open intent keeps it
    const cas = await ctx.keyDriver.putIfMatch(
      tombstoneKey(subjectId),
      JSON.stringify({ ...body, state: "cancelled" } satisfies Tombstone),
      etag,
    );
    if (cas.kind === "updated") return "cancelled";
  }
  throw new TransientStoreError(`cancellation CAS for ${subjectId} kept losing; giving up`);
}

export interface SweepReport {
  hardDeleted: string[];
  reconciledCancellations: string[];
  openSubjects: string[];
}

/**
 * The shred sweeper — the one clock-driven job in the system. Scans
 * $system.key-audit from its checkpoint for open intents and drives each
 * to completion; all idempotent under resume. Intent matching is
 * asymmetric: ShredCancelled closes only the intent it names;
 * ShredCompleted closes every intent for its subject open at its position.
 */
export async function sweepShreds(ctx: ShredContext): Promise<SweepReport> {
  const driver = ctx.keyDriver;

  // Checkpoint: a cursor object, CAS-updated, carrying open intents forward.
  const cpGot = await driver.get(CHECKPOINT_KEY);
  const checkpoint: SweepCheckpoint =
    cpGot.kind === "found"
      ? (JSON.parse(cpGot.body) as SweepCheckpoint)
      : { fromVersion: 0, openIntents: [] };

  let open: OpenIntent[] = [...checkpoint.openIntents];
  const closedByCancellation: OpenIntent[] = [];
  let maxVersion = checkpoint.fromVersion - 1;
  for await (const e of ctx.auditStore.read(AUDIT_STREAM, { fromVersion: checkpoint.fromVersion })) {
    maxVersion = e.version;
    const d = e.data as { subjectId: string; intent?: number };
    if (e.type === "ShredRequested") {
      open.push({ subjectId: d.subjectId, position: e.version });
    } else if (e.type === "ShredCancelled") {
      open = open.filter((i) => !(i.subjectId === d.subjectId && i.position === d.intent));
      closedByCancellation.push({ subjectId: d.subjectId, position: d.intent! });
    } else if (e.type === "ShredCompleted") {
      open = open.filter((i) => !(i.subjectId === d.subjectId && i.position < e.version));
    }
  }

  const report: SweepReport = { hardDeleted: [], reconciledCancellations: [], openSubjects: [] };

  // Drive each subject with open intents.
  const bySubject = new Map<string, OpenIntent[]>();
  for (const intent of open) {
    const list = bySubject.get(intent.subjectId) ?? [];
    list.push(intent);
    bySubject.set(intent.subjectId, list);
  }
  for (const [subjectId, intents] of bySubject) {
    const newest = intents.reduce((a, b) => (b.position > a.position ? b : a));
    // Step 2 on behalf of every open intent (repairs a crashed initiator).
    await ensureTombstone(ctx, subjectId, newest.position);
    const current = await readTombstone(driver, subjectId);
    if (current === null) continue; // cannot happen: ensureTombstone created it
    const { body, etag } = current;

    if (body.state === "pending") {
      if (now(ctx) < body.requestedAt + ctx.waitingPeriodMs) {
        report.openSubjects.push(subjectId); // waiting period still running
        continue;
      }
      // Step 3 — the commit point: cancellation and hard delete race for
      // one CAS on one ETag; exactly one wins.
      const cas = await driver.putIfMatch(
        tombstoneKey(subjectId),
        JSON.stringify({ ...body, state: "committing" } satisfies Tombstone),
        etag,
      );
      if (cas.kind === "precondition-failed") {
        const reread = await readTombstone(driver, subjectId);
        if (reread === null || reread.body.state === "cancelled") continue; // intent closed under us
        if (reread.body.state === "pending") {
          // Step 2 refreshed/reopened underneath us: honor the timestamp it
          // now carries; retrying blind would steal a fresh cancellation window.
          report.openSubjects.push(subjectId);
          continue;
        }
        // committing: a concurrent sweeper won; proceed idempotently.
      }
    } else if (body.state === "cancelled") {
      continue; // closed under us since the scan
    }

    // Hard delete (step 3 tail) + re-list-to-confirm-empty (step 4). The
    // point of irrecoverability. Re-list loop terminates: only mints in
    // flight at tombstone creation can land strays.
    for (let round = 0; round < 10; round++) {
      const generations = await listPrefix(driver, `keys/${subjectId}/`);
      if (generations.length === 0) break;
      await driver.deleteMany(generations);
    }
    const remaining = await listPrefix(driver, `keys/${subjectId}/`);
    if (remaining.length > 0) {
      throw new TransientStoreError(`shred of ${subjectId}: keys kept reappearing`);
    }
    await ctx.auditStore.append(
      AUDIT_STREAM,
      [{ type: "ShredCompleted", data: { subjectId } }],
      { expectedVersion: "any" },
    );
    report.hardDeleted.push(subjectId);
    open = open.filter((i) => i.subjectId !== subjectId);
  }

  // Reconciliation: a cancellation that crashed between its audit append
  // and its CAS. Two guards, both required: the stamp must name the closed
  // intent, and the scan must show zero open intents for the subject.
  for (const closed of closedByCancellation) {
    if (open.some((i) => i.subjectId === closed.subjectId)) continue;
    if (report.hardDeleted.includes(closed.subjectId)) continue;
    const existing = await readTombstone(driver, closed.subjectId);
    if (existing === null || existing.body.state !== "pending") continue;
    if (existing.body.intent !== closed.position) continue;
    const cas = await driver.putIfMatch(
      tombstoneKey(closed.subjectId),
      JSON.stringify({ ...existing.body, state: "cancelled" } satisfies Tombstone),
      existing.etag,
    );
    if (cas.kind === "updated") report.reconciledCancellations.push(closed.subjectId);
  }

  // Save the checkpoint (CAS; a concurrent sweeper's win is accepted —
  // the next run rescans a suffix, and every step is idempotent).
  const next: SweepCheckpoint = { fromVersion: maxVersion + 1, openIntents: open };
  if (cpGot.kind === "found") {
    await driver.putIfMatch(CHECKPOINT_KEY, JSON.stringify(next), cpGot.etag);
  } else {
    await driver.putIfAbsent(CHECKPOINT_KEY, JSON.stringify(next));
  }
  return report;
}

async function listPrefix(driver: StorageDriver, prefix: string): Promise<string[]> {
  const keys: string[] = [];
  let after: string | undefined;
  for (;;) {
    const page =
      after !== undefined ? await driver.list(prefix, { startAfter: after }) : await driver.list(prefix);
    keys.push(...page.keys.map((k) => k.key));
    if (page.nextStartAfter === undefined) return keys;
    after = page.nextStartAfter;
  }
}
