/**
 * Master-key re-wrap (KEYS_DESIGN.md, Key rotation): unwrap-and-rewrap
 * each wrapped key object in the key bucket — a mutation of the one
 * mutable store. Ciphertext, caches, and compaction never notice.
 *
 * One rule keeps it honest: re-wrap is a read-modify-write, so the
 * write-back is CAS — PUT with If-Match on the ETag it read; on 412 or
 * 404 SKIP the key, never retry blind. A blind write-back racing a shred
 * would recreate the deleted key object — a shredded key resurrected
 * after ShredCompleted, the worst erasure failure available.
 *
 * Idempotent under resume: an object already wrapped by `to` (a prior
 * partial run) is recognized and left alone. Rerun until `skipped` and
 * `failed` are both zero, then retire the old master key.
 */

import type { StorageDriver } from "../driver.js";
import type { MasterKey } from "./master-key.js";
import { base64ToBytes, bytesToBase64 } from "./bytes.js";

export interface RewrapReport {
  /** Rewrapped from `from` to `to` this run. */
  rewrapped: number;
  /** Already wrapped by `to` (a prior run's work). */
  alreadyCurrent: number;
  /** Skipped on CAS/404 — shredded or concurrently modified; rerun later. */
  skipped: number;
  /** Keys neither master could unwrap — foreign or corrupt; investigate. */
  failed: string[];
}

export async function rewrapKeys(opts: {
  /** Driver over the KEY bucket. Needs LIST/Get/Put on keys/*; no delete. */
  driver: StorageDriver;
  from: MasterKey;
  to: MasterKey;
}): Promise<RewrapReport> {
  const { driver, from, to } = opts;
  const report: RewrapReport = { rewrapped: 0, alreadyCurrent: 0, skipped: 0, failed: [] };

  let after: string | undefined;
  for (;;) {
    const page =
      after !== undefined ? await driver.list("keys/", { startAfter: after }) : await driver.list("keys/");
    for (const listed of page.keys) {
      const got = await driver.get(listed.key);
      if (got.kind !== "found") {
        report.skipped++; // shredded since the LIST
        continue;
      }
      const object = JSON.parse(got.body) as { wrappedKey: string } & Record<string, unknown>;
      const wrapped = base64ToBytes(object.wrappedKey);

      let raw: Uint8Array;
      try {
        raw = await from.unwrap(wrapped);
      } catch {
        try {
          await to.unwrap(wrapped);
          report.alreadyCurrent++; // a prior partial run got here first
        } catch {
          report.failed.push(listed.key);
        }
        continue;
      }

      const rewrapped = JSON.stringify({ ...object, wrappedKey: bytesToBase64(await to.wrap(raw)) });
      const cas = await driver.putIfMatch(listed.key, rewrapped, got.etag);
      if (cas.kind === "updated") {
        report.rewrapped++;
      } else {
        // 412/404: shredded or concurrently rewrapped underneath us.
        // Never retry blind — that is the resurrection hazard.
        report.skipped++;
      }
    }
    if (page.nextStartAfter === undefined) break;
    after = page.nextStartAfter;
  }
  return report;
}
