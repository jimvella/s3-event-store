/**
 * Browser client SDK against a fake worker implementing the REST wire
 * format (DESIGN.md, Wire format): chunk-aligned pages with
 * complete/next/prev, 308 redirect-to-canonical for cold resumes, and the
 * model-B keyring endpoint. The client decrypts locally, follows links,
 * and caches complete pages forever.
 */

import { describe, expect, it } from "vitest";
import { createStreamClient } from "../src/client";
import { bytesToBase64 } from "../src/crypto/bytes";
import { aesMasterKey } from "../src/crypto/master-key";
import { createS3KeyStore, type KeyStore } from "../src/crypto/keystore";
import { encryptingSerializer } from "../src/crypto/serializer";
import { AUDIT_STREAM, requestShred, type ShredContext } from "../src/crypto/shred";
import { ShreddedDataError } from "../src/errors";
import { createEventStore, type EventStore } from "../src/store";
import type { EventEnvelope } from "../src/types";
import { SIM_PREFIX, directDriver } from "./harness";
import { collect } from "./oracle";
import { SimStore } from "./store";

const SECRET = new Uint8Array(32).fill(7);
const PAGE_SIZE = 2;
const BASE = "https://api.test/app";

function subjectFor(streamId: string): string | null {
  return streamId.startsWith("user-") ? `subject:${streamId}` : null;
}

function setup() {
  const eventSim = new SimStore();
  const keySim = new SimStore();
  const clockRef = { now: 1_700_000_000_000 };
  let n = 0;
  const auditStore = createEventStore({
    driver: directDriver(eventSim),
    prefix: SIM_PREFIX,
    chunkSize: 500,
    ids: () => `id#${n++}`,
    clock: () => new Date(clockRef.now).toISOString(),
    allowReservedStreams: true,
  });
  const keys = createS3KeyStore({
    driver: directDriver(keySim),
    masterKey: aesMasterKey(SECRET),
    clock: () => clockRef.now,
    keyCacheTtlMs: 0,
    tombstoneTtlMs: 0,
    keyringTtlMs: 3_600_000,
  });
  const store = createEventStore({
    driver: directDriver(eventSim),
    prefix: SIM_PREFIX,
    chunkSize: PAGE_SIZE,
    ids: () => `id#${n++}`,
    clock: () => new Date(clockRef.now).toISOString(),
    serializer: encryptingSerializer({ keys, subjectFor }),
  });
  const shredCtx: ShredContext = {
    auditStore,
    keyDriver: directDriver(keySim),
    waitingPeriodMs: 14 * 24 * 3600_000,
    clock: () => clockRef.now,
  };
  // The worker is deployment code: its read store may touch $-streams.
  const workerStore = createEventStore({
    driver: directDriver(eventSim),
    prefix: SIM_PREFIX,
    chunkSize: PAGE_SIZE,
    ids: () => `worker#${n++}`,
    clock: () => new Date(clockRef.now).toISOString(),
    allowReservedStreams: true,
  });
  const worker = fakeWorker(workerStore, keys);
  const client = createStreamClient({
    baseUrl: BASE,
    auth: () => "test-token",
    fetchImpl: worker.handler,
    clock: () => clockRef.now,
  });
  return { store, keys, clockRef, worker, client, shredCtx };
}

/** Minimal model-B worker: raw (ciphertext) pages + keyring delivery. */
function fakeWorker(store: EventStore, keys: KeyStore) {
  const requests: string[] = [];
  const handler = async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    requests.push(url.pathname + url.search);
    if (req.headers.get("authorization") !== "Bearer test-token") {
      return new Response("unauthorized", { status: 401 });
    }
    const m = /^\/app\/streams\/([^/]+)\/(events|key)$/.exec(url.pathname);
    if (!m) return new Response("not found", { status: 404 });
    const streamId = decodeURIComponent(m[1]!);

    if (m[2] === "key") {
      const subject = subjectFor(streamId);
      const ring = subject === null ? [] : await keys.keyring(subject);
      return Response.json({
        keyring: ring.map((k) => ({ keyId: k.keyId, key: bytesToBase64(k.key), expiresAt: k.expiresAt })),
      });
    }

    const all: EventEnvelope[] = await collect(store.read(streamId, { raw: true }));
    const head = all.length > 0 ? all[all.length - 1]!.version : -1;
    const from = Number(url.searchParams.get("from") ?? "0");
    const pageStart = Math.floor(from / PAGE_SIZE) * PAGE_SIZE;
    if (from !== pageStart) {
      // Redirect-to-canonical: one redirect converts any entry point into
      // the canonical URL space.
      return new Response(null, {
        status: 308,
        headers: { location: `/app/streams/${m[1]!}/events?from=${pageStart}` },
      });
    }
    const pageEnd = pageStart + PAGE_SIZE - 1;
    const complete = pageEnd < head;
    return Response.json({
      streamId,
      from: pageStart,
      to: Math.min(pageEnd, head),
      complete,
      events: all.filter((e) => e.version >= pageStart && e.version <= pageEnd),
      next: complete ? `/app/streams/${m[1]!}/events?from=${pageStart + PAGE_SIZE}` : null,
      prev: pageStart > 0 ? `/app/streams/${m[1]!}/events?from=${pageStart - PAGE_SIZE}` : null,
    });
  };
  return { handler, requests };
}

async function seed(store: EventStore, streamId: string, count: number): Promise<void> {
  await store.append(streamId, [{ type: "T", data: { n: 0 }, id: `${streamId}-0` }], {
    expectedVersion: "noStream",
  });
  for (let v = 0; v < count - 1; v++) {
    await store.append(streamId, [{ type: "T", data: { n: v + 1 }, id: `${streamId}-${v + 1}` }], {
      expectedVersion: v,
    });
  }
}

describe("browser client", () => {
  it("reads a plaintext stream by following links; cold resume redirects to canonical", async () => {
    const { store, client } = setup();
    await seed(store, "order-1", 5);

    const all = await collect(client.read("order-1"));
    expect(all.map((e) => e.data)).toEqual([{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }]);

    // Mid-page cursor: 308 → canonical page → local trim to the cursor.
    const tail = await collect(client.read("order-1", { from: 3 }));
    expect(tail.map((e) => e.version)).toEqual([3, 4]);
  });

  it("decrypts an encrypted stream via the keyring, applies upcasters, folds to state", async () => {
    const { store, worker, clockRef } = setup();
    await seed(store, "user-1", 5);
    const client = createStreamClient({
      baseUrl: BASE,
      auth: () => "test-token",
      fetchImpl: worker.handler,
      clock: () => clockRef.now,
      upcasters: [(e) => (e.type === "T" ? { ...e, type: "T.v2" } : e)],
    });

    const all = await collect(client.read("user-1"));
    expect(all.map((e) => e.data)).toEqual([{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }]);
    expect(all.every((e) => e.type === "T.v2")).toBe(true); // upcast client-side

    const { state, version } = await client.fold("user-1", {
      init: () => 0,
      evolve: (sum, e) => sum + (e.data as { n: number }).n,
    });
    expect(state).toBe(10);
    expect(version).toBe(4);
  });

  it("caches complete pages forever; only the incomplete head page re-polls", async () => {
    const { store, worker, client } = setup();
    await seed(store, "user-1", 5); // pages 0-1, 2-3 complete; 4 incomplete

    await collect(client.read("user-1"));
    const eventRequests = () => worker.requests.filter((r) => r.includes("/events")).length;
    const afterFirst = eventRequests();
    await collect(client.read("user-1"));
    // Second replay: complete pages served from the local cache; only the
    // incomplete head page hits the worker again.
    expect(eventRequests()).toBe(afterFirst + 1);
  });

  it("a shredded stream presents as decryption failure, never stale plaintext", async () => {
    const { store, client, shredCtx } = setup();
    await seed(store, "user-1", 3);
    await requestShred(shredCtx, "subject:user-1");
    await expect(collect(client.read("user-1"))).rejects.toThrow(ShreddedDataError);
  });

  it("refetches the keyring when an unknown keyId appears (rotation)", async () => {
    const { store, keys, worker, client, clockRef } = setup();
    await seed(store, "user-1", 2);
    await collect(client.read("user-1")); // keyring cached with gen 0

    await keys.rotate("subject:user-1");
    await store.append("user-1", [{ type: "T", data: { n: 99 }, id: "post-rotation" }], {
      expectedVersion: 1,
    });
    clockRef.now += 1000; // well within the keyring TTL

    const all = await collect(client.read("user-1"));
    expect(all.map((e) => e.data)).toEqual([{ n: 0 }, { n: 1 }, { n: 99 }]);
    // The unknown keyId forced exactly one keyring refetch.
    expect(worker.requests.filter((r) => r.endsWith("/key")).length).toBe(2);
  });

  it("audit stream note: $-streams are readable plaintext without keys", async () => {
    const { shredCtx, worker, clockRef } = setup();
    await requestShred(shredCtx, "subject:user-9");
    const client = createStreamClient({
      baseUrl: BASE,
      auth: () => "test-token",
      fetchImpl: worker.handler,
      clock: () => clockRef.now,
    });
    // Served through the same page shape; no keyId, no key fetch.
    const audit = await collect(client.read(AUDIT_STREAM));
    expect(audit.map((e) => e.type)).toEqual(["ShredRequested"]);
    expect(worker.requests.filter((r) => r.endsWith("/key")).length).toBe(0);
  });
});
