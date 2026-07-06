# s3-event-store

Event sourcing directly on Amazon S3 — no database, no server, no resident
process. A TypeScript library with zero runtime dependencies that runs
anywhere from Node to Cloudflare Workers.

**See it running:** [s3-event-store-cloudflare-demo](https://github.com/jimvella/s3-event-store-cloudflare-demo) — a full Cloudflare Workers + R2 example.

## Preamble

Why does this library exist? Because I'm cheap. Nothing beats S3/R2 on cost, so a serverless app built on short-lived workers with nothing but object storage for persistence is appealing economically.

Event sourcing with archive feeds is a great way to propagate data in multi-user apps, and it became a practical option when S3 introduced conditional writes at the end of 2024. There are pitfalls, so this library codifies the approach as a hedge against them.

Using only object storage does mean tradeoffs, particularly around latency — for example, accepting a few seconds of polling lag to avoid the cost and complexity of Cloudflare Durable Objects, HTTP long-polling, or websockets.

Built with my initial Fable credits.

## Introduction

`s3-event-store` implements a full event store — append with optimistic
concurrency, ordered replay, atomic multi-event commits — using nothing but
an S3-compatible bucket. Correctness rests on two guarantees modern object
stores provide:

1. **Strong read-after-write consistency** (S3, all regions, since 2020).
2. **Conditional writes** — create-only PUT (`If-None-Match: *`) and
   compare-and-swap (`If-Match`).

Every append is one immutable object written with a conditional PUT, so two
writers targeting the same stream version can never both succeed — the
bucket itself is the concurrency control. There is no broker, no lock
service, and no metadata database to operate, scale, or keep consistent
with the log.

Because commits are immutable objects with deterministic keys, replays are
edge-cacheable forever: a long stream read can be served almost entirely
from CDN cache. Background compaction rewrites cold commits into chunk
objects so deep replays stay cheap, and client-side encryption with
crypto-shredding provides GDPR erasure on storage that can never be
rewritten.

**Supported backends:** Amazon S3, Cloudflare R2, MinIO — anything
S3-compatible with strong consistency and conditional-write support.

**What it is not:** a global totally-ordered log, a cross-stream
transaction system, or a sub-10ms store. Per-stream order is guaranteed;
cross-stream feeds (subscriptions/projections) are unplanned future work.
Single-region only — cross-region replication breaks conditional-write
linearizability.

## Software components

The npm package is the only shipped software — it is a dependency, never a
service. Everything else is deployment code you own.

| Component | What it is |
|---|---|
| **Core library** (`@jimvella/s3-event-store`) | Event store (`append`/`read`), storage-driver interface, error taxonomy, serializers (including the encrypting one), `KeyStore` interface, `compactStream()` + trigger check, shred workflow helpers |
| **Storage drivers** (`./drivers/*`) | `aws-sdk` (Node, `@aws-sdk/client-s3` as optional peer), `r2-binding` (native Cloudflare Workers bindings — no SDK weight), `aws4fetch` (lightweight SigV4 for calling S3/R2 from Workers) |
| **Browser client SDK** (`./client`) | Zero-dependency, WebCrypto-only reader for encrypted streams: keyring fetch/cache, per-event key selection, AES-GCM decrypt, cursor iteration, upcasting |
| **Your application worker** | Thin HTTP handlers over the library: authenticate → rehydrate → enforce invariants → append. The library ships no routes — auth and invariants are domain concerns |
| **Shred sweeper** (cron, encrypted stores only) | The one clock-driven job: executes hard deletes after the soft-delete waiting period |

For unencrypted streams read over HTTP, there is deliberately **no client
component**: responses are plain JSON with immutable-page caching, and any
`fetch` loop with a cursor is a complete client.

A repository (aggregate load/save) helper and subscriptions/projections are
deliberately out of scope — sketched as unplanned future work in
[DESIGN.md](DESIGN.md#future-work-unplanned).

## Installation and setup

```sh
npm install @jimvella/s3-event-store

# Node deployments using the AWS SDK driver also need the optional peer:
npm install @aws-sdk/client-s3
```

Requires Node ≥ 20 (or Cloudflare Workers). Ships dual ESM/CJS with types;
all entry points are tree-shakeable for Workers bundle limits.

### Bucket setup

One bucket (or a prefix within one) is all the infrastructure the store
needs:

- **Amazon S3** — works out of the box. On versioned buckets, add a
  lifecycle rule expiring noncurrent versions (compaction DELETEs otherwise
  accumulate delete markers).
- **Cloudflare R2** — S3-compatible conditional PUTs; use the
  `r2-binding` driver in Workers or `aws-sdk` with an endpoint override
  elsewhere. Zero egress fees make it attractive for read-heavy replays.
- **MinIO** — recent versions honor `If-None-Match`/`If-Match`; pin your
  version and run the conformance suite in CI.

The library owns only the `{prefix}/streams/` subtree — unrelated
application objects can live as siblings under the same prefix. Two rules:
nothing else may write below `{prefix}/streams/`, and no PII in stream IDs
or prefixes (identifiers live forever in keys, outside any encryption
boundary).

Minimal IAM: `GetObject`, `PutObject`, `DeleteObject`, `ListBucket` scoped
to the prefix. Writers need conditional-PUT support; nothing needs
`DeleteBucket` or bucket-level configuration rights.

## Getting started

### Create a store and append events

```ts
import { createEventStore, ConcurrencyError } from "@jimvella/s3-event-store";
import { awsSdkDriver } from "@jimvella/s3-event-store/drivers/aws-sdk";
import { S3Client } from "@aws-sdk/client-s3";

const store = createEventStore({
  driver: awsSdkDriver({ client: new S3Client({}), bucket: "my-events" }),
  prefix: "prod",                  // env isolation / multi-tenancy
});

// First event in a new stream
await store.append("order-123", [
  { type: "OrderPlaced", data: { sku: "widget", qty: 2 } },
], { expectedVersion: "noStream" });

// Append with optimistic concurrency — atomic, all-or-nothing
const result = await store.append("order-123", [
  { type: "PaymentReceived", data: { amount: 4200 } },
  { type: "OrderShipped",   data: { at: "2026-07-04T12:00:00Z" } },
], { expectedVersion: 0 });

console.log(result.nextExpectedVersion); // 2
```

`expectedVersion` is `"noStream"` (stream must not exist), a number
(commit must land immediately after that version), or `"any"` (resolve the
head and retry bounded times on conflict).

### Handle concurrency conflicts

A conflict means another writer got there first — re-read, re-decide,
re-append:

```ts
try {
  await store.append(streamId, events, { expectedVersion: version });
} catch (err) {
  if (err instanceof ConcurrencyError) {
    // reload state from the stream and retry the command
  } else {
    throw err;
  }
}
```

### Read a stream

```ts
for await (const event of store.read("order-123", { fromVersion: 0 })) {
  console.log(event.version, event.type, event.data, event.meta);
}
```

Reads are an `AsyncIterable` with internal bounded-concurrency prefetch to
hide S3 latency. Order within a stream is guaranteed.

### Cloudflare Workers + R2

```ts
import { createEventStore } from "@jimvella/s3-event-store";
import { r2BindingDriver } from "@jimvella/s3-event-store/drivers/r2-binding";

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const store = createEventStore({
      driver: r2BindingDriver(env.EVENTS),
      prefix: "app-x",
    });

    const result = await store.append(streamId, events, { expectedVersion });

    // Write-driven compaction: cheap arithmetic check, work off the request path
    if (store.shouldCompact(streamId, result)) {
      ctx.waitUntil(store.compactStream(streamId));
    }

    return Response.json(result);
  },
};
```

No SDK, no request signing — the native R2 binding keeps the bundle small
and the store runs entirely within a Worker request.

### Reading over HTTP — no SDK required

If your worker exposes the stream-shaped REST surface, plain-JSON
(unencrypted) streams need no client library at all:

```ts
let url = `/app-x/orders/streams/order-123/events?from=0`;
while (url) {
  const page = await (await fetch(url)).json();
  for (const event of page.events) apply(event);
  url = page.next;               // follow links, never compute URLs
}
```

Complete pages are served with `Cache-Control: immutable` — a replay
streams from the edge cache, and only the final partial page ever re-polls.

### Serving the feed — the worker side

The library ships the page/head/append helpers; the worker owns routes,
auth, and headers. `pageSize` defaults to the store's `chunkSize`, so page
URLs are chunk-aligned (identical across all clients — the edge-cache hit
rate depends on it):

```ts
import {
  readPage, toWireFeed, canonicalFrom,
  readHead, toWireHead,
  idempotentAppend,
} from "@jimvella/s3-event-store";

const hrefFor = (from: number) => `/app-x/orders/streams/${id}/events?from=${from}`;

// GET …/events?from=v — one page; redirect mid-page cursors to canonical
const page = await readPage(store, id, { from });
if (from !== page.from) return Response.redirect(hrefFor(page.from), 308);
return Response.json(toWireFeed(page, hrefFor), {
  headers: {
    "Cache-Control": page.complete
      ? "public, max-age=31536000, immutable" // frozen forever, incl. its links
      : "no-store",                           // still-growing head page
  },
});

// GET …/head — the poll target; ETag makes an unchanged poll a free 304
const head = await readHead(store, id);
if (request.headers.get("If-None-Match") === head.etag) {
  return new Response(null, { status: 304, headers: { ETag: head.etag } });
}
return Response.json(toWireHead(head, hrefFor), {
  headers: { ETag: head.etag, "Cache-Control": "no-cache" },
});

// POST …/append — raw ingress (only if you expose append instead of domain
// commands). Clients send stable event ids and an explicit expectedVersion;
// a retried request that already committed reports success, no duplicate.
const r = await idempotentAppend(store, id, events, { expectedVersion });
return Response.json(r, { status: r.outcome === "appended" ? 201 : 200 });
```

A poll loop is then: `GET head` with `If-None-Match` → on `304` sleep and
retry; on `200` follow the `head` link and read forward.

### Encrypted streams in the browser

For client-decrypted (model B) streams, the browser SDK handles keyring
delivery, per-event key selection by `keyId`, and AES-GCM decryption —
WebCrypto only, zero dependencies:

```ts
import { createStreamClient } from "@jimvella/s3-event-store/client";

const client = createStreamClient({
  baseUrl: "https://api.example.com/app-x/orders",
  auth: () => getAccessToken(),
});

for await (const event of client.read("order-123", { from: 0 })) {
  // ciphertext fetched from permanent edge cache, decrypted locally
  render(event);
}
```

Decryption fails closed: a crypto-shredded (GDPR-erased) stream presents
as decryption failure, never as stale plaintext.

## Design documents

The full specification — append protocol, head discovery, compaction,
failure-mode analysis, REST surface — is in [DESIGN.md](DESIGN.md).
Client-side encryption, key management, rotation, and crypto-shredding
erasure are specified in [KEYS_DESIGN.md](KEYS_DESIGN.md).

## Status

Pre-release. Implemented so far (`src/`): the core append/read protocol,
head discovery with `head.json` hints and the in-process head cache,
compaction (`compactStream` + sweep) with the write-driven trigger, the
three storage drivers (`aws-sdk`, `r2-binding`, `aws4fetch`) with a shared
conformance suite (fake backends always; real endpoints via
`conformance.local.json`), and the encryption layer per
[KEYS_DESIGN.md](KEYS_DESIGN.md): AES-256-GCM whole-payload encrypting
serializer (compress-then-encrypt, random nonces, `keyId` envelopes), the
`KeyStore` interface with the S3-key-bucket implementation (wrapped keys,
generational rotation, tombstone-authoritative reads, TTL-bounded caches,
startup config verification), and the crypto-shredding workflow
(`requestShred`/`cancelShred`/`sweepShreds`: intent-first audit trail on
`$system.key-audit`, tombstone CAS state machine, soft-delete waiting
period, sweeper-executed hard delete). The deterministic simulation harness
(`sim/`; see [SIMULATOR_PLAN.md](SIMULATOR_PLAN.md)) checks the full
invariant set, including no-forged-heads and every-committed-event-readable
after every mutation. Also done: the worker-facing
HTTP surface (`src/http.ts`: `readPage`/`toWireFeed` chunk-aligned pages
with the `complete ⇔ next` immutability invariant, `readHead`/`toWireHead`
poll target with version-derived ETag for `If-None-Match` → 304, and
`idempotentAppend` — retry-safe raw ingress deduping by event id against
the exact target window); the browser client SDK (`./client` —
keyring fetch/TTLs, local AES-GCM decryption, link-following with
permanent complete-page caching, upcasters, fold) and the build plumbing
(`npm run build`: tsup dual ESM/CJS + `.d.ts` for all five subpath
exports; SDKs as optional peers). The package stays `private: true` until
a publish decision. Still ahead per [DESIGN.md](DESIGN.md#roadmap):
MinIO/testcontainers conformance (deferred — real S3 and R2 are covered by
the file-configured run below), an in-Workers conformance run for the
r2-binding driver's `onlyIf` semantics, and demand-gated field-level
encryption.

### Testing

- `npm test` — the full deterministic suite: pinned regression seed
  ranges, scripted race scenarios, driver conformance against in-memory
  fakes. No network, no credentials.
- `npm run sweep [-- <count> [<startSeed>]]` — manual randomized
  exploration (default 2000 seeds; ~10k seeds ≈ 17 s). Prints a
  `reproduce with:` line; on failure, rerun the named seed, then distill
  the schedule into a scripted regression.
- **Real-backend conformance** (S3, R2, MinIO, …): the same conformance
  suite the fakes pass runs against live endpoints configured in a
  gitignored `conformance.local.json` at the repo root — one target
  object, or an array to test several providers in one run:

  ```json
  [
    {
      "endpoint": "https://s3.us-east-1.amazonaws.com",
      "bucket": "s3-eventsourcing-test",
      "region": "us-east-1",
      "accessKey": "AKIA…",
      "secretKey": "…"
    },
    {
      "endpoint": "https://<account-id>.r2.cloudflarestorage.com",
      "bucket": "s3ev-conformance",
      "region": "auto",
      "accessKey": "…",
      "secretKey": "…"
    }
  ]
  ```

  Then: `npx vitest run sim/conformance.test.ts`. (The
  `S3EV_CONFORMANCE_*` environment variables still work as an
  alternative/override for one-off runs.) Use dedicated buckets, never
  production ones, with credentials scoped to that bucket only. Each run
  writes ~30 small objects under a fresh `conf-<timestamp>/` prefix; add a
  lifecycle rule expiring the `conf-` prefix after a day to keep the
  bucket clean.
