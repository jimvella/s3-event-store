# s3-event-store — Design

A TypeScript library implementing event sourcing directly on Amazon S3, with no
secondary database. Correctness rests on two S3 guarantees:

1. **Strong read-after-write consistency** (all regions, since Dec 2020).
2. **Conditional writes** — `PutObject` with `If-None-Match: *` (create-only,
   GA Aug 2024) and `If-Match: <etag>` (compare-and-swap, Nov 2024).

## Goals / non-goals

**Goals**

- Append events to a named stream with optimistic concurrency (`expectedVersion`).
- Read a stream in order, as an async iterable.
- Atomic multi-event appends (all-or-nothing per commit).
- Snapshots.
- Pluggable serialization, metadata (correlation/causation IDs), upcasting hooks.
- Zero runtime dependencies; pluggable **storage drivers** — `@aws-sdk/client-s3`
  is an optional peer of the Node driver only, never a core dependency
  (see Serverless deployment).
- Runs in Cloudflare Workers (bundle-size and runtime constraints are
  first-class, not an afterthought).

**Non-goals (v1)**

- Totally-ordered global (all-streams) log. Per-stream order is guaranteed;
  cross-stream feeds are eventually ordered (see Subscriptions).
- Transactions spanning multiple streams.
- Sub-10ms latency. Standard S3 appends cost ~20–60 ms; an S3 Express One Zone
  backend is a later optimization.
- **Multi-region**: explicitly out of scope. Cross-region replication breaks
  conditional-write linearizability; document single-region-writer as a hard
  constraint.

**Supported backends**

Any S3-compatible store with strong consistency **and** conditional-write
support (`If-None-Match: *` / `If-Match` on PUT):

- Amazon S3 (primary target)
- **Cloudflare R2** — S3-compatible API, strongly consistent, supports
  conditional PUTs; works via `@aws-sdk/client-s3` with an endpoint override.
  Cheaper request pricing (Class A ~$4.50/M vs S3 PUT $5/M, Class B GETs
  ~$0.36/M) and zero egress fees make it attractive for read-heavy replays.
- MinIO (self-hosted / CI)

CI runs the same integration suite against all of them; backend quirks
(412 vs other status codes on precondition failure, LIST pagination edge
cases) get captured as conformance tests, not scattered `if` statements.

## Core mechanism: commit objects + create-only PUT

Each append writes **one immutable object per commit** (a commit = 1..N events
appended together). The object key encodes the commit's **base version**,
zero-padded so lexicographic order equals numeric order:

```
{prefix}/streams/{streamId}/e/{baseVersion:012d}.json
{prefix}/streams/{streamId}/s/{version:012d}.json      # snapshots
{prefix}/streams/{streamId}/c/{from:012d}-{to:012d}.json  # compacted chunks (see Compaction)
{prefix}/streams/{streamId}/head.json                  # non-authoritative hint
```

**Append protocol**

1. Resolve the stream head (current version) — see Head discovery below.
2. `PutObject` the commit object at key `e/{head+1}` with `If-None-Match: *`.
3. On HTTP 412: another writer won the race → raise `ConcurrencyError`
   (caller retries: re-read, re-decide, re-append).
4. Best-effort update `head.json` (plain PUT, last-writer-wins; it is only a
   hint, never trusted for correctness).

This is lock-free and correct: two writers targeting the same version can never
both succeed, and a commit object containing N events is atomic by construction
(one PUT). The conditional PUT *is* the concurrency check — `expectedVersion`
from the caller just selects which key we attempt.

**Commit object body** (JSON):

```jsonc
{
  "streamId": "order-123",
  "baseVersion": 5,
  "events": [
    {
      "id": "uuid",                // idempotency / dedupe key
      "type": "OrderShipped",
      "version": 5,
      "data": { ... },
      "meta": { "correlationId": "...", "causationId": "...", "ts": "ISO-8601" }
    }
  ],
  "committedAt": "ISO-8601"
}
```

Note: because commits can hold multiple events, event-version keys are **not
dense** (a 3-event commit at base 5 means the next key is 8). Version math must
come from reading, never from key arithmetic.

**Head discovery** — the one awkward part of S3 (you can't LIST descending):

- Fast path: read `head.json` hint, then `ListObjectsV2` with
  `StartAfter: e/{hintVersion}` to pick up anything newer. Usually 1 GET + 1
  short LIST.
- Cold path (no hint): paginate LIST over the `e/` prefix (1000 keys/page).
- If the conditional PUT 412s, the head moved — re-list from the hint and retry.

**Read protocol**: LIST the `e/` prefix (optionally `StartAfter` a snapshot
version), GET each commit object, yield events. Expose as
`AsyncIterable<EventEnvelope>` with internal prefetch (parallel GETs, bounded
concurrency) to hide S3 latency.

## Public API sketch

```ts
import { createEventStore, ConcurrencyError } from "s3-event-store";
import { awsSdkDriver } from "s3-event-store/drivers/aws-sdk";
// or: r2BindingDriver(env.EVENTS) / aws4fetchDriver({...}) in Workers

const store = createEventStore({
  driver: awsSdkDriver({ client: new S3Client({}), bucket: "my-events" }),
  prefix: "prod",                    // multi-tenancy / env isolation
  serializer: jsonSerializer(),      // pluggable (compression, encryption)
});

// Append with optimistic concurrency
const result = await store.append("order-123", [
  { type: "OrderShipped", data: { at: "..." } },
], { expectedVersion: 4 });          // or "any" | "noStream"
// result: { streamId, nextExpectedVersion, committedAt }

// Read
for await (const e of store.read("order-123", { fromVersion: 0 })) { ... }

// Snapshots
await store.writeSnapshot("order-123", { version: 40, state });
const snap = await store.readSnapshot("order-123");

// Optional higher-level aggregate helper (separate entry point)
const repo = createRepository(store, {
  evolve: (state, event) => ...,
  init: () => ...,
  snapshotEvery: 50,
});
const { state, version } = await repo.load("order-123");
await repo.save("order-123", version, newEvents);
```

**Error taxonomy**: `ConcurrencyError` (412 race), `StreamNotFoundError`,
`SerializationError`, plus passthrough of SDK errors. All typed, all exported.

`expectedVersion` semantics:
- number `n` → commit must land at version `n+1`
- `"noStream"` → stream must not exist (PUT at version 0)
- `"any"` → resolve head and retry the conditional PUT on 412 up to a bounded
  retry count (still atomic, just relaxed intent)

## Subscriptions / projections (v2)

Two composable pieces, shipped as a separate entry point (`s3-event-store/subscribe`):

1. **Catch-up reader**: given a checkpoint (`streamId → version` map, or a
   per-projection checkpoint object in S3 written with `If-Match` CAS), scan
   and replay. Works anywhere, purely poll-based.
2. **Live notifications**: S3 Event Notifications → EventBridge/SQS. The
   notification carries the object key (stream + base version); the consumer
   GETs the commit. Delivery is at-least-once and unordered **across** streams,
   ordered enough **within** a stream to trigger "read from checkpoint to head."

This gives correct projections without pretending S3 has a global log: the
notification is a doorbell, the stream read is the source of truth.

## Compaction protocol (phase 3)

Per-commit objects are right for the write path (durability + concurrency) but
make deep replays GET-heavy: 1M commits = 1M GETs. Compaction rewrites cold,
immutable commits into large **chunk objects** in the background, off every
critical path.

**Invariant: every event is readable from at least one object at every
instant.** All ordering below exists to preserve it.

Chunk boundaries are **fixed-width and deterministic** — chunk *k* covers
versions `[k·N, k·N+N−1]` (N = 1000 default), so any compactor computes the
same key for the same range without coordination.

**Compactor steps (per stream):**

1. LIST `c/` to find the last chunk; LIST `e/` for uncompacted commits.
2. Select the next chunk range only if **complete** (all versions present as
   commits) and **cold** — at least one full chunk behind the head (compact
   chunk *k−1* once chunk *k* has started). The lag is structural, not
   clock-based: the compacted range is always ≥ N commits behind the hot tail
   where appends and head-hint readers operate.
3. GET the range's commits, write one chunk object with `If-None-Match: *`.
4. Only after the chunk PUT succeeds, DELETE the source commit objects.
5. Sweep: delete any commit whose version is covered by an existing chunk
   (garbage from a crash between 3 and 4).

**Failure modes, all harmless:**

- *Crash between 3 and 4* → events exist in both chunk and commits.
  Duplication, not corruption; readers dedupe (below), the sweep cleans up.
- *Racing compactors* → deterministic boundaries mean identical chunk keys;
  `If-None-Match: *` picks one winner, the loser 412s and proceeds to deletes.
  Same lock-free primitive as the append path.
- *Reader racing the deletes* → a 404 on a commit the reader just LISTed means
  "compacted": re-check `c/` for a chunk covering that version. Chunk PUT
  strictly precedes deletes, so the data is always in one of the two places.

**Reader path with chunks:** LIST `c/` (few keys) → GET relevant chunks →
LIST `e/` with `StartAfter` the last chunk's end → GET tail commits, ignoring
any commit already covered by a chunk. A 1M-event replay becomes ~1k chunk
GETs plus a short tail.

**Cost:** per 1k-commit chunk ≈ 1k GETs + 1 PUT + 1k DELETEs. DELETEs are free
on S3 and R2, so compaction runs at roughly a tenth of the original write
cost, amortized in the background. On versioned buckets DELETE only adds a
delete marker — add a lifecycle rule expiring noncurrent versions (storage
cost only; readers only see current versions).

**Interactions:** compaction never touches keys at or above the head, so it
cannot conflict with appends — the write path doesn't know it exists. It
copies payload bytes verbatim, so it works on encrypted payloads without any
key-store access (keeps compactor IAM minimal). Snapshots are orthogonal.

**Scheduling — write/read-driven, no cron.** Sloppy triggering is safe by
construction (deterministic keys + `If-None-Match` + sweep make duplicate or
racing invocations harmless), which permits driving compaction from the paths
that already run:

- **Write-triggered (primary):** after a successful append, the writer checks
  the trigger condition with pure arithmetic (it knows the version it just
  wrote) and fires `ctx.waitUntil(compactStream(id))` — response returns
  immediately, ~1k I/O-bound GETs run in background time. The condition is
  **state-derived, not event-derived**: "a complete, uncompacted chunk exists
  behind the head" (tracked as a `compactedTo` watermark piggybacked on
  `head.json`, which the append path already writes) — so a died `waitUntil`
  task is retried by the next boundary-crossing append. Self-healing, no
  lost-trigger failure mode.
- **Read-triggered (backstop):** a reader that traverses complete-but-
  uncompacted chunks fires the same `waitUntil` on its way out. This covers
  quiet-but-read streams; a stream that is neither written nor read doesn't
  benefit from compaction, so no clock-based sweep is needed — coverage is
  complete without cron.
- **Queue variant (high-throughput opt-in):** R2 event notifications → Queue
  → consumer, for retries/DLQ/15-min windows off the request path entirely;
  reuses the notification pipe projections already need.

The library ships `compactStream(streamId)` + the cheap trigger check;
the deployment wires them into its append/read handlers (or the queue).

## Package & tooling

- **Name**: `s3-event-store` (or scoped `@<you>/…`) — check npm availability.
- **Build**: tsup → dual ESM/CJS + `.d.ts`; Node ≥ 20; `"sideEffects": false`.
- **Structure**: single package, subpath exports (`.`, `./subscribe`,
  `./repository`, `./client`, `./drivers/r2-binding`, `./drivers/aws-sdk`,
  `./drivers/aws4fetch`) rather than a monorepo — keep it one dependency;
  drivers and the browser client stay tree-shakeable for Workers bundles.
- **Tests**:
  - Unit: key codec, envelope schema, version math, error mapping (mock SDK).
  - Integration: MinIO or LocalStack via testcontainers — **verify the emulator
    honors `If-None-Match`/`If-Match` semantics** (recent MinIO and LocalStack
    do; pin versions). A small nightly job against real S3 **and real R2**
    (R2 has no local emulator with faithful conditional-write behavior;
    Miniflare/`wrangler dev` R2 simulation is not a conformance substitute)
    for the concurrency race test: spawn N concurrent appenders, assert
    exactly one winner per version, no gaps, no duplicates.
- **Lint/CI**: eslint + prettier, vitest, GitHub Actions, changesets for
  release management, provenance-signed npm publish.

## Cost & performance notes

- PUT/LIST ≈ $0.005 per 1k, GET ≈ $0.0004 per 1k. An aggregate with 1M commits
  costs ~$5 to write, storage is negligible. Reads dominated by request count →
  snapshots and commit batching matter more than payload size.
- Append latency ≈ 1 GET (hint) + 1 LIST + 1 PUT ≈ 60–150 ms on standard S3.
  Cache the head in-process per stream to make hot-stream appends ≈ 1 PUT.
- Long streams: reading 10k commits = 10k GETs. Mitigations, in order:
  snapshots (v1), background **compaction** into chunk objects (see Compaction
  protocol), and encouraging small aggregates (docs).
- **S3 Express One Zone backend** (later): single-digit-ms PUT/GET, supports
  conditional writes; trade-off is one-AZ durability and directory-bucket LIST
  semantics — offer as an opt-in backend behind the same interface.

## Erasure: crypto-shredding and field-level encryption

Physical deletion is a poor fit for an immutable store (versioned buckets,
backups, replicas, caches, and downstream projections all retain copies).
The supported erasure strategy is **crypto-shredding**: encrypt data client-side
with a per-stream or per-user data key before writing; erasure = deleting the
key. Every copy everywhere becomes unreadable at once.

This ships as an encrypting **serializer** (envelope encryption: data keys
wrapped by a master key, pluggable key-store interface — AWS KMS as one
implementation, not assumed, since R2 has no KMS). Two design notes that are
easy to get wrong:

- **Shred propagation delay = key-cache TTL.** Readers cache unwrapped data
  keys in-process; a shredded key remains usable until caches expire. Bound
  and document the TTL (GDPR's ~30-day window makes minutes/hours fine).
  Edge plaintext caches and client-delivered keys join the same budget —
  shred delay = the max of all such TTLs (see Serverless deployment).
- **Key loss is total data loss.** The key store is small but critical,
  stateful infrastructure — the one exception to "no database besides S3."
  (It can itself be S3 objects with versioning off, wrapped by a master key.)
  Per-key deletion should be soft-delete-then-hard-delete with a waiting
  period.

### What GDPR actually requires

GDPR is technology-neutral: Article 17 requires erasure "without undue delay"
(≈ one month, via Article 12(3)'s response deadline), and regulators (EDPB,
UK ICO, Danish DPA — the latter addressed crypto-shredding directly) accept
**rendering data permanently irrecoverable** as erasure. For crypto-shredding
to qualify, three obligations follow:

1. **Key destruction must be total** — no copy of the key may survive in any
   backup, replica, or cache. Demonstrating this is the compliance story; the
   key store's *configuration* matters more than its technology.
2. **State-of-the-art crypto** (Article 32) — AES-256-GCM qualifies; a
   breakable cipher means the data was never erased.
3. **Anything outside the encryption boundary is still personal data** —
   envelope metadata and stream IDs remain subject to erasure on their own.
   Never derive `streamId` from PII (no `user-jane@example.com`); use opaque
   IDs and document this loudly.

Key-cache TTL (above) bounds shred propagation; keep it well inside the
one-month window and document the number.

### Key store as a separate S3 bucket

A dedicated bucket for wrapped keys is acceptable to regulators and keeps the
"only S3" story intact — but it needs the **inverted configuration** from the
event bucket, which is precisely why it must be a separate bucket (these are
all bucket-level settings, on for events, off for keys):

- **Versioning off** — a versioned delete is just a delete marker; the key
  would remain readable via old versions.
- **No Object Lock** — erasure requires the ability to delete.
- **No replication** — S3 replication does not propagate permanent deletions;
  a replica would silently retain shredded keys forever.
- **No backups that outlive the erasure deadline** — either rely on S3's
  11-nines durability with no backups, or cap backup retention below ~30 days
  so shredded keys age out before the compliance clock expires. Accidental-
  deletion protection comes from the soft-delete waiting period, not backups.
- **Keys stored wrapped** by a master key (AWS KMS, or a configured secret on
  R2, which has no KMS) — a bucket leak then discloses nothing.
- **Strict IAM separation + audit logging** — ideally a separate account; the
  principal that reads event ciphertext must not enumerate keys, and key
  deletions need an audit trail you can show a regulator.

**Decision**: a dedicated bucket (as configured above) is the **default
key-store backend**. The library defines the `KeyStore` interface, ships the
S3-bucket implementation as the default, and verifies the settings above at
startup (fail fast if the key bucket has versioning or replication enabled).
KMS- or DynamoDB-backed stores remain possible via the interface but are not
shipped in v1.

### Key audit trail

Three layers with distinct jobs — don't collapse them:

| Layer | Role | Why |
|-------|------|-----|
| Mutable key objects (key bucket) | **State** | Keys must be truly deletable — the one requirement antithetical to event sourcing. A key "aggregate" would embed wrapped key material in immutable events; shredding would then mean rewriting history. Key state is therefore never event-sourced. |
| CloudTrail data events on the key bucket | **Evidence** | The regulator-facing trail must be independent of library correctness: captured by the platform regardless of what our code does, tamper-evident, IAM-separated by construction. A self-written trail, stored in a bucket where the compactor holds delete rights, proves little. (R2 caveat: Cloudflare audit logs are less granular — document this as a compliance gap to assess per deployment.) |
| `$system/key-audit` event stream | **Observability** | An ordinary stream in the event store recording key lifecycle (`KeyCreated`, `ShredRequested`, `ShredCompleted`) so existing read/projection tooling answers "all shreds last quarter" for free. A convenience projection of key lifecycle, not the compliance record. |

Rules for the audit stream:

1. **No key material, ever** — key IDs, timestamps, actor, reason only.
2. **Opaque subject identifiers only.** Proof-of-erasure records are lawful to
   retain (Article 17(3) legal-obligation carve-out) — but only if the subject
   reference is an opaque ID. A PII-bearing audit record would itself need
   shredding, recording which… is a circle.
3. **Plaintext payloads.** No PII → nothing to shred → no dependency on the
   key store it is auditing.

Shred is a dual write (delete key object + append audit events), so order it
intent-first: append `ShredRequested` → delete key → append `ShredCompleted`.
A crash leaves a visible dangling intent — never a silent, unaudited shred —
and a sweeper can resume incomplete shreds by scanning for unmatched intents.

### Whole-payload vs. field-level encryption

**Whole-payload** (encrypt the entire `data` blob; envelope metadata stays
plaintext) is the v1 choice:

- Simple and uniform — no schema knowledge needed; impossible to "forget a
  field."
- Fixed overhead: one nonce + auth tag (~28 bytes) per event; compress before
  encrypting (ciphertext doesn't compress).
- Cost: payloads are opaque to the S3 console, grep, Athena/S3 Select — all
  debugging goes through the library.

**Field-level** (encrypt only annotated personal-data fields) is a possible v2
refinement. Trade-offs:

| | Pro | Con |
|---|---|---|
| Observability | Non-PII fields (types, amounts, references) stay queryable/greppable | — |
| Shred surface | Only what compliance requires becomes unreadable; replays of shredded streams keep their business meaning | — |
| Schema coupling | — | Needs a per-event-type annotation mechanism (field paths / decorators); upcasters must understand encrypted fields; nested and dynamic shapes are awkward |
| Failure mode | — | Forgetting to annotate a field silently leaks plaintext PII forever (immutable!). Default must be fail-closed: unannotated event types get whole-payload encryption, opting *out* is explicit |
| Size overhead | — | ~28 bytes + base64 inflation (+33%) **per field** — many small fields cost proportionally more than one whole-payload envelope |
| Key mapping | Per-user keys become natural (each field tagged with its data subject) | Requires resolving field → data-subject → key at write time |
| Queryability temptation | — | Equality search over encrypted fields requires deterministic encryption, which leaks equality patterns; refuse this in the library, punt to projections |

**Decision**: the core library defines the serializer interface from day one;
the whole-payload encrypting serializer + key store ship in roadmap phase 3.
Field-level is phase 5 with fail-closed defaults, only if real demand shows
up — it's the kind of API that's expensive to get wrong.

## Serverless deployment (Workers + R2)

Reference deployment: Cloudflare Workers + R2 (Lambda + S3 is structurally
identical).

**Write path — always through a worker.** It authenticates, rehydrates the
aggregate (snapshot + tail), enforces invariants, performs the conditional
PUT. Nothing else holds write credentials to the event bucket.

**Read path — worker-gated too, but not for cost reasons.** The math: a
~50-commit stream read costs ~50 Class B GETs ≈ $18/M reads, paid regardless
of route; the worker adds one request ≈ $0.30/M (~2% overhead). The worker
actually *lowers* read cost: commit and chunk objects are immutable →
cache-forever in the edge cache (Cache API), so hot streams and repeated
replays stop hitting R2 entirely. Head discovery (one LIST) is the only
intrinsically uncacheable step. The real read-cost levers are snapshots,
compaction, and edge caching — not bypassing the worker. What forces the
worker is authorization (presigning is per-object and needs a trusted call
anyway) and decryption (clients never touch the key store).

Exception: trusted backend consumers (projection builders, analytics) may
read directly with scoped, ciphertext-only credentials — the worker gate is
for untrusted callers.

**Query shape decides what clients read:**

- *Stream-shaped* ("stream X's history") → serve raw events. Client keeps a
  version cursor; catch-up = 1 LIST from cursor + GETs of new commits, all
  edge-cache hits. Effectively a sync protocol with the stream as the wire
  format — the best-cacheable read path in the system.
- *Cross-stream* ("all open orders for customer X") → projections (D1, KV,
  materialized docs). A projection is a paid-for index: mutable and poorly
  cacheable — the price of the query shape, not a virtue. Few cross-stream
  queries ⇒ few projections.
- Caveat: serving raw events makes **event schemas a public API contract** —
  schema changes become breaking client changes, and upcasting must run
  client-side or be applied by the worker on egress. Adopt deliberately.

**Two read models for encrypted streams** (they compose per-caller):

| | A: worker decrypts | B: client decrypts |
|---|---|---|
| Flow | Worker GETs ciphertext, decrypts, returns plaintext | Worker authenticates and delivers the data key (bounded TTL); client fetches ciphertext (cache-forever) and decrypts locally |
| Infrastructure plaintext | Plaintext responses cacheable only with bounded TTL; cache key must include authorization scope | None, ever — edge holds only ciphertext, shred-compatible by construction |
| Access granularity | Arbitrary (per-event filtering possible) | Key = capability for the whole stream, past and future until rotation — requires **authorization boundary = encryption boundary** |
| Fit | Third parties, partial/filtered access | Per-user streams read by their own user (the dominant case) |

Notes on model B:

- A delivered key's TTL is an **authorization window, not disclosure
  control** — a misbehaving client can retain keys or plaintext forever (as
  it could retain plaintext under model A). GDPR erasure covers *our*
  systems; Article 17(2)'s inform-recipients duty can be driven from the
  `$system/key-audit` stream.
- Keep ciphertext access-controlled, not public-but-encrypted: a leaked key
  or a future cipher break must not expose a stream's history to the world.
  The auth check is the worker request already being paid.

**Shred propagation rule (unified):** shred delay = the **maximum of all
plaintext- and key-cache TTLs** — worker key caches, edge plaintext TTLs
(model A), client-delivered key TTLs (model B). One documented number, well
inside the ~30-day erasure window.

**Storage drivers.** Workers access R2 via native bindings
(`env.BUCKET.put(key, val, { onlyIf })`) — no request signing, no SDK weight.
The core therefore defines a minimal driver interface
(`get` / `putIfAbsent` / `putIfMatch` / `list` / `delete`) with three
implementations:

- `r2-binding` — native bindings; **conformance-test its `onlyIf`
  conditional-put semantics against the S3-API behavior in phase 1** before
  it becomes the default Workers driver.
- `aws-sdk` — `@aws-sdk/client-s3` v3, for Node; the SDK is an optional peer
  of this driver only.
- `aws4fetch` — lightweight SigV4 signer for calling S3/R2's HTTP API from
  Workers without the SDK (bundle-size limits rule the SDK out there).

## Component inventory & REST surface

Scoped to the initial application: **no projections/subscriptions** — all
client reads are stream-shaped. Cross-stream queries add them later (roadmap
phase 4); nothing below needs rework when they arrive.

**1. Core library (the npm package)** — the only shipped software. Storage
drivers, event store (append/read/snapshots), repository helper, serializers
(incl. the encrypting one), `KeyStore` interface + S3-bucket implementation,
`compactStream()` + trigger check, shred workflow helpers. A dependency,
never a service — it owns no process.

**2. Application worker (deployment code)** — thin HTTP handlers over the
library:

- Command endpoints: authenticate → rehydrate (snapshot + tail) → enforce
  invariants → `append` → `waitUntil` compaction trigger.
- Read endpoints: events (plaintext or ciphertext per read model), key
  delivery (model B).

The library ships no routes — auth and invariants are domain concerns.
(A Hono-style middleware helper is a possible later convenience export.)

**3. Background jobs** — with compaction write/read-driven, exactly one
clock-driven job remains: the **shred sweeper** (cron; scans
`$system/key-audit` for unmatched `ShredRequested` intents and resumes them).
It legitimately needs a clock: a crashed shred has no guaranteed future write
or read to revisit it, and erasure correctness has a deadline.

**4. Client** —

- *Model A:* nothing. Plain JSON over HTTP; immutable event responses get
  browser/edge caching free via `ETag` + `Cache-Control: immutable`. Any
  `fetch` loop with a cursor is a complete client.
- *Model B:* thin browser SDK as subpath export **`./client`** — zero
  dependencies, WebCrypto only. Responsibilities: fetch/cache the data key
  respecting its TTL, AES-GCM decrypt (fail closed — a shredded stream
  presents as decryption failure), cursor iteration, optional fold-to-state
  helper. It exists for crypto correctness, not protocol: the REST API
  remains the contract and the SDK is its reference consumer.

**5. Infrastructure (config, not code):** event bucket; key bucket (inverted
config, startup-verified); edge cache; audit logging (CloudTrail /
Cloudflare audit logs). R2 event notifications + Queue only if the queue
compaction variant is adopted.

**REST surface** (shapes, not prescriptions — most apps expose domain
commands rather than raw append):

```
POST /streams/{id}/append          command endpoint; validated events in, AppendResult out
GET  /streams/{id}/events?from=v   events (A) or ciphertext envelopes (B), + next cursor
GET  /streams/{id}/head            current version — the poll target
GET  /streams/{id}/key             wrapped data key + TTL          (model B only)
```

Live updates = polling `GET head` (one cheap LIST behind it, short-TTL
cacheable). SSE/WebSocket via Durable Objects is an optional deployment
upgrade the library stays out of.

### Wire format

Plain JSON, one page = one object. Atom — the historical answer for HTTP
event feeds (EventStoreDB, RFC 5005 archived feeds) — is overkill: its two
load-bearing ideas survive here without the XML.

```jsonc
{
  "streamId": "order-123",
  "from": 0,
  "to": 999,
  "complete": true,          // full page ⇒ served with Cache-Control: immutable
  "events": [
    { "id": "…", "type": "OrderShipped", "version": 5,
      "data": { … },         // model A — or "ciphertext": "base64…" in model B
      "meta": { "ts": "…", "correlationId": "…" } }
  ],
  "next": "/streams/order-123/events?from=1000",  // absent ⇒ at head
  "prev": null                                    // page k−1; null on page 0
}
```

- **`complete: true`** is the machine-readable immutability promise (the
  RFC 5005 archived-page idea): the client SDK caches complete pages locally
  forever and only re-polls the incomplete one.
- **`next`** is the link-relation idea: clients follow links, never compute
  URLs. This is load-bearing for caching, not ceremony: cache keys are URLs,
  and the immutable-page strategy only pays if every client requests
  identical, chunk-aligned page URLs. Link-followers can't mint bespoke
  cache keys; URL-computers would collapse the edge hit rate (and couple to
  page size N, an implementation detail).
- **`prev`** (RFC 5005's `prev-archive`) enables newest→oldest traversal.
  Page boundaries are deterministic, so page *k*'s `prev` is always page
  *k−1* — a complete page's envelope *including its links* stays immutable
  and cache-forever (see Reverse and time-range reads).
- **Redirect-to-canonical**: the one URL a client must construct is a cold
  resume from a stored cursor (`?from=372`, mid-page). The read handler
  responds `308` to the canonical page containing that version (`?from=0`
  for page 0–999); the client SDK follows and skips locally to 372. One
  redirect converts any entry point into the canonical URL space; everything
  after is link-following against permanent cache entries.
- **Envelope field names are CloudEvents-compatible** (`id`, `type`, `time`,
  `data`) — costs nothing now, makes a future CloudEvents egress adapter
  (EventBridge/Knative-style consumers) a mapping instead of a migration.
  Full CloudEvents adoption (per-event `specversion`, `source`) is skipped
  as ceremony for a first-party API.
- **Model B**: ciphertext travels as base64 in the JSON (~33% inflation on
  the encrypted portion; those pages won't gzip — ciphertext is
  incompressible, hence the compress-before-encrypt rule).

Escape hatches, documented but not default: **NDJSON** (one event per line,
pagination via `Link` headers) if pages grow enough that streaming-parse
matters; a **binary content-type** per page if base64 inflation ever matters.
At N = 1000 events per page, neither is needed.

### Compaction and the API

The REST contract is defined over **logical version ranges**; compaction is a
physical relayout invisible to it. Compaction is content-preserving (version
372 has identical bytes before and after, just fetched from a chunk instead
of a commit object), so nothing the API ever returned becomes untrue — and no
cached response is ever invalidated by compaction. Rules that make this hold:

1. **Cursors are version numbers, never storage references.** A client can
   hold a cursor across a full relayout and `?from=v` still means the same
   thing. A cursor that leaked an object key or ETag would break on
   compaction. Load-bearing rule.
2. **The read handler absorbs storage topology.** It runs the chunk-aware
   read path (LIST `c/` → chunks → `e/` tail → dedupe → 404-race fallback)
   internally; responses never distinguish chunk-sourced from commit-sourced
   events, and a mid-request race with a running compactor resolves as
   retry latency, not an error.
3. **Page boundaries are a deterministic function of the version, aligned to
   chunk size N.** Two response classes fall out:
   - *Full pages* (ending before head): immutable forever →
     `Cache-Control: immutable`, permanent edge-cache entries. After
     compaction a cache miss costs exactly one chunk GET — the REST page is
     a 1:1 logical proxy of a chunk.
   - *The final partial page* (containing head): short TTL / no-store, same
     as `GET head`.

Net effect: a long replay streams almost entirely from the edge cache
regardless of whether compaction has run; compaction only changes what a
cache miss costs (~1 chunk GET vs ~1k commit GETs). The read-triggered
compaction backstop lives in this same handler — the reader that pays the
slow uncompacted path fires `waitUntil(compactStream(id))` on the way out,
so the API is what heals uncompacted streams. Cache, API, and storage
converge on the same unit: the chunk.

### Reverse and time-range reads

Governing rule: **version space is the only physical coordinate.** Recency
and time are *resolved to* version space, never made a second key scheme —
no new storage layout, no new invalidation story.

**Newest → oldest** — purely an envelope + SDK feature: client hits
`GET head` (or the incomplete page), walks `prev` links through permanent
edge-cache entries, and the SDK reverses each page's events locally (storage
order stays ascending). Cost identical to forward replay — same canonical
URLs, same cache entries, opposite order. "Most recent 20 events" is at most
two page fetches (partial head page + one complete page). This serves
activity feeds and audit views.

**Time-range** — `?since=T&until=T'` is sugar that resolves each bound to a
version via **binary search over canonical pages** (~log₂(head/N) fetches,
each an edge-cache hit or a permanent new entry — ~10 for a 1M-event
stream), then serves the same canonical pages; the client trims edge events.
Timestamps stay data; versions stay coordinates.

- *Monotonicity invariant:* binary search requires `committedAt` to never
  decrease along a stream. The writer rehydrates before appending anyway, so
  it clamps: `committedAt = max(now, prev.committedAt)`. One line in the
  append path; the invariant becomes structural, not clock-dependent.
- *Per-stream only:* "all events across the system in [T₁, T₂]" is the
  global-feed problem this design deliberately excludes — that's
  subscriptions/projections (phase 4). A time query without a `streamId` has
  no home here; say so in the docs.
- *Deferred upgrade path:* chunk objects (or a tiny per-stream index written
  during compaction) could record `[firstTime, lastTime]` for O(1)
  resolution. Binary search over cached pages is cheap enough that a real
  workload must justify this first.

## Roadmap

| Phase | Scope |
|-------|-------|
| 0 | Scaffold: tsup, vitest, CI, key codec + envelope with 100% unit coverage |
| 1 | Storage-driver interface + `r2-binding`/`aws-sdk`/`aws4fetch` drivers (incl. `onlyIf` conformance tests); `append`/`read` + optimistic concurrency + error taxonomy + integration tests |
| 2 | Snapshots, head hints, in-process head cache, repository helper |
| 3 | Compaction, S3 Express backend, compression + whole-payload crypto-shredding serializer with pluggable key store, browser client SDK (`./client`) |
| 4 | Subscriptions: checkpointed catch-up + EventBridge/SQS adapter |
| 5 | Field-level encryption (fail-closed defaults), if demand warrants |

## Open questions

1. **Commit granularity**: always allow multi-event commits (chosen above), or
   force one-event-per-object for dense keys? Multi-event is the right default
   — atomic use-case appends are the whole point of `expectedVersion`.
2. ~~Key-store choice~~ **Resolved**: a dedicated S3 bucket with wrapped keys
   is the default backend (see Erasure section); alternatives via the
   `KeyStore` interface.
