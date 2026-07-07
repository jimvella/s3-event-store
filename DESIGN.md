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
- Pluggable serialization, metadata (correlation/causation IDs), upcasting hooks.
- Pluggable **storage strategy** — a mutable-tail default optimized for short
  poll-read streams, plus an immutable-object + compaction alternative for
  large-N replay-heavy workloads, selectable per prefix behind one seam
  (see Storage strategies).
- Zero runtime dependencies; pluggable **storage drivers** — `@aws-sdk/client-s3`
  is an optional peer of the Node driver only, never a core dependency
  (see Serverless deployment).
- Runs in Cloudflare Workers (bundle-size and runtime constraints are
  first-class, not an afterthought).

**Non-goals (v1)**

- Totally-ordered global (all-streams) log. Per-stream order is guaranteed;
  cross-stream feeds are eventually ordered (see Future work).
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


## Storage strategies

The store is parameterized by a **storage strategy** — the component that owns
how a commit is appended, how the head is resolved, and how a version range is
materialized for the reader. Two strategies ship, selectable per store (per
prefix); everything above the strategy (the REST/wire surface, cursors, client
SDK) and below it (the storage-driver contract) is identical either way:

- **MutableTail (default)** — the live tail of a stream is a single mutable
  chunk object updated by compare-and-swap. No per-commit objects, no
  background compaction, no `head.json`, no deletes on the write path.
  Specified below.
- **ImmutableChunk** — one immutable object per commit plus background
  compaction into chunk objects. The original design; selectable for large-N
  replay-heavy streams and maximal blast-radius isolation. Specified in
  [DESIGN_IMMUTABLE_CHUNK.md](DESIGN_IMMUTABLE_CHUNK.md); the tradeoff analysis
  motivating both is in
  [DESIGN_MUTABLE_TAIL_PROPOSAL.md](DESIGN_MUTABLE_TAIL_PROPOSAL.md).

Both rest on the same two S3 guarantees and use the same driver primitives
(`putIfAbsent` = create-only PUT, `putIfMatch` = compare-and-swap); they differ
only in how they arrange objects.

## Core mechanism: the mutable tail

Each stream's history is a sequence of **chunk objects**, each holding a run of
commits (a commit = 1..N events appended atomically). The key encodes the
chunk's **base version** — the base of its first commit — zero-padded so
lexicographic order equals numeric order:

```
{prefix}/streams/{streamId}/c/{chunkBase:012d}.json
```

Two terms, kept distinct throughout: the **head** is the stream's current
version — a number, the poll target; the **tail** is the physical last chunk
object, which holds the head. The last chunk is thus the **live tail**: an
append mutates it in place by compare-and-swap, while earlier chunks are sealed
and permanent. There is no `e/` tree and no `head.json` — the head is read from
the tail chunk's body, and nothing is ever deleted on the write path.

The library owns only the `streams/` subtree under `{prefix}` — every LIST it
issues is scoped to `{prefix}/streams/{streamId}/c/` — so unrelated application
objects can live as siblings under the same prefix with zero interaction. Three
ownership rules, unchanged from the shared contract: nothing else may write
below `{prefix}/streams/`; `streamId` contains no slashes (namespacing lives in
the prefix, identity in the id); and stream IDs beginning with `$` are reserved
for library-defined system streams (currently
[KEYS_DESIGN.md](KEYS_DESIGN.md)'s `$system.key-audit`), so the external surface
must reject `$`-IDs (see Prefix routing). Prefixes follow the same no-PII rule
as stream IDs (see the Encryption & erasure contract).

**Append protocol**

1. **Resolve the tail.** LIST `c/` (short — one page unless the stream is very
   long) and GET the last chunk (body + ETag); the head is the last commit's
   base plus its event count − 1. A hot writer caches the tail bytes and ETag
   in-process and skips the LIST + GET.
2. **Reject stale intent.** If `expectedVersion` doesn't match the resolved
   head, raise `ConcurrencyError` before any write. `"noStream"` is verified by
   the `c/` LIST returning nothing; a number `n` requires the resolved head to
   equal `n`.
3. **Append, or roll.** Read the caps `n` and `byteCap` from the tail body.
   - *Tail not yet full* (fewer than `n` commits **and** under `byteCap`
     bytes): append the commit to the chunk locally and PUT it back with
     `If-Match: <etag>` (compare-and-swap).
   - *Tail already full:* create a new chunk keyed by the incoming commit's
     base with `If-None-Match: *`. The previous chunk's final state is already
     its sealed, permanent form — no rewrite, no rename, no copy.

   Fullness is judged on the tail **as read**, never as-it-would-be-after-this-
   commit, so every contender that resolved the same head reaches the same roll
   verdict and targets the same key (see Per-stream N).
4. **On conflict** — a 412 on the CAS, or `exists` on the create — the tail
   moved under us: re-resolve and retry. A lost-response retry recognizes its
   own `commitId` already present in the re-read tail and reports success
   rather than double-appending.

This is lock-free and correct: two writers that resolved the same head hold the
same tail ETag (or both target the same create-only next-chunk key), so the
conditional write lets exactly one win, and a commit of N events is atomic by
construction (one PUT). **The ETag chain *is* the version check.** There is no
freed-key hazard class — the append protocol never deletes, so a create-only
PUT's success is unambiguous and the worst read race is observing a tail that
has since grown by a benign superset. (The ImmutableChunk strategy, which
compacts by copy-then-delete, must instead run an authoritative head gate, a
post-PUT chunk check, pinned anchors, and a sealed-bucket read rule to manage
exactly this hazard; see
[DESIGN_MUTABLE_TAIL_PROPOSAL.md](DESIGN_MUTABLE_TAIL_PROPOSAL.md) for the full
comparison.)

**Chunk object body** (JSON):

```jsonc
{
  "streamId": "order-123",
  "chunkBase": 20,               // base version of the first commit; equals this object's key
  "n": 20,                       // roll policy: max commits per chunk (per-stream; see Per-stream N)
  "byteCap": 262144,             // roll policy: max bytes before rolling
  "commits": [
    {
      "commitId": "uuid",        // writer-generated; disambiguates retried conditional PUTs
      "baseVersion": 20,
      "events": [
        {
          "id": "uuid",          // idempotency / dedupe key; library-generated unless supplied
          "type": "OrderShipped",
          "version": 20,
          "data": { /* ... */ },
          "meta": { "correlationId": "...", "causationId": "...", "ts": "ISO-8601" }
        }
      ],
      "committedAt": "ISO-8601"
    }
    // ... up to n commits, appended by successive CAS PUTs ...
  ]
}
```

The roll policy (`n`, `byteCap`) travels **in the body**, so every appender
reads the same caps the CAS is conditioned on (see Per-stream N). Because
commits can hold multiple events, event-version math comes from reading the
chunk body, never from key arithmetic.

**Per-stream (and evolving) N.** Each chunk is keyed by its first commit's
*exact* base — not by an `N`-multiple — so chunk boundaries are explicit
objects, enumerable by LISTing `c/`. Three consequences:

- **Reads need N not at all.** To locate version *v*, LIST `c/` and GET the
  chunk with the greatest base ≤ *v*; the read path never divides by N and
  needs no per-stream config lookup.
- **N is a per-stream roll policy, carried in the tail.** The only actor that
  consults N is an appender at the roll decision, reading `n`/`byteCap` from the
  tail body it just GET'd. Contenders on the same ETag see the same caps → the
  same roll verdict → the same next key: determinism survives *because* the
  policy travels with the bytes the CAS is conditioned on. N must never come
  from ambient per-writer config — a writer on a stale value could target a
  different next-chunk key and fork the stream.
- **N can evolve within a stream.** Sealed chunks keep the size they had; only
  the live tail's cap changes — no retroactive rewrite, no alignment to
  relitigate.

N defaults to 20, tuned for short poll-read streams (full rationale, cost model,
and per-workload guidance: [CHUNK_SIZING_GUIDE.md](CHUNK_SIZING_GUIDE.md)). A
commit holds at most `n` events (enforced at append), which keeps chunk
boundaries dense. `byteCap` bounds the worst-case tail: the writer holds and
re-uploads up to `byteCap × n` bytes per append, so `byteCap × n` must fit its
memory budget — and large N is exactly where the mutable tail stops fitting and
the ImmutableChunk strategy earns its place (see Storage strategies).

**Configuring N.** The constraint above dictates the API: since the roll
verdict must read the value *in the tail body*, the caller can only ever
**inject** a value when a chunk is minted — at stream creation (the first
chunk) or at a roll (a new chunk) — never as a per-append input that could
override the active window. The surface:

```ts
const store = createEventStore({
  driver, prefix,
  strategy: mutableTail({
    chunkSize: 20,                         // store default N
    byteCap: 256 * 1024,                   // store default byte cap
    // optional per-stream resolver, consulted ONLY when a chunk is minted:
    policyFor: (streamId) =>
      streamId.startsWith("audit-") ? { chunkSize: 200 } : undefined,
  }),
});

// creation-time override, honored only on the stream-creating append:
await store.append(id, events, { expectedVersion: "noStream", chunkSize: 50 });
```

- **Precedence at a mint:** creation-time override → `policyFor(streamId)` →
  store default. The resolved `{ chunkSize, byteCap }` is stamped into the new
  chunk; from then on the *body* governs every verdict for that window.
- **Config seeds a mint; the body feeds the verdict.** `policyFor` is shared,
  deterministic config, so it does not violate the "never from ambient config"
  rule: it only *seeds* the stamped value. Two writers holding different
  resolvers on a contended mint still target the same key (the incoming base),
  so the create-only PUT yields one winner — a value race, never a fork.
- **Evolving N** falls out: `policyFor` is consulted on *every* mint (returning
  `undefined` ⇒ propagate the tail's current policy). Change the resolver and
  the next roll stamps the new value; the still-live chunk finishes its window
  under the old one. Deterministic throughout, no recompaction.
- **Reads take nothing** — the strategy is N-agnostic on read (boundaries are
  explicit); `chunkSize` matters only to writers and to `pageSize`'s default.

**Head discovery.** LIST `c/`, take the greatest base — that chunk is the tail —
and GET it pinned to its listed ETag; derive the head from its body (last
commit's base + event count − 1). Usually one short LIST + one GET, or zero
requests for a hot writer serving from its in-process tail cache. There is no
`head.json` hint to corroborate, no fabricated-hint attack surface, and no
cold/fast path split: the tail is always exactly one object.

**Read protocol.** LIST `c/` → GET the chunks covering the requested range
(greatest base ≤ `fromVersion`, then forward) → yield events. Chunks are dense
and, once sealed, immutable, so there is no dedupe, no contiguity check, and no
freed-key phantom to guard against; the only race is reading the live tail while
it grows, which yields a benign prefix and re-reads on the next poll. Expose as
`AsyncIterable<EventEnvelope>` with internal prefetch (parallel GETs, bounded
concurrency) to hide S3 latency. A 1M-event replay is ~`1M / N` chunk GETs,
served almost entirely from the edge cache.

**Boundary-straddling commits.** A multi-event commit may straddle a chunk's
nominal edge (a 3-event commit whose events span versions 19–21). Membership is
by the commit's **base**, and the roll verdict is judged on the tail as read: a
straddling commit joins whichever chunk was the live tail when its writer read
it (never split), extending that chunk's coverage past the nominal edge
(recorded in the body); the next append reads a now-full tail and rolls to a new
chunk keyed by its own base — dense against the prior chunk's coverage. Because
fullness is read from the tail and the incoming commit's own size never enters
the verdict, two writers that resolved the same head cannot disagree about the
boundary.

## Public API sketch

```ts
import { createEventStore, mutableTail, ConcurrencyError } from "s3-event-store";
import { awsSdkDriver } from "s3-event-store/drivers/aws-sdk";
// or: r2BindingDriver(env.EVENTS) / aws4fetchDriver({...}) in Workers

const store = createEventStore({
  driver: awsSdkDriver({ client: new S3Client({}), bucket: "my-events" }),
  prefix: "prod",                    // multi-tenancy / env isolation / app grouping
  serializer: jsonSerializer(),      // pluggable (compression, encryption)
  strategy: mutableTail({            // default; or immutableChunk() for large-N replay
    chunkSize: 20,                   // store default N; byteCap + optional
    // policyFor: (id) => ...,       //   per-stream policyFor override (see Configuring N)
  }),
});

// Append with optimistic concurrency
const result = await store.append("order-123", [
  { type: "OrderShipped", data: { at: "..." } },
], { expectedVersion: 4 });          // or "any" | "noStream"
// result: { streamId, nextExpectedVersion, committedAt }

// A stream-creating append may pin this stream's N (see Configuring N):
await store.append("audit-42", [ { type: "Opened", data: {} } ],
  { expectedVersion: "noStream", chunkSize: 200 });

// Read
for await (const e of store.read("order-123", { fromVersion: 0 })) { ... }
```

**Error taxonomy**: `ConcurrencyError` (412 race), `StreamNotFoundError`,
`SerializationError`, plus passthrough of SDK errors. All typed, all exported.

`expectedVersion` semantics:
- number `n` → commit must land at version `n+1`; `n ≥ 0` — the first
  append to a stream is expressed as `"noStream"`, and `-1` is rejected
  rather than aliased (one spelling per intent)
- `"noStream"` → stream must not exist, verified by the `c/` LIST returning
  no chunks (under the ImmutableChunk strategy this check has an extra
  freed-key subtlety — see [DESIGN_IMMUTABLE_CHUNK.md](DESIGN_IMMUTABLE_CHUNK.md))
- `"any"` → resolve head and retry the conditional write on conflict up to a
  bounded retry count (still atomic, just relaxed intent). The `commitId`
  self-check makes this loop idempotent: a lost-response retry is recognized as
  our own commit in the re-read tail, never re-appended at a new version.


## Alternative strategy: ImmutableChunk

For large-N, replay-heavy streams and deployments that want maximal
blast-radius isolation, a store can be configured per prefix to use the
**ImmutableChunk** strategy instead of the default mutable tail: one immutable
object per commit under `e/`, compacted into `c/` chunk objects by a
background, write-triggered compactor, with no object ever overwritten in place.
Its append protocol (and the freed-key hazard class it must manage),
pinned-anchor head discovery, and the full compaction subsystem — chunk
membership, sizing bounds, compactor steps, failure modes, scheduling, and cost
— are specified in [DESIGN_IMMUTABLE_CHUNK.md](DESIGN_IMMUTABLE_CHUNK.md). The
analysis that motivates keeping it available is in
[DESIGN_MUTABLE_TAIL_PROPOSAL.md](DESIGN_MUTABLE_TAIL_PROPOSAL.md).

## Package & tooling

- **Name**: `s3-event-store` (or scoped `@<you>/…`) — check npm availability.
- **Build**: tsup → dual ESM/CJS + `.d.ts`; Node ≥ 20; `"sideEffects": false`.
- **Structure**: single package, subpath exports (`.`, `./client`,
  `./drivers/r2-binding`, `./drivers/aws-sdk`, `./drivers/aws4fetch`)
  rather than a monorepo — keep it one dependency;
  drivers and the browser client stay tree-shakeable for Workers bundles.
- **Tests**:
  - Unit: key codec, envelope schema, version math, error mapping (mock SDK).
  - **Simulation (the highest-leverage suite** — implementation plan in
    [SIMULATOR_PLAN.md](SIMULATOR_PLAN.md)**):** a deterministic in-memory
    S3 model — strong consistency, conditional PUTs, conditional GETs
    (`If-Match`), versioned-bucket delete markers, injectable pauses, 412s,
    404s — driving randomized interleavings of writers and readers against a
    chosen strategy. The **invariant set is strategy-agnostic** and transfers
    between both: no lost events, no duplicate versions, no phantom reads (a
    reader never yields a commit whose append was rejected), no forged heads
    (head resolution never derives a head from a substituted anchor), readers
    always observe a contiguous prefix. Under **MutableTail** the schedule
    space is small — the CAS chain admits at most one winner per head and
    nothing is ever deleted, so the invariants reduce to "concurrent CAS PUTs
    and rolls never fork or drop a commit". Under **ImmutableChunk** the same
    invariants must survive a much larger space — the freed-key-orphan
    schedules the pinned-GET and sealed-bucket read rules exist for (including
    the post-LIST substitution where a listed key is compacted, freed, and
    recreated before its GET, and the multi-bucket schedule the sealed-bucket
    check must *iterate* to catch), and the forged-head schedule the pinned
    anchors and `lastCommitEtag` corroboration exist for (detailed in
    [DESIGN_IMMUTABLE_CHUNK.md](DESIGN_IMMUTABLE_CHUNK.md)). Most of that
    strategy's correctness lives in race windows an integration suite can only
    sample; the simulator explores them deterministically and replays any
    failing schedule from its seed.
  - Integration: MinIO or LocalStack via testcontainers — **verify the emulator
    honors `If-None-Match`/`If-Match` semantics** (recent MinIO and LocalStack
    do; pin versions). A small nightly job against real S3 **and real R2**
    (R2 has no local emulator with faithful conditional-write behavior;
    Miniflare/`wrangler dev` R2 simulation is not a conformance substitute)
    for the concurrency race test: spawn N concurrent appenders, assert
    exactly one winner per version, no gaps, no duplicates.
- **Observability**: the library exposes counters/hooks — `ConcurrencyError`
  rate, tail-cache invalidations, and (ImmutableChunk only) compaction lag
  behind the watermark, sweep garbage found. The ImmutableChunk
  accepted-cost arguments (the compaction gap, benign watermark regression,
  occasional sweep) all assume someone can see when "bounded waste" starts
  costing; those counters are that visibility. The mutable-tail default has
  no such backlog to watch — its only failure signal is the conflict rate.
- **Lint/CI**: eslint + prettier, vitest, GitHub Actions, changesets for
  release management, provenance-signed npm publish.

## Cost & performance notes

- PUT/LIST ≈ $0.005 per 1k, GET ≈ $0.0004 per 1k. Storage is negligible;
  reads and writes are dominated by request count. The mutable tail costs
  **one CAS PUT per append** (~$5 per 1M appends) with no per-commit chunk
  check, no `head.json` hint PUT, and no background compaction — roughly half
  the request cost of the ImmutableChunk strategy. Write amplification (the
  tail re-upload) is paid in bandwidth and latency, never in dollars: object
  storage charges a flat rate per PUT with free ingress.
- Append latency: a **hot writer** serving from its in-process tail cache is
  1 CAS PUT uploading an average of N/2 commits (tens of KB at default sizing)
  — one round trip. A **cold** append is 1 short LIST + 1 GET (resolve the
  tail) + 1 CAS PUT ≈ 60–150 ms on standard S3. The in-process tail cache
  (bytes + ETag) is sound with no freed-key caveat: a stale-low cache simply
  412s on the CAS and forces a re-resolve; there is no stale-high orphan to
  guard against because nothing is deleted. Only near `byteCap × N` does the
  upload size bite (see Per-stream N).
- Long streams: a replay is ~`1M / N` chunk GETs — every stream is fully
  packed by construction (no uncompacted tail, no "accepted gap"), and the
  tail itself is always a single GET. Complete pages are immutable and
  edge-cacheable forever, so a deep replay streams almost entirely from cache.
  For replay-heavy streams that want a much larger N than the mutable tail's
  re-upload budget allows, use the ImmutableChunk strategy per prefix
  (see Storage strategies).
- **S3 Express One Zone backend** (later): single-digit-ms PUT/GET, supports
  conditional writes; trade-off is one-AZ durability and directory-bucket LIST
  semantics — offer as an opt-in backend behind the same interface.

## Encryption & erasure contract

Client-side encryption, key management, and GDPR erasure are specified in a
companion document, [KEYS_DESIGN.md](KEYS_DESIGN.md). The split works because the
dependency is one-way: that layer builds on core invariants, and the core
never depends on encryption existing — an unencrypted store is fully
specified by this document alone. What KEYS_DESIGN.md builds on, guaranteed here:

1. **Serializer interface.** Encryption ships as a pluggable serializer
   (compress, then encrypt); the store treats payloads as opaque bytes.
2. **`keyId` is a reserved envelope field** — opaque, plaintext, outside
   the encryption boundary, inside the envelope both strategies copy verbatim.
3. **Payload bytes are copied verbatim, never re-serialized.** The
   mutable-tail appender copies prior ciphertext envelopes verbatim when it
   rewrites the tail (base64 string fields round-trip JSON reserialization
   safely); the ImmutableChunk compactor does the same when assembling chunks.
   Neither path needs key access — a writer never touches key material for
   events it didn't write.
4. **No PII in stream IDs or prefixes** — identifiers live forever in keys,
   hints, and audit records, outside any encryption boundary.
5. **Immutability and no-purge edge caching are permanent facts** — the
   crypto layer must work with ciphertext that can never be re-encrypted or
   reliably purged (this is what forces KEYS_DESIGN.md's generational rotation).
6. **Every cache declares a bounded TTL** — worker key caches, model-A
   plaintext edge entries, client keyrings — feeding KEYS_DESIGN.md's unified
   shred-propagation budget.

## Serverless deployment (Workers + R2)

Reference deployment: Cloudflare Workers + R2 (Lambda + S3 is structurally
identical).

**Write path — always through a worker.** It authenticates, rehydrates the
aggregate (read the chunks), enforces invariants, performs the conditional
PUT (a CAS on the tail chunk). Nothing else holds write credentials to the
event bucket.

**Read path — worker-gated too, but not for cost reasons.** The math: a
~50-commit stream read costs ~50 Class B GETs ≈ $18/M reads, paid regardless
of route; the worker adds one request ≈ $0.30/M (~2% overhead). The worker
actually *lowers* read cost: sealed chunk objects are immutable →
cache-forever in the edge cache (Cache API), so hot streams and repeated
replays stop hitting R2 entirely. Head discovery (one short LIST + a tail GET)
is the only intrinsically uncacheable step. The real read-cost lever is
edge caching — not bypassing the worker. What forces the
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
  schema changes become breaking client changes. Upcasting placement follows
  cache lifetime: a cache-forever page pins whatever bytes rendered it (the
  same no-purge edge-cache fact that rules out re-encrypting history), so
  **egress upcasting is restricted to bounded-TTL paths** — model A, where
  every response a client sees expires and a redeployed upcaster propagates
  within the TTL. Paths serving immutable pages (unencrypted, model B)
  return stored bytes verbatim and upcast client-side — head page included,
  so one replay never mixes transformed and raw shapes. Adopt deliberately.

**Two read models for encrypted streams** (they compose per-caller):

| | A: worker decrypts | B: client decrypts |
|---|---|---|
| Flow | Worker GETs ciphertext, decrypts, returns plaintext | Worker authenticates and delivers the data key (bounded TTL); client fetches ciphertext (cache-forever) and decrypts locally |
| Infrastructure plaintext | Plaintext responses cacheable only with bounded TTL; cache key must include authorization scope | None, ever — edge holds only ciphertext, shred-compatible by construction |
| Access granularity | Arbitrary (per-event filtering possible) | Key = capability for the whole stream, past and future until rotation — requires **authorization boundary = encryption boundary** |
| Fit | Third parties, partial/filtered access | Per-user streams read by their own user (the dominant case) |

Note on model A caching: Cloudflare's Cache API keys on URL and cannot
vary on `Authorization`, so "cache key must include authorization scope"
means synthesizing a cache-key URL that embeds the caller's scope (e.g. a
scope hash as an extra path segment, used only for cache lookup). Never
cache model-A plaintext under the bare request URL.

Notes on model B:

- Keep ciphertext access-controlled, not public-but-encrypted: a leaked key
  or a future cipher break must not expose a stream's history to the world.
  The auth check is the worker request already being paid.
- The key half — keyring delivery, generational rotation,
  TTL-as-authorization-window semantics — is specified in
  [KEYS_DESIGN.md](KEYS_DESIGN.md) under Key delivery.

**Shred propagation:** every cache this deployment introduces — worker key
caches, model-A plaintext edge entries, client-delivered keyrings — must
declare a bounded TTL; those TTLs feed the unified shred-delay budget in
[KEYS_DESIGN.md](KEYS_DESIGN.md) (Shred propagation rule).

**Storage drivers.** Workers access R2 via native bindings
(`env.BUCKET.put(key, val, { onlyIf })`) — no request signing, no SDK weight.
The core therefore defines a minimal driver interface
(`get` / `putIfAbsent` / `putIfMatch` / `list` / `delete` / `deleteMany`)
with three implementations. `get` takes an optional `ifMatch` etag — the
read path's pinned tail GET (and, under ImmutableChunk, its pinned anchors)
depend on it (S3 `GetObject` `IfMatch`, R2 binding
`onlyIf.etagMatches`; conformance-test alongside the conditional PUTs).
Puts return the written object's etag — the mutable-tail writer caches it
in-process as the next CAS precondition without an extra request.
Batch delete is used by the ImmutableChunk compactor and the shred sweeper,
never the default write path (the mutable tail deletes nothing); `list` takes
a prefix plus `startAfter` and returns
lexicographically ordered pages — each key with its etag, which the pinned
GETs consume — with an explicit continuation token — the
contract the head-discovery and read paths assume. Implementations:

- `r2-binding` — native bindings; **conformance-test its `onlyIf`
  conditional-put semantics against the S3-API behavior in phase 1** before
  it becomes the default Workers driver.
- `aws-sdk` — `@aws-sdk/client-s3` v3, for Node; the SDK is an optional peer
  of this driver only.
- `aws4fetch` — lightweight SigV4 signer for calling S3/R2's HTTP API from
  Workers without the SDK (bundle-size limits rule the SDK out there).

## Component inventory & REST surface

Scoped to the initial application: **no projections/subscriptions** — all
client reads are stream-shaped. Cross-stream queries would add them
(unplanned — see Future work); nothing below needs rework if they arrive.

**1. Core library (the npm package)** — the only shipped software. Storage
drivers, event store (append/read), serializers
(incl. the encrypting one), `KeyStore` interface + S3-bucket implementation,
`compactStream()` + trigger check (ImmutableChunk strategy only), shred
workflow helpers (crypto and
erasure behavior: [KEYS_DESIGN.md](KEYS_DESIGN.md)), and the worker-facing
HTTP surface helpers (`readPage`/`toWireFeed` — chunk-aligned pages with the
`complete` immutability flag; `readHead`/`toWireHead` — the poll target with
its version-derived ETag; `idempotentAppend` — retry-safe raw ingress). The
helpers are transport-agnostic (cursors in, wire shapes out via `hrefFor`);
routes, auth, and headers stay in the worker. A dependency, never a service — it
owns no process.

**2. Application worker (deployment code)** — thin HTTP handlers over the
library:

- Command endpoints: authenticate → rehydrate (read chunks) → enforce
  invariants → `append`. (ImmutableChunk deployments also fire the
  `waitUntil` compaction trigger after the append.) Deployments
  that skip domain commands and accept client-built events directly
  should route the endpoint through `idempotentAppend` (stable event ids
  + explicit `expectedVersion`), making client retries safe under
  at-least-once delivery (see Prior art, EventStoreDB).
- Read endpoints: events (plaintext or ciphertext per read model), key
  delivery (model B).

The library ships no routes — auth and invariants are domain concerns.
(A Hono-style middleware helper is a possible later convenience export.)

**3. Background jobs** — the mutable-tail default has **no background jobs at
all** (no compaction, no sweep). The only clock-driven job in the system is
the **shred sweeper** (cron), and only for encrypted stores. (The
ImmutableChunk strategy adds compaction, but that is write-driven, not
clock-driven — see [DESIGN_IMMUTABLE_CHUNK.md](DESIGN_IMMUTABLE_CHUNK.md).) The
sweeper legitimately needs a clock: a crashed shred has no guaranteed future
write or read to
revisit it, the soft-delete waiting period needs a scheduler (every hard
delete is sweeper-executed), and erasure correctness has a deadline.
Mechanics — resumable intents, waiting period, checkpointing, one sweeper
per store — in [KEYS_DESIGN.md](KEYS_DESIGN.md) under Shred sweeper.

**4. Client** —

- *Model A:* nothing. Plain JSON over HTTP; immutable event responses get
  browser/edge caching free via `ETag` + `Cache-Control: immutable`. Any
  `fetch` loop with a cursor is a complete client.
- *Model B:* thin browser SDK as subpath export **`./client`** — zero
  dependencies, WebCrypto only. Responsibilities: fetch/cache the keyring
  respecting its TTLs, pick the key per event by the envelope's `keyId`,
  AES-GCM decrypt (fail closed — a shredded stream
  presents as decryption failure; keyring semantics per
  [KEYS_DESIGN.md](KEYS_DESIGN.md)), cursor iteration, caller-supplied
  upcasters (client-side is the only upcasting on immutable paths — see
  Query shape), optional fold-to-state helper. It exists for crypto correctness, not protocol: the REST API
  remains the contract and the SDK is its reference consumer.

**5. Infrastructure (config, not code):** event bucket; key bucket (inverted
config, startup-verified); edge cache; audit logging (CloudTrail /
Cloudflare audit logs). R2 event notifications + Queue only if the queue
compaction variant is adopted.

**REST surface** (shapes, not prescriptions — most apps expose domain
commands rather than raw append):

```
POST /{prefix...}/streams/{id}/append         command endpoint; validated events in, AppendResult out
GET  /{prefix...}/streams/{id}/events?from=v  events (A) or ciphertext envelopes (B), + next cursor
GET  /{prefix...}/streams/{id}/head           current version — the poll target
GET  /{prefix...}/streams/{id}/key            keyring: data keys + TTLs   (model B only)
```

**Prefix routing.** `{prefix...}` is one or more path segments mapped
**verbatim** onto the S3 key prefix
(`/app-x/orders/streams/order-123/events` →
`app-x/orders/streams/order-123/e/…`), so deployments can group related
application data — event-sourced or not — under shared prefixes (see the
ownership-boundary rule in Key layout). The worker resolves the prefix to a
store instance; stores are stateless beyond the head cache, so a per-prefix
factory is trivial. Link-following makes the extra segments free: clients
never construct URLs, cache keys *are* URLs, so pages from different
prefixes are simply distinct permanent cache entries. Rules:

1. **Verbatim path↔key mapping** — no aliasing or rewriting, or
   redirect-to-canonical and link-following stop being mechanical.
2. **Per-prefix config is per-store config** — N, serializer, key store may
   vary by prefix (page URLs embed the prefix, so differing N values cannot
   collide in the cache); uniform is still simpler.
3. **A prefix is a grouping, not a consistency boundary** — no cross-stream
   transactions within it (same non-goal as ever) — **and not an
   enumeration API**: "all streams under app-x" is a projection, not a LIST.
4. **Validate before mapping.** Reject `streams` as a prefix segment — a
   prefix like `app-x/streams/evil` would nest one store's owned subtree
   inside another's, violating the ownership rule (scoped LISTs happen to
   make it benign today, but the invariant should hold by construction, not
   by accident). Reject externally supplied stream IDs beginning with `$`
   — the reserved system-stream namespace (`$system.key-audit`) is written
   only by library-internal code (see the ownership rules in Key layout).
   Normalize percent-encoding before the path↔key mapping,
   reject empty segments, and enforce S3's 1024-byte key limit against the
   longest key a request can produce (prefix + streamId +
   `/e/{12 digits}.json`).

Side benefit: authorization tends to align with application grouping, so the
worker gets a natural hook for per-app policy — path-prefix middleware,
outside the library.

Live updates = polling `GET head` (full head resolution behind it — a short
`c/` LIST + one tail GET — short-TTL cacheable). The response
carries a strong `ETag` derived from version space (`"v{head}"`, `"empty"`
for an absent stream — never a storage ETag, which changes on every tail CAS
without the logical head moving), so pollers revalidate with
`If-None-Match` and the handler answers `304 Not Modified` while the head
is unmoved — the poll loop costs a conditional request, not a body
(EventStoreDB's AtomPub API demonstrated this contract; see Prior art).
The head body is a pure function of the version given the route's fixed
page size, which is what makes the version a valid strong validator.
An optional middle rung between polling and SSE: a long-poll (the handler
holds the request up to a client-named timeout, re-resolving the head
server-side, responding the moment it advances) — same S3 cost, better
latency, no Durable Objects; deployment-layer, evaluated under Prior art.
SSE/WebSocket via Durable Objects is an optional deployment
upgrade the library stays out of.

### Wire format

Plain JSON, one page = one object. Atom — the historical answer for HTTP
event feeds (EventStoreDB, RFC 5005 archived feeds) — is overkill: its two
load-bearing ideas survive here without the XML.

```jsonc
{
  "streamId": "order-123",
  "from": 0,
  "to": 20,                  // exclusive: the page covers [from, to) — width N
  "complete": true,          // head moved past this page ⇒ frozen (events AND
                             // next link) ⇒ served with Cache-Control: immutable
  "events": [
    { "id": "…", "type": "OrderShipped", "version": 5,
      "data": { … },         // model A — or "ciphertext": "base64…" in model B
      "meta": { "ts": "…", "correlationId": "…" } }
  ],
  "next": "/streams/order-123/events?from=20",    // null ⇒ at head
  "prev": null                                    // page k−1; null on page 0
}
```

`complete ⇔ next ≠ null` by construction: a page is declared immutable
only once the head has advanced *past* it (an event exists at `to` or
beyond) — the same fact that determines its `next` link. Declaring a
brim-full page complete while its `next` is still `null` would freeze a
body that can never surface its successor. A partial (or brim-full, or
past-the-head) page is `complete: false`, `next: null`, and served
short-TTL / no-store.

The head resource (`GET …/head`) is the second wire shape:

```jsonc
{
  "streamId": "order-123",
  "version": 371,            // current head; null ⇒ stream does not exist yet
  "head": "/streams/order-123/events?from=0",     // link to the page containing
                                                  // the head — always incomplete
  "etag": "\"v371\""         // also emitted as the ETag header; If-None-Match ⇒ 304
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
  and cache-forever (see Reverse reads).
- **Redirect-to-canonical**: the one URL a client must construct is a cold
  resume from a stored cursor (`?from=12`, mid-page). The read handler
  responds `308` to the canonical page containing that version (`?from=0`
  for page 0–19 at N = 20); the client SDK follows and skips locally to 12. One
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
At the modest page sizes a small N implies, neither is needed; only a
deliberately large N would make either worth enabling.

### Storage topology and the API

The REST contract is defined over **logical version ranges**; the physical
layout — which strategy, where a stream's chunk boundaries fall, whether an
ImmutableChunk compactor has run — is invisible to it. A page for versions
`[from, to)` has identical bytes however the store arranges them, so nothing
the API ever returned becomes untrue and no cached response is invalidated by
a relayout. Rules that make this hold:

1. **Cursors are version numbers, never storage references.** A client can
   hold a cursor across any relayout and `?from=v` still means the same
   thing. A cursor that leaked an object key or ETag would break. Load-bearing
   rule.
2. **The read handler absorbs storage topology.** It runs the active
   strategy's read path internally (mutable tail: LIST `c/` → GET the covering
   chunks; ImmutableChunk additionally reconciles the `e/` tail and dedupes),
   so responses never reveal which chunk an event came from, and a mid-request
   race with a concurrent writer or compactor resolves as retry latency, not
   an error.
3. **Page boundaries are a deterministic function of the version.** Pages
   align to fixed version-width windows (`pageSize`, defaulting to the store's
   N), *not* to chunk boundaries — so page URLs are stable and identical across
   clients even though chunk boundaries are per-stream and explicit. Two
   response classes fall out:
   - *Full pages* (ending before head): immutable **content**, but
     cacheability depends on what the bytes are. Unencrypted streams and
     model-B ciphertext pages get `Cache-Control: immutable` and permanent
     edge-cache entries. Model-A plaintext pages are just as immutable but
     must be cached with **auth-scoped keys and a bounded TTL** that joins
     the shred-propagation budget ([KEYS_DESIGN.md](KEYS_DESIGN.md)) — immutability
     is a property of the content, cache lifetime a property of the erasure
     story. A cache miss costs ~1–2 chunk GETs: pages align to event-version
     ranges while chunks are keyed by *base*, so a page straddling a chunk
     boundary draws from two adjacent chunks.
   - *The final partial page* (containing head): short TTL / no-store, same
     as `GET head`.

Net effect: a long replay streams almost entirely from the edge cache; the
live tail page is the only intrinsically uncacheable one, and every sealed
page serves from cache after the first miss per edge location. Cache, API,
and storage converge on the same unit: the chunk.

### Reverse reads

Governing rule: **version space is the only physical coordinate.** Recency
is *resolved to* version space, never made a second key scheme — no new
storage layout, no new invalidation story.

**Newest → oldest** — purely an envelope + SDK feature: client hits
`GET head` (or the incomplete page), walks `prev` links through permanent
edge-cache entries, and the SDK reverses each page's events locally (storage
order stays ascending). Cost identical to forward replay — same canonical
URLs, same cache entries, opposite order. "Most recent 20 events" is at most
two page fetches (partial head page + one complete page). This serves
activity feeds and audit views.

**Time is data, not a coordinate.** There is no server-side time-range query
(`?since=T`). `committedAt` and `meta.ts` come from writers' wall clocks and
carry no monotonicity guarantee — clock skew between writers can make
timestamps run backwards along a stream, and a server API over them would
imply a precision the store cannot honestly provide. Clients that need time
filtering scan by version and filter locally, treating timestamps as
approximate; document this explicitly. ("All events across the system in
[T₁, T₂]" is the global-feed problem this design deliberately excludes —
that's subscriptions/projections, unplanned future work.)

## Alternatives & prior art

Surveyed 2026-07 (registry sweeps, HN, GitHub; plus a code review of
s2-lite). Two conclusions: the core mechanism is well-validated by
production systems, and the specific product — a coordination-free
event-sourcing **library** on a bare bucket — does not exist.

**Nearest alternative: S2** ([s2.dev](https://s2.dev), plus the MIT-licensed
self-hostable [s2-lite](https://github.com/s2-streamstore/s2/tree/main/lite)).
The only product with real per-stream optimistic concurrency on object
storage: `match_seq_num` is `expectedVersion`, fencing tokens add writer
exclusion, appends ack only after the WAL flushes to the bucket. But the
architecture is disjoint from this design's: s2-lite is a **stateful
single-writer server** over SlateDB (an LSM whose SSTs live on the bucket).
Sequence numbers are assigned by an in-memory per-stream actor; the bucket's
conditional writes are used once, to fence a single live writer instance
(SlateDB manifest CAS — startup sleeps a manifest-poll interval to fence out
a predecessor), never per append. Consequences, each the inverse of a goal
here: it cannot run on Workers (resident actor state); failover means
booting a replacement process; records live inside LSM SSTs that compaction
rewrites, so reads always go through the server and an immutable-URL
edge-cached read surface is structurally unavailable; encryption is CSEK
(caller sends raw key material per request, no key management — appends
with a *different* key silently succeed, the inconsistency the `keyId`
envelope field here prevents; no erasure workflow). What it does better,
conceded honestly:

- **Throughput economics.** WAL group-commit amortizes many appends across
  many streams into one PUT per flush interval. This design's
  one-conditional-PUT-per-commit has no amortization — above some
  appends/sec per store, a log-structured server wins on both dollars and
  latency ceiling. That boundary is out of scope by design: the trade buys
  zero infrastructure and true multi-writer.
- **Live tailing** (SSE with resume tokens) vs. polling `GET head` here;
  the Durable Objects upgrade path covers this if it matters.
- **Fencing tokens** as an application primitive (implemented as commands
  in the log). Storage CAS makes them unnecessary for correctness here,
  but designated-writer patterns could want them — implementable later as
  an ordinary event type checked at append time.
- A **deterministic simulation harness** (`sim/`: seed-replayable,
  linearizability-checked, simulated object store) — read in detail
  2026-07; what transfers and what doesn't is worked through in
  [SIMULATOR_PLAN.md](SIMULATOR_PLAN.md).

**EventStoreDB's AtomPub HTTP API** (GetEventStore → EventStoreDB →
KurrentDB; the AtomPub surface is deprecated since v20, gRPC-first —
surveyed 2026-07 from the archived docs). The direct ancestor of the wire
format here, and the design already shares its load-bearing ideas
independently: RFC 5005 archived pages (its immutable
`max-age=31536000, public` older pages vs. `no-cache, must-revalidate`
head page = the `complete` flag and immutable/no-store split), strict
link-following ("never construct URLs except the head"), and
`ES-ExpectedVersion`'s special values (`-2`/`-1` = `any`/`noStream`).
Ironically it abandoned the HTTP feed for gRPC because it *has* a server;
this design leans into it because it doesn't. Reviewed feature-by-feature
for further borrowings:

- **Adopted — `ETag`/`If-None-Match` → `304` on `GET head`** (see HTTP
  reads): the version-derived strong validator that makes the poll loop
  nearly free on the wire.
- **Adopted — idempotent append on retry** (as the opt-in ingress helper
  `idempotentAppend`). ESDB dedups by client-supplied event UUID:
  re-POSTing after a timeout succeeds without a duplicate. Here,
  driver-level lost responses are already recovered (the 412 → GET →
  `commitId` comparison in append step 3), but an *application-level*
  retry mints a fresh `commitId`, so it double-appends under `"any"` or
  raises `ConcurrencyError` under an explicit version even though the
  first attempt won. The helper closes it for the raw-append worker (the
  deployment that accepts client-built events rather than domain
  commands): on `ConcurrencyError` it reads the exact window the append
  targeted and reports success iff every version holds the matching event
  `id` — our own earlier win — else rethrows. Client contract: stable
  event `id`s and the same explicit `expectedVersion` on retry;
  `"any"` is refused (no deterministic window ⇒ a retry is
  indistinguishable from an intentional duplicate). Domain-command
  workers get idempotency at the command layer instead (command ids),
  where the semantics belong.
- **Evaluated, deferred — long-poll** (`ES-LongPoll: <seconds>`): the
  middle rung between short-TTL polling and the Durable Objects SSE
  upgrade — a held `GET head` re-resolving server-side. Same S3 request
  cost, better latency, fewer round-trips; pure deployment-layer,
  noted under HTTP reads.
- **Rejected — client-chosen page size in the URL**
  (`/{start}/forward/{count}`): every client mints bespoke cache keys —
  exactly the edge-hit-rate collapse the fixed, chunk-aligned page URLs
  exist to prevent. ESDB could absorb arbitrary windows because a server
  answered every read; nothing absorbs them here.
- **Rejected — `embed` modes / per-event URLs**: event-granular caching
  at one round-trip per event vs. page-granular economics (cache, API,
  and storage converge on the chunk). This API is permanently
  `embed=body`; moot for model B anyway, where ciphertext ships
  regardless.
- **Rejected — retention metadata** (`$maxAge`/`$maxCount`/`$tb`):
  policy deletion contradicts immutable cache-forever pages; erasure here
  is crypto-shredding, designed to survive un-purgeable caches. `$acl` is
  the worker's auth domain, not the library's.

**Other candidates, and why they don't overlap:**

- *The pattern itself*: Oskar Dudycz published essentially this append
  mechanism (versioned keys + `If-None-Match: *` + 412 retry) in 2024
  ([Architecture Weekly](https://www.architecture-weekly.com/p/using-s3-but-not-the-way-you-expected))
  — never implemented, including in his own Emmett (PG/Mongo/ESDB only).
  Compared in detail (2026-07): same foundation (create-only PUT as
  optimistic concurrency, immutable objects, notifications as doorbell,
  request pricing as the governing cost), but the sketch predates
  `If-Match` — it lists CAS as a missing wish-list feature, where both
  strategies here lean on it: the mutable-tail default makes the tail CAS its
  *sole* concurrency primitive, and ImmutableChunk adds pinned GETs and
  anchors. Three divergences are fixes, not taste: latest-chunk
  discovery there sorts LIST results by `LastModified` (no ordering
  guarantee) vs. lexicographic keys here; "412 ⇒
  conflict" is treated as the version check, sound only while nothing is
  deleted — its own chunk reconciliation deletes objects, minting a
  freed-key hazard it never analyzes. The default here sidesteps that class
  entirely by never deleting (the tail is rewritten in place, not
  compacted-then-freed); the ImmutableChunk strategy confronts it head-on
  (the class its head resolution, `commitId`, and chunk check exist for).
  It also suggests S3 Select,
  closed to new AWS customers since mid-2024. One divergence is a
  deliberate trade: it embeds `snapshot + events` in each chunk for O(1)
  rehydration — rejected here because snapshots couple storage to fold
  logic and schema versions and would force the compactor to decrypt,
  breaking the verbatim-bytes rule [KEYS_DESIGN.md](KEYS_DESIGN.md)
  depends on; callers wanting snapshots can CAS a `{state, version}`
  object in their own prefix, outside the store's invariants. Its
  lifecycle-tiering idea (cold chunks to Glacier) is noted under Cost:
  at most Intelligent-Tiering, S3-only — Glacier-class retrieval would
  break replay latency assumptions.
- *Kafka-on-S3* (WarpStream, AutoMQ, Bufstream, Confluent Freight): all
  keep a metadata store or database in the write path; Kafka protocol has
  no conditional append. [Tansu](https://github.com/tansu-io/tansu) is the
  interesting one — a stateless broker coordinating via S3 conditional PUT
  (works on MinIO/Tigris) — but it's a broker binary with Kafka semantics.
- *Workers-native experiments* (DurableStreams, semaflare, workers-es):
  all serialize through Durable Objects, abandoning bucket portability —
  the platform's default answer this design deliberately avoids.
- *Established event stores* (KurrentDB, Axon, Marten, Equinox, Castore,
  MessageDB, EventSourcingDB): every one requires a server process or a
  database.
- *Crypto-shredding as a feature*: AxonIQ's GDPR module (commercial, JVM)
  and Patchlevel (PHP) only; never on an object-storage-primary store,
  never in TypeScript/serverless.

**Mechanism validation** (same primitive, different problems): Turbopuffer
and Chroma's wal3 run concurrency control on S3 conditional writes in
production; SlateDB adopted native CAS; Icechunk achieves serializable
isolation on a bare bucket via one CAS'd root pointer; Terraform now does
native S3 state locking. The Nov 2024
[HN thread](https://news.ycombinator.com/item?id=42240678) on `If-Match`
(524 points) is full of WALs, locks, and LSMs — no event store.

**Net**: the empty niche is not "durable streams on a bucket" (s2-lite now
occupies that as a server); it is expectedVersion event sourcing as a
library — no resident process, multi-writer via bucket CAS, edge-cacheable
replay, erasure built in. No two of those coexist in any product found.

## Roadmap

| Phase | Scope |
|-------|-------|
| 0 | Scaffold: tsup, vitest, CI, key codec + envelope with 100% unit coverage |
| 1 | Storage-driver interface + `r2-binding`/`aws-sdk`/`aws4fetch` drivers (incl. `onlyIf` conformance tests); the **MutableTail** strategy — `append`/`read` + optimistic concurrency + per-stream N + error taxonomy — + **deterministic simulation harness** + integration tests |
| 2 | In-process tail cache, the worker-facing HTTP surface (pages/head/idempotent ingress); the **ImmutableChunk** alternative strategy — create-only commits + compaction — behind the per-prefix strategy seam (per [DESIGN_IMMUTABLE_CHUNK.md](DESIGN_IMMUTABLE_CHUNK.md)) |
| 3 | S3 Express backend, compression + whole-payload crypto-shredding serializer with pluggable key store (per [KEYS_DESIGN.md](KEYS_DESIGN.md)), browser client SDK (`./client`) |
| 4 | Field-level encryption (fail-closed defaults), if demand warrants |

## Future work (unplanned)

Explicitly outside the roadmap — sketched only to record the pieces already
worked out and to show the design doesn't foreclose them:

- **Repository helper** (`./repository`): optional aggregate load/save over
  `append`/`read` — an `init`/`evolve` fold, `load` returning
  `{ state, version }`, `save` appending with the loaded version. Pure
  convenience; nothing in the core assumes it.
- **Subscriptions / projections** (`./subscribe`): two composable pieces —
  a checkpointed catch-up reader (per-projection checkpoint object in S3
  written with `If-Match` CAS; works anywhere, purely poll-based), and live
  notifications (S3 Event Notifications → EventBridge/SQS, or R2 → Queues;
  the notification carries the object key, delivery is at-least-once and
  unordered across streams, ordered enough within one to trigger "read from
  checkpoint to head"). The notification is a doorbell, the stream read the
  source of truth — correct projections without pretending S3 has a global
  log. Nothing in the current surface needs rework if this arrives;
  cross-stream query demand is the trigger to build it.

## Open questions

1. ~~Commit granularity~~ **Resolved**: multi-event commits, capped at N
   events per commit — atomic use-case appends are the whole point of
   `expectedVersion`, and the cap is what keeps chunk boundaries dense (see
   Core mechanism). One-event-per-object for dense keys was rejected; version
   math comes from reading regardless, so dense keys buy nothing.
2. ~~Key-store choice~~ **Resolved**: a dedicated S3 bucket with wrapped keys
   is the default backend (see [KEYS_DESIGN.md](KEYS_DESIGN.md)); alternatives via the
   `KeyStore` interface.
3. ~~Model B key rotation~~ **Resolved**: generational, future-events-only
   — rotation appends a key generation, events carry a plaintext `keyId`,
   the key endpoint delivers a keyring, and re-encrypting history is
   reframed as stream migration to a new prefix. Rotation revokes future
   access only; retroactive protection is impossible in an immutable store
   (see Key rotation in [KEYS_DESIGN.md](KEYS_DESIGN.md)).
