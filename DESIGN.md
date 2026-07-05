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

## Core mechanism: commit objects + create-only PUT

Each append writes **one immutable object per commit** (a commit = 1..N events
appended together). The object key encodes the commit's **base version**,
zero-padded so lexicographic order equals numeric order:

```
{prefix}/streams/{streamId}/e/{baseVersion:012d}.json
{prefix}/streams/{streamId}/c/{chunkBase:012d}.json    # compacted chunks; chunkBase = k·N (see Compaction)
{prefix}/streams/{streamId}/head.json                  # non-authoritative hint
```

The library owns only the `streams/` subtree under `{prefix}` — every LIST
it issues is scoped to `{prefix}/streams/{streamId}/e|c/` — so unrelated
application objects can live as siblings under the same prefix
(`app-x/streams/…` next to `app-x/attachments/…`) with zero interaction.
Three rules: nothing else may write below `{prefix}/streams/` (a foreign
object under `e/` would be parsed as a commit by head discovery);
`streamId` contains no slashes — namespacing lives in the prefix, identity
in the id; and stream IDs beginning with `$` are **reserved for
library-defined system streams** (currently
[KEYS_DESIGN.md](KEYS_DESIGN.md)'s `$system.key-audit`) — ordinary stream
mechanics, but appended only by library-internal writers, so the external
surface must reject `$`-IDs (see Prefix routing). Prefixes follow the same
no-PII rule as stream IDs (see the Encryption & erasure contract).

**Append protocol**

1. Resolve the stream head (current version) — see Head discovery below.
   Head resolution is **mandatory and authoritative for rejection**: if
   `expectedVersion` doesn't match the resolved head, raise
   `ConcurrencyError` before any PUT. The conditional PUT is not the
   version check — `If-None-Match: *` proves only that the key is absent,
   and once compaction can delete keys, absence no longer implies "next
   version" (see below).
2. `PutObject` the commit object at key `e/{head+1}` with `If-None-Match: *`.
   The body carries a writer-generated `commitId`. A commit holds at most
   **N events** (the chunk size — see Compaction); larger atomic appends are
   rejected at the API.
3. On HTTP 412: GET the key just targeted and compare `commitId`. If it is
   ours, the usual cause is that the original PUT succeeded but its response
   was lost, and the retry collided with our own object. (Without this
   check, a retried conditional PUT is indistinguishable from a lost race,
   and a timed-out-but-successful append surfaces as `ConcurrencyError` —
   the caller re-reads, sees its own events, and may double-append.) But
   **ours-at-the-key alone is not proof we won**: when the *first* PUT's
   response was lost without the PUT applying (another writer held the key),
   a later retry can land after compaction freed the key and itself be the
   freed-key recreation — our own commit body, sitting as an orphan in a
   chunked bucket, with the final retry 412ing against it. So ours-at-key
   still runs the step-4 chunk check: no chunk for the bucket → we won;
   chunk containing our `commitId` → we won (the object at the key may be
   our orphan, but the chunk carries the commit); chunk without us → we
   lost, and the object at the key is sweep garbage. (This branch was found
   by the simulator's storage invariant, not the original analysis — a
   double-lost-response schedule.) If the GET 404s, the key was compacted since the
   412 — fetch the bucket's chunk and compare `commitId` there instead.
   A foreign `commitId` at the key is **not yet** proof we lost: check the
   bucket's chunk for our `commitId` too before giving up — our original
   PUT may have succeeded, been compacted, and had its freed key recreated
   by a stalled writer's orphan, in which case the object at the key is the
   orphan and our commit lives in the chunk. In the common lost-race case
   the bucket is the unsealed tail with no chunk, so this extra check is
   one fast 404. Only when neither the key nor its bucket's chunk carries
   our `commitId` did another writer win → raise `ConcurrencyError`
   (caller retries: re-read, re-decide, re-append).
4. On success, **verify the target bucket has no chunk**: GET
   `c/{⌊b/N⌋·N}` for the **baseVersion** b just written. Base, not last
   event version: membership is by base, and for a boundary-straddling
   commit the last event's version selects the *next* bucket — which can
   never be chunked before this one (chunks are dense, compacted
   lowest-first), so the mis-keyed check would 404 and false-pass exactly
   when it must fire. 404 — the overwhelmingly
   common case — confirms the append. An existing chunk that lacks our
   `commitId` means the PUT recreated a freed key: the events are unreadable
   — readers ignore commits in chunked buckets, and the pinned-GET and
   sealed-bucket read
   rules (see Compaction failure modes) keep the orphan invisible even to
   readers holding a pre-chunk `c/` listing — and the sweep will delete the
   object, so raise `ConcurrencyError` (the orphan is sweep garbage, never
   corruption). A chunk that *contains* our `commitId` is the lost-response
   case again: report success.
5. Best-effort update `head.json` with the new commit's key and the ETag
   the PUT response returned (plain PUT, last-writer-wins; contents and
   the corroboration rule under Head discovery below).

This is lock-free and correct: two writers targeting the same version can never
both succeed, and a commit object containing N events is atomic by construction
(one PUT). Division of labor: head resolution rejects stale intent; the
conditional PUT guards the resolve→PUT race window; the step-4 chunk check
closes the one hole the PUT cannot see. That hole: the window is unbounded
on the client side (GC pause, SDK retry backoff spanning tens of seconds),
so a writer stalled long enough for a full bucket of later commits *plus* a
compaction cycle to land inside it finds its target key freed — the
create-only PUT "succeeds", but readers ignore commits in chunked buckets
and the sweep deletes the object. Silent data loss, reported as success.
The chunk check is airtight, not best-effort: contenders for the same
resolved head always target the same key (bases are deterministic), so an
absent target key implies it was created and then compacted — the chunk
existed *before* our PUT, and strong read-after-write guarantees the step-4
GET observes it.

Why the head check cannot be skipped — silent corruptions otherwise:

- Stale-high `expectedVersion`: the PUT lands at an unoccupied future key,
  creating an orphan commit past a version gap; head discovery (LIST max)
  then treats the orphan as the head and the stream is permanently
  corrupted. Head resolution is the **only** defense — a future bucket has
  no chunk, so the step-4 check passes.
- `"noStream"` on a compacted stream: bucket 0's commits are deleted, so a
  blind create-only PUT at `e/0` **succeeds** — but readers ignore commits
  whose bucket has a chunk, so the append reports success while its events
  are unreadable forever. Same mechanism for a stale-low `expectedVersion`
  below the compaction watermark. Step 4 backstops both, but head
  resolution rejects them with the right error before garbage is written.

**Commit object body** (JSON):

```jsonc
{
  "commitId": "uuid",            // writer-generated; disambiguates retried conditional PUTs
  "streamId": "order-123",
  "baseVersion": 5,
  "events": [
    {
      "id": "uuid",                // idempotency / dedupe key; library-generated
                                   // unless the caller supplies one (e.g. for
                                   // domain-level idempotent retries)
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

- `head.json` records `{ headVersion, lastCommitKey, lastCommitEtag,
  compactedTo }` — the hint, its evidence (key *and* ETag, written by the
  same PUT so the pair can regress together but never split), and the
  compaction watermark (see Scheduling).
- **Every anchor GET is pinned.** Whichever path runs, resolution ends by
  GETting one commit and deriving the head from its body (base + event
  count − 1) — and that GET always carries `If-Match`: the ETag the `e/`
  LIST reported for the key, or `lastCommitEtag` when the hint itself is
  the anchor. Unpinned, the freed-key substitution the read path pins
  against (see Compaction failure modes) forges heads *upward*: between
  the LIST and the GET, later appends seal the anchor's bucket, a
  compactor frees its key, and a stalled writer's recreation — base
  matching the key, event count its own — derives a head past the real
  one. Stale-high is precisely the input head resolution exists to reject
  (the PUT lands at an unoccupied future key and step 4 sees an unchunked
  future bucket — the orphan-past-a-gap corruption with no failing check),
  so an unpinned anchor would poison the only defense. The pin costs
  nothing — LISTs already return ETags. On 412 or 404 the anchor is gone
  or replaced: re-resolve (re-LIST, or fall to the cold path when the
  hint was the anchor).
- Fast path: read the hint, then `ListObjectsV2` with
  `StartAfter: lastCommitKey` — the key, not a version the hint asserts,
  so `headVersion` is never load-bearing for resolution. If the LIST
  returns keys, the newest commit anchors the head — paginate to the
  final page first when the hint is very stale (never anchor on an
  intermediate page's max), then GET the anchor pinned to its listed
  ETag (version math
  comes from reading). If it returns nothing, the hint is the only
  evidence: **GET `lastCommitKey` pinned to `lastCommitEtag` and derive
  the head from its contents**
  (base + event count − 1); on 404 or 412, fall back to the cold path.
  Within
  the protocol the hint can only
  be stale-low (it is written after successful appends), but a stale-high
  hint from outside — manual deletion, a bucket restore, a foreign writer —
  would otherwise be accepted silently and mint the stale-high orphan
  corruption above. Existence alone is not corroboration: a fabricated
  `headVersion` beside a real `lastCommitKey` would pass a bare HEAD and
  still mint the orphan — and the key alone is not either: a freed-key
  recreation at `lastCommitKey` is a plausible commit body that is *not*
  the commit the hint referenced, which is what the ETag pin closes.
  Deriving the head from the pinned commit body makes
  every hint corruption fail safe instead of
  fail corrupt. Usually 1 GET (hint) + 1 short LIST + 1 GET (tail anchor
  or corroboration). `headVersion` stays in `head.json` as a
  human-readable diagnostic only.
- Cold path (no or invalidated hint): LIST `c/` first and GET the last
  chunk — its recorded last commit key seeds `StartAfter` for the `e/`
  LIST, so the walk covers only the uncompacted tail rather than every
  commit ever written. Then paginate LIST over `e/` (1000 keys/page) to
  the end; the final key anchors the head, GET it pinned to its listed
  ETag like any anchor. A
  non-empty stream always has commits under `e/` — the highest occupied
  bucket can never seal (sealing requires a successor bucket to have
  started), so compaction never empties the tail.
- If the conditional PUT 412s, the head moved — re-list from the hint and retry.

**Read protocol**: LIST the `e/` prefix, GET each commit object, yield
events. A `fromVersion` may fall mid-commit (keys encode *base* versions), so
never `StartAfter` a raw version number — start the LIST at a known commit
boundary (stream start, or a chunk's recorded last commit key) and trim
locally. Expose as
`AsyncIterable<EventEnvelope>` with internal prefetch (parallel GETs, bounded
concurrency) to hide S3 latency.

## Public API sketch

```ts
import { createEventStore, ConcurrencyError } from "s3-event-store";
import { awsSdkDriver } from "s3-event-store/drivers/aws-sdk";
// or: r2BindingDriver(env.EVENTS) / aws4fetchDriver({...}) in Workers

const store = createEventStore({
  driver: awsSdkDriver({ client: new S3Client({}), bucket: "my-events" }),
  prefix: "prod",                    // multi-tenancy / env isolation / app grouping
  serializer: jsonSerializer(),      // pluggable (compression, encryption)
});

// Append with optimistic concurrency
const result = await store.append("order-123", [
  { type: "OrderShipped", data: { at: "..." } },
], { expectedVersion: 4 });          // or "any" | "noStream"
// result: { streamId, nextExpectedVersion, committedAt }

// Read
for await (const e of store.read("order-123", { fromVersion: 0 })) { ... }
```

**Error taxonomy**: `ConcurrencyError` (412 race), `StreamNotFoundError`,
`SerializationError`, plus passthrough of SDK errors. All typed, all exported.

`expectedVersion` semantics:
- number `n` → commit must land at version `n+1`; `n ≥ 0` — the first
  append to a stream is expressed as `"noStream"`, and `-1` is rejected
  rather than aliased (one spelling per intent)
- `"noStream"` → stream must not exist, verified by head resolution finding
  no commits *and* no chunks — never by the PUT alone (on a compacted
  stream `e/0` is a freed key and a blind create-only PUT would "succeed")
- `"any"` → resolve head and retry the conditional PUT on 412 up to a bounded
  retry count (still atomic, just relaxed intent). The `commitId` self-check
  makes this loop idempotent: a lost-response retry is recognized as our own
  commit, never re-appended at a new version.

## Compaction protocol (phase 2)

Per-commit objects are right for the write path (durability + concurrency) but
make deep replays GET-heavy: 1M commits = 1M GETs. Compaction rewrites cold,
immutable commits into large **chunk objects** in the background, off every
critical path.

**Invariant: every event is readable from at least one object at every
instant.** All ordering below exists to preserve it.

Chunk membership is **by commit baseVersion, fixed-width and deterministic** —
chunk *k* holds every commit whose *base* falls in `[k·N, k·N+N−1]`, keyed
`c/{k·N:012d}`. Membership by base, not by event version: a multi-event
commit whose events straddle `k·N+N−1` belongs wholly to chunk *k*, whose
actual coverage then extends past the nominal boundary (recorded in the
chunk body, along with its last commit key). This makes every commit live in
exactly one chunk, so "delete the source commit once its chunk exists" is
always safe. Slicing by event version instead would let a straddling commit
be needed by two chunks and deleted after only the first exists — data loss.
Any compactor still computes the same key for the same bucket without
coordination.

Buckets are fixed windows over base-version space and never resize — that
determinism is what lets uncoordinated compactors compute identical chunk
keys. Commits are capped at **N events** (enforced at append; an atomic
append of 500+ events is beyond any reasonable use case), and the cap keeps
bucket space dense: a straddling commit's next base always lands in the
immediately following bucket (base b ∈ [kN, kN+N−1] with count ≤ N ⇒ next
base < (k+2)·N), so **no bucket is ever empty** and chunk keys are dense up
to the compaction watermark. Readers still discover chunks by LISTing `c/`,
never by key arithmetic — defense-in-depth, not a load-bearing assumption.

**Chunk size N = 500, fixed per store.** The Workers deployment runs
compaction inside `waitUntil`, which shares the invoking request's
~1,000-subrequest budget (R2 binding calls count); one chunk costs ~N GETs +
1 PUT + 1 batched DELETE, so N = 500 leaves headroom for the request's own
work — which is always an append's few calls, since compaction is
write-triggered only (see Scheduling). `compactStream` therefore compacts
**at most one bucket per invocation**: a backlog of several sealed buckets
(a long-idle stream healing, or adopting compaction over existing data)
would blow the budget in one shot; the state-derived trigger drains a
backlog across subsequent appends, and the queue variant is the
bulk-migration path. `waitUntil`'s post-response wall-clock allowance
(~30 s) bounds the GET phase. "Parallel" is really ~6-wide — Workers cap
simultaneous open connections at 6 — so N = 500 GETs run as ~84 waves,
roughly 4–5 s at typical R2 latencies: comfortable, but the connection
width, not N, is the variable to re-check if chunk assembly ever nears
the limit (verify whether R2 binding calls share the fetch cap).
N is a store-level constant: chunk keys and REST page URLs derive from
it, so changing it under existing data means a full recompaction — pick once,
record it in store config. N bounds request count, not bytes: the compactor
buffers a whole bucket while assembling its chunk, and a bucket can hold up
to N commits (all single-event), so worst-case chunk size is N × the
**per-commit byte cap**. The two caps are one constraint — **byteCap × N ≤
compactor memory budget** — and must be picked together: against Workers'
128 MB limit, N = 500 implies a cap of ~128 KB per commit, not megabytes
(256 KB × 500 would already equal the entire limit, leaving no assembly
headroom). Both live in store config alongside
N; the cap also keeps REST page assembly cheap.

**Compactor steps (per stream):**

1. LIST `c/` to find the last chunk; LIST `e/` for uncompacted commits.
2. Select bucket *k* only once bucket *k+1* has started (a commit with base
   ≥ (k+1)·N exists). Bases progress densely, so the bucket is then
   **sealed** — it can never gain another commit — and it sits behind the hot
   tail where appends and head-hint readers operate. The lag is structural,
   not clock-based. Selection is always the **lowest** sealed uncompacted
   bucket, keeping chunk keys dense up to the watermark — an invariant, not
   a scheduling accident: the sealed-bucket read rule's single-check-on-404
   argument fails if a compactor skips ahead.
3. GET the bucket's commits, write one chunk object with `If-None-Match: *`.
   A 404 on a source commit mid-GET means a racing winner is already
   deleting sources; its chunk necessarily exists (chunk PUT strictly
   precedes deletes) — confirm the chunk and stand down. The same race can
   also complete entirely between step 1's two LISTs: the winner's deletes
   land before the `e/` LIST, and the selected bucket arrives sealed but
   **empty**. Also a stand-down, not corruption: an empty sealed bucket
   whose chunk exists was compacted since the `c/` LIST — confirm the
   chunk and stand down. Only sealed, empty, *and* chunkless is impossible
   (bases are dense), and only that raises. (Found by the simulator's
   randomized compactor races, not the original analysis.)
4. Only after the chunk PUT succeeds, batch-DELETE the source commit objects
   (`DeleteObjects` / the binding's array `delete` — one call, ≤1000 keys).
5. Sweep: delete garbage commits — leftovers of a crash between 3 and 4,
   or freed-key recreations already rejected by append step 4. The scan
   range is defined by the watermark, not by the bucket just compacted:
   every `e/` key with base below `compactedTo` is garbage *by
   definition* (chunks are dense up to the watermark), so the sweep is
   "LIST `e/` from the stream start up to the watermark, delete
   everything found." It must scan from the stream start, not from the
   previous watermark — an arbitrarily stalled writer can recreate a
   freed key in a bucket the watermark passed long ago. This is hygiene,
   not correctness (readers seed their `e/` LIST from the last chunk's
   anchor, so sub-watermark garbage is invisible to them); run it
   occasionally (every M-th compaction), not per invocation.

**Failure modes, all harmless:**

- *Crash between 3 and 4* → events exist in both chunk and commits.
  Duplication, not corruption; readers dedupe (below), the sweep cleans up.
- *Racing compactors* → deterministic boundaries mean identical chunk keys;
  `If-None-Match: *` picks one winner, the loser 412s and proceeds to deletes
  (or stands down earlier via step 3's 404 and empty-bucket rules). Same
  lock-free primitive as the append path.
- *Reader racing the deletes* → two shapes, both recoverable because chunk
  PUT strictly precedes deletes (the data is always in one of the two
  places). **GET-time**: a 404 on a commit the reader just LISTed means
  "compacted" — re-check `c/` for a chunk covering that version.
  **LIST-time**: if the compactor runs between the reader's `c/` LIST and
  its `e/` LIST (or between `e/` pages), a whole bucket's commits are
  simply absent from the listing and no GET ever 404s. Readers therefore
  **verify contiguity as they yield**: each commit's base must equal the
  previous base plus the previous commit's event count (event versions are
  dense even though keys are not). A discontinuity means "compacted since
  our `c/` LIST" — re-LIST `c/` and fill the gap from the new chunk.
- *Contiguous phantom (freed-key orphan)* → contiguity has one blind spot.
  A stalled writer's recreation at a freed key (rejected by append step 4,
  but the object persists until the sweep) sits at exactly the base where
  a compacted commit used to be, and can fill a discontinuity — a reader
  would yield events whose writer was told they failed, in place of the
  real ones in the chunk. Two closing rules, jointly airtight at zero
  added request cost:
  1. **Pin GETs to the listing.** Every tail-commit GET carries
     `If-Match` with the ETag the `e/` LIST reported for that key (the
     driver contract includes conditional GET). A 412 means the object
     was replaced after the LIST — same recovery as the GET-time 404:
     re-check `c/`. This closes every post-LIST substitution, including
     the schedule where a bucket that was genuinely unsealed at LIST time
     seals, is chunked, and has a key recreated all before the reader's
     GET lands — a window no sealed-bucket check can see, because the
     reader's listing correctly showed the bucket as hot tail.
  2. **Sealed-bucket check**, for orphans already present at LIST time:
     **never yield tail commits from a sealed bucket without confirming
     it has no chunk.** An orphan in the listing implies its bucket's
     chunk predates the listing, which implies the bucket is *visibly*
     sealed in that same listing (sealing bucket *k* required a live
     commit with base ≥ (k+1)·N, and the tail never empties). The check
     starts at the first sealed bucket past the reader's last known
     chunk: one `get` on `c/{k·N}` — a fast 404 in the common case, and
     on a hit the chunk body is needed anyway, which is why the driver
     interface offers no HEAD — issued **after** the `e/` LIST it
     guards (before it, the chunk may not exist yet and the check
     passes vacuously), paid only when the tail spans a bucket
     boundary. The two outcomes propagate differently, and the
     difference is load-bearing:
     - **404**: chunks are dense, so no later bucket has a chunk either
       — no LIST-time orphan is possible anywhere in the tail, and this
       single check clears every sealed bucket in the listing.
     - **Hit**: the chunk is authoritative — discard that bucket's `e/`
       commits and read the chunk — but it clears *only that bucket*.
       The reader's last known chunk has advanced, so the rule
       re-applies to the next sealed bucket (equivalently: a hit proves
       the listing predates a compaction pass — re-LIST `c/` and
       re-anchor the tail from the new last chunk). A single
       non-iterated check re-opens the phantom: a compactor that chunks
       buckets *k* and *k+1* between the reader's `c/` and `e/` LISTs,
       with freed keys recreated in both, passes the bucket-*k* check
       via the chunk and would hand bucket *k+1*'s orphans to a reader
       that stopped checking — and the pinned GETs cannot catch them,
       because LIST-time orphans carry the ETags the LIST reported.
  A bucket unsealed at LIST time can never hold a LIST-time orphan, so
  the hot tail — the common case — pays nothing beyond the ETags the
  LIST already returned. All three races resolve as retry latency, never
  a silent gap or substitution.

**Reader path with chunks:** LIST `c/` (few keys) → GET relevant chunks →
LIST `e/` with `StartAfter` the last chunk's last commit key (recorded in the
chunk body) → GET tail commits pinned with `If-Match` to the LIST's ETags,
ignoring any commit whose base falls in an existing chunk's bucket,
verifying version contiguity and the sealed-bucket
rule throughout (see failure modes above). A 1M-event replay becomes ~2k
chunk GETs plus a short tail.

**Cost:** per chunk ≈ N GETs + 1 PUT + 1 batched DELETE. DELETEs are free
on S3 and R2, so compaction runs at roughly a tenth of the original write
cost, amortized in the background. On versioned buckets DELETE only adds a
delete marker — add a lifecycle rule expiring noncurrent versions (storage
cost only; readers only see current versions).

**Interactions:** compaction never touches keys at or above the head, so it
cannot conflict with appends — the write path doesn't know it exists. It
copies payload bytes verbatim, so it works on encrypted payloads without any
key-store access (keeps compactor IAM minimal).

**Scheduling — write-driven, no cron.** Sloppy triggering is safe by
construction (deterministic keys + `If-None-Match` + sweep make duplicate or
racing invocations harmless), which permits driving compaction from the write
path that already runs:

- **Write-triggered (the mechanism):** after a successful append, the writer
  checks the trigger condition with pure arithmetic (it knows the version it
  just wrote) and fires `ctx.waitUntil(compactStream(id))` — response returns
  immediately, ~N I/O-bound GETs run in background time. The condition is
  **state-derived, not event-derived**: "a sealed bucket with uncompacted
  commits exists behind the head" (tracked as a `compactedTo` watermark
  piggybacked on `head.json`, which the append path already writes). The
  check runs on every append, not only boundary-crossing ones — that is
  what state-derived buys — so a died `waitUntil` task is retried by
  whichever append comes next.
  Two writer populations update `head.json` (appenders bump the hint,
  compactors advance the watermark) with plain last-writer-wins PUTs, so a
  field can regress — an appender's stale read-modify-write can undo a
  watermark bump. Benign by construction: every consumer treats `head.json`
  as a hint, and a regressed watermark costs at most a redundant,
  idempotent compaction pass. Upgrading these updates to `If-Match` CAS is
  an optimization to reach for if the waste ever shows up, not a
  correctness fix.
  Appends are also the **only** invocations that fire compaction: an
  append's own subrequest cost is small, so ~N compaction GETs fit
  comfortably in the shared budget. (A read-triggered backstop was
  considered and dropped: it fires precisely when the request has already
  spent ~N GETs traversing the uncompacted path, stacking compaction's ~N
  more against the ~1,000-subrequest ceiling.)
- **Accepted gap:** a stream that crosses a chunk boundary, loses its
  `waitUntil` task, and is then never written again keeps its final sealed
  buckets uncompacted indefinitely. Bounded cost, not a correctness issue:
  readers pay ~N origin GETs per such bucket, and because complete REST
  pages are immutable and cached regardless of compaction state, the
  penalty is paid once per edge location, not per replay. Any future
  append heals it.
- **Queue variant (high-throughput opt-in):** R2 event notifications → Queue
  → consumer, for retries/DLQ/15-min windows off the request path entirely;
  the same notification pipe subscriptions would use if ever adopted (see
  Future work). Also the upgrade path if the accepted gap above ever
  matters in practice.

The library ships `compactStream(streamId)` + the cheap trigger check;
the deployment wires them into its append/read handlers (or the queue).

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
    (`If-Match`), versioned-bucket
    delete markers (DELETE then `If-None-Match: *` re-create succeeds: the
    freed-key mechanism append step 4 exists for), injectable pauses, 412s,
    404s — driving randomized interleavings of appenders, compactors, and
    readers. Property-checked invariants: no lost events, no duplicate
    versions, no phantom reads (a reader never yields a commit whose
    append was rejected — the freed-key-orphan schedules the pinned-GET
    and sealed-bucket
    read rules exist for, including the post-LIST substitution where a
    listed key is compacted, freed, and recreated before its GET, and the
    multi-bucket schedule where two buckets are chunked between a reader's
    `c/` and `e/` LISTs with keys recreated in both — the case the
    sealed-bucket check must *iterate* to catch), no forged heads (head
    resolution never derives a head from a substituted anchor — the
    schedule where the newest listed commit's bucket is sealed, chunked,
    and its key recreated between a resolver's LIST and its anchor GET,
    which the pinned anchors and `lastCommitEtag` corroboration exist
    for), readers
    always observe a contiguous prefix.
    Most of this
    design's correctness lives in race windows an integration suite can
    only sample; the simulator explores them deterministically and replays
    any failing schedule from its seed.
  - Integration: MinIO or LocalStack via testcontainers — **verify the emulator
    honors `If-None-Match`/`If-Match` semantics** (recent MinIO and LocalStack
    do; pin versions). A small nightly job against real S3 **and real R2**
    (R2 has no local emulator with faithful conditional-write behavior;
    Miniflare/`wrangler dev` R2 simulation is not a conformance substitute)
    for the concurrency race test: spawn N concurrent appenders, assert
    exactly one winner per version, no gaps, no duplicates.
- **Observability**: the library exposes counters/hooks — `ConcurrencyError`
  rate, compaction lag behind the watermark (sealed uncompacted buckets),
  sweep garbage found, head-cache invalidations. The design's
  accepted-cost arguments (the compaction gap, benign watermark
  regression, occasional sweep) all assume someone can see when "bounded
  waste" starts costing; these counters are that visibility.
- **Lint/CI**: eslint + prettier, vitest, GitHub Actions, changesets for
  release management, provenance-signed npm publish.

## Cost & performance notes

- PUT/LIST ≈ $0.005 per 1k, GET ≈ $0.0004 per 1k. An aggregate with 1M commits
  costs ~$5 to write, storage is negligible. Reads dominated by request count →
  commit batching and compaction matter more than payload size.
- Append latency ≈ 1 GET (hint) + 1 LIST + 1 GET (hint corroboration) +
  1 PUT + 1 GET (step-4 chunk check, a fast 404) ≈ 60–150 ms on standard S3.
  The step-5 `head.json` PUT is fire-and-forget (`waitUntil` on Workers) —
  off the latency path, but it still counts against the shared subrequest
  budget, as does any compaction the trigger fires.
  Cache the head in-process per stream to make hot-stream appends ≈ 1 PUT +
  1 GET: the cache is a hint that skips resolution, and skipping is sound
  *only because of* the step-4 chunk check — a stale-low cache plus
  compaction is exactly the freed-key case it catches (a 412 forces
  re-resolution, a step-4 `ConcurrencyError` must invalidate the cache the
  same way, and stale-high cannot arise: the process only caches heads
  it observed). That last claim holds *within the protocol*; external
  interference during process lifetime — a bucket restore, manual deletion
  — can make an observed head stale-high and mint the orphan-past-a-gap
  corruption with no failing check, the same class the hint's
  derive-from-commit-body rule closes. The cache is deliberately exempt
  from that hardening: process lifetime is short, and a restore under live
  writers violates assumptions everywhere, not just here. Bound the cache
  TTL if the asymmetry ever matters.
- Long streams: reading 10k commits = 10k GETs. Mitigations, in order:
  background **compaction** into chunk objects (see Compaction protocol —
  with no snapshot layer it is the primary replay mitigation, hence early in
  the roadmap), and encouraging small aggregates (docs).
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
   the encryption boundary, inside the envelope compaction copies.
3. **Compaction copies payload bytes verbatim** (see Interactions under
   Compaction) — chunking never re-serializes, so it needs no key access.
4. **No PII in stream IDs or prefixes** — identifiers live forever in keys,
   hints, and audit records, outside any encryption boundary.
5. **Immutability and no-purge edge caching are permanent facts** — the
   crypto layer must work with ciphertext that can never be rewritten or
   reliably purged (this is what forces KEYS_DESIGN.md's generational rotation).
6. **Every cache declares a bounded TTL** — worker key caches, model-A
   plaintext edge entries, client keyrings — feeding KEYS_DESIGN.md's unified
   shred-propagation budget.

## Serverless deployment (Workers + R2)

Reference deployment: Cloudflare Workers + R2 (Lambda + S3 is structurally
identical).

**Write path — always through a worker.** It authenticates, rehydrates the
aggregate (chunks + tail), enforces invariants, performs the conditional
PUT. Nothing else holds write credentials to the event bucket.

**Read path — worker-gated too, but not for cost reasons.** The math: a
~50-commit stream read costs ~50 Class B GETs ≈ $18/M reads, paid regardless
of route; the worker adds one request ≈ $0.30/M (~2% overhead). The worker
actually *lowers* read cost: commit and chunk objects are immutable →
cache-forever in the edge cache (Cache API), so hot streams and repeated
replays stop hitting R2 entirely. Head discovery (one LIST) is the only
intrinsically uncacheable step. The real read-cost levers are compaction and
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
read path's pinned GETs and head resolution's pinned anchors depend on it
(S3 `GetObject` `IfMatch`, R2 binding
`onlyIf.etagMatches`; conformance-test alongside the conditional PUTs).
Puts return the created object's etag — append step 5 records it as
`lastCommitEtag` without an extra request.
Batch delete is first-class because compaction
needs it; `list` takes a prefix plus `startAfter` and returns
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
`compactStream()` + trigger check, shred workflow helpers (crypto and
erasure behavior: [KEYS_DESIGN.md](KEYS_DESIGN.md)), and the worker-facing
HTTP surface helpers (`readPage`/`toWireFeed` — chunk-aligned pages with the
`complete` immutability flag; `readHead`/`toWireHead` — the poll target with
its version-derived ETag; `idempotentAppend` — retry-safe raw ingress). The
helpers are transport-agnostic (cursors in, wire shapes out via `hrefFor`);
routes, auth, and headers stay in the worker. A dependency, never a service — it
owns no process.

**2. Application worker (deployment code)** — thin HTTP handlers over the
library:

- Command endpoints: authenticate → rehydrate (chunks + tail) → enforce
  invariants → `append` → `waitUntil` compaction trigger. Deployments
  that skip domain commands and accept client-built events directly
  should route the endpoint through `idempotentAppend` (stable event ids
  + explicit `expectedVersion`), making client retries safe under
  at-least-once delivery (see Prior art, EventStoreDB).
- Read endpoints: events (plaintext or ciphertext per read model), key
  delivery (model B).

The library ships no routes — auth and invariants are domain concerns.
(A Hono-style middleware helper is a possible later convenience export.)

**3. Background jobs** — with compaction write-driven, exactly one
clock-driven job remains: the **shred sweeper** (cron). It legitimately
needs a clock: a crashed shred has no guaranteed future write or read to
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

Live updates = polling `GET head` (full head resolution behind it — hint
GET + short LIST + one anchor GET — short-TTL cacheable). The response
carries a strong `ETag` derived from version space (`"v{head}"`, `"empty"`
for an absent stream — never a storage ETag, which compaction changes
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
  "to": 500,                 // exclusive: the page covers [from, to)
  "complete": true,          // head moved past this page ⇒ frozen (events AND
                             // next link) ⇒ served with Cache-Control: immutable
  "events": [
    { "id": "…", "type": "OrderShipped", "version": 5,
      "data": { … },         // model A — or "ciphertext": "base64…" in model B
      "meta": { "ts": "…", "correlationId": "…" } }
  ],
  "next": "/streams/order-123/events?from=500",   // null ⇒ at head
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
  resume from a stored cursor (`?from=372`, mid-page). The read handler
  responds `308` to the canonical page containing that version (`?from=0`
  for page 0–499); the client SDK follows and skips locally to 372. One
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
At N = 500 events per page, neither is needed.

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
   - *Full pages* (ending before head): immutable **content**, but
     cacheability depends on what the bytes are. Unencrypted streams and
     model-B ciphertext pages get `Cache-Control: immutable` and permanent
     edge-cache entries. Model-A plaintext pages are just as immutable but
     must be cached with **auth-scoped keys and a bounded TTL** that joins
     the shred-propagation budget ([KEYS_DESIGN.md](KEYS_DESIGN.md)) — immutability
     is a property of the content, cache lifetime a property of the erasure
     story. After compaction a cache miss costs ~1–2 chunk GETs: pages
     align to event-version ranges while chunks bucket by *base*, so a
     commit straddling a page boundary makes that page draw from two
     adjacent chunks.
   - *The final partial page* (containing head): short TTL / no-store, same
     as `GET head`.

Net effect: a long replay streams almost entirely from the edge cache
regardless of whether compaction has run; compaction only changes what a
cache miss costs (~1–2 chunk GETs vs ~N commit GETs). This is also what makes
write-only compaction triggering acceptable (see Scheduling): even a stream
whose final buckets never get compacted pays the uncompacted price once per
edge location, after which its immutable pages serve from cache. Cache, API,
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
  `If-Match` — it lists CAS as a missing wish-list feature, where this
  design leans on it as the second load-bearing primitive (pinned GETs,
  pinned anchors). Three divergences are fixes, not taste: latest-chunk
  discovery there sorts LIST results by `LastModified` (no ordering
  guarantee) vs. lexicographic keys + pinned anchors here; "412 ⇒
  conflict" is treated as the version check, sound only while nothing is
  deleted — its own chunk reconciliation deletes objects, minting the
  freed-key hazard it never analyzes (the class head resolution,
  `commitId`, and append step 4 exist for); and it suggests S3 Select,
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
| 1 | Storage-driver interface + `r2-binding`/`aws-sdk`/`aws4fetch` drivers (incl. `onlyIf` conformance tests); `append`/`read` + optimistic concurrency + error taxonomy + **deterministic simulation harness** + integration tests |
| 2 | Head hints, in-process head cache, **compaction** (the replay mitigation, now snapshot-free) |
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
   `expectedVersion`, and the cap is what keeps bucket space dense (see
   Compaction). One-event-per-object for dense keys was rejected; version
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
