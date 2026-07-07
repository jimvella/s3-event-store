# s3-event-store — ImmutableChunk strategy (alternative)

**Status: alternative storage strategy.** The library's **default** strategy is
the mutable tail specified in [DESIGN.md](DESIGN.md). This document specifies the
**ImmutableChunk** strategy — one immutable object per commit plus background
compaction — selectable per store (per prefix) for the workloads the mutable
tail does not fit:

- **Large-N, replay-heavy streams.** The mutable tail re-uploads up to
  `byteCap × N` on every append; ImmutableChunk writes a single small commit
  object per append and amortizes chunk assembly into the background, so N in
  the hundreds–thousands stays viable.
- **Maximal blast-radius isolation.** No writer ever overwrites committed data
  in place: every authoritative write is create-only at a fresh key, and the
  only rewriting is a background compactor's verbatim copy-then-delete of sealed
  data behind the head.

The tradeoff analysis that motivates keeping both strategies is in
[DESIGN_MUTABLE_TAIL_PROPOSAL.md](DESIGN_MUTABLE_TAIL_PROPOSAL.md).

Everything the two strategies **share** — prefix routing, the REST/wire surface,
the encryption & erasure contract, storage drivers, and the two read models — is
specified in [DESIGN.md](DESIGN.md); cross-references below to "Prefix routing",
"Encryption & erasure contract", "REST surface", and the driver interface point
there. What is **specific** to this strategy — the create-only append protocol
with its freed-key hazard class, pinned-anchor head discovery, and the
compaction subsystem (including its write-driven scheduling) — is specified
here.

## Key layout

```
{prefix}/streams/{streamId}/e/{baseVersion:012d}.json   # one immutable object per commit
{prefix}/streams/{streamId}/c/{chunkBase:012d}.json     # compacted chunks; chunkBase = k·N (see Compaction)
{prefix}/streams/{streamId}/head.json                   # non-authoritative hint
```

The library owns only the `streams/` subtree under `{prefix}`, with the same
ownership rules as the default strategy (nothing else may write below
`{prefix}/streams/`; `streamId` contains no slashes; `$`-prefixed IDs are
reserved for library-internal system streams) — see Key layout and Prefix
routing in [DESIGN.md](DESIGN.md).

## Append, head discovery, and reads

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


## Compaction protocol

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
keys. Commits are capped at **N events** (enforced at append — so N must be at
least as large as your biggest atomic commit), and the cap keeps
bucket space dense: a straddling commit's next base always lands in the
immediately following bucket (base b ∈ [kN, kN+N−1] with count ≤ N ⇒ next
base < (k+2)·N), so **no bucket is ever empty** and chunk keys are dense up
to the compaction watermark. Readers still discover chunks by LISTing `c/`,
never by key arithmetic — defense-in-depth, not a load-bearing assumption.

**Chunk size N is a configurable store-level constant; it defaults to 20.**
The default is tuned for short, poll-read streams — a small N freezes feed
pages into cacheable objects quickly and bounds the re-downloaded live tail
(full rationale, cost model, and per-workload guidance:
[CHUNK_SIZING_GUIDE.md](CHUNK_SIZING_GUIDE.md)). What follows is the *upper*
bound on N — why, on Workers, you cannot simply make it huge.

The Workers deployment runs compaction inside `waitUntil`, which shares the
invoking request's ~1,000-subrequest budget (R2 binding calls count); one
chunk costs ~N GETs + 1 PUT + 1 batched DELETE, so N must stay well under
that budget to leave headroom for the request's own work — which is always an
append's few calls, since compaction is write-triggered only (see
Scheduling). `compactStream` compacts **at most one bucket per invocation**:
a backlog of several sealed buckets (a long-idle stream healing, or adopting
compaction over existing data) would otherwise risk the budget in one shot;
the state-derived trigger drains a backlog across subsequent appends, and the
queue variant is the bulk-migration path. `waitUntil`'s post-response
wall-clock allowance (~30 s) bounds the GET phase. "Parallel" is really
~6-wide — Workers cap simultaneous open connections at 6 — so N GETs run as
~N/6 waves (a large N ≈ 500 is ~84 waves, roughly 4–5 s at typical R2
latencies: comfortable, but the connection width, not N, is the variable to
re-check if chunk assembly ever nears the limit; verify whether R2 binding
calls share the fetch cap).
N is a store-level constant: chunk keys and REST page URLs derive from
it, so changing it under existing data means a full recompaction — pick once,
record it in store config. N bounds request count, not bytes: the compactor
buffers a whole bucket while assembling its chunk, and a bucket can hold up
to N commits (all single-event), so worst-case chunk size is N × the
**per-commit byte cap**. The two caps are one constraint — **byteCap × N ≤
compactor memory budget** — and must be picked together: against Workers'
128 MB limit a large N ≈ 500 would imply a cap of only ~128 KB per commit
(256 KB × 500 already equals the entire limit, leaving no assembly headroom),
whereas the default N = 20 leaves ample room. Both live in store config
alongside N; the cap also keeps REST page assembly cheap.

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

