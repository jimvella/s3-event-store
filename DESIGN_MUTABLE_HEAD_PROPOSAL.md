# Mutable head chunk — proposal & analysis

**Status: exploratory. Not on the roadmap.** This document analyses an
alternative **storage strategy** — not a replacement protocol: maintaining
the live tail of a stream as a **single mutable chunk object** updated by
compare-and-swap, instead of the shipped strategy's
one-immutable-object-per-commit plus background compaction
([DESIGN.md](DESIGN.md)). It plugs in behind the same storage-driver
contract and REST surface, selectable per store (per prefix) alongside the
shipped strategy, so the two coexist rather than fork the codebase. It
records the tradeoffs, feasibility, and economics so the decision can be
revisited with the reasoning intact.

**TL;DR:** the mutable tail is feasible on both S3 and R2 and slots in as an
**alternative storage strategy behind the existing driver contract** —
selected per store (per prefix), coexisting with the shipped immutable-object
strategy rather than replacing it. It deletes roughly half of DESIGN.md's
correctness machinery, cuts per-append request cost about in half (write
amplification is free in dollars on object storage), and — because chunk
boundaries become explicit objects rather than `k·N` arithmetic — makes
chunk size **N a per-stream, even per-epoch, knob that drops out of the read
path entirely**. What it trades away: the *no object is ever overwritten in
place* property (every appender rewrites live committed data on every
append), an unpublished per-key write-rate ceiling on R2 lands on the append
critical path, and large-N replay tuning is foreclosed. For the default
workload (N = 20, poll-read) it is arguably the better design; for the
library's premise of arbitrary uncoordinated multi-writer deployments, the
shipped strategy's blast-radius posture is more defensible.

- [The alternative, concretely](#the-alternative-concretely)
- [Feasibility: correctness gets simpler](#feasibility-correctness-gets-simpler)
- [A pluggable strategy, not a rewrite](#a-pluggable-strategy-not-a-rewrite)
- [Per-stream (and evolving) N](#per-stream-and-evolving-n)
- [Economics](#economics)
- [Tradeoffs](#tradeoffs)
- [Boundary-straddling commits](#boundary-straddling-commits)
- [Hybrid variants considered](#hybrid-variants-considered)
- [Verdict and de-risking steps](#verdict-and-de-risking-steps)

---

## The alternative, concretely

In the shipped strategy, an append writes one create-only object per commit
under `e/`, and a background compactor later rewrites sealed buckets into
chunk objects under `c/` (copy, then delete the sources). In this strategy
the tail chunk object **is** the live tail, keyed by the base version of its
first commit (the `c/{chunkBase:012d}` codec already in `keys.ts` — no
key-format change):

1. **Append** = GET the tail chunk (body + ETag) → append the commit locally
   → PUT the whole object back with `If-Match: <etag>` (CAS).
2. **Roll** = when the tail *as read* is already full — ≥ N commits or ≥
   `byteCap` bytes, both read from the chunk body — the writer instead
   creates a new chunk keyed by the incoming commit's base with
   `If-None-Match: *`. Fullness is judged on the tail as-read, not as-it-
   would-be-after-this-commit, so every contender that resolved the same
   head reaches the same roll verdict and targets the same key (see
   [Per-stream N](#per-stream-and-evolving-n)). The previous chunk's final
   state is already its sealed, permanent form — no rewrite, no rename, no
   copy.
3. The `e/` tree, `compactStream`, the sweep, and (optionally) `head.json`
   disappear. Head discovery = short LIST of `c/` + GET the last chunk;
   the head is derived from the chunk body. A hot writer caches the tail
   bytes + ETag in-process and appends with a single PUT.

Both primitives are already in the driver contract (`putIfMatch`,
`putIfAbsent`), and both backends support overwrite-CAS (S3 `If-Match` on
PUT since Nov 2024; R2 `onlyIf.etagMatches`).

The REST surface is untouched: the wire format, cursors, `complete ⇔ next`,
and the client SDK are all defined over logical version ranges, and the
read handler already absorbs storage topology (DESIGN.md, "Compaction and
the API", rule 2). That absorption is what lets this drop in as a per-prefix
*strategy* rather than a fork — see
[A pluggable strategy](#a-pluggable-strategy-not-a-rewrite).

## Feasibility: correctness gets simpler

The striking property of this variant is how much of DESIGN.md's hazard
analysis evaporates. Every subtle hazard there traces to one root cause —
**multi-object state plus deletes** — and this design has neither.

- **Optimistic concurrency collapses into the CAS.** The ETag chain *is*
  the version check. Two writers that resolved the same head hold the same
  ETag (or both target the same create-only next-chunk key); exactly one
  wins. Head resolution stops being "mandatory and authoritative for
  rejection" backed by a five-branch 412 protocol — a 412 means "re-GET
  the tail, re-check", nothing more.
- **Sealing is enforced by the ETag chain automatically.** Bucket *k+1*
  can only start after bucket *k* reached its full state, which changed
  its ETag — so a stalled writer holding a pre-seal ETag structurally
  cannot mutate a sealed chunk. No structural-lag rule, no clock.
- **Nothing is ever deleted → the freed-key hazard class is gone.**
  Append step 4's chunk check, the sweep, pinned GETs against post-LIST
  substitution, the iterated sealed-bucket rule, reader contiguity
  verification, and the compactor stand-down rules all exist because a
  key can be freed and recreated. The simulator found two of those
  branches that the original analysis missed — direct evidence of how
  expensive this complexity class is. Here, a reader LISTs `c/`, GETs
  dense chunks, done; the worst race is GETting a tail that has since
  grown — a benign superset.
- **Lost-response retries simplify.** A retry 412s on its stale ETag →
  GET the one chunk the commit's base falls in, look for our `commitId`.
  One branch instead of four.
- **The encryption contract survives.** KEYS_DESIGN.md's "compaction
  copies payload bytes verbatim, no key access" becomes "the appender
  copies prior ciphertext envelopes verbatim" — base64 string fields
  round-trip JSON reserialization safely, and a writer never needs key
  material for events it didn't write.

## A pluggable strategy, not a rewrite

This is not a new core protocol; it is a second implementation of one seam.
The pieces that differ between the two designs — how a commit is appended,
how the head is resolved, how a logical version range is materialized for
the reader — already sit between two stable boundaries:

- **Below**, the `StorageDriver` contract
  (`get`/`putIfAbsent`/`putIfMatch`/`list`/`delete`) is untouched; the
  mutable tail uses only primitives the shipped strategy already uses, and
  the conformance suite already pins their semantics.
- **Above**, the REST surface, wire format, cursors, and client SDK are
  defined over logical version ranges, and the read handler already absorbs
  storage topology (DESIGN.md, "Compaction and the API"). Neither end cares
  which strategy produced the chunks.
- **Config** already varies per store: DESIGN.md establishes that per-prefix
  config is per-store config (N, serializer, key store vary by prefix; page
  URLs embed the prefix, so differing layouts cannot collide in the cache).
  The storage strategy joins that list — `ImmutableChunk` (shipped: `e/`
  commits + `c/` compaction + `head.json`) or `MutableTail` (this document),
  chosen per store.

So the change is additive: a `MutableTail` strategy behind the existing
per-prefix factory, the shipped strategy left byte-for-byte intact for the
prefixes that keep it, and a second strategy variant under the deterministic
simulator — whose invariant oracle (no lost events, no duplicate versions,
no phantom reads, no forged heads) is strategy-agnostic and transfers
directly. It is a substantial implementation — append/read/head in
`store.ts` and a family of new simulator scenarios — but it is bounded to
one seam, not a rewrite, and the two strategies are independently testable.

## Per-stream (and evolving) N

The shipped strategy fixes N per prefix because chunk boundaries are
*implicit*: the compactor computes a target key `⌊base/N⌋·N` without
reading, so every actor must share one N to agree where a bucket ends. The
mutable tail never computes a target by arithmetic — it reads the tail and
decides — which lets boundaries become *explicit objects*. `keys.ts`
already stores the chunk's base in the key and bans version math from key
arithmetic ("Version math must come from reading commit bodies, never from
key arithmetic"); this strategy honours that literally, keying each chunk on
the exact base of its first commit rather than on an N-multiple. Three
consequences:

- **Reads need N not at all.** To locate version *v*, LIST `c/` and GET the
  chunk with the greatest base ≤ *v*; the boundaries are enumerable, so the
  read path never divides by N and needs no per-stream config lookup.
- **N is a per-stream roll policy, carried in the tail.** The only actor
  that consults N is an appender at the roll decision, and it reads the caps
  (N and `byteCap`) from the tail body it just GET'd. Two contenders that
  resolved the same head hold the same ETag → the same bytes → the same caps
  → the same roll verdict and the same next key: determinism survives
  *because* the policy travels with the bytes the CAS is conditioned on. Put
  N in per-writer local config instead and a writer on a stale value can
  disagree about where the bucket ends, target a different next-chunk key,
  and fork the stream — so the policy must be self-describing (chunk body)
  or drawn from an immutable per-stream config, never ambient.
- **N can even evolve within a stream.** Because each boundary is an
  explicit object and the roll verdict is evaluated per append on observed
  state, a stream may raise or lower N over its life: sealed chunks keep the
  size they had, only the live tail's cap changes — no retroactive rewrite,
  no alignment to relitigate.

The knob earns its keep precisely because the viable band is narrow
(Tradeoff 3): a deployment can run a chatty hot stream at a small N to bound
upload amplification and a colder stream at a larger N for fewer objects,
under one store — subject to a floor, since an N below the REST page size
makes a single page draw from more than two chunks and inflates read
fan-out.

## Economics

The governing fact: **S3 and R2 charge a flat rate per PUT request with
free ingress, so rewriting an ever-growing tail costs zero extra
dollars.** Write amplification is paid in latency and bandwidth, never
money. Per 1M appends at N = 20, S3 prices (PUT/LIST $5/M, GET $0.40/M),
hot writer with in-process cache:

| | Shipped design | Mutable tail |
|---|---|---|
| Commit write | 1 PUT — $5.00 | 1 CAS PUT — $5.00 |
| Step-4 chunk check | 1 GET — $0.40 | — |
| `head.json` hint | 1 PUT — $5.00 | — (the tail *is* the head) |
| Compaction, amortized | ~1 GET + 1/N PUT — $0.65 | — |
| Sweep, sealed-bucket GETs | small | — |
| **Total / M appends** | **~$11** | **~$5** |

Cold-start resolution is comparable either way (short `c/` LIST + one
GET). R2 has the same shape at slightly lower rates (Class A $4.50/M).

Reads are identical for sealed history — chunk objects look the same —
and *better* for the tail: an uncompacted tail today costs up to N commit
GETs on a cache miss; the mutable tail is always one object, one GET.
DESIGN.md's "accepted gap" (a stream that dies with sealed-but-uncompacted
buckets) disappears entirely: every stream is always fully packed by
construction, with no compaction to trigger and no `waitUntil` task to
lose.

Latency is roughly a wash: the shipped hot append is 1 small PUT + 1
chunk-check GET (two round trips); the mutable tail is one round trip
uploading an average of N/2 commits — tens of KB at default sizing. Only
near `byteCap × N` does upload time bite.

## Tradeoffs

### 1. In-place overwrite of live committed data

The shipped design's property, stated precisely: **no object is ever
overwritten in place.** Every authoritative write is create-only at a
fresh key. Compaction *does* re-materialize committed bytes — it is a
trusted copy, and a logically corrupt chunk followed by the compactor's
own deletes would lose data — but it is copy-then-delete, never rewrite:

- The chunk PUT strictly precedes the source deletes, so **two copies
  exist during the window** and a crash anywhere leaves duplication,
  never loss. The compactor could cheaply read back and verify the chunk
  before deleting sources (one extra GET per bucket), converting
  "trusted copy" into "verified copy". That hardening is structurally
  unavailable to the RMW design: the moment the CAS PUT lands, it is the
  *only* copy of the prior ≤ N−1 commits (absent bucket versioning).
- Rewrite capability today is confined to **one background role**,
  running library-owned code, once per bucket, on sealed data behind the
  head, doing a deliberately trivial job (verbatim payload copy, no
  decrypt, no re-serialization — the minimized transformation surface is
  the mitigation). With a mutable tail, **every appender in every
  deployment rewrites live committed data on every append** — one buggy
  serializer, one undetected truncated GET body, one writer on a stale
  library version, and the PUT destroys other writers' committed events.

This is a quantitative difference in blast radius and auditability, not
an absolute — but for a *library* whose premise is arbitrary
uncoordinated multi-writer deployments with no server choke-point, it is
the shipped design's strongest card. Mitigations for the RMW design are
procedural where the current guarantee is structural: writers validate
parse + version contiguity of the tail before PUT; bucket versioning as
an undo log (noncurrent churn ≈ N²/2 × commit size per bucket — trivial
at N = 20, needs a lifecycle rule regardless). Relatedly, the tail format — now including the roll
policy every writer reads to agree on bucket boundaries
([Per-stream N](#per-stream-and-evolving-n)) — becomes a flag-day
compatibility surface across all writers, where today commit objects are
self-contained.

### 2. Per-key write-rate ceilings move onto the critical path

R2 documents that concurrent writes to the same key at a high rate return
HTTP 429, without publishing the threshold (community reports suggest on
the order of ~1/s). The shipped design PUTs each commit at a unique key,
bound only by the CAS chain's round-trip latency (~tens of appends/s);
the mutable tail hits the same key on every append until the bucket
rolls. Nuance: `head.json` is *already* a same-key PUT per append, so the
exposure is not new — but a dropped hint PUT is benign, while a 429'd
tail CAS fails the append. On S3 there is no documented per-key cap
(3,500 PUT/s per prefix), but concurrent conditional writes to one key
can return retryable `409 ConditionalRequestConflict` alongside 412s.
Irrelevant at the reference workload (one event per ~9 s); the binding
constraint for hot streams on R2. Sources:
[R2 limits](https://developers.cloudflare.com/r2/platform/limits/),
[S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

### 3. Large N is foreclosed

The RMW writer holds and re-uploads up to `byteCap × N` on the append
path — every append, not once per bucket in `waitUntil`. N = 20 is
comfortable; the replay-heavy configurations
[CHUNK_SIZING_GUIDE.md](CHUNK_SIZING_GUIDE.md) reserves N in the
hundreds–thousands for become untenable (multi-MB GET+PUT per append,
Workers memory on the request path). The escape is a hybrid — mutable
small-N tail plus background super-chunk compaction — which reinvites the
very machinery this design removes. The mutable tail fits *only* the
small-N, poll-read regime the default targets. Per-stream N
([above](#per-stream-and-evolving-n)) makes this a per-stream verdict rather
than a prefix-wide one — a replay-heavy stream can stay on the shipped
strategy under its own prefix while poll-read streams take the mutable tail
— but it does not raise the ceiling: the binding cost is the per-append
re-upload, which no choice of N on this strategy escapes.

### 4. Storage-layer immutability weakens until seal

A chunk key's bytes change over its live window, so trusted direct
readers and any cache keyed on **storage** URLs can hold stale tails
under a permanent key. The worker-gated REST surface absorbs this (pages
are version-keyed; `complete` derives from the head), but "every object
is immutable" stops being true at the bucket layer.

## Boundary-straddling commits

Not a problem — worth restating under explicit-base keys. The clean case is
`expectedVersion: 19` (base 20): the tail is already full at N = 20, so the
commit simply opens the next chunk, keyed `c/20`. The real straddle is
`expectedVersion: 18` with 3 events — base 19, versions 19–21 — arriving at
a tail holding 19 single-event commits (bases 0–18, head 18). Its writer
read the tail as *not yet full* (19 < 20), so the commit CAS-appends into
the current chunk, becoming its 20th commit and extending the chunk's
coverage to v21 (recorded in the body). The *next* append reads a now-full
tail and rolls, creating a new chunk keyed by its own base, 22 — gap-free
against the prior chunk's coverage of 21. The load-bearing rule is the one
both strategies share: **a commit joins whichever chunk was the live tail
when its writer read it — membership follows the base, never the event
versions, and a commit is never split.** Specifically:

- **Atomicity untouched** — the commit is one CAS PUT; never split across
  chunks.
- **Roll verdict stays deterministic** — fullness is read from the tail, so
  contenders that resolved the same head reach the same verdict and target
  the same key (CAS this chunk, or create the same next base): one winner.
  The incoming commit's own size never enters the verdict — which is what
  stops a large straddling commit from making two writers disagree about the
  boundary.
- **The ETag seal holds** — the straddling commit is the tail's final
  mutation before it fills; a writer can only learn head 21 by reading that
  final state.
- **Density holds** — the ≤ N-events-per-commit cap keeps the next base one
  past the prior chunk's coverage, so explicit-base keys stay gap-free.

The one shared cost (already in DESIGN.md): version-aligned REST pages
straddling a chunk boundary draw from two adjacent chunks — ~1 extra GET
on a cache miss. A property of straddling itself, not of the strategy.

## Hybrid variants considered

- **Fixed-key tail** (`t.json` mutable; server-side copy to `c/{chunkBase}`
  on seal, then CAS-reset): buys 1-GET head discovery with no LIST, but the
  copy+reset two-step is exactly the multi-object state machine whose
  elimination is this proposal's main virtue. Rejected.
- **Mutable tail + background super-chunk compaction** (for replay-heavy
  workloads): reintroduces compaction and its hazard class; at that point
  the shipped design is simpler. Rejected — pick per workload instead
  (per-prefix layout choice).

## Verdict and de-risking steps

For the workload the library defaults to, the mutable tail dominates on
economics (~2× cheaper appends, cheaper tail reads, no compaction, sweep,
or accepted gap) and on protocol simplicity — most of DESIGN.md's
subtlest sections manage consequences of the multi-object-plus-delete
choice. The honest reasons the shipped design stands:

1. **Blast radius under uncoordinated writers** — copy-then-delete with a
   two-copy window, a verification hook, and one infrequent trusted
   actor, versus in-place replacement of the only copy by every writer on
   every commit (Tradeoff 1).
2. **The unquantified R2 same-key 429 ceiling** on the append path
   (Tradeoff 2).
3. **Keeping large-N replay tuning open** — per-stream N narrows this to
   "don't put large-N streams on the mutable strategy", but the ceiling
   itself is intrinsic (Tradeoff 3).

If revisited, the cheapest de-risking steps, in order:

1. **Conformance-test sustained same-key CAS throughput on R2** —
   empirically find the 429 threshold; it is the feasibility gate.
2. **Port the CAS-chain protocol to the simulator** — the invariant set
   (no lost events, no duplicate versions, no phantom reads, no forged
   heads) transfers directly; the schedule space is far smaller.
3. **Prototype as a per-prefix `MutableTail` strategy** behind the existing
   per-store factory ([A pluggable strategy](#a-pluggable-strategy-not-a-rewrite)),
   leaving the shipped strategy untouched for large-N prefixes.
