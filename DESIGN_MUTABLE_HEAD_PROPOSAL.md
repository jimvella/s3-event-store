# Mutable head chunk — proposal & analysis

**Status: exploratory. Not on the roadmap.** This document analyses an
alternative core storage protocol: maintaining the live tail of a stream as
a **single mutable chunk object** updated by compare-and-swap, instead of
the shipped design's one-immutable-object-per-commit plus background
compaction ([DESIGN.md](DESIGN.md)). It records the tradeoffs, feasibility,
and economics so the decision can be revisited with the reasoning intact.

**TL;DR:** the mutable tail is feasible on both S3 and R2, deletes roughly
half of DESIGN.md's correctness machinery, and cuts per-append request cost
about in half — write amplification is free in dollars on object storage.
What it trades away: the *no object is ever overwritten in place* property
(every appender rewrites live committed data on every append), an
unpublished per-key write-rate ceiling on R2 lands on the append critical
path, and large-N replay tuning is foreclosed. For the default workload
(N = 20, poll-read) it is arguably the better design; for the library's
premise of arbitrary uncoordinated multi-writer deployments, the shipped
design's blast-radius posture is more defensible.

- [The alternative, concretely](#the-alternative-concretely)
- [Feasibility: correctness gets simpler](#feasibility-correctness-gets-simpler)
- [Economics](#economics)
- [Tradeoffs](#tradeoffs)
- [Boundary-straddling commits](#boundary-straddling-commits)
- [Hybrid variants considered](#hybrid-variants-considered)
- [Verdict and de-risking steps](#verdict-and-de-risking-steps)

---

## The alternative, concretely

In the shipped design, an append writes one create-only object per commit
under `e/`, and a background compactor later rewrites sealed buckets into
chunk objects under `c/` (copy, then delete the sources). In this proposal
the bucket-*k* chunk object **is** the live tail:

1. **Append** = GET `c/{k·N}` (body + ETag) → append the commit locally →
   PUT the whole object back with `If-Match: <etag>` (CAS).
2. **Bucket roll** = when the next base overflows the bucket, the writer
   creates `c/{(k+1)·N}` with `If-None-Match: *`. The previous chunk's
   final state is already its sealed, permanent form — no rewrite, no
   rename, no copy.
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
the API", rule 2). This could ship as a per-prefix layout v2 — prefixes
already isolate store config.

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

As a change to this codebase it is a core-protocol rewrite — append/read
in `store.ts`, the key layout, head discovery, and most simulator
scenarios — big, but the deterministic simulator makes it validatable.

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
at N = 20, needs a lifecycle rule regardless). Relatedly, the tail format
becomes a flag-day compatibility surface across all writers, where today
commit objects are self-contained.

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
small-N, poll-read regime the default targets.

### 4. Storage-layer immutability weakens until seal

A chunk key's bytes change over its live window, so trusted direct
readers and any cache keyed on **storage** URLs can hold stale tails
under a permanent key. The worker-gated REST surface absorbs this (pages
are version-keyed; `complete` derives from the head), but "every object
is immutable" stops being true at the bucket layer.

## Boundary-straddling commits

Not a problem, in either design — and the arithmetic matters. At N = 20,
`expectedVersion: 19` (head = 19, base = 20) is the *clean* case: the
commit opens bucket 1 via the create-only PUT of `c/20`. The real
straddle is e.g. `expectedVersion: 18` with 3 events — base 19, events at
versions 19–21, spilling past bucket 0's nominal edge. Both designs
handle it with the same load-bearing rule: **chunk membership is by the
commit's base, not its event versions.** The commit belongs wholly to
bucket 0, whose actual coverage extends to 21 (recorded in the chunk
body); the next base is 22, landing in bucket 1 under `c/20`. In the
mutable-tail variant, specifically:

- **Atomicity untouched** — the commit is one CAS PUT; never split.
- **Target computation stays deterministic** — the base alone decides
  CAS-current-chunk vs create-next-chunk, so contenders that resolved the
  same head target the same object with the same operation: one winner.
- **The ETag seal holds** — the straddling commit is bucket 0's final
  mutation; a writer can only learn head 21 by reading that final state.
- **Density holds** — the ≤ N-events-per-commit cap keeps the next base
  in the immediately following bucket, so chunk keys stay dense.

The one shared cost (already in DESIGN.md): version-aligned REST pages
straddling a chunk boundary draw from two adjacent chunks — ~1 extra GET
on a cache miss. A property of straddling itself, not of the layout.

## Hybrid variants considered

- **Fixed-key tail** (`t.json` mutable; server-side copy to `c/{k·N}` on
  seal, then CAS-reset): buys 1-GET head discovery with no LIST, but the
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
3. **Keeping large-N replay tuning open** (Tradeoff 3).

If revisited, the cheapest de-risking steps, in order:

1. **Conformance-test sustained same-key CAS throughput on R2** —
   empirically find the 429 threshold; it is the feasibility gate.
2. **Port the CAS-chain protocol to the simulator** — the invariant set
   (no lost events, no duplicate versions, no phantom reads, no forged
   heads) transfers directly; the schedule space is far smaller.
3. **Prototype as a per-prefix layout v2** behind the existing store
   config, leaving the shipped layout untouched for large-N prefixes.
