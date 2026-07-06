# Chunk sizing guide

`chunkSize` (called **N** throughout) is the single most important cost knob in
this library. It is a store-level constant — chunk keys *and* REST page URLs
derive from it — so changing it under existing data means a **full
recompaction**. Pick it once, deliberately, and record the choice in store
config.

This document explains the default, the general cost model behind choosing N,
and the platform trade-off between long- and short-polling that sits alongside
it.

- [What N controls](#what-n-controls)
- [The default: N = 20](#the-default-n--20)
- [Choosing N in the general case](#choosing-n-in-the-general-case)
- [Per-stream N: routing by store](#per-stream-n-routing-by-store)
- [Polling: long vs short, by platform](#polling-long-vs-short-by-platform)

---

## What N controls

N does three things at once, and they are coupled:

1. **Chunk bucket width.** Compaction packs commits into chunk objects of up to
   N commits each (`c/{k·N}`). Fewer, wider chunks mean fewer GETs on a deep
   replay; more, narrower chunks mean more objects but faster freezing (below).
2. **HTTP feed page width.** The paging helpers default `pageSize` to the
   store's N (`src/http.ts:86`), so page boundaries align to chunk boundaries.
   A page is served `Cache-Control: immutable` **the moment the head advances
   past it** — `complete: headVersion >= to` (`src/http.ts:112`). This is a
   pure version comparison; it does **not** depend on compaction having run.
   Narrow pages freeze sooner, so repeat reads bank into client/edge cache
   sooner and the still-growing `no-store` tail stays small.
3. **Per-commit event cap.** An atomic append of more than N events is rejected
   (`src/store.ts:424` — `a commit holds at most ${chunkSize} events`). N is the
   bucket width *and* the largest atomic multi-event commit you can make.

N is a **request-count and cacheability lever, not a storage lever.** The same
events occupy roughly the same bytes whether packed into a few wide chunks or
many narrow ones. What N changes is how many objects you GET/PUT/LIST, and how
quickly feed pages become cacheable.

Two structural facts to keep in mind:

- **Compaction only fires once a bucket *seals*** — i.e. the *next* bucket has
  started (a commit with base ≥ (k+1)·N exists; `src/store.ts:621`). A stream
  shorter than N never fills a bucket, so it never compacts — it just stays as
  cheap per-commit objects served from the `e/` tail. That is fine for small
  streams, but it also means **a page as wide as the whole stream never freezes
  and is never cacheable.**
- **The live tail page is page-aligned and `no-store`.** `readPage` snaps any
  cursor down to the page boundary and 308-redirects mid-page cursors to the
  canonical `from` (`src/http.ts:91`). A client therefore cannot ask for "just
  events since my cursor" within the live page — every fetch of the live page
  re-downloads the whole accumulated page from its aligned start. Wide N ⇒ the
  live page can accumulate a lot before it freezes ⇒ each poll-triggered
  re-fetch is large.

---

## The default: N = 20

The default is tuned for a concrete, common shape of workload rather than for
maximum replay throughput.

### The reference scenario

- **~200 events per stream**, produced over ~30 minutes (roughly one event
  every ~9 seconds).
- **~3 reads per event committed** in aggregate.
- **Clients poll for updates** — the read pattern is bursty around commit time.
- **Clients cache locally** wherever the response allows it.

### Why 20 and not the wider values you would pick for replay

Under this workload a *wide* N (e.g. the old 500) is the **worst** choice:

- The stream (200 events) never fills a 500-wide bucket, so compaction never
  runs and — far more importantly — the single feed page `[0, 500)` **never
  completes**. `headVersion` reaches 200, never ≥ 500, so `complete` is always
  false and the page is always `no-store`. **Nothing is ever cacheable**, and a
  polling client re-downloads the entire growing tail on every catch-up. This
  is the most expensive possible configuration for a poll-read stream.

A *narrow* N fixes both halves:

- **Pages freeze fast.** At ~9 s/event, a page of width N freezes ~`9·N`
  seconds after it starts. At N = 20 that is ~3 minutes. The bulk of the
  3-reads-per-event that occur after a page freezes are served from cache, at
  zero origin cost.
- **The re-downloaded live tail stays small.** A polling client re-fetches at
  most the current ≤ N-event tail, not a page that has been growing for the
  whole stream.

N = 20 sits at the low end deliberately: for a poll-read workload the dominant
avoidable cost is redundant re-download of the `no-store` tail, and a small N
bounds it while freezing history into immutable cache every few minutes. The
counter-costs of a small N at this scale are all negligible:

| Counter-cost of small N | Magnitude at 200 events, N = 20 |
|---|---|
| Extra chunk PUTs/DELETEs (Class A) | ~10 ops over the stream's life |
| Compaction GETs | ~200 total regardless of N (each commit read once) |
| Pages on a cold full replay | ~10 immutable, edge-shareable page GETs |
| `c/` LIST entries | ~10 keys — one LIST page |

**When the default is wrong for you:** if your streams are long-lived and
read primarily as deep replays (not polled), or if you make large atomic
commits, N = 20 is too small — see the general case below.

---

## Choosing N in the general case

### The cost model

On pure request cost, **larger N is cheaper to replay**:

| Cost | Effect of larger N |
|---|---|
| Deep-replay GETs (Class B) | ⬇️ a replay is `ceil(events/N)` chunk GETs |
| Compaction write ops (Class A) | ⬇️ chunk count = `events/N` |
| Compaction read GETs | ➖ ~`events` regardless of N |
| Storage bytes | ➖ essentially flat |
| Feed-page cacheability under polling | ⬆️ **wider N freezes slower** — worse for poll-read, growing `no-store` tail |

So replay-heavy streams want N *large*; poll-read streams want N *small enough
to freeze pages quickly*. These pull in opposite directions — pick N for the
dominant read pattern of the stream.

### The upper bound is the compactor's runtime, not cost

N is capped by wherever compaction runs. Compaction assembles one whole bucket
in memory and issues ~N GETs + 1 PUT + 1 batched DELETE per chunk. On
Cloudflare Workers (the primary target):

- **Subrequest budget.** Compaction runs in `waitUntil`, sharing the request's
  ~1,000-subrequest budget; one chunk ≈ N GETs. Keep N well under that ceiling.
- **Wall clock.** With the ~6-way connection cap, N GETs run in ~`N/6` waves
  inside the ~30 s `waitUntil` allowance.
- **Memory.** The compactor buffers the whole bucket: **byteCap × N ≤ memory
  budget.** Against Workers' 128 MB, a large N forces a small per-commit byte
  cap (N = 500 implies ~128 KB/commit; N = 20 leaves far more headroom).

`compactStream` compacts **at most one bucket per invocation** — a backlog of
sealed buckets drains across subsequent appends rather than in one shot.

### Other constraints and coupling

- **N is the per-commit event cap.** An atomic append of more than N events is
  rejected (`src/store.ts:424`). If you make large multi-event commits, N must
  be at least as large as your biggest atomic append. **At the default of 20,
  no atomic commit may exceed 20 events.**
- **N vs stream length.** Compaction only triggers once a bucket seals
  (`src/store.ts:621`). Set N *below* your typical long-stream length or hot
  streams never get packed. Streams shorter than N simply never compact — cheap
  and fine, but their whole feed is a single page that never freezes.
- **Changing N is a full recompaction.** Chunk keys and `immutable`-cached page
  URLs both derive from N; changing it under existing data orphans every cached
  page and requires rewriting every chunk. Choose once per prefix.
- **Page width should equal N.** The helpers default `pageSize` to the store's
  N so pages are chunk-aligned; a misaligned page straddles two chunks and
  costs an extra GET on a cache miss. Only override `pageSize` deliberately, and
  keep it fixed forever per prefix (`src/http.ts:26`–`35`).

### A rule of thumb

- **Poll-read, short streams** (the default's target): small N (10–25). Freeze
  pages within minutes; bound the re-downloaded live tail.
- **Replay-read, long streams**: large N (hundreds–thousands), as large as the
  compactor's memory (`byteCap × N`) and subrequest budget safely allow, but
  below typical stream length.
- **Large atomic commits**: N ≥ your biggest atomic append, whatever the read
  pattern.

When the dominant read pattern is unclear, favour the smaller N: it is cheap to
serve a few extra immutable pages on replay, but expensive to re-serve a wide
`no-store` tail to pollers.

---

## Per-stream N: routing by store

N is a **store-level** constant — captured once in `createEventStore`
(`src/store.ts:107`) and read by every actor that touches a stream: the
per-commit cap and compaction trigger on append, the `bucketBase(base, N)` key
arithmetic in compaction (`src/keys.ts:64`), the `lastChunkBase + N` watermark
on read, and the page width in the HTTP helpers (`src/http.ts:86`). There is no
per-stream `chunkSize` parameter.

The reason it is a single constant is correctness, not convenience:
**uncoordinated compactors must compute identical chunk keys for the same
bucket.** That only holds if every actor touching a given stream agrees on that
stream's N — forever. Two actors using different N on the *same* stream produce
divergent chunk keys and orphan commits (data loss).

You do not need arbitrary per-stream N to get most of the benefit, though. If
your streams fall into a few classes — poll-read vs replay-read, small vs large
atomic commits — run **one store instance per class** and route by stream:

```ts
// Keys are namespaced per stream ({prefix}/streams/{streamId}/…), so two
// stores with different N never collide as long as they own DISJOINT streams.
const fastChurn = createEventStore({ driver, prefix: "prod", chunkSize: 20 });   // poll-read
const archival  = createEventStore({ driver, prefix: "prod", chunkSize: 1000 }); // replay-read

function storeFor(streamId: string) {
  return isArchival(streamId) ? archival : fastChurn;
}

// Every access — append, read, compact, page, sweep — goes through storeFor().
await storeFor(id).append(id, events, opts);
```

This is safe with no library change because chunk keys and page URLs live under
`{prefix}/streams/{streamId}/`, so disjoint stream sets never share a key even
under the same bucket and prefix. The **entire** burden is one invariant:

> A stream's N is fixed at creation, and **every** actor — append, read,
> compaction, HTTP paging, and the shred sweeper — must resolve the *same*
> store (same N) for that stream, forever.

Make `storeFor` (or an equivalent deterministic mapping) the single source of
truth for that routing, and use it everywhere a stream is touched. The failure
mode is unforgiving: route one stream to the wrong N once — including from a
background compaction or sweep job that resolves the mapping differently from
the request path — and that stream's chunk keys diverge and it corrupts. Prefer
a mapping that is a pure function of the `streamId` (a prefix convention, an id
namespace) over an external lookup that could drift or be unavailable to a
background worker.

**When routing-by-store is not enough:** if you need genuinely arbitrary N per
stream (not a handful of classes), the store approach does not scale — you would
be managing an unbounded set of instances. That calls for a true per-stream N:
threading N through every entry point keyed by `streamId`, plus a durable,
create-only, strongly-consistent `streamId → N` record (e.g. a per-stream
`meta.json` read on the cold path) that every uncoordinated actor consults
before touching the stream. That is a design change with a sharp failure mode,
not a config tweak — reach for it only when the class-based split above
genuinely cannot express your workload.

---

## Polling: long vs short, by platform

Feed cacheability (tuned by N) governs the *body* cost of reads. The other half
of a poll-read workload is the **head-poll count** — how often clients ask "has
anything changed?" That is independent of N, and how you serve it is a
deployment-layer choice with a sharp platform dependency.

### The two models

- **Short poll.** The client sends `GET …/head` on an interval. The server
  answers immediately — usually a `304 Not Modified` via the version-derived
  ETag (`"v{head}"`), no body. The client learns of a new event up to one
  interval late, and you pay a request round-trip + a head-resolution (hint GET
  + short LIST + anchor GET) on **every** tick, even when nothing moved.
- **Long poll.** The client sends one request with a client-named timeout. The
  server **holds it open**, re-resolving the head internally on a short cadence,
  and responds the instant the head advances (or when the timeout elapses). The
  client learns of a new event within milliseconds, then immediately
  reconnects.

Long poll is the optional middle rung between short polling and a true push
channel (SSE / WebSockets via Durable Objects). It is deployment-layer code you
own in the worker — the library ships the head resource and its ETag; whether
your handler answers immediately or holds and re-resolves is up to you.

### What long poll does and does not save

| | Short poll | Long poll |
|---|---|---|
| Client round-trips / invocations (30 s idle, 2 s cadence) | ~15 | **1** |
| S3/R2 head-resolutions | ~15 | ~15 (server re-checks at the same cadence) |
| Update latency | up to one interval | ~instant |

Long poll cuts **invocation count and latency**. It does **not** cut
head-resolution cost — the server still re-resolves the head at roughly the
polling cadence, so the number of bucket reads is about the same. If S3/R2
head-resolution GETs are your cost floor, the levers are poll interval, N
alignment, and **edge micro-caching the head** (below) — not long polling.

### The platform trade-off

A held-open request occupies an execution context for its whole lifetime.
Whether that is "more resources" depends entirely on how the platform bills:

| Platform | Billing model | Long poll verdict |
|---|---|---|
| **Cloudflare Workers** | CPU time; idle `await` is nearly free | **Cheaper.** One invocation replaces ~15; the idle wait between head-checks costs almost no CPU. Same S3 cost, better latency, no Durable Objects. |
| **AWS Lambda** | Wall-clock GB-seconds | **More expensive.** One 25–30 s held-open invocation is billed for its full duration — roughly 20× the wall-clock of the short polls it replaces. Prefer short polling. |
| **Long-running server / container** | Fixed capacity; concurrency-bound | Depends on connection ceiling. Thousands of parked requests tie up threads/memory (the classic C10K concern) unless the runtime is built for it (async I/O, event loop). |

### Collapsing fan-out: micro-cache the head

Poll interval and long poll both optimize *one client's* head-poll count. Neither
touches the case that usually dominates a hot stream: **many clients polling the
same head at once.** Every tick from every client is a distinct origin
head-resolution (hint GET + short LIST + anchor GET), even though they would all
get the identical answer.

A short **shared-cache TTL on `GET …/head`** collapses them. The reference
handler serves the head with `Cache-Control: s-maxage=1, max-age=0` (README,
*Serving the feed*): a CDN serves every poll landing in the same ~1 s window
from one origin resolution, while `max-age=0` keeps browsers revalidating so no
client pins a stale head locally. Plain `no-cache` will not do this — it is
storable but revalidate-always, so every poll still reaches origin; only a
freshness lifetime collapses the fan-out. This is the only lever that scales
*down* with fan-out instead of up: 100 clients at a 2 s cadence cost ~50 origin
resolutions/s without it, ~1/s with it (with origin shielding — otherwise ~1/s
per edge PoP).

- **Orthogonal to the rest.** Interval and long poll cut *per-client* ticks; N
  tunes the *body*; micro-caching cuts *origin head-resolutions under fan-out*.
  Independent knobs.
- **Latency cost** is the TTL, added on top of the poll interval — at 1 s it sits
  inside the "few seconds of lag" this store already trades for (see the README
  preamble).
- **Safe by construction.** The cache sits on the read/subscribe path only. The
  append concurrency check never consults an HTTP-cached head — writers resolve
  the head through the in-process cache and the protocol's own step-4 chunk
  check ([store.ts](src/store.ts)) — so a stale-low cached head can only make a
  *reader* lag by the TTL; it can never admit a bad append.
- **Platform-neutral.** Cloudflare Cache (with Tiered Cache for origin shielding)
  and CloudFront both honor `s-maxage`. The one constraint: a shared cache keys on
  URL and **cannot vary on `Authorization`**, so an authenticated head must be
  authorized *before* the cache lookup and cached under a content-only (or
  scope-embedded) key — never the bearer token. On Workers that is the Cache API
  inside the handler you already run; on AWS it is a CloudFront-Function /
  Lambda@Edge check or a signed URL. (Same auth-scoped-key rule as the plaintext
  feed pages — see [DESIGN.md](DESIGN.md).)
- **When it buys nothing:** one poller per stream (a per-user stream with a single
  reader). The saving is proportional to concurrent readers of the *same* stream.

**Practical guidance:**

- **Cloudflare Workers + R2** (this library's home turf): long poll is a good
  middle rung — fewer invocations, near-instant latency, no extra
  infrastructure. It will not reduce your head-resolution GETs; tune those with
  poll cadence, N, and — under concurrent readers — head micro-caching.
- **Lambda or wall-clock-billed compute**: short poll. Long poll inverts the
  economics — you pay for idle held-open time.
- **Either way**, the dominant cost in a polling system is usually the
  head-poll count, not the feed body. Poll interval and long poll optimize *one
  client's* count; micro-caching the head optimizes it across *concurrent*
  clients; N optimizes the body. They are orthogonal — tune all three.
