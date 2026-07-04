# Deterministic simulation harness — implementation plan

Companion to [DESIGN.md](DESIGN.md)'s Tests section, which names the
simulation suite the highest-leverage suite: most of the design's
correctness lives in race windows an integration suite can only sample.
This plan turns that requirement into an implementable architecture,
informed by a code reading (2026-07) of S2's deterministic simulation
harness ([s2/sim](https://github.com/s2-streamstore/s2/tree/main/sim),
MIT-licensed), which DESIGN.md's prior-art survey flagged as worth reading
before building ours.

## What s2-sim does, and what transfers

s2-sim runs three hosts on [turmoil](https://github.com/tokio-rs/turmoil)'s
simulated network: a mock S3 (in-memory `BTreeMap` behind the `s3s`
wire-protocol crate), the real s2-lite server pointed at it, and a workload
driving SDK clients. Determinism comes from a seeded global RNG, libc-level
shadowed clocks, tokio's seeded internal RNG, and log timestamps in
simulated time. Faults are injected as network message loss. Histories of
operation start/finish events go to JSONL for offline linearizability
checking (Porcupine); a *meta test* runs the same seed twice and requires
byte-identical output.

**Adopted directly** (concepts, no code — it's Rust, we're TypeScript):

1. **Seed-replayable everything.** One seed determines the whole run;
   every failure message prints the seed; replaying the seed reproduces
   the schedule exactly.
2. **The determinism meta test.** Run the same seed twice, require
   identical traces. Determinism rots silently (someone adds a `Date.now()`
   or iterates a nondeterministically-ordered map); this is the cheap
   tripwire. s2 compares child-process bytes; we compare in-process trace
   hashes, same idea.
3. **Mock-S3 semantic details** worth stealing from their `s3.rs`:
   content-hash ETags; conditional-header evaluation (`If-None-Match`
   checked before `If-Match`, condition-vs-absent-object truth table);
   LIST continuation token = last key consumed, resume strictly after
   `max(continuationToken, startAfter)`; idempotent DELETE; loud
   `NotImplemented` for any operation the model doesn't cover, so new
   library behavior can't silently run against unmodeled semantics.
4. **Indefinite-outcome bookkeeping.** An append whose response is lost is
   *maybe applied*; s2 defers those history events until all clients
   finish and the checker treats them as optional. Our oracle needs the
   same three-valued outcome (committed / rejected / indefinite).
5. **Bounded workload retries.** A wedged system must surface as a
   deterministic failure, not an infinite retry loop — workload ops get
   small retry budgets; only provisioning gets a generous one.
6. **Simulated-time log stamps** (step counter + event ordinal, never wall
   clock), which is what makes traces comparable across runs.
7. **A validating lesson**: slatedb stamps each PUT with a write-ID and,
   after a lost response, re-reads to check whether the write was its own
   — their sim caught a mock that dropped the metadata this relies on.
   That is exactly our `commitId` self-check (append step 3); the
   simulator must model lost responses as *applied but undelivered* or the
   protocol's central disambiguation mechanism goes untested.

**Deliberately different:**

- **No network layer.** s2 must simulate TCP because the system under
  test is a server behind HTTP. Ours is a library whose every interaction
  with the world goes through the storage-driver interface (`get` /
  `putIfAbsent` / `putIfMatch` / `list` / `delete` / `deleteMany`). In
  production, concurrent writers interleave at S3-request granularity —
  so a scheduler that interleaves *driver operations* captures every
  ordering that can occur in reality, with none of the HTTP plumbing and
  orders of magnitude more schedules per CPU-second.
- **Online model-based checker instead of offline Porcupine.** General
  linearizability checking is NP-hard and needed when the model is a
  generic KV store. Our model is much stronger: a per-stream CAS'd log.
  The oracle can therefore be checked online and exactly (see Oracle
  below), and — something a black-box history checker cannot do — we can
  also assert *storage-level* invariants ("every committed event is
  readable from at least one object") after every single mutation.
- **Fault model at the op boundary, not the wire.** Message-loss
  probability is the right knob for a network; ours are per-op: delayed
  apply, lost response, injected error, actor stall (the GC-pause /
  SDK-backoff window the design worries about), actor crash. A global
  `failRate` knob survives as a convenience multiplier.

## Architecture

Four layers, all under `sim/` in the package (test-only, not shipped):

```
sim/
  rng.ts          seeded PRNG (PCG32 or xoshiro128**) — the only randomness
  scheduler.ts    cooperative task runtime + virtual clock + trace recorder
  store.ts        SimStore: the S3 semantic model
  driver.ts       SimDriver: storage-driver interface over SimStore + scheduler
  faults.ts       fault plans: probabilistic knobs + scripted injections
  oracle.ts       committed/rejected/indefinite bookkeeping + invariant checks
  actors/         appender, reader, headResolver, compactor, sweeper
  scenarios/      smoke, randomMix, and one file per named race (regressions)
  harness.ts      run(seed, scenario, faults) → Trace; replay/meta helpers
```

### Scheduler

The heart. Actors are plain async functions written against the ordinary
driver interface; they cannot tell they're in a simulation. Every driver
call suspends the actor and registers a **pending op**; the scheduler loop
picks the next op with the seeded PRNG, applies it **atomically** to the
SimStore (matching S3's per-request atomicity), resolves the actor's
promise, and drains microtasks so the actor runs exactly to its next
driver call (or completion) before the next pick. JavaScript's
single-threaded event loop makes this airtight: with no timers and no I/O,
an actor between driver calls is pure synchronous computation, so the
schedule — the sequence of (actor, op) picks — fully determines the run.

Two details are load-bearing:

- **Two-phase ops.** Each op has *apply* (mutate the store, produce the
  response) and *deliver* (resolve the actor's promise) phases the
  scheduler can separate. Lost response = apply without deliver, then fail
  the actor's promise with a timeout error after further steps. This is
  the mechanism behind the retried-conditional-PUT races (`commitId`
  self-check, append step 3) and must exist from day one.
- **LIST pages are separate ops.** A paginated LIST is one op per page,
  holding a continuation token between them — so a compactor can run
  between a reader's `c/` LIST and its `e/` LIST, or between `e/` pages.
  Every LIST-time race in DESIGN.md's failure modes lives in exactly these
  gaps; a LIST modeled as one atomic op would make them unreachable.

Virtual clock: `sleep(ms)` is also a scheduler op; timestamps come from
the scheduler's clock (advancing a deterministic amount per step). No
`Date`, no `Math.random`, no `crypto.randomUUID` anywhere in sim or
library code paths under test — enforced by an ESLint rule scoped to
`sim/` and `src/`, plus a runtime guard in the harness that patches these
to throw during simulation runs.

**Library API implication (phase-1 requirement, discovered here):** the
core library must accept injectable `random`/`ids` and `clock` hooks in
store config (defaulting to WebCrypto and `Date`), or its `commitId`/event
IDs and `committedAt` stamps would be nondeterministic under simulation.
This lands in the driver/store interface work, not as a sim hack.

Scheduling policy: uniform random pick by default, plus optional **PCT
mode** (probabilistic concurrency testing: random actor priorities with
`d` priority-change points) as a cheap, principled way to hit deep
orderings that uniform random rarely finds. Targeted schedules (below)
cover the known races; PCT and uniform sweeps hunt unknown ones.

### SimStore (the S3 model)

An ordered map (`key → {bytes, etag}`) with exactly the semantics
DESIGN.md assumes of a backend, no more:

- Strong read-after-write consistency (trivial in-memory).
- `putIfAbsent` (`If-None-Match: *`): fails 412 if the key exists.
- `putIfMatch` (`If-Match: <etag>`): fails 412 unless etags match.
- `get` with optional `ifMatch`: 412 on mismatch, 404 on absence.
- `list(prefix, startAfter)`: lexicographic pages (page size configurable
  and deliberately small — 3–10 in sim, vs 1000 in production — so
  pagination races occur constantly), each key with its etag,
  continuation token per the s2 rule.
- `delete` / `deleteMany`: idempotent; **deleting frees the key** — a
  subsequent `putIfAbsent` succeeds. This models the versioned-bucket
  delete-marker behavior the design's freed-key hazard depends on
  (DELETE writes a marker; create-only PUT then sees no current version).
  No separate unversioned mode: for every property we check, the two are
  equivalent, and this is the adversarial one.
- **ETags are content hashes** (e.g. FNV/xxhash of bytes), matching real
  S3's MD5-for-simple-PUT. This is the *weakest* guarantee: two identical
  bodies share an etag, so an ETag pin defeats a substitution only
  because commit bodies always differ (writer-generated `commitId`). The
  simulator must not accidentally strengthen the primitive with
  random-per-write etags, or the pinned-GET rules pass for the wrong
  reason. A dedicated test asserts the simulator itself would *fail* to
  catch a substitution of byte-identical bodies — documenting the
  assumption the protocol actually rests on.
- Anything else → throw `NotImplemented` loudly (the s2 rule).

SimStore doubles as the **conformance reference**: the same conformance
suite that runs against MinIO/S3/R2 in CI runs against SimDriver first.
The sim store is the executable spec of what the library assumes from a
backend; a backend quirk is a divergence from it.

### Faults

A fault plan is data, interpreted at op boundaries:

| Fault | Mechanism | Exercises |
|---|---|---|
| Response loss | apply without deliver | `commitId` self-check, `"any"` retry idempotency, step-3 412-then-GET |
| Actor stall | don't schedule actor for k steps (k up to "a full bucket plus a compaction cycle") | freed-key recreation, append step 4, sweep |
| Actor crash | cancel actor between any two ops | compactor crash between chunk PUT and source deletes; abandoned appenders |
| Injected error | fail deliver with 500/timeout without apply | driver retry paths, bounded workload retries |
| Head-hint damage | scripted store mutation (delete/replace `head.json`) | hint corroboration, derive-from-body rule, cold path |

Global knobs (`failRate`, `stallRate`, seedable per scenario) plus
**scripted injections** keyed to trace predicates ("when actor A has
resolved the head but not yet PUT, run compactor C to completion") for the
named-race regressions.

### Oracle and invariants

The oracle tracks, per stream, the outcome of every append attempt:
**committed** (actor observed success, or lost-response later self-resolved
via `commitId`), **rejected** (`ConcurrencyError` — its `commitId` must
never be readable), or **indefinite** (actor crashed or ended with the
outcome unknown). Checked continuously:

1. **No duplicate versions / no lost events**: committed commits form a
   dense, gapless version sequence per stream; every committed event is
   yielded by a full replay.
2. **No phantom reads**: no reader ever yields a `commitId` in the
   rejected set. (The freed-key-orphan schedules make this the sharpest
   property — a rejected writer's object sitting at a freed key.)
3. **Contiguous prefix**: every read yields versions `fromVersion..k`
   contiguously, event data byte-identical to the oracle's.
4. **No forged heads**: every head resolution returns a value in
   `[max committed version, max committed + indefinite]` at the moment of
   resolution (the scheduler knows the moment exactly).
5. **Storage invariant, checked after every mutation** (cheap in memory):
   every committed event is decodable from at least one live object
   (commit or chunk) — DESIGN.md's compaction invariant, verified at
   every instant rather than sampled.
6. **Quiescent equivalence**: after all actors finish, a final replay
   equals committed events plus a subset of indefinite commits, each
   wholly present or wholly absent (commit atomicity), and each present
   indefinite commit is then reclassified committed and re-checked.

Failures dump the seed, the fault plan, and the full trace (JSONL: step,
actor, op, args-hash, result-hash) as a test artifact; `SIM_SEED=` (env,
mirroring s2's flag) replays it under vitest.

## Scenarios

- **smoke** — one appender, one reader, no faults. The harness's own test.
- **randomMix** — N appenders (mixed `expectedVersion` strategies: exact /
  `"any"` / `"noStream"`, some sharing streams), M readers (cold replays,
  `fromVersion` resumes), compactors triggered both by the write path and
  adversarially, occasional sweeper — under global fault knobs. The
  property-based sweep.
- **Named-race regressions**, one scripted schedule each, written as the
  corresponding library feature lands (the schedule scripts are the
  acceptance tests for phases 1–2):
  - lost-response retry collides with own commit (step 3, key present)
  - lost response + compaction: own `commitId` found in chunk (step 3, 404 branch)
  - orphan at freed key while own commit is in chunk (step 3, foreign-commitId branch)
  - freed-key recreation caught by step-4 chunk check, boundary-straddling
    commit variant (check keys on base, not last version)
  - `"noStream"` on a compacted stream; stale-low `expectedVersion` below
    the watermark
  - stale-high `expectedVersion` rejected by head resolution (the
    no-failing-check corruption)
  - post-LIST substitution caught by pinned GET (listed key compacted,
    freed, recreated before its GET)
  - multi-bucket phantom requiring the *iterated* sealed-bucket check
    (two buckets chunked between `c/` and `e/` LISTs, keys recreated in both)
  - forged head anchor (newest listed commit sealed, chunked, recreated
    between resolver's LIST and anchor GET) caught by pinned anchor
  - damaged/stale-high `head.json` caught by `lastCommitEtag` corroboration
  - compactor crash between chunk PUT and deletes (duplication, reader
    dedupe, sweep cleanup); racing compactors (one 412s and stands down)
  - reader GET-time 404 and LIST-time discontinuity recoveries
- **meta** — run any scenario's seed twice, compare trace hashes
  (determinism tripwire, in CI on every run).

CI: fixed regression seeds + a batch of fresh random seeds per PR (seed
logged on failure); a nightly long sweep with higher fault rates and PCT
scheduling. Shrinking (delta-debugging the schedule's pick sequence) is a
later nice-to-have — seed replay alone already gives exact reproduction,
and scripted regressions capture minimized forms of known races by hand.

## Phasing

| Step | Deliverable | Depends on |
|---|---|---|
| 1 | `rng` + `scheduler` + `SimStore` + `SimDriver`; smoke scenario; meta test | driver interface (roadmap phase 1) |
| 2 | Oracle + invariants 1–4 + randomMix (append/read only); injectable `clock`/`ids` hooks in core config | `append`/`read` |
| 3 | Faults (loss, stall, crash, scripted); append-path regression scenarios (step-3/step-4/head-resolution races) | phase-1 core |
| 4 | Compactor/sweeper actors; storage invariant 5; compaction regression scenarios; conformance suite parity (SimDriver + real backends) | compaction (roadmap phase 2) |
| 5 | CI wiring: PR seeds, nightly sweep, PCT mode, trace artifacts | 1–4 |

## Open questions

1. **Ship SimStore as `./drivers/memory`?** Users would get a free
   in-memory driver for their own tests, but a shipped driver invites
   production use and semantic-drift pressure. Lean no for v1; revisit.
2. **Shrinking** — worth building only if random-sweep failures prove too
   long to read; PCT + scripted regressions may make it unnecessary.
3. **Trace format stability** — whether the JSONL trace is a public
   debugging artifact (documented, versioned) or internal. Internal until
   someone outside the repo needs it.
