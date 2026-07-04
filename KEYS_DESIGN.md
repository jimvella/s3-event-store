# s3-event-store — Key Management & Erasure

Companion to [DESIGN.md](DESIGN.md), covering client-side encryption, key
management, GDPR erasure, and key delivery for read model B. The dependency
is one-way: this layer builds on the core store through the seams listed in
DESIGN.md's **Encryption & erasure contract** — encryption is a serializer,
`keyId` is a reserved opaque plaintext envelope field, compaction copies
ciphertext verbatim, identifiers carry no PII, immutable objects and
no-purge edge caches are permanent facts, and every cache the core
introduces declares a bounded TTL. Nothing here changes the core protocol.

## Erasure: crypto-shredding

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
  keys in-process; a shredded key remains readable until caches expire. Bound
  and document the TTL (GDPR's ~30-day window makes minutes/hours fine).
  Edge plaintext caches and client-delivered keys join the same budget —
  shred delay = the max of all such TTLs (see Shred propagation rule below).
  Writers get a much shorter leash: appends re-check the tombstone on a
  separate negative-cache TTL and fail closed (see Shred protocol) — a
  cached key must never keep *minting* ciphertext under a shredded
  generation for the full read-side TTL.
- **Key loss is total data loss.** The key store is small but critical,
  stateful infrastructure — the one exception to "no database besides S3."
  (It can itself be S3 objects with versioning off, wrapped by a master key.)
  Per-key deletion is soft-delete-then-hard-delete: the shred tombstone is
  the soft delete (instant unreadability), and hard deletion happens only
  after a waiting period, driven by the sweeper (see Shred protocol).
- **Erasure completeness is a data-modeling obligation.** Shredding a
  subject deletes that subject's keys, so erasure reaches exactly the
  streams encrypted under them — no further. Personal data written into a
  stream keyed to a different subject (a customer's address inside an
  order stream carrying the order's key) survives the customer's shred
  untouched. The rule, as loud as the no-PII-identifier rule: **every
  stream containing a subject's personal data must be encrypted under that
  subject's key(s)** — which makes the key store's subject→key mapping the
  authoritative map of where a subject's data lives, and is what makes the
  shred protocol's "enumerate the subject's generations" complete. The
  library cannot enforce this; it is an application contract, and a miss
  silently voids the erasure guarantee for the leaked data.

## What GDPR actually requires

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

Key-cache TTL (above) bounds shred propagation, but the Article 17 clock
is satisfied only at **hard delete** — cache TTLs, the soft-delete waiting
period, and the sweep cadence must fit inside the month together (see
Shred propagation rule).

## Key store as a separate S3 bucket

A dedicated bucket for wrapped keys is acceptable to regulators and keeps the
"only S3" story intact — but it needs the **inverted configuration** from the
event bucket, which is precisely why it must be a separate bucket (these are
all bucket-level settings, on for events, off for keys):

- **Versioning off — never enabled, not merely suspended** — a versioned
  delete is just a delete marker; the key would remain readable via old
  versions. Suspension doesn't help: a suspended bucket retains every
  version created while versioning was on, and deleting the current
  version still leaves the old ones readable.
- **No Object Lock** — erasure requires the ability to delete.
- **No replication** — S3 replication does not propagate permanent deletions;
  a replica would silently retain shredded keys forever.
- **No backups that outlive the erasure deadline** — either rely on S3's
  11-nines durability with no backups, or cap backup retention below ~30 days
  so shredded keys age out before the compliance clock expires. Accidental-
  deletion protection comes from the tombstone waiting period (see Shred
  protocol), not backups.
- **Keys stored wrapped** by a master key (AWS KMS, or a configured secret on
  R2, which has no KMS) — a bucket leak then discloses nothing.
- **Strict IAM separation + audit logging** — ideally a separate account; the
  principal that reads event ciphertext must not enumerate keys, and key
  deletions need an audit trail you can show a regulator.

**Decision**: a dedicated bucket (as configured above) is the **default
key-store backend**. The library defines the `KeyStore` interface, ships the
S3-bucket implementation as the default, and verifies the settings above at
startup — fail fast unless `GetBucketVersioning` returns the never-enabled
empty state (**`Suspended` fails too**, per the versioning bullet above;
and since a bucket cannot be un-versioned, remediation is migrating the
keys to a fresh bucket) and replication is absent.
KMS- or DynamoDB-backed stores remain possible via the interface but are not
shipped in v1.

## Key rotation

Immutability constrains rotation sharply, so name the operations precisely —
there are three candidates, and only two exist:

- **Master-key re-wrap.** Unwrap-and-rewrap each wrapped key object in the
  key bucket — a mutation of the one mutable store. Ciphertext, caches, and
  compaction never notice. This is what envelope encryption is for (KMS
  does it transparently). One rule keeps it honest: re-wrap is a
  read-modify-write, so the write-back must be CAS — `PUT` with `If-Match`
  on the ETag it read; on 412 or 404 skip the key, never retry blind (it
  was shredded or concurrently rewrapped underneath us). A blind write-back
  racing a shred would recreate the deleted key object: a shredded key
  resurrected after `ShredCompleted` was appended — the worst erasure
  failure available. With that one rule, routine hygiene.
- **Data-key rotation = appending a generation.** The key is never
  replaced; the keyring grows. Each event's plaintext envelope metadata
  carries an opaque `keyId` (a generation identifier — outside the
  encryption boundary, same no-PII rule as stream IDs). Rotation mints
  generation n+1 in the key bucket (tombstone-guarded — see Shred
  protocol); subsequent appends encrypt under it; history stays under its
  original generations forever. Nothing immutable is rewritten: cached
  ciphertext pages stay byte-identical (new pages simply reference the new
  keyId), compaction's verbatim-copy rule holds (the keyId travels inside
  the envelope it copies), and shredding a subject deletes **all**
  generations. `KeyRotated` joins the audit-stream vocabulary.
- **Re-encrypting history is not rotation — it's migration.** Ciphertext is
  bound to its key; making old events decrypt under a new key means
  rewriting bytes, which collides with every immutability promise at once:
  commit and chunk objects change content, and `immutable`-marked pages at
  unchanged URLs keep serving the old ciphertext from edge caches that
  cannot be reliably purged. If a cipher deprecation ever forces it, copy
  the stream into a new prefix under new keys — new URLs, new cache
  entries — and retire the old one.

What rotation buys — and doesn't: it protects the **future**, never the
past. A leaked generation-n key still decrypts all pre-rotation history;
that ciphertext is immutable and edge-cached forever. This is the same
accepted fact as model B's "TTL is an authorization window, not disclosure
control" (see Key delivery below) — a past reader could have retained
plaintext anyway. The residual risk is bounded by a rule already in force:
ciphertext is access-controlled, not public-but-encrypted (DESIGN.md's read
path enforces this), so a leaked key alone is useless without also passing
the worker gate. Rotation as revocation of future access works; rotation as
retroactive protection is a promise no immutable store can make — document
it in those words.

## Key audit trail

Three layers with distinct jobs — don't collapse them:

| Layer | Role | Why |
|-------|------|-----|
| Mutable key objects (key bucket) | **State** | Keys must be truly deletable — the one requirement antithetical to event sourcing. A key "aggregate" would embed wrapped key material in immutable events; shredding would then mean rewriting history. Key state is therefore never event-sourced. |
| CloudTrail data events on the key bucket | **Evidence** | The regulator-facing trail must be independent of library correctness: captured by the platform regardless of what our code does, tamper-evident, IAM-separated by construction. A self-written trail, stored in a bucket where the compactor holds delete rights, proves little. (R2 caveat: Cloudflare audit logs are less granular — document this as a compliance gap to assess per deployment.) |
| `$system.key-audit` event stream | **Observability** | An ordinary stream in the event store recording key lifecycle (`KeyCreated`, `KeyRotated`, `ShredRequested`, `ShredCancelled`, `ShredCompleted`) so existing read/projection tooling answers "all shreds last quarter" for free. A convenience projection of key lifecycle, not the compliance record. |

Rules for the audit stream:

1. **No key material, ever** — key IDs, timestamps, actor, reason only.
2. **Opaque subject identifiers only.** Proof-of-erasure records are lawful to
   retain (Article 17(3) legal-obligation carve-out) — but only if the subject
   reference is an opaque ID. A PII-bearing audit record would itself need
   shredding, recording which… is a circle.
3. **Plaintext payloads.** No PII → nothing to shred → no dependency on the
   key store it is auditing.
4. **Reserved name.** `$system.key-audit` sits in the `$`-reserved stream
   namespace (DESIGN.md's key-layout rules): slash-free like every stream
   ID — namespacing lives in the prefix, hence one audit stream *per
   prefix* — and appended only by library-internal writers (shred workflow,
   rotation). The REST surface rejects externally supplied `$`-IDs.

## Shred protocol

Shredding a subject deletes **every** generation of its keys — and it races
the two writers that mint new ones: rotation, and an append lazily minting
a first generation. A bare enumerate-then-delete can append `ShredCompleted`
while a freshly minted generation lives on. The protocol is intent-first
and tombstone-guarded — and the tombstone is a small **state machine**,
never a bare marker. Its body carries a `state` — `pending` →
`committing` (sweeper) or `pending` → `cancelled` (cancellation) — and
every transition is a CAS `PUT` with `If-Match` on the ETag the writer
read. Overwrite is the only mutation: the driver interface (and S3
itself) offers conditional PUT but no conditional DELETE, so expressing
cancellation as deletion would reintroduce exactly the races the CAS
exists to close. A tombstone object, once created, is never deleted.
`pending` and `committing` are the **soft-deleted** states; `cancelled`
(or no tombstone) means live.

1. Append `ShredRequested` to `$system.key-audit`.
2. Write the per-subject **tombstone** in the key bucket, state
   `pending` (`If-None-Match: *`). If one already exists, read it:
   `pending` or `committing` means a shred is already in flight —
   proceed, the remaining steps are idempotent; `cancelled` means a
   prior shred was reversed — reopen it with a CAS
   `cancelled → pending`. Contents: opaque subject ID, state,
   timestamp, the intent's audit-stream position, no key material —
   lawful to retain under Article 17(3), and the fail-closed signal
   that this subject may never silently re-key. **The tombstone is the
   soft delete**: keyring reads, key delivery, and appends fail closed
   immediately (unreadability propagates within the cache-TTL budget),
   while the generations stay physically in place — recoverable only by
   audited cancellation (below) — and the tombstone's timestamp starts
   the waiting period.
3. **After the waiting period** — executed by the sweeper, never by the
   initiating request — CAS the tombstone `pending → committing`. This
   is the **commit point**: cancellation and hard delete race for one
   CAS on one ETag, so exactly one wins. On 412, re-read — `cancelled`:
   the intent closed under us, stand down; `committing`: a concurrent
   sweeper run won, proceed idempotently. Only after winning, enumerate
   the subject's generations and delete them. This is the **hard
   delete**: the point of irrecoverability, where Article 17 is
   satisfied. A `committing` tombstone is never transitioned again — it
   is the permanent never-re-key signal, mechanism included.
   Hard-delete rights on the key bucket can be confined to the
   sweeper's principal — one identity destroys long-lived keys, and it
   only acts on an expired, audited intent it has visibly committed.
4. Re-list to confirm empty — if a crashed minter's stray landed
   mid-shred the re-list is non-empty: delete it and re-list again
   (terminates, because post-tombstone mints self-delete: the sweep
   runs entirely inside `committing`, a state with no outbound
   transition, so every mint's re-check during it must observe the
   tombstone) — then append `ShredCompleted`.

Three rules make the tombstone airtight rather than advisory:

- **Minting pairs with it check → write → re-check** (the same shape as
  append's step-4 chunk verification): read the tombstone (must be
  absent or `cancelled`), `PUT` the generation (`If-None-Match: *`),
  read the tombstone again — if a soft-deleted state appeared, delete
  the just-minted generation and fail the mint. A mint whose PUT landed
  before step 3's enumeration is deleted by it; one that landed after
  is self-deleting, because the tombstone was durable before
  enumeration began and the re-check must observe it. And the re-check
  cannot be fooled by a concurrent cancellation: a sweep runs entirely
  inside `committing`, which has no outbound transition — the CAS at
  the commit point already decided that race before the first
  generation was destroyed.
- **Keyring reads treat the tombstone as authoritative.** The `KeyStore`
  read path and the key-delivery endpoint return an empty keyring for a
  soft-deleted subject, whatever objects a crashed minter left behind. A
  stray generation (crash between PUT and re-check) is therefore inert —
  it decrypts nothing pre-shred (generations only move forward) and is
  never delivered — and gets deleted as hygiene whenever a sweep next
  observes it.
- **Appends consult the tombstone before encrypting.** The append
  path's key lookup goes through the same tombstone-authoritative read
  path — a cached *unwrapped key* may live out its documented TTL, but
  tombstone freshness is bounded separately, by a short **negative-cache
  TTL** (minutes, not hours; it joins the propagation budget below). A
  soft-deleted tombstone fails the append with a typed error
  (`SubjectErasedError`) before any PUT — the semantically correct
  outcome, not merely a safeguard: after erasure the application is
  obligated to stop writing the subject's personal data at all. Without
  this rule a writer holding a cached key keeps encrypting new events
  under a doomed generation for the full key-cache TTL — unreadable at
  birth, reported as success, the exact silent-loss-as-success class
  the core append protocol exists to kill. One residual window survives
  and is accepted: an append already in flight when the tombstone lands
  cannot be retracted (commits are immutable — check → write → re-check
  has no delete step here), so it lands encrypted under a generation
  the sweep will destroy. Erasure is unweakened — those events are
  cryptographically erased exactly as if they had landed a millisecond
  before the tombstone — and the *next* append fails loudly; only the
  doomed writer's success report is wrong, for one in-flight request
  rather than a cache lifetime.

**Cancellation — the waiting period's purpose.** Until the commit
point, a shred can be reversed — deliberately, never silently: append
`ShredCancelled` to `$system.key-audit`, *then* CAS the tombstone
`pending → cancelled`. That order is fail-safe: a crash between the two
leaves a cancelled-but-still-`pending` subject — visible, still
unreadable, and completed by the sweeper's reconciliation rule (see
Shred sweeper) — rather than an unaudited recovery or a silent re-key.
Against the deadline, cancellation is not a fuzzy race but a decided
one: once the waiting period expires the sweeper may CAS
`pending → committing` at any moment, and whichever transition wins the
ETag, the other 412s. A cancellation that loses observes `committing`
and reports the shred as committed — it can never find itself
half-recovered over generations already being destroyed. Cancel well
before the deadline, or expect to lose the CAS. Cancelled tombstones
persist (one small object per subject ever shredded or cancelled —
negligible, and a free local record of the subject's erasure state); a
later shred reopens one via step 2's `cancelled → pending` CAS.

A crash anywhere leaves a visible dangling intent — never a silent,
unaudited shred. The initiating request performs only steps 1–2 (intent +
instant unreadability); the sweeper owns everything after, resuming
incomplete shreds, executing due hard deletes, and completing crashed
cancellations by scanning for open intents — a `ShredRequested` with
neither `ShredCompleted` nor `ShredCancelled` (see Shred sweeper below).
Steps 2–4 are idempotent under resume.

## Whole-payload vs. field-level encryption

**Whole-payload** (encrypt the entire `data` blob; envelope metadata stays
plaintext) is the v1 choice:

- Simple and uniform — no schema knowledge needed; impossible to "forget a
  field."
- Fixed overhead: one nonce + auth tag (~28 bytes) per event; compress before
  encrypting (ciphertext doesn't compress).
- **Nonce discipline** — the easy-to-get-wrong of AES-GCM, so it's a rule,
  not an implementation detail: many uncoordinated writers (concurrent
  worker invocations) encrypt under the *same* generation key, so nonces
  are **96-bit random, fresh per encryption — never counters, timestamps,
  or anything coordinated** (sequences collide across writers, and GCM
  nonce reuse is catastrophic: key-stream reuse plus forgeable auth
  tags). Random nonces bound safe use to ~2³² encryptions per key (NIST
  collision margin); treat that ceiling as an input to rotation cadence —
  mint a new generation long before any key approaches it.
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
the whole-payload encrypting serializer + key store ship in roadmap phase 3
(see DESIGN.md's Roadmap). Field-level is phase 5 with fail-closed defaults,
only if real demand shows up — it's the kind of API that's expensive to get
wrong.

## Key delivery (read model B)

DESIGN.md's two read models decide *who* decrypts; the A/B comparison and
its caching rules live there. The key half of model B lives here:

- The key endpoint (`GET /{prefix...}/streams/{id}/key`) delivers a
  **keyring** — every generation the caller is authorized for, each with
  its `keyId` and TTL — and the client picks per-event by the envelope's
  `keyId`. "Key = capability for the whole stream" reads precisely as: the
  keyring is the capability, and rotation ends its future — an old keyring
  keeps reading pre-rotation history (which its holder could have retained
  anyway) but nothing after. A soft-deleted subject's keyring is empty (see
  Shred protocol).
- A delivered key's TTL is an **authorization window, not disclosure
  control** — a misbehaving client can retain keys or plaintext forever (as
  it could retain plaintext under model A). GDPR erasure covers *our*
  systems; Article 17(2)'s inform-recipients duty can be driven from the
  `$system.key-audit` stream.
- Keyring size grows by one entry per rotation and is unbounded in
  principle; at any sane rotation cadence it is noise. If a pathological
  cadence ever makes it matter, cap delivery to the generations spanning
  the caller's read window.

## Shred propagation rule

Two clocks, two budgets:

- **Unreadability** (starts at the tombstone): delay = the **maximum of
  all plaintext- and key-cache TTLs** — worker key caches, edge plaintext
  TTLs (model A), client-delivered keyring TTLs (model B). One documented
  number. DESIGN.md's contract obliges every cache the core and
  deployment introduce to declare a bounded TTL; this budget is where
  they land. The **write side** has its own, shorter line in the same
  budget: appends fail closed within the tombstone negative-cache TTL
  (see the third tombstone rule), plus at most one in-flight append that
  lands doomed — erased by the shred like everything before it.
- **Irrecoverability** (the clock Article 17 counts): soft-delete waiting
  period + sweep cadence + retry slack, ending at the hard delete that
  `ShredCompleted` records.

Erasure is complete at the later of the two; keep the total well inside
the ~30-day window (a 14-day waiting period, daily sweep, and hour-scale
cache TTLs leave half the month as slack).

## Shred sweeper

With compaction write-driven, this is the one clock-driven job in the
system (cron): it scans `$system.key-audit` for open `ShredRequested`
intents (no matching `ShredCompleted` or `ShredCancelled`) and drives each
to completion — ensuring the tombstone exists (step 2, if the initiator
crashed first), then executing hard delete and confirmation (steps 3–4)
once the intent's waiting period expires; all idempotent under resume.
One reconciliation rule joins the scan: an intent closed by
`ShredCancelled` whose tombstone still reads `pending` — matched by the
audit position recorded in the tombstone body, so a stale scan can never
touch a newer intent's tombstone — is a cancellation that crashed
between its append and its CAS; the sweeper completes the
`pending → cancelled` transition on its behalf. If that CAS 412s against
`committing`, a mid-flight sweep had already won the commit point before
the cancellation was appended; the audit trail then truthfully reads
Cancelled-then-Completed — the cancellation was requested, and it lost.
The clock is not incidental but the design: every hard delete is
sweeper-executed, so the waiting period needs no scheduler beyond the job
that already exists, and a crashed shred is just an open intent the next
run picks up. Erasure correctness has a deadline, hence cron rather than
opportunistic triggering. It checkpoints like any
catch-up reader (a cursor object, CAS-updated), carrying still-open intents
forward in its checkpoint state — each run scans only audit events since
the last, never the whole stream. One sweeper per store: each prefix has
its own `$system.key-audit` stream.
