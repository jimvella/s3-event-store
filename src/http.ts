/**
 * HTTP read / egress model (DESIGN.md, "HTTP reads").
 *
 * Turns the append-only stream into a sequence of fixed-size, deterministically
 * aligned pages that a plain `fetch` loop can walk via `prev`/`next`. A
 * *complete* page — one the head has advanced strictly past — can never change
 * again: both its events and its `next` link are frozen. That is the
 * machine-readable immutability promise that lets a caller serve it with
 * `Cache-Control: immutable` and cache it at the edge forever. The page that
 * still contains the head (and any page ahead of it) is partial and must not be
 * cached that way.
 *
 * This module is transport-agnostic: it returns page *cursors* (`from`
 * versions), not URLs. Callers own their URL space — see `toWireFeed` for the
 * literal `{ …, prev, next }` wire shape once you supply an `hrefFor` mapping.
 *
 * Ingress counterpart: {@link idempotentAppend}, for deployments whose worker
 * exposes raw append (client-supplied events in) rather than domain commands —
 * it makes a client's retry of a lost response safe.
 */

import { ConcurrencyError } from "./errors.js";
import type { EventStore } from "./store.js";
import type { AppendResult, EventEnvelope, EventInput } from "./types.js";

/**
 * Fallback page width, used only when a store's `chunkSize` is somehow
 * unavailable. Normally the page helpers default to the store's own `chunkSize`
 * so page boundaries align to chunk boundaries automatically — a misaligned
 * page straddles two chunks and costs an extra GET on a cache miss (DESIGN.md,
 * "Compaction and the API"). Pass an explicit `pageSize` only when you
 * deliberately want a width other than the store's N, and keep it fixed forever
 * per prefix: page URLs are cached `immutable`, so changing N orphans them all.
 */
export const DEFAULT_PAGE_SIZE = 100;

/** One page of a stream, with cursor-based navigation. */
export interface FeedPage {
  streamId: string;
  /** Inclusive lower bound of this page's version range (page-aligned). */
  from: number;
  /** Exclusive upper bound: this page covers `[from, to)`. */
  to: number;
  /**
   * True once the head has advanced past this page (to version `to` or beyond),
   * freezing both its events and its `next` link forever — safe to cache as
   * `immutable`. Equivalent to `next !== null`. False for the partial,
   * still-growing page that contains the head, and for any page ahead of it.
   */
  complete: boolean;
  events: EventEnvelope[];
  /** Cursor for the next page, or `null` at the head (no next page yet). */
  next: number | null;
  /** Cursor for the previous page, or `null` on page 0. */
  prev: number | null;
}

export interface ReadPageOptions {
  /** Requested start version; snapped down to the page boundary. Default 0. */
  from?: number;
  /** Page width. Defaults to the store's `chunkSize` (chunk-aligned pages);
   * falls back to {@link DEFAULT_PAGE_SIZE} only if that is unavailable. */
  pageSize?: number;
}

/**
 * Snap an arbitrary version to the start of the page that contains it. A
 * mid-page cursor (`?from=372`, pageSize 100) resolves to 300 — the canonical,
 * cacheable page URL. Serve a 3xx redirect to this when the two differ so every
 * client requests identical URLs (DESIGN.md, "redirect-to-canonical").
 */
export function canonicalFrom(from: number, pageSize: number): number {
  if (!Number.isFinite(from) || from <= 0) return 0;
  return Math.floor(from / pageSize) * pageSize;
}

/**
 * Read a single page of a stream. `from` is snapped to its page boundary; the
 * returned page always covers `[from, to)` on aligned boundaries.
 */
export async function readPage(
  store: EventStore,
  streamId: string,
  opts: ReadPageOptions = {},
): Promise<FeedPage> {
  const pageSize = opts.pageSize ?? store.chunkSize ?? DEFAULT_PAGE_SIZE;
  if (!Number.isInteger(pageSize) || pageSize < 1) {
    throw new RangeError(`pageSize must be a positive integer, got ${pageSize}`);
  }

  const from = canonicalFrom(opts.from ?? 0, pageSize);
  const to = from + pageSize;

  const events: EventEnvelope[] = [];
  for await (const event of store.read(streamId, { fromVersion: from })) {
    if (event.version >= to) break;
    events.push(event);
  }

  const head = await store.resolveHead(streamId);
  const headVersion = head.kind === "head" ? head.version : -1;

  return {
    streamId,
    from,
    to,
    // Immutable only once the head has moved to the next page (version `to` or
    // beyond): that is what freezes this page's events *and* its `next` cursor
    // together. Gating on `to - 1` would mark the page immutable one event
    // before `next` appears, so an edge-cached body would keep `next: null`
    // forever and pagination would dead-end. Complete iff `next !== null`.
    complete: headVersion >= to,
    events,
    // A next page exists only once an event has landed at or beyond `to`.
    next: headVersion >= to ? to : null,
    prev: from > 0 ? from - pageSize : null,
  };
}

/** The literal wire shape: like {@link FeedPage} but with `prev`/`next` as
 * links (or `null`), ready to serialize as the HTTP response body. */
export interface WireFeedPage {
  streamId: string;
  from: number;
  to: number;
  complete: boolean;
  events: EventEnvelope[];
  next: string | null;
  prev: string | null;
}

/**
 * Render a {@link FeedPage} into the wire format, mapping each cursor through
 * `hrefFor` to a URL in the caller's own route space.
 */
export function toWireFeed(page: FeedPage, hrefFor: (from: number) => string): WireFeedPage {
  return {
    streamId: page.streamId,
    from: page.from,
    to: page.to,
    complete: page.complete,
    events: page.events,
    next: page.next === null ? null : hrefFor(page.next),
    prev: page.prev === null ? null : hrefFor(page.prev),
  };
}

/**
 * The head resource (DESIGN.md, `GET …/head` — "current version — the poll
 * target"). This is the one intrinsically uncacheable read: a poller re-fetches
 * it under a short TTL / `no-store`, compares {@link version} against its own
 * cursor, and — if new events exist — follows `head` into the paging space.
 *
 * `head` is the cursor of the page that currently contains the head: always the
 * partial, still-growing page ({@link FeedPage.complete} `=== false`), so the
 * poll → follow flow lands directly on the only page worth re-reading.
 */
export interface HeadResource {
  streamId: string;
  /** Current head version, or `null` if the stream does not exist yet. */
  version: number | null;
  /**
   * Page-aligned cursor of the page containing the head (page 0 for an empty
   * stream). A transparent `from` version, same coordinate space as
   * {@link FeedPage.from} — never a storage reference (DESIGN.md, "Cursors are
   * version numbers, never storage references").
   */
  head: number;
  /**
   * Strong entity tag for the head resource, pre-quoted for direct use as an
   * HTTP `ETag` header (`"v7"`, or `"empty"` for an absent stream). The head
   * body is a pure function of the version (given the route's fixed page
   * size), so the version is a valid strong validator: a poller sends
   * `If-None-Match` with its last ETag and the handler answers
   * `304 Not Modified` when the head hasn't moved — making the poll loop
   * nearly free on the wire even though head resolution behind it is not.
   * Derived from version space, never from storage ETags (which compaction
   * relayouts change without the logical head moving).
   */
  etag: string;
}

export interface ReadHeadOptions {
  /** Page width used to align the {@link HeadResource.head} cursor. Must match
   * the size the paging routes use; defaults to the store's `chunkSize` (same
   * default as {@link readPage}), falling back to {@link DEFAULT_PAGE_SIZE}. */
  pageSize?: number;
}

/**
 * Resolve the current head of a stream into the pollable {@link HeadResource}.
 * Runs one authoritative head resolution (hint GET + short LIST + anchor GET);
 * do not cache the result as `immutable` — it changes on every append.
 */
export async function readHead(
  store: EventStore,
  streamId: string,
  opts: ReadHeadOptions = {},
): Promise<HeadResource> {
  const pageSize = opts.pageSize ?? store.chunkSize ?? DEFAULT_PAGE_SIZE;
  if (!Number.isInteger(pageSize) || pageSize < 1) {
    throw new RangeError(`pageSize must be a positive integer, got ${pageSize}`);
  }

  const resolved = await store.resolveHead(streamId);
  const version = resolved.kind === "head" ? resolved.version : null;
  return {
    streamId,
    version,
    // The head lives in the page that contains its version; an absent stream
    // polls page 0. `canonicalFrom` handles the `null → 0` snap.
    head: canonicalFrom(version ?? 0, pageSize),
    etag: version === null ? '"empty"' : `"v${version}"`,
  };
}

/** The literal wire shape of {@link HeadResource}: `head` rendered as a link.
 * `etag` is carried for the caller to emit as the `ETag` response header (and
 * compare against `If-None-Match` for a `304`) — harmless in the body too. */
export interface WireHead {
  streamId: string;
  version: number | null;
  head: string;
  etag: string;
}

/**
 * Render a {@link HeadResource} into the wire format, mapping the head cursor
 * through `hrefFor` to a URL in the caller's own route space — the same
 * `hrefFor` used for {@link toWireFeed}, so the link resolves to a real page.
 */
export function toWireHead(head: HeadResource, hrefFor: (from: number) => string): WireHead {
  return {
    streamId: head.streamId,
    version: head.version,
    head: hrefFor(head.head),
    etag: head.etag,
  };
}

/** Result of {@link idempotentAppend}: either this call committed the events,
 * or a prior (lost-response) attempt already did. Both are success to the
 * client — map `appended` to `201` and `alreadyCommitted` to `200`, or return
 * both as `200`; either way the retry is invisible. */
export type IdempotentAppendResult =
  /** This call won the conditional PUT; the store's full result. */
  | { outcome: "appended"; result: AppendResult }
  /**
   * The exact events (matched by `id`, element-wise in order) already sit at
   * the versions this append targeted: a previous attempt committed them and
   * its response was lost. `nextExpectedVersion` equals what that attempt
   * would have returned, so the client's cursor advances identically.
   * (`compactionSuggested` is unavailable on this path — moot under the
   * default mutable-tail strategy, which never compacts; on an ImmutableChunk
   * prefix the original attempt's write-driven trigger already fired.)
   */
  | { outcome: "alreadyCommitted"; streamId: string; nextExpectedVersion: number };

/**
 * Append for the raw-ingress worker — the deployment where the worker exposes
 * `POST …/append` accepting client-built events rather than domain commands —
 * made safe under at-least-once delivery (EventStoreDB's `ES-EventId` dedup,
 * re-derived for conditional-PUT storage; DESIGN.md, Prior art).
 *
 * The client contract: supply a **stable `id` on every event** (minted once,
 * reused verbatim on retry — the retry key), and retry with the **same
 * `expectedVersion`**. On `ConcurrencyError` this helper reads the exact
 * window the append targeted (`[expected+1, expected+1+len)`, or `[0, len)`
 * for `"noStream"`) and, if every version holds an event with the matching
 * `id`, reports `alreadyCommitted` instead of failing: the "conflict" was our
 * own earlier win. Any mismatch rethrows — a genuine concurrent writer.
 *
 * `expectedVersion: "any"` is deliberately unsupported: without a
 * deterministic target window, a lost-response retry is indistinguishable
 * from an intentional duplicate (scanning backwards from the head is unsound
 * once another writer interleaves), so `"any"` + retries would silently
 * double-append. An idempotent raw endpoint therefore requires the client to
 * state its expected version — which the `GET head` poll gives it for free.
 *
 * What replaces `"any"` when the worker wants to absorb contention itself —
 * event types that never logically conflict (per-user last-write-wins
 * reactions, unique-id inserts, idempotent deletes) shouldn't bounce a 409
 * to the client just because *some* writer moved the head: wrap this helper
 * in a bounded retry anchored on the client's stated version. On
 * `ConcurrencyError`, scan
 * `store.read(id, { fromVersion: clientVersion + 1, raw: true })` for the
 * event ids — a prior attempt may have landed anywhere above the client's
 * version once the loop has re-targeted — and report success if every id is
 * present; otherwise `resolveHead` and call this helper again at the fresh
 * head with the same events. The forward scan from a *fixed* lower bound is
 * exactly what `"any"` lacks: it keeps a lost-response retry distinguishable
 * from an intentional duplicate at every window the events could occupy.
 * (Under the default mutable-tail strategy both the scan and the head
 * resolution are the same few GETs an append does anyway — for the short,
 * hot streams this pattern suits, the retry is nearly free.)
 *
 * The recovery read is `raw` (envelope `id`s live outside the encryption
 * boundary), so this works on ciphertext-only workers and costs no key
 * fetches.
 */
export async function idempotentAppend(
  store: EventStore,
  streamId: string,
  events: EventInput[],
  opts: { expectedVersion: number | "noStream" },
): Promise<IdempotentAppendResult> {
  if (events.length === 0) {
    throw new RangeError("idempotentAppend requires at least one event");
  }
  events.forEach((e, i) => {
    if (typeof e.id !== "string" || e.id.length === 0) {
      throw new TypeError(
        `idempotentAppend requires a caller-supplied id on every event (missing at index ${i}) — the id is the retry key`,
      );
    }
  });
  const expected = opts.expectedVersion;
  // The opts type excludes "any"; this guards untyped callers, and explains why.
  if (expected !== "noStream" && !(Number.isInteger(expected) && expected >= 0)) {
    throw new RangeError(
      `idempotentAppend requires a deterministic expectedVersion (>= 0 or "noStream"), got ${String(expected)} — ` +
        `"any" has no fixed target window, so a retry could not be distinguished from a duplicate`,
    );
  }

  try {
    return { outcome: "appended", result: await store.append(streamId, events, opts) };
  } catch (err) {
    if (!(err instanceof ConcurrencyError)) throw err;

    // Did a prior attempt of *this* append win? Our events, had they
    // committed, occupy exactly this window — versions are dense, so the
    // window's occupants are fully determined.
    const base = expected === "noStream" ? 0 : expected + 1;
    const window: EventEnvelope[] = [];
    for await (const event of store.read(streamId, { fromVersion: base, raw: true })) {
      if (event.version >= base + events.length) break;
      window.push(event);
    }
    const ours =
      window.length === events.length && window.every((e, i) => e.id === events[i]!.id);
    if (!ours) throw err;
    return {
      outcome: "alreadyCommitted",
      streamId,
      nextExpectedVersion: base + events.length - 1,
    };
  }
}
