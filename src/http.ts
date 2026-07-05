/**
 * HTTP read / egress model (DESIGN.md, "HTTP reads").
 *
 * Turns the append-only stream into a sequence of fixed-size, deterministically
 * aligned pages that a plain `fetch` loop can walk via `prev`/`next`. A
 * *complete* page — one whose whole version range sits at or below the head —
 * can never change again, which is the machine-readable immutability promise
 * that lets a caller serve it with `Cache-Control: immutable` and cache it at
 * the edge forever. The page containing the head is partial and must not be
 * cached that way.
 *
 * This module is transport-agnostic: it returns page *cursors* (`from`
 * versions), not URLs. Callers own their URL space — see `toWireFeed` for the
 * literal `{ …, prev, next }` wire shape once you supply an `hrefFor` mapping.
 */

import type { EventStore } from "./store.js";
import type { EventEnvelope } from "./types.js";

/** Default page width. Align this to the store's `chunkSize` for the best
 * edge-cache hit rate: identical, chunk-aligned page URLs across all clients. */
export const DEFAULT_PAGE_SIZE = 100;

/** One page of a stream, with cursor-based navigation. */
export interface FeedPage {
  streamId: string;
  /** Inclusive lower bound of this page's version range (page-aligned). */
  from: number;
  /** Exclusive upper bound: this page covers `[from, to)`. */
  to: number;
  /**
   * True once the head has advanced to (or past) this page's last slot, so the
   * page's contents are frozen forever — safe to cache as `immutable`. False
   * for the partial, still-growing page that contains the head.
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
  /** Page width. Default {@link DEFAULT_PAGE_SIZE}. */
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
  const pageSize = opts.pageSize ?? DEFAULT_PAGE_SIZE;
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
    // The page is frozen once the head reaches its last slot (`to - 1`).
    complete: headVersion >= to - 1,
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
