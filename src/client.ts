/**
 * Browser client SDK — the `./client` entry point (DESIGN.md, Component
 * inventory #4, model B). Zero dependencies, WebCrypto only.
 *
 * Responsibilities: fetch and cache the keyring respecting its TTLs, pick
 * the key per event by the envelope's `keyId`, AES-GCM decrypt fail-closed
 * (a shredded stream presents as decryption failure), cursor iteration,
 * caller-supplied upcasters (client-side is the only upcasting on
 * immutable paths), and an optional fold-to-state helper.
 *
 * It exists for crypto correctness, not protocol: the REST API remains the
 * contract and this SDK is its reference consumer. Clients follow links,
 * never compute URLs — the one URL constructed is the cold-resume entry
 * (`?from=v`); the read handler 308s to the canonical page and everything
 * after is link-following against permanent cache entries. Pages marked
 * `complete` are immutable and cached locally forever; only the incomplete
 * head page is ever re-polled.
 */

import { ShreddedDataError } from "./errors.js";
import { base64ToBytes } from "./crypto/bytes.js";
import { decryptPayload } from "./crypto/payload.js";
import type { EventEnvelope } from "./types.js";

/** Wire page (DESIGN.md, Wire format). */
export interface WirePage {
  streamId: string;
  from: number;
  to: number;
  /** Machine-readable immutability promise: cache forever. */
  complete: boolean;
  events: EventEnvelope[];
  /** Link relation to the next page; absent/null ⇒ at head. */
  next?: string | null;
  prev?: string | null;
}

/** Keyring wire format (`GET .../streams/{id}/key`, model B). */
export interface WireKeyring {
  keyring: { keyId: string; key: string; expiresAt: string }[];
}

export type Upcaster = (event: EventEnvelope) => EventEnvelope;

export interface StreamClientConfig {
  /** Deployment base including the prefix, e.g. "https://api.example.com/app-x". */
  baseUrl: string;
  /** Bearer-token supplier; sent as Authorization on every request. */
  auth?: () => string | Promise<string>;
  fetchImpl?: (request: Request) => Promise<Response>;
  /** Applied in order after decryption — client-side upcasting. */
  upcasters?: Upcaster[];
  clock?: () => number;
}

export interface ReadOpts {
  from?: number;
}

export interface StreamClient {
  read(streamId: string, opts?: ReadOpts): AsyncIterable<EventEnvelope>;
  fold<S>(
    streamId: string,
    opts: { init: () => S; evolve: (state: S, event: EventEnvelope) => S; from?: number },
  ): Promise<{ state: S; version: number }>;
  /** Drop cached pages and keyrings (tests, logout). */
  clearCache(): void;
}

interface CachedKeyring {
  keys: Map<string, Uint8Array>;
  expiresAt: number;
}

export function createStreamClient(config: StreamClientConfig): StreamClient {
  const base = config.baseUrl.replace(/\/$/, "");
  const doFetch = config.fetchImpl ?? ((req: Request) => fetch(req));
  const clock = config.clock ?? (() => Date.now());
  const upcasters = config.upcasters ?? [];

  const pageCache = new Map<string, WirePage>(); // complete pages only — immutable
  const keyrings = new Map<string, CachedKeyring>();
  /** Empty-keyring negative cache: brief, so a shredded stream fails fast
   * without hammering the key endpoint. */
  const EMPTY_KEYRING_TTL = 30_000;

  async function request(url: string): Promise<Response> {
    const headers = new Headers();
    if (config.auth) headers.set("authorization", `Bearer ${await config.auth()}`);
    let current = url;
    // fetchImpl is injected, so redirects are handled here (bounded): the
    // cold-resume 308-to-canonical is the one redirect the contract uses.
    for (let hop = 0; hop < 4; hop++) {
      const resp = await doFetch(new Request(current, { headers }));
      if (resp.status >= 300 && resp.status < 400) {
        const location = resp.headers.get("location");
        if (location === null) throw new Error(`redirect without Location from ${current}`);
        current = new URL(location, base).toString();
        continue;
      }
      if (!resp.ok) throw new Error(`GET ${current}: ${resp.status}`);
      return resp;
    }
    throw new Error(`too many redirects from ${url}`);
  }

  async function fetchPage(url: string): Promise<WirePage> {
    const cached = pageCache.get(url);
    if (cached) return cached;
    const resp = await request(url);
    const page = (await resp.json()) as WirePage;
    if (page.complete) pageCache.set(url, page);
    return page;
  }

  async function fetchKeyring(streamId: string): Promise<CachedKeyring> {
    const resp = await request(`${base}/streams/${encodeURIComponent(streamId)}/key`);
    const wire = (await resp.json()) as WireKeyring;
    const keys = new Map(wire.keyring.map((k) => [k.keyId, base64ToBytes(k.key)]));
    const expiresAt =
      wire.keyring.length === 0
        ? clock() + EMPTY_KEYRING_TTL
        : Math.min(...wire.keyring.map((k) => Date.parse(k.expiresAt)));
    const cached = { keys, expiresAt };
    keyrings.set(streamId, cached);
    return cached;
  }

  async function keyFor(streamId: string, keyId: string): Promise<Uint8Array> {
    let ring = keyrings.get(streamId);
    // Refetch when expired (TTL is the authorization window) or when the
    // keyId is unknown (a rotation minted a generation since we fetched).
    if (!ring || clock() >= ring.expiresAt || !ring.keys.has(keyId)) {
      ring = await fetchKeyring(streamId);
    }
    const key = ring.keys.get(keyId);
    if (key === undefined) {
      // Empty or missing after a fresh fetch: shredded (or unauthorized).
      throw new ShreddedDataError(
        `key ${keyId} for stream ${streamId} is not deliverable — shredded or revoked`,
      );
    }
    return key;
  }

  async function* read(streamId: string, opts?: ReadOpts): AsyncGenerator<EventEnvelope> {
    const from = opts?.from ?? 0;
    // The one constructed URL: the cold-resume entry point.
    let url: string | null = `${base}/streams/${encodeURIComponent(streamId)}/events?from=${from}`;
    while (url !== null) {
      const page = await fetchPage(url);
      for (const event of page.events) {
        if (event.version < from) continue; // canonical page starts before the cursor
        const data =
          event.keyId !== undefined
            ? await decryptPayload(await keyFor(streamId, event.keyId), event.data as string)
            : event.data;
        let envelope: EventEnvelope = { ...event, data };
        for (const upcast of upcasters) envelope = upcast(envelope);
        yield envelope;
      }
      url = page.next != null ? new URL(page.next, base).toString() : null;
    }
  }

  return {
    read,
    async fold(streamId, opts) {
      let state = opts.init();
      let version = (opts.from ?? 0) - 1;
      for await (const event of read(streamId, opts.from !== undefined ? { from: opts.from } : {})) {
        state = opts.evolve(state, event);
        version = event.version;
      }
      return { state, version };
    },
    clearCache() {
      pageCache.clear();
      keyrings.clear();
    },
  };
}
