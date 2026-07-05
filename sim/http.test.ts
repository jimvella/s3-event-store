/**
 * HTTP egress paging (DESIGN.md, "Wire format" / "Compaction and the API"):
 * fixed, page-aligned windows with prev/next cursors and a `complete` flag that
 * is the machine-readable `Cache-Control: immutable` promise.
 *
 * The load-bearing invariant under test is `complete <=> next !== null`: a page
 * is only frozen once the head has moved strictly past it, so its `next` link is
 * determined *before* it is ever declared immutable. Gating `complete` on the
 * page's last slot instead would freeze a body whose `next` is still `null`,
 * dead-ending pagination for anyone who edge-cached it.
 */

import { describe, expect, it } from "vitest";
import { ConcurrencyError } from "../src/errors";
import { createEventStore, type EventStore } from "../src/store";
import {
  canonicalFrom,
  idempotentAppend,
  readHead,
  readPage,
  toWireFeed,
  toWireHead,
} from "../src/http";
import { SIM_PREFIX, directDriver } from "./harness";
import { SimStore } from "./store";

const STREAM = "s";

function makeStore(sim: SimStore, chunkSize = 500): EventStore {
  let n = 0;
  return createEventStore({
    driver: directDriver(sim),
    prefix: SIM_PREFIX,
    chunkSize,
    ids: () => `W#${n++}`,
    clock: () => "1970-01-01T00:00:00.000Z",
  });
}

/** Append `count` single-event commits, so versions run 0..count-1. */
async function seed(store: EventStore, count: number): Promise<void> {
  if (count === 0) return;
  await store.append(STREAM, [{ type: "E", data: 0, id: "e0" }], { expectedVersion: "noStream" });
  for (let v = 0; v < count - 1; v++) {
    await store.append(STREAM, [{ type: "E", data: v + 1, id: `e${v + 1}` }], { expectedVersion: v });
  }
}

function freshStore(chunkSize = 500): EventStore {
  return makeStore(new SimStore(), chunkSize);
}

describe("canonicalFrom", () => {
  it("snaps a mid-page cursor down to its page boundary", () => {
    expect(canonicalFrom(372, 100)).toBe(300);
    expect(canonicalFrom(7, 3)).toBe(6);
  });

  it("is a no-op on an exact boundary", () => {
    expect(canonicalFrom(300, 100)).toBe(300);
    expect(canonicalFrom(0, 100)).toBe(0);
  });

  it("floors junk input (negative, NaN, Infinity) to page 0", () => {
    expect(canonicalFrom(-5, 100)).toBe(0);
    expect(canonicalFrom(Number.NaN, 100)).toBe(0);
    expect(canonicalFrom(Number.POSITIVE_INFINITY, 100)).toBe(0);
  });
});

describe("readPage option validation", () => {
  it("rejects a non-positive or non-integer pageSize", async () => {
    const store = freshStore();
    await expect(readPage(store, STREAM, { pageSize: 0 })).rejects.toThrow(RangeError);
    await expect(readPage(store, STREAM, { pageSize: -3 })).rejects.toThrow(RangeError);
    await expect(readPage(store, STREAM, { pageSize: 1.5 })).rejects.toThrow(RangeError);
  });

  it("defaults pageSize to the store's chunkSize, aligning pages to chunks", async () => {
    const store = freshStore(4); // distinctive N, not the DEFAULT_PAGE_SIZE constant
    await seed(store, 10); // versions 0..9
    const page = await readPage(store, STREAM, { from: 6 });
    expect(page.to - page.from).toBe(store.chunkSize); // width follows chunkSize
    expect(page).toMatchObject({ from: 4, to: 8 }); // 6 snaps to the N=4 boundary
    expect(page.events.map((e) => e.version)).toEqual([4, 5, 6, 7]);
  });

  it("still honors an explicit pageSize over the store default", async () => {
    const store = freshStore(4);
    await seed(store, 10);
    const page = await readPage(store, STREAM, { from: 0, pageSize: 3 });
    expect(page.to - page.from).toBe(3);
  });
});

describe("readPage boundaries", () => {
  it("returns an empty, non-complete page 0 for a stream that does not exist", async () => {
    const store = freshStore();
    const page = await readPage(store, STREAM, { from: 0, pageSize: 3 });
    expect(page).toMatchObject({
      streamId: STREAM,
      from: 0,
      to: 3,
      complete: false,
      next: null,
      prev: null,
    });
    expect(page.events).toEqual([]);
  });

  it("serves the partial head page as incomplete with no next", async () => {
    const store = freshStore();
    await seed(store, 2); // versions 0,1; head = 1
    const page = await readPage(store, STREAM, { from: 0, pageSize: 3 });
    expect(page.events.map((e) => e.version)).toEqual([0, 1]);
    expect(page.complete).toBe(false);
    expect(page.next).toBeNull();
    expect(page.prev).toBeNull();
  });

  it("does NOT mark a brim-full page complete until the head moves past it", async () => {
    // The regression: page [0,3) is physically full at head=2, but nothing has
    // landed at version 3 yet, so `next` is still null. Marking it immutable
    // here would freeze a body that can never surface its next page.
    const store = freshStore();
    await seed(store, 3); // versions 0,1,2; head = 2, exactly filling page [0,3)
    const full = await readPage(store, STREAM, { from: 0, pageSize: 3 });
    expect(full.events.map((e) => e.version)).toEqual([0, 1, 2]);
    expect(full.complete).toBe(false);
    expect(full.next).toBeNull();

    // One more event lands (version 3) — now the page is frozen AND has a next.
    await store.append(STREAM, [{ type: "E", data: 3, id: "e3" }], { expectedVersion: 2 });
    const frozen = await readPage(store, STREAM, { from: 0, pageSize: 3 });
    expect(frozen.events.map((e) => e.version)).toEqual([0, 1, 2]); // contents unchanged
    expect(frozen.complete).toBe(true);
    expect(frozen.next).toBe(3);
  });

  it("gives a complete interior page next/prev cursors", async () => {
    const store = freshStore();
    await seed(store, 10); // versions 0..9; head = 9
    const page = await readPage(store, STREAM, { from: 3, pageSize: 3 });
    expect(page).toMatchObject({ from: 3, to: 6, complete: true, next: 6, prev: 0 });
    expect(page.events.map((e) => e.version)).toEqual([3, 4, 5]);
  });

  it("snaps a mid-page `from` to the canonical page before reading", async () => {
    const store = freshStore();
    await seed(store, 10); // head = 9
    const page = await readPage(store, STREAM, { from: 7, pageSize: 3 });
    expect(page).toMatchObject({ from: 6, to: 9, prev: 3 });
    expect(page.events.map((e) => e.version)).toEqual([6, 7, 8]);
  });

  it("returns an empty, non-complete page for a window past the head", async () => {
    const store = freshStore();
    await seed(store, 2); // head = 1
    const page = await readPage(store, STREAM, { from: 6, pageSize: 3 });
    expect(page).toMatchObject({ from: 6, to: 9, complete: false, next: null, prev: 3 });
    expect(page.events).toEqual([]);
  });
});

describe("readPage invariant: complete <=> next !== null", () => {
  it("holds across every page of a growing stream", async () => {
    const pageSize = 3;
    // Walk the stream length across page boundaries and one past the head.
    for (let count = 0; count <= 12; count++) {
      const store = freshStore();
      await seed(store, count);
      for (let from = 0; from <= count + pageSize; from += pageSize) {
        const page = await readPage(store, STREAM, { from, pageSize });
        expect(page.complete).toBe(page.next !== null);
        // Events stay within the half-open page window, in version order.
        for (const e of page.events) {
          expect(e.version).toBeGreaterThanOrEqual(page.from);
          expect(e.version).toBeLessThan(page.to);
        }
      }
    }
  });
});

describe("toWireFeed", () => {
  it("maps cursors through hrefFor and preserves nulls", async () => {
    const store = freshStore();
    await seed(store, 10); // head = 9
    const page = await readPage(store, STREAM, { from: 3, pageSize: 3 });
    const wire = toWireFeed(page, (from) => `/streams/${STREAM}/events?from=${from}`);
    expect(wire).toMatchObject({
      streamId: STREAM,
      from: 3,
      to: 6,
      complete: true,
      next: `/streams/${STREAM}/events?from=6`,
      prev: `/streams/${STREAM}/events?from=0`,
    });
    expect(wire.events).toBe(page.events);
  });

  it("leaves a null cursor as null rather than calling hrefFor", async () => {
    const store = freshStore();
    await seed(store, 2); // partial head page 0: next and prev both null
    const page = await readPage(store, STREAM, { from: 0, pageSize: 3 });
    const wire = toWireFeed(page, () => {
      throw new Error("hrefFor must not be called for a null cursor");
    });
    expect(wire.next).toBeNull();
    expect(wire.prev).toBeNull();
  });
});

describe("readHead", () => {
  it("reports version null and head page 0 for a stream that does not exist", async () => {
    const store = freshStore();
    const head = await readHead(store, STREAM, { pageSize: 3 });
    expect(head).toEqual({ streamId: STREAM, version: null, head: 0, etag: '"empty"' });
  });

  it("reports the current version and the page cursor containing the head", async () => {
    const store = freshStore();
    await seed(store, 8); // versions 0..7; head = 7, which sits in page [6,9)
    const head = await readHead(store, STREAM, { pageSize: 3 });
    expect(head).toEqual({ streamId: STREAM, version: 7, head: 6, etag: '"v7"' });
  });

  it("moves the etag exactly when the version moves — the 304 revalidation contract", async () => {
    const store = freshStore();
    await seed(store, 2); // head = 1
    const before = await readHead(store, STREAM, { pageSize: 3 });
    const again = await readHead(store, STREAM, { pageSize: 3 });
    expect(again.etag).toBe(before.etag); // unchanged head ⇒ identical etag ⇒ 304
    await store.append(STREAM, [{ type: "E", data: 2, id: "e2" }], { expectedVersion: 1 });
    const after = await readHead(store, STREAM, { pageSize: 3 });
    expect(after.etag).not.toBe(before.etag); // new event ⇒ new etag ⇒ 200
    expect(after.etag).toBe('"v2"');
    // Distinguishes empty from v0: the first-ever event must invalidate.
    expect(before.etag).toBe('"v1"');
  });

  it("points at the still-growing (incomplete) page, so poll → follow lands on live data", async () => {
    const store = freshStore();
    await seed(store, 8); // head = 7
    const head = await readHead(store, STREAM, { pageSize: 3 });
    const headPage = await readPage(store, STREAM, { from: head.head, pageSize: 3 });
    expect(headPage.complete).toBe(false); // never cache the head page as immutable
    expect(headPage.events.map((e) => e.version)).toContain(head.version);
  });

  it("keeps a brim-full stream's head on the last written page until it overflows", async () => {
    const store = freshStore();
    await seed(store, 6); // versions 0..5; head = 5, last slot of page [3,6)
    expect(await readHead(store, STREAM, { pageSize: 3 })).toMatchObject({ version: 5, head: 3 });
    // The 7th event opens page [6,9); the head cursor advances with it.
    await store.append(STREAM, [{ type: "E", data: 6, id: "e6" }], { expectedVersion: 5 });
    expect(await readHead(store, STREAM, { pageSize: 3 })).toMatchObject({ version: 6, head: 6 });
  });

  it("defaults its head cursor to the store's chunkSize and validates pageSize", async () => {
    const store = freshStore(4); // N=4
    await seed(store, 7); // versions 0..6; head = 6, in page [4,8)
    expect(await readHead(store, STREAM)).toMatchObject({ version: 6, head: 4 });
    await expect(readHead(store, STREAM, { pageSize: 0 })).rejects.toThrow(RangeError);
    await expect(readHead(store, STREAM, { pageSize: 2.5 })).rejects.toThrow(RangeError);
  });
});

describe("idempotentAppend", () => {
  const A = [
    { type: "E", data: "a", id: "id-a" },
    { type: "E", data: "b", id: "id-b" },
  ];

  it("requires ids on every event, a non-empty batch, and a deterministic expectedVersion", async () => {
    const store = freshStore();
    await expect(idempotentAppend(store, STREAM, [], { expectedVersion: "noStream" })).rejects.toThrow(RangeError);
    await expect(
      idempotentAppend(store, STREAM, [{ type: "E", data: 1 }], { expectedVersion: "noStream" }),
    ).rejects.toThrow(TypeError);
    await expect(
      // Untyped callers can still pass "any"; the runtime guard explains the refusal.
      idempotentAppend(store, STREAM, A, { expectedVersion: "any" as never }),
    ).rejects.toThrow(RangeError);
  });

  it("appends normally when there is no conflict", async () => {
    const store = freshStore();
    const r = await idempotentAppend(store, STREAM, A, { expectedVersion: "noStream" });
    expect(r.outcome).toBe("appended");
    if (r.outcome === "appended") expect(r.result.nextExpectedVersion).toBe(1);
  });

  it("treats an exact retry as alreadyCommitted, without double-appending", async () => {
    const store = freshStore();
    await idempotentAppend(store, STREAM, A, { expectedVersion: "noStream" });
    // The client's response was lost; it retries the identical request.
    const retry = await idempotentAppend(store, STREAM, A, { expectedVersion: "noStream" });
    expect(retry).toEqual({ outcome: "alreadyCommitted", streamId: STREAM, nextExpectedVersion: 1 });
    // Nothing was duplicated: head is still 1.
    expect((await readHead(store, STREAM)).version).toBe(1);
  });

  it("dedups a retry at an explicit expectedVersion", async () => {
    const store = freshStore();
    await seed(store, 2); // head = 1
    const events = [{ type: "E", data: "x", id: "id-x" }];
    await idempotentAppend(store, STREAM, events, { expectedVersion: 1 });
    const retry = await idempotentAppend(store, STREAM, events, { expectedVersion: 1 });
    expect(retry).toEqual({ outcome: "alreadyCommitted", streamId: STREAM, nextExpectedVersion: 2 });
  });

  it("still dedups after other writers have appended beyond our commit", async () => {
    const store = freshStore();
    await seed(store, 2); // head = 1
    await idempotentAppend(store, STREAM, A, { expectedVersion: 1 }); // our events at 2,3
    await store.append(STREAM, [{ type: "E", data: "z", id: "id-z" }], { expectedVersion: 3 });
    const retry = await idempotentAppend(store, STREAM, A, { expectedVersion: 1 });
    expect(retry).toEqual({ outcome: "alreadyCommitted", streamId: STREAM, nextExpectedVersion: 3 });
  });

  it("rethrows when a different writer won the slot — a genuine conflict", async () => {
    const store = freshStore();
    await seed(store, 1); // head = 0
    // Foreign commit of the SAME length: only the id comparison can tell.
    await store.append(
      STREAM,
      [
        { type: "E", data: "theirs", id: "id-theirs-1" },
        { type: "E", data: "theirs", id: "id-theirs-2" },
      ],
      { expectedVersion: 0 },
    );
    await expect(idempotentAppend(store, STREAM, A, { expectedVersion: 0 })).rejects.toThrow(ConcurrencyError);
  });

  it("rethrows on a length mismatch — a shorter foreign commit is not our retry", async () => {
    const store = freshStore();
    await seed(store, 1); // head = 0
    // A foreign writer put ONE event where our TWO would have gone.
    await store.append(STREAM, [{ type: "E", data: "solo", id: "id-solo" }], { expectedVersion: 0 });
    await expect(idempotentAppend(store, STREAM, A, { expectedVersion: 0 })).rejects.toThrow(ConcurrencyError);
  });

  it("rethrows for a stale-ahead client whose window does not exist", async () => {
    const store = freshStore();
    await seed(store, 2); // head = 1
    await expect(idempotentAppend(store, STREAM, A, { expectedVersion: 5 })).rejects.toThrow(ConcurrencyError);
  });
});

describe("toWireHead", () => {
  it("renders the head cursor as a link through the same hrefFor as pages", async () => {
    const store = freshStore();
    await seed(store, 8); // head = 7 in page [6,9)
    const head = await readHead(store, STREAM, { pageSize: 3 });
    const wire = toWireHead(head, (from) => `/streams/${STREAM}/events?from=${from}`);
    expect(wire).toEqual({
      streamId: STREAM,
      version: 7,
      head: `/streams/${STREAM}/events?from=6`,
      etag: '"v7"',
    });
  });
});
