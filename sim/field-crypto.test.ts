/**
 * Field-level encrypting serializer (KEYS_DESIGN.md, "Whole-payload vs.
 * field-level"): multi-author streams, per-event subjects, fail-closed
 * configuration, shredded-field sentinel, per-field AAD. Deterministic:
 * direct drivers, manual clock, real WebCrypto.
 */

import { describe, expect, it } from "vitest";
import { SerializationError, ShreddedDataError, SubjectErasedError } from "../src/errors";
import { cryptoRandom } from "../src/crypto/bytes";
import { aesMasterKey } from "../src/crypto/master-key";
import { decryptPayload, encryptPayload, fieldAad, payloadAad } from "../src/crypto/payload";
import { createS3KeyStore, generationKey, tombstoneKey } from "../src/crypto/keystore";
import {
  SHREDDED_FIELD,
  fieldEncryptingSerializer,
  isShreddedField,
} from "../src/crypto/field-serializer";
import { requestShred, sweepShreds, type ShredContext } from "../src/crypto/shred";
import { createEventStore, type EventStore } from "../src/store";
import { SIM_PREFIX, directDriver } from "./harness";
import { collect } from "./oracle";
import { SimStore } from "./store";

const DAY = 24 * 3600 * 1000;
const SECRET = new Uint8Array(32).fill(7);
const ALICE = "subject:alice";
const BOB = "subject:bob";
const ROOM = "room-1";

const FIELDS = {
  MessagePosted: ["text", "attachments"],
  MessageDeleted: "plaintext",
} as const;

function setup() {
  const eventSim = new SimStore();
  const keySim = new SimStore();
  const clockRef = { now: 1_000_000_000 };
  const clock = () => clockRef.now;
  const isoClock = () => new Date(clockRef.now).toISOString();
  let n = 0;
  const ids = () => `id#${n++}`;

  const auditStore = createEventStore({
    driver: directDriver(eventSim),
    prefix: SIM_PREFIX,
    chunkSize: 500,
    ids,
    clock: isoClock,
    allowReservedStreams: true,
  });
  const keyDriver = directDriver(keySim);
  const keys = createS3KeyStore({
    driver: keyDriver,
    masterKey: aesMasterKey(SECRET),
    clock,
    keyCacheTtlMs: 0, // 0 = always fresh (no caching)
    tombstoneTtlMs: 0,
    keyringTtlMs: 3_600_000,
  });
  const serializer = fieldEncryptingSerializer({
    keys,
    subjectFor: (event) => (event.data as { author?: string }).author ?? null,
    fields: FIELDS,
  });
  const store = createEventStore({
    driver: directDriver(eventSim),
    prefix: SIM_PREFIX,
    chunkSize: 4,
    ids,
    clock: isoClock,
    serializer,
  });
  const shredCtx: ShredContext = {
    auditStore,
    keyDriver,
    waitingPeriodMs: 14 * DAY,
    clock,
  };
  return { eventSim, keySim, clockRef, keys, serializer, store, shredCtx };
}

async function post(store: EventStore, author: string, text: string, expected: number | "noStream", id: string) {
  return store.append(
    ROOM,
    [{ type: "MessagePosted", data: { author, messageId: `m-${id}`, text }, id }],
    { expectedVersion: expected },
  );
}

describe("field-level serializer end to end", () => {
  it("round-trips a multi-author stream: annotated fields ciphertext at rest, identifiers greppable", async () => {
    const { store, eventSim } = setup();
    await post(store, ALICE, "alpha-secret", "noStream", "e0");
    await post(store, BOB, "bravo-secret", 0, "e1");
    await store.append(
      ROOM,
      [{ type: "MessageDeleted", data: { author: ALICE, messageId: "m-e0" }, id: "e2" }],
      { expectedVersion: 1 },
    );

    const replay = await collect(store.read(ROOM));
    expect(replay.map((e) => (e.data as { text?: string }).text)).toEqual([
      "alpha-secret",
      "bravo-secret",
      undefined,
    ]);
    // Encrypted events carry the generation; the plaintext opt-out doesn't.
    expect(replay.map((e) => e.keyId)).toEqual(["000000", "000000", undefined]);

    // No plaintext attribute at rest — but the identifiers (subject,
    // messageId) stay greppable: that is the field-level point.
    const bodies = [...eventSim.dump().values()].map((o) => o.body);
    for (const body of bodies) {
      expect(body).not.toContain("alpha-secret");
      expect(body).not.toContain("bravo-secret");
    }
    expect(bodies.some((b) => b.includes(ALICE) && b.includes("m-e0"))).toBe(true);

    // Raw read (model-B egress): the field marker verbatim.
    const raw = await collect(store.read(ROOM, { raw: true }));
    const marker = (raw[0]!.data as { text: { $enc: string } }).text;
    expect(typeof marker.$enc).toBe("string");
    expect(marker.$enc).not.toContain("alpha");
  });

  it("fails closed on config gaps: unannotated types and null subjects refuse to store anything", async () => {
    const { store, eventSim, serializer } = setup();

    await expect(
      store.append(ROOM, [{ type: "Unannotated", data: { author: ALICE, text: "leak?" } }], {
        expectedVersion: "noStream",
      }),
    ).rejects.toThrow(SerializationError);
    await expect(
      store.append(ROOM, [{ type: "MessagePosted", data: { messageId: "m-9", text: "leak?" } }], {
        expectedVersion: "noStream",
      }),
    ).rejects.toThrow(SerializationError);
    // Refused before any PUT: the bucket is untouched.
    expect(eventSim.dump().size).toBe(0);

    // An empty field list is ambiguous — rejected, not treated as plaintext.
    const ambiguous = fieldEncryptingSerializer({
      keys: setup().keys,
      subjectFor: () => ALICE,
      fields: { T: [] },
    });
    await expect(ambiguous.serialize(ROOM, { type: "T", data: {} })).rejects.toThrow(
      /empty field list/,
    );

    // System streams are plaintext by rule (no PII, no key-store dependency).
    const audit = await serializer.serialize("$system.key-audit", {
      type: "KeyCreated",
      data: { subjectId: ALICE },
    });
    expect(audit).toEqual({ data: { subjectId: ALICE } });
  });

  it("shreds exactly one author: their fields degrade to the sentinel; the stream keeps reading", async () => {
    const { store, shredCtx, clockRef, keySim } = setup();
    await post(store, ALICE, "alice-words", "noStream", "e0");
    await post(store, BOB, "bob-words", 0, "e1");

    await requestShred(shredCtx, ALICE);

    // Soft-deleted: alice's values are gone, the replay and bob are not.
    const replay = await collect(store.read(ROOM));
    expect(isShreddedField((replay[0]!.data as { text: unknown }).text)).toBe(true);
    expect((replay[0]!.data as { messageId: string }).messageId).toBe("m-e0");
    expect((replay[1]!.data as { text: string }).text).toBe("bob-words");
    // The sentinel is JSON-stable, so projections can store and replay it.
    expect(JSON.parse(JSON.stringify((replay[0]!.data as { text: unknown }).text))).toEqual(
      SHREDDED_FIELD,
    );

    // The append path fails closed for the shredded author only.
    await expect(post(store, ALICE, "more", 1, "e2")).rejects.toThrow(SubjectErasedError);
    await post(store, BOB, "still fine", 1, "e3");

    // Hard delete changes nothing observable: still the sentinel, still bob.
    clockRef.now += 15 * DAY;
    const report = await sweepShreds(shredCtx);
    expect(report.hardDeleted).toEqual([ALICE]);
    // The wrapped keys are destroyed; the tombstone survives by design
    // (the identity can never be reincarnated).
    expect(keySim.dump().has(generationKey(ALICE, 0))).toBe(false);
    expect(keySim.dump().has(tombstoneKey(ALICE))).toBe(true);
    const after = await collect(store.read(ROOM));
    expect(isShreddedField((after[0]!.data as { text: unknown }).text)).toBe(true);
    expect((after[2]!.data as { text: string }).text).toBe("still fine");
  });

  it("rotation: old fields decrypt under their recorded generation, new fields under the new one", async () => {
    const { store, keys } = setup();
    await post(store, ALICE, "gen0-words", "noStream", "e0");
    await keys.rotate(ALICE);
    await post(store, ALICE, "gen1-words", 0, "e1");

    const replay = await collect(store.read(ROOM));
    expect(replay.map((e) => [(e.data as { text: string }).text, e.keyId])).toEqual([
      ["gen0-words", "000000"],
      ["gen1-words", "000001"],
    ]);
  });

  it("a field annotated after the fact reads back as its stored plaintext", async () => {
    const { store, eventSim, keys } = setup();
    // Written when only `text`/`attachments` were annotated: `topic` plaintext.
    await store.append(
      ROOM,
      [{ type: "MessagePosted", data: { author: ALICE, messageId: "m-0", text: "secret", topic: "open" }, id: "e0" }],
      { expectedVersion: "noStream" },
    );

    // Read under a config that has since annotated `topic` too.
    const widened = createEventStore({
      driver: directDriver(eventSim),
      prefix: SIM_PREFIX,
      chunkSize: 4,
      serializer: fieldEncryptingSerializer({
        keys,
        subjectFor: (event) => (event.data as { author?: string }).author ?? null,
        fields: { ...FIELDS, MessagePosted: ["text", "attachments", "topic"] },
      }),
    });
    const replay = await collect(widened.read(ROOM));
    expect(replay[0]!.data).toMatchObject({ text: "secret", topic: "open" });
  });

  it("refuses to annotate the subject-bearing field: the subject must survive encryption", async () => {
    const { keys, keySim } = setup();
    const bad = fieldEncryptingSerializer({
      keys,
      subjectFor: (event) => (event.data as { author?: string }).author ?? null,
      fields: { MessagePosted: ["author", "text"] },
    });
    await expect(
      bad.serialize(ROOM, { type: "MessagePosted", data: { author: ALICE, text: "x" } }),
    ).rejects.toThrow(/subject-bearing field must stay plaintext/);
    // Refused before any key-store side effect: nothing minted, no audit.
    expect(keySim.dump().size).toBe(0);
  });

  it("refuses plaintext values shaped like reserved markers on encrypted events", async () => {
    const { store, eventSim } = setup();
    await expect(
      store.append(
        ROOM,
        [{ type: "MessagePosted", data: { author: ALICE, quoted: { $enc: "spoofed" }, text: "x" }, id: "e0" }],
        { expectedVersion: "noStream" },
      ),
    ).rejects.toThrow(SerializationError);
    await expect(
      store.append(
        ROOM,
        [{ type: "MessagePosted", data: { author: ALICE, status: { $shredded: true }, text: "x" }, id: "e1" }],
        { expectedVersion: "noStream" },
      ),
    ).rejects.toThrow(SerializationError);
    expect(eventSim.dump().size).toBe(0);
  });

  it("config drift never strands ciphertext: reads are marker-driven, not annotation-driven", async () => {
    const { store, eventSim, keys } = setup();
    await store.append(
      ROOM,
      [
        {
          type: "MessagePosted",
          data: { author: ALICE, messageId: "m-0", text: "secret", attachments: ["a.png"] },
          id: "e0",
        },
      ],
      { expectedVersion: "noStream" },
    );
    const readWith = (fields: Record<string, readonly string[] | "plaintext">) =>
      createEventStore({
        driver: directDriver(eventSim),
        prefix: SIM_PREFIX,
        chunkSize: 4,
        serializer: fieldEncryptingSerializer({
          keys,
          subjectFor: (event) => (event.data as { author?: string }).author ?? null,
          fields,
        }),
      });

    // Narrowed annotation: the de-annotated field's marker still decrypts.
    const narrowed = await collect(readWith({ MessagePosted: ["text"] }).read(ROOM));
    expect(narrowed[0]!.data).toMatchObject({ text: "secret", attachments: ["a.png"] });
    // Migrated to plaintext, or dropped from the config entirely: same.
    const plain = await collect(readWith({ MessagePosted: "plaintext" }).read(ROOM));
    expect(plain[0]!.data).toMatchObject({ text: "secret", attachments: ["a.png"] });
    const dropped = await collect(readWith({}).read(ROOM));
    expect(dropped[0]!.data).toMatchObject({ text: "secret", attachments: ["a.png"] });
  });

  it("a keyId naming a never-minted generation fails loud — tampering cannot impersonate erasure", async () => {
    const { store, eventSim, keys } = setup();
    await post(store, ALICE, "alice-words", "noStream", "e0");

    const [chunkKey] = [...eventSim.dump().keys()].filter((k) => k.includes(ROOM));
    const chunk = JSON.parse(eventSim.dump().get(chunkKey!)!.body) as {
      commits: { events: { keyId?: string }[] }[];
    };
    chunk.commits[0]!.events[0]!.keyId = "999999";
    eventSim.put(chunkKey!, JSON.stringify(chunk));

    await expect(collect(store.read(ROOM))).rejects.toThrow(/never minted/);
    // The keystore contract behind it: a proven shred returns null (degrade);
    // a generation with no key AND no tombstone throws (stay loud).
    await expect(keys.keyById(ALICE, "999999")).rejects.toThrow(ShreddedDataError);
  });
});

describe("field context binding (AAD)", () => {
  it("rejects a field ciphertext moved to another stream, generation, or field — even under the same key", async () => {
    const key = cryptoRandom(32);
    const aad = fieldAad(ROOM, "000000", "text");
    const ct = await encryptPayload(key, "alpha", { compress: false, random: cryptoRandom, aad });
    expect(await decryptPayload(key, ct, aad)).toBe("alpha");

    await expect(decryptPayload(key, ct, fieldAad("room-2", "000000", "text"))).rejects.toThrow(
      ShreddedDataError,
    );
    await expect(decryptPayload(key, ct, fieldAad(ROOM, "000001", "text"))).rejects.toThrow(
      ShreddedDataError,
    );
    await expect(decryptPayload(key, ct, fieldAad(ROOM, "000000", "title"))).rejects.toThrow(
      ShreddedDataError,
    );
    // Domain separation: a field ciphertext never authenticates as a payload.
    await expect(decryptPayload(key, ct, payloadAad(ROOM, "000000"))).rejects.toThrow(
      ShreddedDataError,
    );
  });

  it("a transplant between two authors' stored events fails loudly, not as the sentinel", async () => {
    const { store, eventSim } = setup();
    await post(store, ALICE, "alice-words", "noStream", "e0");
    await post(store, BOB, "bob-words", 0, "e1");

    // Swap the two events' `text` markers inside the stored chunk: each
    // ciphertext now sits under the other subject's plaintext identifier.
    const [chunkKey] = [...eventSim.dump().keys()].filter((k) => k.includes(ROOM));
    const chunk = JSON.parse(eventSim.dump().get(chunkKey!)!.body) as {
      commits: { events: { data: { text: unknown } }[] }[];
    };
    const a = chunk.commits[0]!.events[0]!.data;
    const b = chunk.commits[1]!.events[0]!.data;
    [a.text, b.text] = [b.text, a.text];
    eventSim.put(chunkKey!, JSON.stringify(chunk));

    // Decryption under the wrong author's key is authentication failure —
    // tampering stays loud; only a MISSING key degrades to the sentinel.
    await expect(collect(store.read(ROOM))).rejects.toThrow(ShreddedDataError);
  });
});
