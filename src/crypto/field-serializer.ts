/**
 * Field-level encrypting serializer (KEYS_DESIGN.md, "Whole-payload vs.
 * field-level") — the multi-author shape. The whole-payload serializer keys
 * a stream to ONE subject (`subjectFor(streamId)`); this one takes the
 * subject from the EVENT, so a shared stream (chat room, collaborative
 * log) can hold many authors and shredding one of them erases exactly
 * their values, nobody else's.
 *
 * Identifiers vs. attributes: the fields that stay plaintext are the ones
 * key selection, authorization, and projection need BEFORE any key is
 * available — so they must be non-PII by construction (the same rule the
 * store applies to stream ids). Everything human-readable is an encrypted
 * attribute.
 *
 * Fail-closed configuration, twice over (the KEYS_DESIGN.md rule —
 * forgetting an annotation must never silently store plaintext PII in an
 * immutable log):
 *
 *  - an event type absent from `fields` REFUSES to serialize
 *    (SerializationError). Whole-payload fallback is unavailable here by
 *    construction: a multi-author stream has no stream-level subject to
 *    encrypt under. Plaintext is an explicit `"plaintext"` opt-out.
 *  - an annotated type whose `subjectFor` resolves null refuses too — a
 *    sensitive event with no data subject is a config bug, not a plaintext
 *    event.
 *
 * Wire shape: each encrypted field's value is replaced by the reserved
 * marker `{ "$enc": "<base64 nonce||ciphertext>" }` (the payload wire
 * format of crypto/payload.ts), AAD-bound to `{streamId}\n{keyId}\n{field}`
 * — see {@link fieldAad}. The envelope's reserved `keyId` records the one
 * generation that encrypted the whole event.
 *
 * Reads are marker-driven, not annotation-driven: `deserialize` decrypts
 * whatever markers the stored event actually carries. The log is immutable
 * but the config is not — annotations widened, narrowed, dropped, or
 * migrated to `"plaintext"` since an event was written change nothing about
 * how it reads back. Two write-time guards keep that sound, both checked
 * before any key-store side effect:
 *
 *  - a plaintext value on an encrypted event may not take a reserved shape
 *    ({$enc} / {$shredded}) — it would read back as ciphertext or as an
 *    erasure that never happened;
 *  - the subject must still resolve, unchanged, from the marker-substituted
 *    data — annotating the subject-bearing field itself would make every
 *    event permanently undecryptable (the log is immutable; refuse now).
 *
 * Shredded fields degrade, they don't destroy the replay: when the key is
 * gone (soft- or hard-deleted), `deserialize` substitutes the reserved
 * {@link SHREDDED_FIELD} sentinel for each encrypted field instead of
 * throwing — a shredded author's events keep their structure and the
 * stream keeps its business meaning for everyone else (the whole-payload
 * serializer, whose stream has exactly one subject, correctly throws
 * instead). Ciphertext that fails authentication under a DELIVERED key is
 * different — that is tampering or a transplant, and stays loud
 * (ShreddedDataError from decryptPayload). So is a keyId naming a
 * generation that was never minted: the key store throws rather than
 * returning null (a shred is provable by its surviving tombstone, so a
 * rewritten keyId can never impersonate a lawful erasure — see
 * KeyStore.keyById).
 *
 * Read models: `deserialize` here is model A (the worker decrypts). For
 * model B serve envelopes raw (`read(…, { raw: true })` / the HTTP page
 * helpers) and decrypt in the browser with `decryptPayload` + `fieldAad`
 * (exported from `./client` too); the shipped stream client decrypts field
 * markers only through its deployment-supplied `fieldKeyFor` hook
 * (per-subject key delivery is deployment-owned) and yields them raw
 * without one.
 */

import { SerializationError, ShreddedDataError } from "../errors.js";
import type { PayloadSerializer, SerializedPayload } from "../serializer.js";
import type { EventEnvelope } from "../types.js";
import { cryptoRandom, type RandomFn } from "./bytes.js";
import type { KeyStore } from "./keystore.js";
import { decryptPayload, encryptPayload, fieldAad } from "./payload.js";

/**
 * What a shredded field deserializes to. JSON-serializable, so a projection
 * built over decrypted events can be stored and replayed; test with
 * {@link isShreddedField}, render as "erased".
 */
export const SHREDDED_FIELD = Object.freeze({ $shredded: true as const });

/** True for the {@link SHREDDED_FIELD} sentinel (by shape, not identity —
 * it survives JSON round-trips). */
export function isShreddedField(value: unknown): boolean {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { $shredded?: unknown }).$shredded === true
  );
}

/** The reserved on-wire marker an encrypted field's value is replaced by. */
export interface FieldEnvelope {
  $enc: string;
}

/** True for a {@link FieldEnvelope} field marker. By shape — and the shape
 * is trustworthy at rest: `serialize` refuses plaintext values that would
 * collide with it (see module doc), so on an encrypted event marker-shaped
 * ⇔ ciphertext. */
export function isFieldEnvelope(value: unknown): value is FieldEnvelope {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { $enc?: unknown }).$enc === "string"
  );
}

export interface FieldEncryptingSerializerConfig {
  keys: KeyStore;
  /**
   * The application contract, per event: map an event to its data subject
   * (the erasure unit). Consulted on both write (plaintext event in) and
   * read (stored envelope's `data` in — so the subject must be derivable
   * from the fields that stay plaintext). Returning null on an annotated
   * type is a refused write, not a plaintext one.
   */
  subjectFor(event: { type: string; data: unknown }): Promise<string | null> | string | null;
  /**
   * Per event type: the top-level `data` fields to encrypt under the
   * subject's key, or `"plaintext"` to store the type unencrypted
   * (structural events — deletes, reactions). Types absent from the map
   * fail closed: `serialize` throws. An empty field list is ambiguous
   * (annotated, yet indistinguishable from plaintext at rest) and is
   * rejected — say `"plaintext"` if that is what you mean.
   *
   * Annotations govern the WRITE side only. Reads decrypt whatever markers
   * the stored event carries (see module doc), so changing an annotation
   * never strands already-written ciphertext.
   */
  fields: Record<string, readonly string[] | "plaintext">;
  /**
   * Compress each field before encrypting. Default false: field values are
   * typically short, and gzip inflates short inputs — enable only for types
   * whose annotated fields carry long text.
   */
  compress?: boolean;
  random?: RandomFn;
}

export function fieldEncryptingSerializer(config: FieldEncryptingSerializerConfig): PayloadSerializer {
  const random = config.random ?? cryptoRandom;
  const compress = config.compress ?? false;

  function specFor(type: string): readonly string[] | "plaintext" | undefined {
    const spec = config.fields[type];
    if (Array.isArray(spec) && spec.length === 0) {
      throw new SerializationError(
        `event type "${type}" is annotated with an empty field list — use "plaintext" to opt out explicitly`,
      );
    }
    return spec;
  }

  return {
    async serialize(streamId, event): Promise<SerializedPayload> {
      // System streams are plaintext by rule (audit stream: no PII, no
      // dependency on the key store it audits) — same rule as whole-payload.
      if (streamId.startsWith("$")) return { data: event.data };

      const spec = specFor(event.type);
      if (spec === undefined) {
        throw new SerializationError(
          `event type "${event.type}" has no field annotation — list its encrypted fields or opt out ` +
            `with "plaintext" (unannotated types fail closed: the log is immutable, so silently stored ` +
            `plaintext PII would be stored forever)`,
        );
      }
      if (spec === "plaintext") return { data: event.data };

      const subject = await config.subjectFor(event);
      if (subject === null) {
        throw new SerializationError(
          `event type "${event.type}" is annotated with encrypted fields but subjectFor returned null — ` +
            `a sensitive event with no data subject is a config bug, not a plaintext event`,
        );
      }
      if (typeof event.data !== "object" || event.data === null || Array.isArray(event.data)) {
        throw new SerializationError(
          `event type "${event.type}" has annotated fields but non-object data`,
        );
      }

      // Every guard below runs before currentKey: a refused write must have
      // zero side effects, and currentKey lazily mints a key and appends a
      // KeyCreated audit event.
      const data = { ...(event.data as Record<string, unknown>) };
      for (const field of spec) {
        if (field.includes("\n")) {
          throw new SerializationError(`field name ${JSON.stringify(field)} would break AAD framing`);
        }
      }
      for (const [field, value] of Object.entries(data)) {
        // Reserved shapes: reads are marker-driven, so a plaintext value
        // shaped like a marker would read back as ciphertext (and a fake
        // $shredded as an erasure that never happened). Annotated fields
        // are exempt — they are about to become real markers.
        if (spec.includes(field)) continue;
        if (isFieldEnvelope(value) || isShreddedField(value)) {
          throw new SerializationError(
            `event type "${event.type}" field ${JSON.stringify(field)} is plaintext but shaped like ` +
              `a reserved marker ({$enc}/{$shredded}) — it would be misread on replay`,
          );
        }
      }
      // The subject must survive marker substitution: deserialize resolves
      // it from the STORED data, so a config that annotates the
      // subject-bearing field would write events nobody can ever decrypt.
      const probe = { ...data };
      for (const field of spec) {
        if (field in probe && probe[field] !== undefined) probe[field] = { $enc: "" };
      }
      const derived = await config.subjectFor({ type: event.type, data: probe });
      if (derived !== subject) {
        throw new SerializationError(
          `event type "${event.type}": subjectFor resolves ${JSON.stringify(subject)} from the ` +
            `plaintext event but ${JSON.stringify(derived ?? null)} once its fields are encrypted — ` +
            `the subject-bearing field must stay plaintext (identifiers vs. attributes; see module doc)`,
        );
      }

      // Fails closed (SubjectErasedError) on a soft-deleted subject — the
      // append path's tombstone consult, before any PUT.
      const { keyId, key } = await config.keys.currentKey(subject);
      for (const field of spec) {
        const value = data[field];
        if (!(field in data) || value === undefined) continue;
        data[field] = {
          $enc: await encryptPayload(key, value, {
            compress,
            random,
            aad: fieldAad(streamId, keyId, field),
          }),
        } satisfies FieldEnvelope;
      }
      return { data, keyId };
    },

    async deserialize(streamId, envelope: EventEnvelope): Promise<unknown> {
      if (envelope.keyId === undefined) return envelope.data; // plaintext event

      const raw = envelope.data;
      if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
        throw new ShreddedDataError(
          `stream ${streamId}: encrypted event "${envelope.type}" has non-object data`,
        );
      }
      // Marker-driven: decrypt what the stored event carries, not what the
      // CURRENT config says it should carry — annotations may have changed
      // since the event was written (see module doc). The marker shape is
      // reserved at write time, so shape ⇔ ciphertext.
      const data = { ...(raw as Record<string, unknown>) };
      const encrypted = Object.entries(data).filter(([, value]) => isFieldEnvelope(value)) as [
        string,
        FieldEnvelope,
      ][];
      if (encrypted.length === 0) return data; // keyId but nothing encrypted

      const subject = await config.subjectFor({ type: envelope.type, data: raw });
      if (subject === null) {
        throw new ShreddedDataError(
          `stream ${streamId}: encrypted event "${envelope.type}" resolves no subject`,
        );
      }

      // null = proven shred: substitute the sentinel per field (degrade,
      // don't destroy the replay — see the module doc). A keyId that was
      // never minted throws instead of degrading (KeyStore.keyById).
      const key = await config.keys.keyById(subject, envelope.keyId);
      for (const [field, marker] of encrypted) {
        data[field] =
          key === null
            ? SHREDDED_FIELD
            : await decryptPayload(key, marker.$enc, fieldAad(streamId, envelope.keyId, field));
      }
      return data;
    },
  };
}
