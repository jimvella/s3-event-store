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
 * Shredded fields degrade, they don't destroy the replay: when the key is
 * gone (soft- or hard-deleted), `deserialize` substitutes the reserved
 * {@link SHREDDED_FIELD} sentinel for each encrypted field instead of
 * throwing — a shredded author's events keep their structure and the
 * stream keeps its business meaning for everyone else (the whole-payload
 * serializer, whose stream has exactly one subject, correctly throws
 * instead). Ciphertext that fails authentication under a DELIVERED key is
 * different — that is tampering or a transplant, and stays loud
 * (ShreddedDataError from decryptPayload).
 *
 * Read models: `deserialize` here is model A (the worker decrypts). For
 * model B serve envelopes raw (`read(…, { raw: true })` / the HTTP page
 * helpers) and decrypt in the browser with `decryptPayload` + `fieldAad`
 * (exported from `./client` too); the shipped stream client's automatic
 * decryption remains whole-payload (its keyring is per-stream — per-subject
 * keyring delivery is deployment-owned).
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

interface FieldEnvelope {
  $enc: string;
}

function isFieldEnvelope(value: unknown): value is FieldEnvelope {
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

      // Fails closed (SubjectErasedError) on a soft-deleted subject — the
      // append path's tombstone consult, before any PUT.
      const { keyId, key } = await config.keys.currentKey(subject);
      const data = { ...(event.data as Record<string, unknown>) };
      for (const field of spec) {
        if (field.includes("\n")) {
          throw new SerializationError(`field name ${JSON.stringify(field)} would break AAD framing`);
        }
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

      const spec = specFor(envelope.type);
      if (spec === undefined || spec === "plaintext") {
        throw new ShreddedDataError(
          `stream ${streamId}: event type "${envelope.type}" carries keyId ${envelope.keyId} ` +
            `but has no field annotation to decrypt by`,
        );
      }
      const raw = envelope.data;
      if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
        throw new ShreddedDataError(
          `stream ${streamId}: encrypted event "${envelope.type}" has non-object data`,
        );
      }
      const subject = await config.subjectFor({ type: envelope.type, data: raw });
      if (subject === null) {
        throw new ShreddedDataError(
          `stream ${streamId}: encrypted event "${envelope.type}" resolves no subject`,
        );
      }

      // null = shredded or undeliverable: substitute the sentinel per field
      // (degrade, don't destroy the replay — see the module doc).
      const key = await config.keys.keyById(subject, envelope.keyId);
      const data = { ...(raw as Record<string, unknown>) };
      for (const field of spec) {
        const value = data[field];
        // Not a marker: stored plaintext (e.g. the field was annotated after
        // this event was written) — pass through unchanged.
        if (!isFieldEnvelope(value)) continue;
        data[field] =
          key === null
            ? SHREDDED_FIELD
            : await decryptPayload(key, value.$enc, fieldAad(streamId, envelope.keyId, field));
      }
      return data;
    },
  };
}
