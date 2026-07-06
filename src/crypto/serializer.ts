/**
 * Whole-payload encrypting serializer (KEYS_DESIGN.md, "Whole-payload vs.
 * field-level"): compress, then AES-256-GCM encrypt the entire `data`
 * blob; envelope metadata stays plaintext, `keyId` records the generation.
 *
 * Nonce discipline (a rule, not an implementation detail): 96-bit random,
 * fresh per encryption — never counters or timestamps; uncoordinated
 * writers share generation keys, and GCM nonce reuse is catastrophic.
 * Random nonces bound safe use to ~2^32 encryptions per key: treat as an
 * input to rotation cadence.
 *
 * Wire format: see crypto/payload.ts (shared with the browser client).
 */

import { ShreddedDataError } from "../errors.js";
import type { PayloadSerializer } from "../serializer.js";
import { cryptoRandom, type RandomFn } from "./bytes.js";
import type { KeyStore } from "./keystore.js";
import { decryptPayload, encryptPayload, payloadAad } from "./payload.js";

export interface EncryptingSerializerConfig {
  keys: KeyStore;
  /**
   * The application contract (KEYS_DESIGN.md, erasure completeness): every
   * stream containing a subject's personal data must map to that subject.
   * Return null for streams to store in plaintext.
   */
  subjectFor(streamId: string): string | null;
  /** Compress before encrypting (ciphertext doesn't compress). Default true. */
  compress?: boolean;
  random?: RandomFn;
}

export function encryptingSerializer(config: EncryptingSerializerConfig): PayloadSerializer {
  const random = config.random ?? cryptoRandom;
  const compress = config.compress ?? true;

  function subjectOf(streamId: string): string | null {
    // System streams are plaintext by rule (audit stream: no PII, no
    // dependency on the key store it audits).
    if (streamId.startsWith("$")) return null;
    return config.subjectFor(streamId);
  }

  return {
    async serialize(streamId, event) {
      const subjectId = subjectOf(streamId);
      if (subjectId === null) return { data: event.data };
      // Fails closed (SubjectErasedError) on a soft-deleted subject —
      // the append path's tombstone consult, before any PUT.
      const { keyId, key } = await config.keys.currentKey(subjectId);
      // AAD binds the ciphertext to its stream and generation: transplanted
      // ciphertext fails authentication instead of decrypting in the wrong
      // context (see crypto/payload.ts).
      return {
        data: await encryptPayload(key, event.data, {
          compress,
          random,
          aad: payloadAad(streamId, keyId),
        }),
        keyId,
      };
    },

    async deserialize(streamId, envelope) {
      if (envelope.keyId === undefined) return envelope.data; // plaintext event
      const subjectId = subjectOf(streamId);
      if (subjectId === null) {
        throw new ShreddedDataError(
          `stream ${streamId} has encrypted events but no subject mapping`,
        );
      }
      const key = await config.keys.keyById(subjectId, envelope.keyId);
      if (key === null) {
        throw new ShreddedDataError(
          `key ${envelope.keyId} for subject ${subjectId} is shredded or undeliverable`,
        );
      }
      return decryptPayload(key, envelope.data as string, payloadAad(streamId, envelope.keyId));
    },
  };
}
