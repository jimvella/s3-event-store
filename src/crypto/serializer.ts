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
 * Wire format: data = base64( nonce(12) || GCM(header(1) || body) ) where
 * header 0x01 = gzip'd JSON, 0x00 = raw JSON.
 */

import { ShreddedDataError } from "../errors.js";
import type { PayloadSerializer } from "../serializer.js";
import {
  base64ToBytes,
  bytesToBase64,
  concatBytes,
  cryptoRandom,
  gunzip,
  gzip,
  type RandomFn,
} from "./bytes.js";
import type { KeyStore } from "./keystore.js";

const NONCE_LENGTH = 12;
const FORMAT_JSON = 0x00;
const FORMAT_GZIP_JSON = 0x01;

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
  const utf8 = new TextEncoder();
  const utf8dec = new TextDecoder();

  function subjectOf(streamId: string): string | null {
    // System streams are plaintext by rule (audit stream: no PII, no
    // dependency on the key store it audits).
    if (streamId.startsWith("$")) return null;
    return config.subjectFor(streamId);
  }

  async function importKey(raw: Uint8Array): Promise<CryptoKey> {
    return globalThis.crypto.subtle.importKey("raw", raw as BufferSource, "AES-GCM", false, [
      "encrypt",
      "decrypt",
    ]);
  }

  return {
    async serialize(streamId, event) {
      const subjectId = subjectOf(streamId);
      if (subjectId === null) return { data: event.data };

      // Fails closed (SubjectErasedError) on a soft-deleted subject —
      // the append path's tombstone consult, before any PUT.
      const { keyId, key } = await config.keys.currentKey(subjectId);
      const json = utf8.encode(JSON.stringify(event.data === undefined ? null : event.data));
      const body = compress
        ? concatBytes(new Uint8Array([FORMAT_GZIP_JSON]), await gzip(json))
        : concatBytes(new Uint8Array([FORMAT_JSON]), json);
      const nonce = random(NONCE_LENGTH);
      const ct = await globalThis.crypto.subtle.encrypt(
        { name: "AES-GCM", iv: nonce as BufferSource },
        await importKey(key),
        body as BufferSource,
      );
      return { data: bytesToBase64(concatBytes(nonce, new Uint8Array(ct))), keyId };
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
      const bytes = base64ToBytes(envelope.data as string);
      const nonce = bytes.subarray(0, NONCE_LENGTH);
      let body: Uint8Array;
      try {
        body = new Uint8Array(
          await globalThis.crypto.subtle.decrypt(
            { name: "AES-GCM", iv: nonce as BufferSource },
            await importKey(key),
            bytes.subarray(NONCE_LENGTH) as BufferSource,
          ),
        );
      } catch {
        // GCM authentication failure: tampered or mis-keyed. Fail closed.
        throw new ShreddedDataError(`event ${envelope.id}: ciphertext failed authentication`);
      }
      const format = body[0];
      const payload = body.subarray(1);
      const json = format === FORMAT_GZIP_JSON ? await gunzip(payload) : payload;
      return JSON.parse(utf8dec.decode(json)) as unknown;
    },
  };
}
