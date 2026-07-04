/**
 * Master-key wrapping (KEYS_DESIGN.md, "Keys stored wrapped"): data keys
 * are stored wrapped by a master key, so a key-bucket leak discloses
 * nothing. AWS KMS is one implementation of this interface; the shipped
 * default is a locally-configured 256-bit secret (R2 has no KMS).
 */

import { concatBytes, cryptoRandom, type RandomFn } from "./bytes.js";

export interface MasterKey {
  wrap(rawKey: Uint8Array): Promise<Uint8Array>;
  unwrap(wrapped: Uint8Array): Promise<Uint8Array>;
}

const NONCE_LENGTH = 12;

/** AES-256-GCM wrapping under a locally-held 32-byte secret. */
export function aesMasterKey(secret: Uint8Array, random: RandomFn = cryptoRandom): MasterKey {
  if (secret.length !== 32) throw new RangeError("master key secret must be 32 bytes");
  const keyPromise = globalThis.crypto.subtle.importKey(
    "raw",
    secret as BufferSource,
    "AES-GCM",
    false,
    ["encrypt", "decrypt"],
  );
  return {
    async wrap(rawKey) {
      const iv = random(NONCE_LENGTH);
      const ct = await globalThis.crypto.subtle.encrypt(
        { name: "AES-GCM", iv: iv as BufferSource },
        await keyPromise,
        rawKey as BufferSource,
      );
      return concatBytes(iv, new Uint8Array(ct));
    },
    async unwrap(wrapped) {
      const iv = wrapped.subarray(0, NONCE_LENGTH);
      const ct = wrapped.subarray(NONCE_LENGTH);
      const pt = await globalThis.crypto.subtle.decrypt(
        { name: "AES-GCM", iv: iv as BufferSource },
        await keyPromise,
        ct as BufferSource,
      );
      return new Uint8Array(pt);
    },
  };
}
