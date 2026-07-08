/**
 * The encrypted-payload wire format, shared by the encrypting serializer
 * (write side) and the browser client (read side):
 *
 *   data = base64( nonce(12) || AES-256-GCM( header(1) || body ) )
 *   header 0x01 = gzip'd JSON, 0x00 = raw JSON
 *   AAD    = "{streamId}\n{keyId}" (see payloadAad)
 *
 * The AAD binds each ciphertext to its context: a valid ciphertext
 * transplanted to another stream, subject, or generation fails
 * authentication instead of decrypting cleanly in the wrong place.
 * Version is deliberately NOT bound: serialization precedes version
 * assignment (a retried conditional PUT must carry byte-identical
 * content), so the accepted residual is duplication of a ciphertext at
 * another version of the same stream under the same generation.
 *
 * WebCrypto only — runs in Node ≥ 20, Workers, and browsers.
 */

import { ShreddedDataError } from "../errors.js";
import {
  base64ToBytes,
  bytesToBase64,
  concatBytes,
  gunzip,
  gzip,
  type RandomFn,
} from "./bytes.js";

const NONCE_LENGTH = 12;
const FORMAT_JSON = 0x00;
const FORMAT_GZIP_JSON = 0x01;

const utf8 = new TextEncoder();
const utf8dec = new TextDecoder();

async function importKey(raw: Uint8Array): Promise<CryptoKey> {
  return globalThis.crypto.subtle.importKey("raw", raw as BufferSource, "AES-GCM", false, [
    "encrypt",
    "decrypt",
  ]);
}

/** The context a payload ciphertext is bound to (GCM AAD). */
export function payloadAad(streamId: string, keyId: string): Uint8Array {
  return utf8.encode(`${streamId}\n${keyId}`);
}

/**
 * The context a field ciphertext is bound to (GCM AAD): stream, generation,
 * and field path — the field-level serializer's counterpart of
 * {@link payloadAad}. The third segment both pins the field (a `title`
 * ciphertext moved into `body` fails authentication) and separates the
 * domains (a field ciphertext can never authenticate as a whole payload or
 * vice versa). Same deliberate omission of the version, same accepted
 * residual as payloadAad.
 */
export function fieldAad(streamId: string, keyId: string, field: string): Uint8Array {
  return utf8.encode(`${streamId}\n${keyId}\n${field}`);
}

export async function encryptPayload(
  rawKey: Uint8Array,
  plaintext: unknown,
  opts: { compress: boolean; random: RandomFn; aad: Uint8Array },
): Promise<string> {
  const json = utf8.encode(JSON.stringify(plaintext === undefined ? null : plaintext));
  const body = opts.compress
    ? concatBytes(new Uint8Array([FORMAT_GZIP_JSON]), await gzip(json))
    : concatBytes(new Uint8Array([FORMAT_JSON]), json);
  const nonce = opts.random(NONCE_LENGTH);
  const ct = await globalThis.crypto.subtle.encrypt(
    { name: "AES-GCM", iv: nonce as BufferSource, additionalData: opts.aad as BufferSource },
    await importKey(rawKey),
    body as BufferSource,
  );
  return bytesToBase64(concatBytes(nonce, new Uint8Array(ct)));
}

/** Fail-closed: GCM authentication failure (wrong key, tampered bytes, or
 * a ciphertext transplanted to a foreign context) throws ShreddedDataError. */
export async function decryptPayload(
  rawKey: Uint8Array,
  encoded: string,
  aad: Uint8Array,
): Promise<unknown> {
  const bytes = base64ToBytes(encoded);
  const nonce = bytes.subarray(0, NONCE_LENGTH);
  let body: Uint8Array;
  try {
    body = new Uint8Array(
      await globalThis.crypto.subtle.decrypt(
        { name: "AES-GCM", iv: nonce as BufferSource, additionalData: aad as BufferSource },
        await importKey(rawKey),
        bytes.subarray(NONCE_LENGTH) as BufferSource,
      ),
    );
  } catch {
    throw new ShreddedDataError("ciphertext failed authentication");
  }
  const payload = body.subarray(1);
  const json = body[0] === FORMAT_GZIP_JSON ? await gunzip(payload) : payload;
  return JSON.parse(utf8dec.decode(json)) as unknown;
}
