/**
 * The encrypted-payload wire format, shared by the encrypting serializer
 * (write side) and the browser client (read side):
 *
 *   data = base64( nonce(12) || AES-256-GCM( header(1) || body ) )
 *   header 0x01 = gzip'd JSON, 0x00 = raw JSON
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

export async function encryptPayload(
  rawKey: Uint8Array,
  plaintext: unknown,
  opts: { compress: boolean; random: RandomFn },
): Promise<string> {
  const json = utf8.encode(JSON.stringify(plaintext === undefined ? null : plaintext));
  const body = opts.compress
    ? concatBytes(new Uint8Array([FORMAT_GZIP_JSON]), await gzip(json))
    : concatBytes(new Uint8Array([FORMAT_JSON]), json);
  const nonce = opts.random(NONCE_LENGTH);
  const ct = await globalThis.crypto.subtle.encrypt(
    { name: "AES-GCM", iv: nonce as BufferSource },
    await importKey(rawKey),
    body as BufferSource,
  );
  return bytesToBase64(concatBytes(nonce, new Uint8Array(ct)));
}

/** Fail-closed: GCM authentication failure throws ShreddedDataError. */
export async function decryptPayload(rawKey: Uint8Array, encoded: string): Promise<unknown> {
  const bytes = base64ToBytes(encoded);
  const nonce = bytes.subarray(0, NONCE_LENGTH);
  let body: Uint8Array;
  try {
    body = new Uint8Array(
      await globalThis.crypto.subtle.decrypt(
        { name: "AES-GCM", iv: nonce as BufferSource },
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
