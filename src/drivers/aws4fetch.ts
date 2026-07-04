/**
 * Storage driver over S3/R2's HTTP API signed with `aws4fetch` — for
 * Workers where bundle-size limits rule out the AWS SDK (DESIGN.md,
 * Storage drivers). Path-style addressing; `aws4fetch` is an optional peer
 * of this driver only.
 *
 * `deleteMany` is sequential single-key DELETEs: the batch DeleteObjects
 * API requires a Content-MD5 header and WebCrypto has no MD5. Semantically
 * identical (deletes are idempotent), one request per key instead of one
 * per thousand — acceptable until a Workers deployment compacts at a scale
 * where it isn't, at which point add a CRC32 checksum variant.
 */

import { AwsClient } from "aws4fetch";
import type {
  GetResult,
  ListPage,
  PutIfAbsentResult,
  PutIfMatchResult,
  StorageDriver,
} from "../driver.js";
import { TransientStoreError } from "../errors.js";

export interface Aws4FetchDriverOptions {
  accessKeyId: string;
  secretAccessKey: string;
  /** e.g. "https://<account>.r2.cloudflarestorage.com" or "https://s3.us-east-1.amazonaws.com" */
  endpoint: string;
  bucket: string;
  region?: string;
  /** Injectable for tests; defaults to global fetch. */
  fetchImpl?: (request: Request) => Promise<Response>;
}

function encodeKey(key: string): string {
  return key.split("/").map(encodeURIComponent).join("/");
}

const XML_UNESCAPES: Record<string, string> = {
  "&quot;": '"',
  "&amp;": "&",
  "&lt;": "<",
  "&gt;": ">",
  "&apos;": "'",
};

function xmlUnescape(value: string): string {
  return value.replace(/&(?:quot|amp|lt|gt|apos);/g, (m) => XML_UNESCAPES[m] ?? m);
}

/** Values of every <tag>…</tag> in the document, in order. */
function xmlValues(xml: string, tag: string): string[] {
  const out: string[] = [];
  const re = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, "g");
  for (let m = re.exec(xml); m !== null; m = re.exec(xml)) out.push(m[1]!);
  return out;
}

export function aws4fetchDriver(opts: Aws4FetchDriverOptions): StorageDriver {
  const client = new AwsClient({
    accessKeyId: opts.accessKeyId,
    secretAccessKey: opts.secretAccessKey,
    service: "s3",
    region: opts.region ?? "auto",
  });
  const doFetch = opts.fetchImpl ?? ((req: Request) => fetch(req));
  const base = `${opts.endpoint.replace(/\/$/, "")}/${opts.bucket}`;

  async function send(
    method: string,
    url: string,
    headers?: Record<string, string>,
    body?: string,
  ): Promise<Response> {
    const signed = await client.sign(url, {
      method,
      ...(headers !== undefined ? { headers } : {}),
      ...(body !== undefined ? { body } : {}),
    });
    let resp: Response;
    try {
      resp = await doFetch(signed);
    } catch (err) {
      throw new TransientStoreError(`s3 http request failed: ${String(err)}`);
    }
    // 409 = ConditionalRequestConflict (concurrent conditional writes): retryable.
    if (resp.status >= 500 || resp.status === 429 || resp.status === 409) {
      throw new TransientStoreError(`s3 http ${resp.status} for ${method} ${url}`);
    }
    return resp;
  }

  function objectUrl(key: string): string {
    return `${base}/${encodeKey(key)}`;
  }

  return {
    async get(key, o): Promise<GetResult> {
      const resp = await send(
        "GET",
        objectUrl(key),
        o?.ifMatch !== undefined ? { "if-match": o.ifMatch } : {},
      );
      if (resp.status === 404) return { kind: "not-found" };
      if (resp.status === 412) return { kind: "precondition-failed" };
      if (!resp.ok) throw new TransientStoreError(`s3 http GET ${key}: ${resp.status}`);
      return { kind: "found", body: await resp.text(), etag: resp.headers.get("etag") ?? "" };
    },

    async put(key, body): Promise<{ etag: string }> {
      const resp = await send("PUT", objectUrl(key), {}, body);
      if (!resp.ok) throw new TransientStoreError(`s3 http PUT ${key}: ${resp.status}`);
      return { etag: resp.headers.get("etag") ?? "" };
    },

    async putIfAbsent(key, body): Promise<PutIfAbsentResult> {
      const resp = await send("PUT", objectUrl(key), { "if-none-match": "*" }, body);
      if (resp.status === 412) return { kind: "exists" };
      if (!resp.ok) throw new TransientStoreError(`s3 http PUT ${key}: ${resp.status}`);
      return { kind: "created", etag: resp.headers.get("etag") ?? "" };
    },

    async putIfMatch(key, body, etag): Promise<PutIfMatchResult> {
      const resp = await send("PUT", objectUrl(key), { "if-match": etag }, body);
      if (resp.status === 412 || resp.status === 404) return { kind: "precondition-failed" };
      if (!resp.ok) throw new TransientStoreError(`s3 http PUT ${key}: ${resp.status}`);
      return { kind: "updated", etag: resp.headers.get("etag") ?? "" };
    },

    async list(prefix, o): Promise<ListPage> {
      const params = new URLSearchParams({ "list-type": "2", prefix });
      if (o?.startAfter !== undefined) params.set("start-after", o.startAfter);
      if (o?.maxKeys !== undefined) params.set("max-keys", String(o.maxKeys));
      const resp = await send("GET", `${base}?${params.toString()}`);
      if (!resp.ok) throw new TransientStoreError(`s3 http LIST: ${resp.status}`);
      const xml = await resp.text();
      const keys = xmlValues(xml, "Contents").map((block) => ({
        key: xmlUnescape(xmlValues(block, "Key")[0] ?? ""),
        etag: xmlUnescape(xmlValues(block, "ETag")[0] ?? ""),
      }));
      const truncated = (xmlValues(xml, "IsTruncated")[0] ?? "false") === "true";
      if (truncated && keys.length > 0) {
        return { keys, nextStartAfter: keys[keys.length - 1]!.key };
      }
      return { keys };
    },

    async delete(key): Promise<void> {
      const resp = await send("DELETE", objectUrl(key));
      // 404 is success: S3 deletes are idempotent.
      if (!resp.ok && resp.status !== 404) {
        throw new TransientStoreError(`s3 http DELETE ${key}: ${resp.status}`);
      }
    },

    async deleteMany(keys): Promise<void> {
      for (const key of keys) await this.delete(key);
    },
  };
}
