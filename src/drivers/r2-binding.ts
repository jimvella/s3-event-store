/**
 * Storage driver over the native Cloudflare Workers R2 binding — no request
 * signing, no SDK weight (DESIGN.md, Storage drivers).
 *
 * Types are structural (no @cloudflare/workers-types dependency): a real
 * `R2Bucket` satisfies `R2BucketLike`.
 *
 * CONFORMANCE CAVEAT (DESIGN.md phase 1): the `onlyIf` conditional-put
 * semantics assumed here — `etagDoesNotMatch: "*"` as create-only,
 * `etagMatches` as CAS, `null` return on precondition failure — must be
 * conformance-tested against a real R2 bucket before this becomes the
 * default Workers driver. ETag *format* also differs from S3 (unquoted);
 * that is fine within one driver (etags are opaque) but means pins are not
 * portable across drivers over the same bucket.
 */

import type {
  GetResult,
  ListPage,
  PutIfAbsentResult,
  PutIfMatchResult,
  StorageDriver,
} from "../driver.js";
import { TransientStoreError } from "../errors.js";

/** R2's get returns a body-less object on precondition failure. */
export interface R2ObjectLike {
  etag: string;
  text?: () => Promise<string>;
}

export interface R2BucketLike {
  get(
    key: string,
    options?: { onlyIf?: { etagMatches?: string } },
  ): Promise<R2ObjectLike | null>;
  put(
    key: string,
    value: string,
    options?: { onlyIf?: { etagMatches?: string; etagDoesNotMatch?: string } },
  ): Promise<{ etag: string } | null>;
  list(options?: {
    prefix?: string;
    startAfter?: string;
    limit?: number;
  }): Promise<{ objects: { key: string; etag: string }[]; truncated: boolean }>;
  delete(keys: string | string[]): Promise<void>;
}

export function r2BindingDriver(bucket: R2BucketLike): StorageDriver {
  const run = async <T>(op: string, fn: () => Promise<T>): Promise<T> => {
    try {
      return await fn();
    } catch (err) {
      // Binding failures are infrastructure-level; the outcomes the protocol
      // branches on (absent, precondition) are encoded as nulls, not throws.
      throw new TransientStoreError(`r2 ${op} failed: ${String(err)}`);
    }
  };

  return {
    async get(key, o): Promise<GetResult> {
      const result = await run("get", () =>
        bucket.get(key, o?.ifMatch !== undefined ? { onlyIf: { etagMatches: o.ifMatch } } : {}),
      );
      if (result === null) return { kind: "not-found" };
      if (typeof result.text !== "function") return { kind: "precondition-failed" };
      return { kind: "found", body: await result.text(), etag: result.etag };
    },

    async put(key, body): Promise<{ etag: string }> {
      const result = await run("put", () => bucket.put(key, body));
      if (result === null) throw new TransientStoreError("r2 put returned null unconditionally");
      return { etag: result.etag };
    },

    async putIfAbsent(key, body): Promise<PutIfAbsentResult> {
      const result = await run("putIfAbsent", () =>
        bucket.put(key, body, { onlyIf: { etagDoesNotMatch: "*" } }),
      );
      if (result === null) return { kind: "exists" };
      return { kind: "created", etag: result.etag };
    },

    async putIfMatch(key, body, etag): Promise<PutIfMatchResult> {
      const result = await run("putIfMatch", () =>
        bucket.put(key, body, { onlyIf: { etagMatches: etag } }),
      );
      if (result === null) return { kind: "precondition-failed" };
      return { kind: "updated", etag: result.etag };
    },

    async list(prefix, o): Promise<ListPage> {
      const result = await run("list", () =>
        bucket.list({
          prefix,
          ...(o?.startAfter !== undefined ? { startAfter: o.startAfter } : {}),
          ...(o?.maxKeys !== undefined ? { limit: o.maxKeys } : {}),
        }),
      );
      const keys = result.objects.map((obj) => ({ key: obj.key, etag: obj.etag }));
      if (result.truncated && keys.length > 0) {
        return { keys, nextStartAfter: keys[keys.length - 1]!.key };
      }
      return { keys };
    },

    async delete(key): Promise<void> {
      await run("delete", () => bucket.delete(key));
    },

    async deleteMany(keys): Promise<void> {
      if (keys.length === 0) return;
      await run("deleteMany", () => bucket.delete(keys));
    },
  };
}
