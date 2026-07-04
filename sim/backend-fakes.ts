/**
 * In-memory fakes of each backend's native API, backed by SimStore — they
 * exercise the real drivers' *mapping* code (command construction, error
 * classification, XML parsing, onlyIf translation) against the reference
 * semantics. They do not replace conformance runs against real backends;
 * they catch mapping bugs cheaply and deterministically.
 */

import {
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import type { S3ClientLike } from "../src/drivers/aws-sdk.js";
import type { R2BucketLike, R2ObjectLike } from "../src/drivers/r2-binding.js";
import type { SimStore } from "./store.js";

function s3Error(name: string, httpStatusCode: number): Error {
  return Object.assign(new Error(name), { name, $metadata: { httpStatusCode } });
}

/** Fake `S3Client` — SDK wire behavior over SimStore semantics. */
export function fakeS3Client(store: SimStore): S3ClientLike {
  return {
    async send(command: unknown): Promise<unknown> {
      if (command instanceof PutObjectCommand) {
        const { Key, Body, IfNoneMatch, IfMatch } = command.input;
        const body = String(Body);
        if (IfNoneMatch === "*") {
          const r = store.putIfAbsent(Key!, body);
          if (r.kind === "exists") throw s3Error("PreconditionFailed", 412);
          return { ETag: r.etag };
        }
        if (IfMatch !== undefined) {
          const existing = store.get(Key!);
          if (existing.kind === "not-found") throw s3Error("NoSuchKey", 404);
          const r = store.putIfMatch(Key!, body, IfMatch);
          if (r.kind === "precondition-failed") throw s3Error("PreconditionFailed", 412);
          return { ETag: r.etag };
        }
        return { ETag: store.put(Key!, body).etag };
      }
      if (command instanceof GetObjectCommand) {
        const { Key, IfMatch } = command.input;
        const r = store.get(Key!, IfMatch !== undefined ? { ifMatch: IfMatch } : undefined);
        if (r.kind === "not-found") throw s3Error("NoSuchKey", 404);
        if (r.kind === "precondition-failed") throw s3Error("PreconditionFailed", 412);
        return { Body: { transformToString: async () => r.body }, ETag: r.etag };
      }
      if (command instanceof ListObjectsV2Command) {
        const { Prefix, StartAfter, MaxKeys } = command.input;
        const page = store.list(Prefix ?? "", {
          ...(StartAfter !== undefined ? { startAfter: StartAfter } : {}),
          ...(MaxKeys !== undefined ? { maxKeys: MaxKeys } : {}),
        });
        return {
          Contents: page.keys.map((x) => ({ Key: x.key, ETag: x.etag })),
          IsTruncated: page.nextStartAfter !== undefined,
        };
      }
      if (command instanceof DeleteObjectCommand) {
        store.delete(command.input.Key!);
        return {};
      }
      if (command instanceof DeleteObjectsCommand) {
        store.deleteMany((command.input.Delete?.Objects ?? []).map((o) => o.Key!));
        return {};
      }
      // The s2-sim rule: unimplemented operations fail loudly.
      throw new Error(`fakeS3Client: unimplemented command ${command?.constructor?.name}`);
    },
  };
}

/** Fake R2 bucket binding — the onlyIf semantics the r2 driver assumes. */
export function fakeR2Bucket(store: SimStore): R2BucketLike {
  // R2 etags are unquoted; strip SimStore's quotes to mimic the format gap.
  const r2etag = (etag: string) => etag.replace(/^"|"$/g, "");
  const matches = (cond: string, etag: string) => cond === "*" || r2etag(etag) === cond;
  return {
    async get(key, options): Promise<R2ObjectLike | null> {
      const r = store.get(key);
      if (r.kind !== "found") return null;
      const only = options?.onlyIf;
      if (only?.etagMatches !== undefined && !matches(only.etagMatches, r.etag)) {
        return { etag: r2etag(r.etag) }; // body-less object on precondition failure
      }
      return { etag: r2etag(r.etag), text: async () => r.body };
    },
    async put(key, value, options) {
      const existing = store.get(key);
      const only = options?.onlyIf;
      if (only?.etagDoesNotMatch !== undefined) {
        if (existing.kind === "found" && matches(only.etagDoesNotMatch, existing.etag)) {
          return null;
        }
      }
      if (only?.etagMatches !== undefined) {
        if (existing.kind !== "found" || !matches(only.etagMatches, existing.etag)) {
          return null;
        }
      }
      return { etag: r2etag(store.put(key, value).etag) };
    },
    async list(options) {
      const page = store.list(options?.prefix ?? "", {
        ...(options?.startAfter !== undefined ? { startAfter: options.startAfter } : {}),
        ...(options?.limit !== undefined ? { maxKeys: options.limit } : {}),
      });
      return {
        objects: page.keys.map((x) => ({ key: x.key, etag: r2etag(x.etag) })),
        truncated: page.nextStartAfter !== undefined,
      };
    },
    async delete(keys) {
      store.deleteMany(Array.isArray(keys) ? keys : [keys]);
    },
  };
}

const XML_ESCAPES: Record<string, string> = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
};

function xmlEscape(value: string): string {
  return value.replace(/[&<>"]/g, (c) => XML_ESCAPES[c] ?? c);
}

/** Fake S3 HTTP endpoint (path-style) for the aws4fetch driver. */
export function fakeS3Http(store: SimStore, bucket: string): (req: Request) => Promise<Response> {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const path = decodeURIComponent(url.pathname).replace(/^\//, "");
    if (!path.startsWith(bucket)) return new Response("NoSuchBucket", { status: 404 });
    const key = path.slice(bucket.length).replace(/^\//, "");

    if (req.method === "GET" && url.searchParams.get("list-type") === "2") {
      const startAfter = url.searchParams.get("start-after");
      const maxKeys = url.searchParams.get("max-keys");
      const page = store.list(url.searchParams.get("prefix") ?? "", {
        ...(startAfter !== null ? { startAfter } : {}),
        ...(maxKeys !== null ? { maxKeys: Number(maxKeys) } : {}),
      });
      const contents = page.keys
        .map(
          (x) =>
            `<Contents><Key>${xmlEscape(x.key)}</Key><ETag>${xmlEscape(x.etag)}</ETag></Contents>`,
        )
        .join("");
      const xml = `<?xml version="1.0"?><ListBucketResult><IsTruncated>${
        page.nextStartAfter !== undefined
      }</IsTruncated>${contents}</ListBucketResult>`;
      return new Response(xml, { status: 200, headers: { "content-type": "application/xml" } });
    }

    if (req.method === "GET") {
      const ifMatch = req.headers.get("if-match");
      const r = store.get(key, ifMatch !== null ? { ifMatch } : undefined);
      if (r.kind === "not-found") return new Response("NoSuchKey", { status: 404 });
      if (r.kind === "precondition-failed") return new Response("", { status: 412 });
      return new Response(r.body, { status: 200, headers: { etag: r.etag } });
    }

    if (req.method === "PUT") {
      const body = await req.text();
      if (req.headers.get("if-none-match") === "*") {
        const r = store.putIfAbsent(key, body);
        if (r.kind === "exists") return new Response("", { status: 412 });
        return new Response("", { status: 200, headers: { etag: r.etag } });
      }
      const ifMatch = req.headers.get("if-match");
      if (ifMatch !== null) {
        const r = store.putIfMatch(key, body, ifMatch);
        if (r.kind === "precondition-failed") return new Response("", { status: 412 });
        return new Response("", { status: 200, headers: { etag: r.etag } });
      }
      return new Response("", { status: 200, headers: { etag: store.put(key, body).etag } });
    }

    if (req.method === "DELETE") {
      store.delete(key);
      return new Response(null, { status: 204 });
    }

    return new Response("NotImplemented", { status: 501 });
  };
}
