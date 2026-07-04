/**
 * Storage driver over `@aws-sdk/client-s3` v3 (Node; also R2/MinIO via
 * endpoint override + `forcePathStyle`). The SDK is an optional peer of
 * this driver only — never a core dependency.
 *
 * Error mapping: outcomes the protocol branches on (404 / 412) become
 * result values; 5xx, 429, 409 (ConditionalRequestConflict — S3's "another
 * conditional write is in flight", retryable) and transport errors become
 * `TransientStoreError`; everything else rethrows.
 */

import {
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import type {
  GetResult,
  ListPage,
  PutIfAbsentResult,
  PutIfMatchResult,
  StorageDriver,
} from "../driver.js";
import { TransientStoreError } from "../errors.js";

/** Structural: satisfied by a real `S3Client`; narrow enough to fake in tests. */
export interface S3ClientLike {
  send(command: unknown): Promise<unknown>;
}

export interface AwsSdkDriverOptions {
  client: S3ClientLike;
  bucket: string;
}

function status(err: unknown): number | undefined {
  if (typeof err === "object" && err !== null && "$metadata" in err) {
    const meta = (err as { $metadata?: { httpStatusCode?: number } }).$metadata;
    return meta?.httpStatusCode;
  }
  return undefined;
}

function errName(err: unknown): string {
  return err instanceof Error ? err.name : "";
}

/** Throw TransientStoreError for retryable failures; rethrow the rest. */
function classify(err: unknown): never {
  const code = status(err);
  if (code === undefined || code >= 500 || code === 429 || code === 409) {
    // No status = transport-level (timeout, connection reset).
    throw new TransientStoreError(`s3 request failed: ${String(err)}`);
  }
  throw err;
}

function is404(err: unknown): boolean {
  return status(err) === 404 || errName(err) === "NoSuchKey" || errName(err) === "NotFound";
}

function is412(err: unknown): boolean {
  return status(err) === 412 || errName(err) === "PreconditionFailed";
}

export function awsSdkDriver(opts: AwsSdkDriverOptions): StorageDriver {
  const { client, bucket } = opts;

  return {
    async get(key, o): Promise<GetResult> {
      try {
        const resp = (await client.send(
          new GetObjectCommand({
            Bucket: bucket,
            Key: key,
            ...(o?.ifMatch !== undefined ? { IfMatch: o.ifMatch } : {}),
          }),
        )) as { Body?: { transformToString(): Promise<string> }; ETag?: string };
        return {
          kind: "found",
          body: (await resp.Body?.transformToString()) ?? "",
          etag: resp.ETag ?? "",
        };
      } catch (err) {
        if (is404(err)) return { kind: "not-found" };
        if (is412(err)) return { kind: "precondition-failed" };
        classify(err);
      }
    },

    async put(key, body): Promise<{ etag: string }> {
      try {
        const resp = (await client.send(
          new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }),
        )) as { ETag?: string };
        return { etag: resp.ETag ?? "" };
      } catch (err) {
        classify(err);
      }
    },

    async putIfAbsent(key, body): Promise<PutIfAbsentResult> {
      try {
        const resp = (await client.send(
          new PutObjectCommand({ Bucket: bucket, Key: key, Body: body, IfNoneMatch: "*" }),
        )) as { ETag?: string };
        return { kind: "created", etag: resp.ETag ?? "" };
      } catch (err) {
        if (is412(err)) return { kind: "exists" };
        classify(err);
      }
    },

    async putIfMatch(key, body, etag): Promise<PutIfMatchResult> {
      try {
        const resp = (await client.send(
          new PutObjectCommand({ Bucket: bucket, Key: key, Body: body, IfMatch: etag }),
        )) as { ETag?: string };
        return { kind: "updated", etag: resp.ETag ?? "" };
      } catch (err) {
        // A CAS PUT against a missing key surfaces as 404 on S3: same outcome.
        if (is412(err) || is404(err)) return { kind: "precondition-failed" };
        classify(err);
      }
    },

    async list(prefix, o): Promise<ListPage> {
      try {
        const resp = (await client.send(
          new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: prefix,
            ...(o?.startAfter !== undefined ? { StartAfter: o.startAfter } : {}),
            ...(o?.maxKeys !== undefined ? { MaxKeys: o.maxKeys } : {}),
          }),
        )) as {
          Contents?: { Key?: string; ETag?: string }[];
          IsTruncated?: boolean;
        };
        const keys = (resp.Contents ?? []).map((c) => ({ key: c.Key ?? "", etag: c.ETag ?? "" }));
        if (resp.IsTruncated === true && keys.length > 0) {
          return { keys, nextStartAfter: keys[keys.length - 1]!.key };
        }
        return { keys };
      } catch (err) {
        classify(err);
      }
    },

    async delete(key): Promise<void> {
      try {
        await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
      } catch (err) {
        classify(err);
      }
    },

    async deleteMany(keys): Promise<void> {
      if (keys.length === 0) return;
      try {
        await client.send(
          new DeleteObjectsCommand({
            Bucket: bucket,
            Delete: { Objects: keys.map((Key) => ({ Key })), Quiet: true },
          }),
        );
      } catch (err) {
        classify(err);
      }
    },
  };
}
