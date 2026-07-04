/**
 * One suite, every driver (SIMULATOR_PLAN.md, conformance parity):
 * - the direct SimStore driver — the executable reference itself;
 * - each real driver over an in-memory fake of its backend API — exercises
 *   the driver's mapping code against the reference semantics;
 * - optionally, the aws-sdk driver against real S3-compatible endpoints
 *   (S3, R2, MinIO, LocalStack), configured either by a gitignored
 *   `conformance.local.json` at the repo root — one target object or an
 *   array of them:
 *
 *     [{ "endpoint": "https://s3.us-east-1.amazonaws.com",
 *        "bucket": "s3-eventsourcing-test", "region": "us-east-1",
 *        "accessKey": "...", "secretKey": "..." },
 *      { "endpoint": "https://<account>.r2.cloudflarestorage.com",
 *        "bucket": "s3ev-conformance", "region": "auto",
 *        "accessKey": "...", "secretKey": "..." }]
 *
 *   — or by S3EV_CONFORMANCE_{ENDPOINT,BUCKET,REGION,ACCESS_KEY,SECRET_KEY}
 *   environment variables. Then: npx vitest run sim/conformance.test.ts
 */

import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, it } from "vitest";
import { awsSdkDriver } from "../src/drivers/aws-sdk";
import { aws4fetchDriver } from "../src/drivers/aws4fetch";
import { r2BindingDriver } from "../src/drivers/r2-binding";
import { fakeR2Bucket, fakeS3Client, fakeS3Http } from "./backend-fakes";
import { conformanceSuite } from "./conformance";
import { directDriver } from "./harness";
import { SimStore } from "./store";

conformanceSuite("SimStore (reference)", () => ({
  driver: directDriver(new SimStore()),
  ns: "conf",
}));

conformanceSuite("aws-sdk driver over fake S3Client", () => ({
  driver: awsSdkDriver({ client: fakeS3Client(new SimStore()), bucket: "fake" }),
  ns: "conf",
}));

conformanceSuite("r2-binding driver over fake R2 bucket", () => ({
  driver: r2BindingDriver(fakeR2Bucket(new SimStore())),
  ns: "conf",
}));

conformanceSuite("aws4fetch driver over fake S3 HTTP endpoint", () => ({
  driver: aws4fetchDriver({
    accessKeyId: "fake-access-key",
    secretAccessKey: "fake-secret-key",
    endpoint: "https://s3.fake.example",
    bucket: "fake-bucket",
    fetchImpl: fakeS3Http(new SimStore(), "fake-bucket"),
  }),
  ns: "conf",
}));

interface RealTarget {
  endpoint: string;
  bucket: string;
  region?: string;
  accessKey: string;
  secretKey: string;
}

function realTargets(): RealTarget[] {
  const targets: RealTarget[] = [];
  const configPath = fileURLToPath(new URL("../conformance.local.json", import.meta.url));
  if (existsSync(configPath)) {
    const raw = readFileSync(configPath, "utf8").trim();
    if (raw.length > 0) {
      const parsed = JSON.parse(raw) as RealTarget | RealTarget[];
      targets.push(...(Array.isArray(parsed) ? parsed : [parsed]));
    }
  }
  if (process.env.S3EV_CONFORMANCE_ENDPOINT) {
    targets.push({
      endpoint: process.env.S3EV_CONFORMANCE_ENDPOINT,
      bucket: process.env.S3EV_CONFORMANCE_BUCKET ?? "conformance",
      region: process.env.S3EV_CONFORMANCE_REGION ?? "us-east-1",
      accessKey: process.env.S3EV_CONFORMANCE_ACCESS_KEY ?? "",
      secretKey: process.env.S3EV_CONFORMANCE_SECRET_KEY ?? "",
    });
  }
  return targets;
}

const targets = realTargets();
for (const target of targets) {
  conformanceSuite(`aws-sdk driver against ${target.endpoint}`, async () => {
    const { S3Client } = await import("@aws-sdk/client-s3");
    return {
      driver: awsSdkDriver({
        client: new S3Client({
          endpoint: target.endpoint,
          region: target.region ?? "us-east-1",
          forcePathStyle: true,
          credentials: {
            accessKeyId: target.accessKey,
            secretAccessKey: target.secretKey,
          },
        }),
        bucket: target.bucket,
      }),
      // Real buckets accumulate runs; timestamped namespaces keep them apart.
      ns: `conf-${Date.now()}`,
    };
  });
}
describe.skipIf(targets.length > 0)(
  "real backends (create conformance.local.json or set S3EV_CONFORMANCE_*)",
  () => {
    it.skip("no real targets configured", () => {});
  },
);
