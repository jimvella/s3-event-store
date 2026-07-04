/**
 * One suite, every driver (SIMULATOR_PLAN.md, conformance parity):
 * - the direct SimStore driver — the executable reference itself;
 * - each real driver over an in-memory fake of its backend API — exercises
 *   the driver's mapping code against the reference semantics;
 * - optionally, the aws-sdk driver against a real S3-compatible endpoint,
 *   gated on environment variables (MinIO, LocalStack, real S3 or R2):
 *
 *     S3EV_CONFORMANCE_ENDPOINT=http://localhost:9000 \
 *     S3EV_CONFORMANCE_BUCKET=conformance \
 *     S3EV_CONFORMANCE_ACCESS_KEY=... S3EV_CONFORMANCE_SECRET_KEY=... \
 *     npx vitest run sim/conformance.test.ts
 */

import { describe } from "vitest";
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

const endpoint = process.env.S3EV_CONFORMANCE_ENDPOINT;
describe.skipIf(!endpoint)("real backend (env-gated)", () => {
  conformanceSuite(`aws-sdk driver against ${endpoint}`, async () => {
    const { S3Client } = await import("@aws-sdk/client-s3");
    return {
      driver: awsSdkDriver({
        client: new S3Client({
          endpoint: endpoint!,
          region: process.env.S3EV_CONFORMANCE_REGION ?? "us-east-1",
          forcePathStyle: true,
          credentials: {
            accessKeyId: process.env.S3EV_CONFORMANCE_ACCESS_KEY ?? "",
            secretAccessKey: process.env.S3EV_CONFORMANCE_SECRET_KEY ?? "",
          },
        }),
        bucket: process.env.S3EV_CONFORMANCE_BUCKET ?? "conformance",
      }),
      // Real buckets accumulate runs; timestamped namespaces keep them apart.
      ns: `conf-${Date.now()}`,
    };
  });
});
