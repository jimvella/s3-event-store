import { defineConfig } from "tsup";

// One entry per subpath export (DESIGN.md, Package & tooling): dual
// ESM/CJS + .d.ts, tree-shakeable for Workers bundles; the SDK packages
// stay external (optional peers of their drivers only).
export default defineConfig({
  entry: {
    index: "src/index.ts",
    client: "src/client.ts",
    "drivers/aws-sdk": "src/drivers/aws-sdk.ts",
    "drivers/r2-binding": "src/drivers/r2-binding.ts",
    "drivers/aws4fetch": "src/drivers/aws4fetch.ts",
  },
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: true,
  clean: true,
  target: "es2022",
  external: ["@aws-sdk/client-s3", "aws4fetch"],
});
