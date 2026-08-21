import { copyFile, mkdir } from "node:fs/promises";

const distribution = new URL("../dist/", import.meta.url);
await mkdir(distribution, { recursive: true });
await copyFile(
  new URL("../src/styles.css", import.meta.url),
  new URL("styles.css", distribution),
);
