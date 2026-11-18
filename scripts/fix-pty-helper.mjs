import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { chmodSync, existsSync, lstatSync } from "node:fs";
if (process.platform === "darwin") {
  const require = createRequire(import.meta.url);
  const root = dirname(require.resolve("node-pty/package.json"));
  for (const path of [
    join(root, "prebuilds", `darwin-${process.arch}`, "spawn-helper"),
    join(root, "build", "Release", "spawn-helper"),
  ]) {
    if (!existsSync(path)) continue;
    const info = lstatSync(path);
    if (
      !info.isFile() ||
      info.isSymbolicLink() ||
      info.uid !== process.getuid()
    )
      throw new Error(`Unsafe PTY spawn helper: ${path}`);
    chmodSync(path, info.mode | 0o111);
  }
}
