import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
const project = dirname(dirname(fileURLToPath(import.meta.url)));
const workspace = join(project, "examples", "cursor-smoke");
const runtime = mkdtempSync("/tmp/ltmcp-cursor-");
mkdirSync(join(workspace, ".cursor"), { recursive: true, mode: 0o700 });
const config = {
  mcpServers: {
    "challenge4-local-terminal": {
      type: "stdio",
      command: process.execPath,
      args: [join(project, "dist", "mcp.js")],
      env: {
        LTMCP_RUNTIME_DIR: runtime,
        LTMCP_DATA_DIR: join(workspace, ".runtime", "data"),
      },
    },
  },
};
writeFileSync(
  join(workspace, ".cursor", "mcp.json"),
  JSON.stringify(config, null, 2) + "\n",
  { mode: 0o600 },
);
writeFileSync(
  join(workspace, "runtime.json"),
  JSON.stringify(
    { workspace, runtime, dataDir: join(workspace, ".runtime", "data") },
    null,
    2,
  ) + "\n",
  { mode: 0o600 },
);
console.log(
  JSON.stringify({ workspace, runtime, server: "challenge4-local-terminal" }),
);
