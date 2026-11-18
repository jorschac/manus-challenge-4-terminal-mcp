import { mkdtempSync } from "node:fs";
import { join } from "node:path";
import { BrokerClient } from "../dist/client.js";
import { getConfig } from "../dist/config.js";
const runtime = mkdtempSync("<TEST_TMP_PATH>");
const client = new BrokerClient(
  getConfig({ runtimeDir: runtime, dataDir: join(runtime, "data") }),
);
let terminal;
try {
  terminal = await client.request("terminal_create", { cwd: runtime });
  console.log("Created:", JSON.stringify(terminal));
  const exec = await client.request("terminal_exec", {
    link_id: terminal.link_id,
    command: 'export MVP_DEMO=ready; printf "MVP:%s\\n" "$MVP_DEMO"',
    request_key: "demo-command",
    wait_ms: 2000,
  });
  console.log("Executed:", JSON.stringify(exec));
  const reader = new BrokerClient(client.config);
  try {
    console.log(
      "Linked by PID:",
      JSON.stringify(
        await reader.request("terminal_link", {
          pid: terminal.shell_pid,
          access: "read",
        }),
      ),
    );
  } finally {
    reader.close();
  }
  const interactive = await client.request("terminal_exec", {
    link_id: terminal.link_id,
    command: 'read "answer?Your input: "; printf "REPLY:%s\\n" "$answer"',
    request_key: "demo-interactive",
    wait_ms: 100,
  });
  console.log("Interactive running:", interactive.command_state);
  await client.request("terminal_write", {
    link_id: terminal.link_id,
    text: "hello",
  });
  await client.request("terminal_write", {
    link_id: terminal.link_id,
    keys: ["Enter"],
  });
  await new Promise((resolve) => setTimeout(resolve, 250));
  console.log(
    "Output:",
    JSON.stringify(
      await client.request("terminal_read", {
        link_id: terminal.link_id,
        after_cursor: terminal.cursor,
      }),
    ),
  );
  console.log(
    "Terminated:",
    JSON.stringify(
      await client.request("terminal_terminate", {
        link_id: terminal.link_id,
        grace_ms: 100,
      }),
    ),
  );
} finally {
  await client.request("broker_stop").catch(() => {});
  client.close();
}
console.log("Private demo data:", runtime);
