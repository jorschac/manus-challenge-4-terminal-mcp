import { mkdtemp, rm, access, realpath } from "node:fs/promises";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { describe, it, expect } from "vitest";
import {
  Client,
  type CallToolResult,
  type McpSubscription,
} from "@modelcontextprotocol/client";
import {
  StdioClientTransport,
  getDefaultEnvironment,
} from "@modelcontextprotocol/client/stdio";
import { BrokerClient } from "../src/client.js";
import { getConfig } from "../src/config.js";
import type { BrokerConfig } from "../src/types.js";

const projectDir = dirname(dirname(fileURLToPath(import.meta.url)));
const toolNames = [
  "terminal_create",
  "terminal_link",
  "terminal_exec",
  "terminal_read",
  "terminal_write",
  "terminal_status",
  "terminal_terminate",
  "terminal_unlink",
  "terminal_resize",
].sort();
type Era = "legacy" | "modern";
type Json = Record<string, any>;

function structured(result: CallToolResult, error = false): Json {
  expect(result.isError).toBe(error);
  expect(result.structuredContent).toBeTypeOf("object");
  const text = result.content.find((block) => block.type === "text");
  expect(text?.type).toBe("text");
  if (!text || text.type !== "text")
    throw new Error("Expected JSON text content");
  expect(JSON.parse(text.text)).toEqual(result.structuredContent);
  return result.structuredContent as Json;
}
async function until<T>(
  read: () => Promise<T>,
  ready: (value: T) => boolean,
  timeoutMs = 8000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await read();
    if (ready(value)) return value;
    if (Date.now() >= deadline)
      throw new Error("Timed out waiting for terminal condition");
    await delay(30);
  }
}
function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
async function open(era: Era, config: BrokerConfig) {
  const client = new Client(
    { name: `ltmcp-${era}-test`, version: "1.0.0" },
    {
      versionNegotiation: {
        mode: era === "legacy" ? "legacy" : { pin: "2026-07-28" },
      },
    },
  );
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [join(projectDir, "dist/mcp.js")],
    cwd: projectDir,
    env: {
      ...getDefaultEnvironment(),
      LTMCP_RUNTIME_DIR: config.runtimeDir,
      LTMCP_DATA_DIR: config.dataDir,
      LTMCP_DISCONNECT_GRACE_MS: "100",
      LTMCP_IDLE_EXIT_MS: "60000",
    },
    stderr: "pipe",
  });
  let stderr = "";
  transport.stderr?.on("data", (chunk) => {
    stderr += String(chunk);
  });
  try {
    await client.connect(transport);
  } catch (error) {
    await transport.close();
    throw new Error(`MCP connect failed: ${String(error)}\n${stderr}`);
  }
  expect(client.getProtocolEra()).toBe(era);
  const protocolErrors: Error[] = [];
  const previousError = transport.onerror;
  transport.onerror = (error) => {
    protocolErrors.push(error);
    previousError?.(error);
  };
  return { client, transport, protocolErrors, stderr: () => stderr };
}

async function stopOwnBroker(config: BrokerConfig) {
  const control = new BrokerClient(config);
  let pid: number | undefined;
  try {
    await control.connect();
    pid = (await control.request("broker_ping")).pid as number;
    await control.request("broker_stop");
  } finally {
    control.close();
  }
  await until(
    async () => {
      try {
        await access(config.socketPath);
        return false;
      } catch {
        return true;
      }
    },
    (gone) => gone,
  );
  if (pid !== undefined)
    await until(
      async () => alive(pid),
      (running) => !running,
    );
}

/** Each era owns its /tmp directory, broker, shells, IPC clients and cleanup. */
describe.each<Era>(["legacy", "modern"])("MCP %s SDK", (era) => {
  it("exposes all nine tools, durable reads and standard subscriptions; sessions survive stdin EOF", async () => {
    const directory = await mkdtemp("/tmp/ltmcp-mcp-");
    const config = getConfig({
      runtimeDir: join(directory, "runtime"),
      dataDir: join(directory, "data"),
      disconnectGraceMs: 100,
    });
    const connections: Awaited<ReturnType<typeof open>>[] = [];
    let subscription: McpSubscription | undefined;
    try {
      const first = await open(era, config);
      connections.push(first);
      const { client } = first;
      const listed = await client.listTools();
      expect(listed.tools.map((tool) => tool.name).sort()).toEqual(toolNames);
      for (const tool of listed.tools)
        expect(tool.outputSchema?.type).toBe("object");
      const createSchema = listed.tools.find(
        (tool) => tool.name === "terminal_create",
      )!.inputSchema as Json;
      expect(createSchema.properties.cols).toMatchObject({
        minimum: 2,
        maximum: 1000,
        default: 80,
      });
      expect(createSchema.properties.rows).toMatchObject({
        minimum: 2,
        maximum: 1000,
        default: 24,
      });
      const call = async (name: string, args: Json, error = false) =>
        structured(await client.callTool({ name, arguments: args }), error);

      // Every tool is validated at the gateway before broker side effects.
      const invalid: [string, Json][] = [
        ["terminal_create", { cols: 1 }],
        ["terminal_link", { pid: 0 }],
        ["terminal_exec", { link_id: "x", command: "true" }],
        ["terminal_read", { link_id: "x", max_bytes: 63 }],
        ["terminal_write", { link_id: "x" }],
        ["terminal_write", { link_id: "x", text: "x", keys: ["Enter"] }],
        ["terminal_write", { link_id: "x", keys: ["F1"] }],
        ["terminal_write", { link_id: "x", text: "界".repeat(6000) }],
        ["terminal_status", { link_id: "" }],
        ["terminal_terminate", { link_id: "x", grace_ms: 10001 }],
        ["terminal_unlink", { link_id: 2 }],
        ["terminal_resize", { link_id: "x", cols: 80, rows: 0 }],
        ["terminal_read", { link_id: "x", wait_ms: 20001 }],
        ["terminal_create", { surprise: true }],
      ];
      for (const [name, args] of invalid)
        expect((await call(name, args, true)).error.code).toBe(
          "INVALID_PARAMS",
        );

      const created = await call("terminal_create", {
        cwd: directory,
        env: { LTMCP_TEST_VALUE: "isolated" },
      });
      expect(created.session_id).toBeTypeOf("string");
      expect(created.link_id).toBeTypeOf("string");
      const id = created.link_id as string;
      const uri = `terminal://link/${encodeURIComponent(id)}/events`;
      expect(created.resource_uri).toBe(uri);
      const initialStatus = await call("terminal_status", { link_id: id });
      expect(initialStatus).toMatchObject({
        session_id: created.session_id,
        cwd: await realpath(directory),
        cols: 80,
        rows: 24,
      });
      const shellPid = initialStatus.shell_pid as number;
      expect(alive(shellPid)).toBe(true);

      const readLink = await call("terminal_link", { pid: shellPid });
      expect(readLink.session_id).toBe(created.session_id);
      const denied = await call(
        "terminal_exec",
        { link_id: readLink.link_id, command: "true", request_key: "denied" },
        true,
      );
      expect(denied.error.code).toBeTypeOf("string");
      await call("terminal_unlink", { link_id: readLink.link_id });
      await call("terminal_resize", { link_id: id, cols: 92, rows: 31 });
      expect(await call("terminal_status", { link_id: id })).toMatchObject({
        cols: 92,
        rows: 31,
      });

      const updates: string[] = [];
      const notificationMethods: string[] = [];
      client.setNotificationHandler(
        "notifications/resources/updated",
        (notification) => {
          updates.push(notification.params.uri);
        },
      );
      client.fallbackNotificationHandler = async (notification) => {
        notificationMethods.push(notification.method);
      };
      if (era === "modern") {
        subscription = await client.listen({ resourceSubscriptions: [uri] });
        expect(subscription.honoredFilter.resourceSubscriptions).toEqual([uri]);
      } else await client.subscribeResource({ uri });

      const execution = await call("terminal_exec", {
        link_id: id,
        command: "printf 'mcp-output:%s\\n' \"$LTMCP_TEST_VALUE\"; false",
        request_key: "nonzero",
        wait_ms: 2000,
      });
      const completed = await until(
        () =>
          call("terminal_status", {
            link_id: id,
            command_id: execution.command_id,
          }),
        (value) => value.command?.state === "COMPLETED",
      );
      expect(completed.command.exit_code).toBe(1);
      const duplicate = await call("terminal_exec", {
        link_id: id,
        command: "printf 'mcp-output:%s\\n' \"$LTMCP_TEST_VALUE\"; false",
        request_key: "nonzero",
        wait_ms: 2000,
      });
      expect(duplicate.command_id).toBe(execution.command_id);
      expect(duplicate.exit_code).toBe(1); // command failure is still MCP isError:false.
      const conflict = await call(
        "terminal_exec",
        { link_id: id, command: "true", request_key: "nonzero" },
        true,
      );
      expect(conflict.error.code).toBe("REQUEST_KEY_CONFLICT");
      await until(
        async () => updates.slice(),
        (value) => value.includes(uri),
      );

      const waitCancel = new AbortController();
      const waitingExec = client.callTool(
        {
          name: "terminal_exec",
          arguments: {
            link_id: id,
            command: "sleep 0.25; printf 'cancelled-wait-survives\\n'",
            request_key: "cancel-wait",
            wait_ms: 10000,
          },
        },
        { signal: waitCancel.signal },
      );
      const waitRejected = expect(waitingExec).rejects.toThrow();
      const active = await until(
        () => call("terminal_status", { link_id: id }),
        (value) => value.command?.request_key === "cancel-wait",
      );
      waitCancel.abort();
      await waitRejected;
      const survivedCancel = await until(
        () =>
          call("terminal_status", {
            link_id: id,
            command_id: active.command.command_id,
          }),
        (value) => value.command?.state === "COMPLETED",
      );
      expect(survivedCancel.command.exit_code).toBe(0);

      const read = await call("terminal_read", { link_id: id });
      expect(read).toMatchObject({
        session_id: created.session_id,
        eof: false,
        has_more: false,
        storage_degraded: false,
      });
      expect(read.next_cursor).toBeTypeOf("string");
      expect(read.earliest_cursor).toBeTypeOf("string");
      expect(
        read.events.map((event: Json) => event.data?.text ?? "").join(""),
      ).toContain("mcp-output:isolated");
      expect(
        (await client.listResources()).resources.some(
          (resource) => resource.uri === uri,
        ),
      ).toBe(true);
      expect(
        (await client.listResourceTemplates()).resourceTemplates.some(
          (template) => template.uriTemplate.includes("terminal://link/"),
        ),
      ).toBe(true);
      const snapshot = await client.readResource({
        uri: `${uri}?max_bytes=16384`,
      });
      const snapshotText = snapshot.contents[0] as { text: string };
      expect(JSON.parse(snapshotText.text).events).toEqual(read.events);
      const after = await client.readResource({
        uri: `${uri}?after_cursor=${encodeURIComponent(read.next_cursor)}&max_bytes=16384`,
      });
      expect(
        JSON.parse((after.contents[0] as { text: string }).text).events,
      ).toEqual([]);
      await expect(
        client.readResource({ uri: `${uri}?max_bytes=1` }),
      ).rejects.toThrow();
      await expect(
        client.readResource({ uri: `${uri}?max_bytes=16384&max_bytes=16384` }),
      ).rejects.toThrow();
      await expect(
        client.readResource({ uri: `${uri}?other=1` }),
      ).rejects.toThrow();
      await expect(
        client.readResource({ uri: `${uri}#fragment` }),
      ).rejects.toThrow();

      // A cancelled long-poll is just a cancelled request, not CtrlC or terminate.
      const cancel = new AbortController();
      const pending = client.callTool(
        {
          name: "terminal_read",
          arguments: {
            link_id: id,
            after_cursor: read.next_cursor,
            wait_ms: 20000,
          },
        },
        { signal: cancel.signal },
      );
      const rejection = expect(pending).rejects.toThrow();
      await delay(40);
      cancel.abort();
      await rejection;
      expect(await call("terminal_status", { link_id: id })).toMatchObject({
        session_id: created.session_id,
        state: "IDLE",
      });
      const idleWrite = await client.callTool({
        name: "terminal_write",
        arguments: { link_id: id, keys: ["Tab"] },
      });
      expect(idleWrite.isError).toBe(true);
      const interactive = await call("terminal_exec", {
        link_id: id,
        command: 'read "input?MCP_INPUT: "; printf "MCP_REPLY:%s\\n" "$input"',
        request_key: "mcp-real-input",
        wait_ms: 100,
      });
      await call("terminal_write", { link_id: id, text: "mcp-key-test" });
      await call("terminal_write", { link_id: id, keys: ["Enter"] });
      await until(
        () =>
          call("terminal_status", {
            link_id: id,
            command_id: interactive.command_id,
          }),
        (value) => value.command?.state === "COMPLETED",
      );
      const reply = await call("terminal_read", {
        link_id: id,
        after_cursor: read.next_cursor,
      });
      expect(
        reply.events.map((event: Json) => event.data?.text ?? "").join(""),
      ).toContain("MCP_REPLY:mcp-key-test");

      if (subscription) {
        await subscription.close();
        subscription = undefined;
      } else await client.unsubscribeResource({ uri });
      await delay(80);
      const updateCount = updates.length;
      await call("terminal_exec", {
        link_id: id,
        command: "true",
        request_key: "unsubscribed",
        wait_ms: 2000,
      });
      await delay(80);
      expect(updates.length).toBe(updateCount);
      expect(notificationMethods).toEqual([]); // no private/custom terminal notification protocol.
      expect(first.protocolErrors).toEqual([]); // stdout contains only valid MCP JSON-RPC.

      const running = await call("terminal_exec", {
        link_id: id,
        command: "sleep 0.7; printf 'after-disconnect\\n'",
        request_key: "survives",
        wait_ms: 0,
      });
      const gatewayPid = first.transport.pid!;
      const closeStarted = Date.now();
      await client.close(); // official transport closes stdin first; it would escalate after 2 seconds.
      expect(Date.now() - closeStarted).toBeLessThan(1500);
      expect(alive(gatewayPid)).toBe(false);
      expect(alive(shellPid)).toBe(true);
      await delay(160); // exceed disconnectGraceMs without killing the broker-owned shell.
      const second = await open(era, config);
      connections.push(second);
      const callSecond = async (name: string, args: Json) =>
        structured(await second.client.callTool({ name, arguments: args }));
      const relink = await callSecond("terminal_link", {
        pid: shellPid,
        access: "control",
      });
      expect(relink.session_id).toBe(created.session_id);
      const final = await until(
        () =>
          callSecond("terminal_status", {
            link_id: relink.link_id,
            command_id: running.command_id,
          }),
        (value) => value.command?.state === "COMPLETED",
      );
      expect(final.command.exit_code).toBe(0);
      const continued = await callSecond("terminal_read", {
        link_id: relink.link_id,
        after_cursor: read.next_cursor,
      });
      expect(
        continued.events.map((event: Json) => event.data?.text ?? "").join(""),
      ).toContain("after-disconnect");
      await callSecond("terminal_terminate", {
        link_id: relink.link_id,
        grace_ms: 100,
      });
      await until(
        async () => alive(shellPid),
        (value) => !value,
      );
      await callSecond("terminal_unlink", { link_id: relink.link_id });
      expect(second.protocolErrors).toEqual([]);
    } finally {
      try {
        await subscription?.close();
      } finally {
        for (const connection of connections) await connection.client.close();
        try {
          await stopOwnBroker(config);
        } finally {
          await rm(directory, { recursive: true, force: true });
        }
      }
    }
  }, 45000);
});
