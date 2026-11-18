#!/usr/bin/env node
import { homedir } from "node:os";
import { pathToFileURL } from "node:url";
import {
  McpServer,
  ResourceTemplate,
  ProtocolError,
  ProtocolErrorCode,
  type CallToolResult,
  type McpRequestContext,
  type ServerContext,
} from "@modelcontextprotocol/server";
import {
  serveStdio,
  type StdioServerHandle,
} from "@modelcontextprotocol/server/stdio";
import { z } from "zod";
import { BrokerClient } from "./client.js";
import { getConfig } from "./config.js";
import { TerminalError, asError } from "./errors.js";
import type { BrokerConfig, WireNotification } from "./types.js";

const linkId = z
  .string()
  .min(1)
  .describe("Opaque broker link ID; not a session ID.");
const dimension = z.number().int().min(2).max(1000);
const maxBytes = z.number().int().min(64).max(65536).default(16384);
const key = z.enum([
  "Enter",
  "Tab",
  "ArrowUp",
  "ArrowDown",
  "ArrowLeft",
  "ArrowRight",
  "CtrlC",
  "CtrlD",
  "Escape",
  "Backspace",
]);

/** These same schemas validate calls and supply the advertised MCP input schemas. */
export const toolSchemas = {
  terminal_create: z.strictObject({
    cwd: z.string().min(1).default(homedir()),
    cols: dimension.default(80),
    rows: dimension.default(24),
    env: z.record(z.string(), z.string()).optional(),
  }),
  terminal_link: z.strictObject({
    pid: z.number().int().positive(),
    access: z.enum(["read", "control"]).default("read"),
  }),
  terminal_exec: z.strictObject({
    link_id: linkId,
    command: z.string().min(1),
    request_key: z.string().min(1),
    wait_ms: z.number().int().min(0).max(10000).default(2000),
  }),
  terminal_read: z.strictObject({
    link_id: linkId,
    after_cursor: z.string().optional(),
    max_bytes: maxBytes,
    wait_ms: z.number().int().min(0).max(20000).default(0),
  }),
  terminal_write: z
    .strictObject({
      link_id: linkId,
      text: z
        .string()
        .max(16384)
        .describe(
          "Literal UTF-8 text, at most 16384 bytes; exactly one of text or keys.",
        )
        .optional(),
      keys: z.array(key).min(1).optional(),
    })
    .superRefine((value, ctx) => {
      if ((value.text !== undefined) === (value.keys !== undefined)) {
        ctx.addIssue({
          code: "custom",
          message: "Exactly one of text or keys is required.",
        });
      }
      if (
        value.text !== undefined &&
        Buffer.byteLength(value.text, "utf8") > 16384
      ) {
        ctx.addIssue({
          code: "custom",
          path: ["text"],
          message: "text exceeds 16384 UTF-8 bytes.",
        });
      }
    }),
  terminal_status: z.strictObject({
    link_id: linkId,
    command_id: z.string().min(1).optional(),
  }),
  terminal_terminate: z.strictObject({
    link_id: linkId,
    grace_ms: z.number().int().min(0).max(10000).default(3000),
  }),
  terminal_unlink: z.strictObject({ link_id: linkId }),
  terminal_resize: z.strictObject({
    link_id: linkId,
    cols: dimension,
    rows: dimension,
  }),
} as const;

type ToolName = keyof typeof toolSchemas;
const descriptions: Record<ToolName, string> = {
  terminal_create:
    "Create a broker-owned persistent PTY shell. Defaults to HOME and 80x24. Disconnecting MCP does not terminate it.",
  terminal_link:
    "Link only to a shell PID already managed by this broker. Arbitrary external PIDs return NOT_MANAGED. Read is the default; control is explicitly requested.",
  terminal_exec:
    "Submit a shell command on a control link using an idempotency request_key. wait_ms only bounds waiting; cancellation never kills the command. Nonzero exit_code is not a tool error.",
  terminal_read:
    "Read durable ordered terminal events after an opaque cursor. Follow next_cursor until has_more is false; wait_ms allows bounded long-polling.",
  terminal_write:
    "Write exactly one of literal text (at most 16 KiB UTF-8) or supported keys to the running foreground program. Nonempty input in IDLE returns SESSION_IDLE; start a command with terminal_exec first. CtrlC is an explicit interrupt, not MCP cancellation.",
  terminal_status:
    "Inspect session state and optionally a command record. COMPLETED is proven by broker boundaries, not inferred from elapsed time.",
  terminal_terminate:
    "Explicitly terminate a controlled broker-owned shell, allowing grace_ms before escalation. This is not unlink.",
  terminal_unlink:
    "Release this link without terminating its persistent terminal session.",
  terminal_resize:
    "Resize a controlled PTY; cols and rows must be integers from 2 through 1000.",
};
const outputSchema = z.record(z.string(), z.unknown());
const outputJsonSchema = z.toJSONSchema(outputSchema);
const resourceTemplate =
  "terminal://link/{link_id}/events{?after_cursor,max_bytes}";

function canonicalUri(id: string): string {
  return `terminal://link/${encodeURIComponent(id)}/events`;
}

/** Parse a snapshot URI. Queries are read options, never separate subscription identities. */
function resourceParams(
  uri: URL,
  canonicalOnly = false,
): Record<string, unknown> {
  const match = /^\/([^/]+)\/events$/.exec(uri.pathname);
  if (
    uri.protocol !== "terminal:" ||
    uri.host !== "link" ||
    uri.username ||
    uri.password ||
    uri.hash ||
    !match?.[1]
  ) {
    throw new TerminalError(
      "INVALID_RESOURCE_URI",
      "Expected terminal://link/<link_id>/events.",
    );
  }
  let id: string;
  try {
    id = decodeURIComponent(match[1]);
  } catch {
    throw new TerminalError(
      "INVALID_RESOURCE_URI",
      "Invalid link ID encoding.",
    );
  }
  if (canonicalOnly && uri.href !== canonicalUri(id)) {
    throw new TerminalError(
      "INVALID_RESOURCE_URI",
      "Subscribe to the canonical resource URI without a query.",
    );
  }
  const params: Record<string, unknown> = { link_id: id, wait_ms: 0 };
  for (const name of new Set(uri.searchParams.keys())) {
    const values = uri.searchParams.getAll(name);
    if (
      values.length !== 1 ||
      (name !== "after_cursor" && name !== "max_bytes")
    ) {
      throw new TerminalError(
        "INVALID_RESOURCE_URI",
        "Only one after_cursor and one max_bytes query parameter are allowed.",
      );
    }
    const value = values[0]!;
    if (name === "max_bytes") {
      if (!/^\d+$/.test(value))
        throw new TerminalError(
          "INVALID_PARAMS",
          "max_bytes must be an integer from 64 through 65536.",
        );
      params.max_bytes = Number(value);
    } else params.after_cursor = value;
  }
  const parsed = toolSchemas.terminal_read.safeParse(params);
  if (!parsed.success)
    throw new TerminalError(
      "INVALID_PARAMS",
      "Invalid terminal resource read options.",
      parsed.error.issues,
    );
  return parsed.data;
}

function toolResult(
  value: Record<string, unknown>,
  isError = false,
): CallToolResult {
  return {
    structuredContent: value,
    content: [{ type: "text", text: JSON.stringify(value) }],
    isError,
  };
}
function errorResult(error: unknown): CallToolResult {
  const terminalError = asError(error);
  return toolResult(
    {
      error: {
        code: terminalError.code,
        message: terminalError.message,
        ...(terminalError.details === undefined
          ? {}
          : { details: terminalError.details }),
      },
    },
    true,
  );
}
function resourceError(error: unknown): never {
  const terminalError = asError(error);
  throw new ProtocolError(
    terminalError.code === "INVALID_PARAMS" ||
    terminalError.code === "INVALID_RESOURCE_URI"
      ? ProtocolErrorCode.InvalidParams
      : ProtocolErrorCode.InternalError,
    terminalError.message,
    {
      terminal_error: {
        code: terminalError.code,
        ...(terminalError.details === undefined
          ? {}
          : { details: terminalError.details }),
      },
    },
  );
}

/** One SDK factory instance owns one IPC client, never the broker or the sessions. */
export function createTerminalMcpServer(
  context: Pick<McpRequestContext, "era">,
  config: BrokerConfig = getConfig(),
): McpServer {
  const server = new McpServer(
    { name: "local-terminal-mcp", version: "0.1.0" },
    {
      capabilities: {
        tools: { listChanged: false },
        resources: { subscribe: true, listChanged: false },
      },
      instructions:
        "All terminal capabilities are available as tools. Resources are optional event snapshots and update hints, not a PTY byte stream. MCP disconnection and request cancellation never terminate sessions.",
    },
  );
  const broker = new BrokerClient(config);
  const links = new Map<string, string>();
  const subscribed = new Set<string>();
  let connecting: Promise<void> | undefined;
  let closed = false;

  const remember = (
    result: Record<string, unknown>,
    params: Record<string, unknown>,
  ) => {
    const id =
      typeof result.link_id === "string" ? result.link_id : params.link_id;
    if (typeof id === "string" && typeof result.session_id === "string")
      links.set(id, result.session_id);
  };
  const connect = async () => {
    if (closed)
      throw new TerminalError(
        "GATEWAY_CLOSED",
        "The MCP connection is closed.",
      );
    // Lazy connection means a protocol-discovery probe cannot accidentally start a broker.
    connecting ??= broker.connect().catch((error: unknown) => {
      connecting = undefined;
      throw error;
    });
    await connecting;
    if (closed) {
      broker.close();
      throw new TerminalError(
        "GATEWAY_CLOSED",
        "The MCP connection is closed.",
      );
    }
  };
  const request = async (
    method: string,
    params: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<Record<string, unknown>> => {
    await connect();
    const result: unknown = await broker.request(method, params, signal);
    if (
      typeof result !== "object" ||
      result === null ||
      Array.isArray(result)
    ) {
      throw new TerminalError(
        "INVALID_BROKER_RESPONSE",
        "Expected a broker result object.",
      );
    }
    remember(result as Record<string, unknown>, params);
    return result as Record<string, unknown>;
  };
  const notifyResourceUpdated = (notification: WireNotification) => {
    if (closed || notification.notification !== "session_changed") return;
    for (const [id, session] of links) {
      const uri = canonicalUri(id);
      if (
        session !== notification.session_id ||
        (context.era === "legacy" && !subscribed.has(uri))
      )
        continue;
      // The official serveStdio modern router filters and stamps this standard notification.
      void server.server.sendResourceUpdated({ uri }).catch((error) => {
        if (!closed)
          process.stderr.write(
            `[local-terminal-mcp] resource update: ${asError(error).message}\n`,
          );
      });
    }
  };
  broker.on("notification", notifyResourceUpdated);
  server.server.onclose = () => {
    closed = true;
    broker.off("notification", notifyResourceUpdated);
    broker.close();
    links.clear();
    subscribed.clear();
  };

  const invoke = async (
    name: ToolName,
    params: Record<string, unknown>,
    signal: AbortSignal,
  ): Promise<CallToolResult> => {
    try {
      // Only a read or the wait on an accepted exec can be cancelled. No terminate/write is synthesized.
      const result = await request(
        name,
        params,
        name === "terminal_read" || name === "terminal_exec"
          ? signal
          : undefined,
      );
      if (name === "terminal_unlink") {
        const id = params.link_id as string;
        links.delete(id);
        subscribed.delete(canonicalUri(id));
      }
      return toolResult(result); // exit_code (including nonzero) is deliberately not an isError predicate.
    } catch (error) {
      return errorResult(error);
    }
  };

  for (const name of Object.keys(toolSchemas) as ToolName[]) {
    server.registerTool(
      name,
      {
        description: descriptions[name],
        inputSchema: toolSchemas[name],
        outputSchema,
        annotations: {
          readOnlyHint: name === "terminal_read" || name === "terminal_status",
          destructiveHint: name === "terminal_terminate",
          idempotentHint:
            name === "terminal_exec" ||
            name === "terminal_read" ||
            name === "terminal_status",
          openWorldHint: true,
        },
      },
      async (params: Record<string, unknown>, ctx: ServerContext) =>
        invoke(name, params, ctx.mcpReq.signal),
    );
  }
  // The high-level SDK normally renders validation errors as plain non-JSON text. Use its
  // documented low-level handler to retain the same structured JSON contract on that path.
  server.server.setRequestHandler("tools/call", async (call, ctx) => {
    const name = call.params.name;
    if (!Object.hasOwn(toolSchemas, name)) {
      return server.server.projectCallToolResult(
        errorResult(
          new TerminalError("UNKNOWN_TOOL", `Unknown terminal tool: ${name}`),
        ),
        outputJsonSchema,
      );
    }
    const schema = toolSchemas[name as ToolName];
    const parsed = schema.safeParse(call.params.arguments ?? {});
    const result = parsed.success
      ? await invoke(name as ToolName, parsed.data, ctx.mcpReq.signal)
      : errorResult(
          new TerminalError(
            "INVALID_PARAMS",
            `Invalid arguments for ${name}.`,
            parsed.error.issues,
          ),
        );
    return server.server.projectCallToolResult(result, outputJsonSchema);
  });

  server.registerResource(
    "terminal-events",
    new ResourceTemplate(resourceTemplate, {
      list: async () => ({
        resources: [...links.keys()].map((id) => ({
          uri: canonicalUri(id),
          name: `terminal-events-${id}`,
          mimeType: "application/json",
        })),
      }),
    }),
    {
      description:
        "Durable terminal event snapshot. Optional after_cursor and max_bytes; subscribe only to the canonical URI.",
      mimeType: "application/json",
    },
    async (uri, _variables, ctx) => {
      try {
        const result = await request(
          "terminal_read",
          resourceParams(uri),
          ctx.mcpReq.signal,
        );
        return {
          contents: [
            {
              uri: uri.href,
              mimeType: "application/json",
              text: JSON.stringify(result),
            },
          ],
        };
      } catch (error) {
        return resourceError(error);
      }
    },
  );
  // v2.4.0's URI-template reverse matcher does not match a query containing only
  // the second optional variable. Parse the standard resources/read URI ourselves
  // so either query option, any order, and duplicate/unknown rejection are reliable.
  server.server.setRequestHandler("resources/read", async ({ params }, ctx) => {
    try {
      const uri = new URL(params.uri);
      const result = await request(
        "terminal_read",
        resourceParams(uri),
        ctx.mcpReq.signal,
      );
      return {
        contents: [
          {
            uri: uri.href,
            mimeType: "application/json",
            text: JSON.stringify(result),
          },
        ],
      };
    } catch (error) {
      return resourceError(error);
    }
  });

  if (context.era === "legacy") {
    server.server.setRequestHandler(
      "resources/subscribe",
      async ({ params }) => {
        try {
          const options = resourceParams(new URL(params.uri), true);
          await request("terminal_status", { link_id: options.link_id });
          subscribed.add(params.uri);
          return {};
        } catch (error) {
          return resourceError(error);
        }
      },
    );
    server.server.setRequestHandler(
      "resources/unsubscribe",
      async ({ params }) => {
        try {
          const options = resourceParams(new URL(params.uri), true);
          subscribed.delete(canonicalUri(options.link_id as string));
          return {};
        } catch (error) {
          return resourceError(error);
        }
      },
    );
  }
  return server;
}

/** Official v2 factory entry: omitted legacy option intentionally keeps dual-era serving. */
export function startStdioGateway(
  config: BrokerConfig = getConfig(),
  exitOnClose = false,
): StdioServerHandle {
  const handle = serveStdio(
    (context) => createTerminalMcpServer(context, config),
    {
      onerror: (error) =>
        process.stderr.write(`[local-terminal-mcp] ${error.message}\n`),
    },
  );
  let stopping = false;
  const stop = () => {
    if (stopping) return;
    stopping = true;
    process.stdin.off("end", stop);
    process.stdin.off("close", stop);
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
    void handle
      .close()
      .catch((error) =>
        process.stderr.write(
          `[local-terminal-mcp] close: ${asError(error).message}\n`,
        ),
      )
      .finally(() => {
        // The executable must not be kept alive by an abandoned IPC startup retry.
        // Closing MCP/IPC above does not own or stop any detached broker/session.
        if (exitOnClose) process.exit(0);
      });
  };
  process.stdin.once("end", stop);
  process.stdin.once("close", stop);
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  return handle;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  try {
    startStdioGateway(getConfig(), true);
  } catch (error) {
    process.stderr.write(`[local-terminal-mcp] ${asError(error).message}\n`);
    process.exitCode = 1;
  }
}
