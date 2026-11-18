import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { chmodSync, lstatSync, mkdirSync } from "node:fs";
import { TerminalError } from "./errors.js";
import type { BrokerConfig } from "./types.js";

export function getConfig(overrides: Partial<BrokerConfig> = {}): BrokerConfig {
  const runtimeDir = resolve(
    overrides.runtimeDir ??
      process.env.LTMCP_RUNTIME_DIR ??
      join(homedir(), "Library", "Application Support", "local-terminal-mcp"),
  );
  const dataDir = resolve(
    overrides.dataDir ?? process.env.LTMCP_DATA_DIR ?? join(runtimeDir, "data"),
  );
  const config: BrokerConfig = {
    runtimeDir,
    dataDir,
    socketPath: join(runtimeDir, "b.sock"),
    controlSocketPath: join(runtimeDir, "c.sock"),
    maxSessions: 16,
    cacheBytes: 4 * 1024 * 1024,
    sessionLogBytes: 100 * 1024 * 1024,
    maxRetainedSessions: Number(process.env.LTMCP_MAX_RETAINED_SESSIONS ?? 64),
    totalLogBytes: 1024 * 1024 * 1024,
    retentionMs: 7 * 86400000,
    disconnectGraceMs: Number(process.env.LTMCP_DISCONNECT_GRACE_MS ?? 1000),
    linkTtlMs: 3600000,
    idleExitMs: Number(process.env.LTMCP_IDLE_EXIT_MS ?? 60000),
    ...overrides,
  };
  for (const path of [config.socketPath, config.controlSocketPath]) {
    if (Buffer.byteLength(path) > 100)
      throw new TerminalError(
        "SOCKET_PATH_TOO_LONG",
        "macOS Unix socket path is too long. Set LTMCP_RUNTIME_DIR to a shorter private directory.",
      );
  }
  for (const key of [
    "maxSessions",
    "maxRetainedSessions",
    "cacheBytes",
    "sessionLogBytes",
    "totalLogBytes",
    "retentionMs",
    "disconnectGraceMs",
    "linkTtlMs",
    "idleExitMs",
  ] as const) {
    if (!Number.isFinite(config[key]) || config[key] < 0)
      throw new TerminalError("INVALID_CONFIG", `Invalid ${key}`);
  }
  return config;
}

export function privateDirectory(path: string): void {
  mkdirSync(path, { recursive: true, mode: 0o700 });
  const info = lstatSync(path);
  if (
    !info.isDirectory() ||
    info.isSymbolicLink() ||
    info.uid !== process.getuid?.()
  ) {
    throw new TerminalError(
      "UNSAFE_DIRECTORY",
      `Expected a real private directory owned by the current user: ${path}`,
    );
  }
  chmodSync(path, 0o700);
}
