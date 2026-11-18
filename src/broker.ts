import {
  createServer,
  createConnection,
  type Server,
  type Socket,
} from "node:net";
import {
  chmodSync,
  readFileSync,
  readdirSync,
  statSync,
  lstatSync,
  rmSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { TerminalSession } from "./session.js";
import { getConfig, privateDirectory } from "./config.js";
import { asError, TerminalError } from "./errors.js";
import {
  processIdentity,
  removeOwnedSocket,
  terminateProcesses,
} from "./system.js";
import type {
  Access,
  BrokerConfig,
  SessionRecord,
  WireRequest,
} from "./types.js";

interface Link {
  id: string;
  sessionId: string;
  access: Access;
  owner: string;
  expires: number;
  disconnectTimer?: NodeJS.Timeout;
}
interface Connection {
  id: string;
  socket: Socket;
  pending: Map<string, { controller: AbortController; linkId?: string }>;
}
const KEYS: Record<string, string> = {
  Enter: "\r",
  Tab: "\t",
  ArrowUp: "\x1b[A",
  ArrowDown: "\x1b[B",
  ArrowRight: "\x1b[C",
  ArrowLeft: "\x1b[D",
  CtrlC: "\x03",
  CtrlD: "\x04",
  Escape: "\x1b",
  Backspace: "\x7f",
};
function string(
  params: Record<string, unknown>,
  key: string,
  max = 65536,
): string {
  const value = params[key];
  if (typeof value !== "string" || !value || Buffer.byteLength(value) > max)
    throw new TerminalError(
      "INVALID_ARGUMENT",
      `${key} must be a nonempty string up to ${max} bytes`,
    );
  return value;
}
function integer(
  params: Record<string, unknown>,
  key: string,
  fallback: number,
  min: number,
  max: number,
): number {
  const value = params[key] ?? fallback;
  if (
    typeof value !== "number" ||
    !Number.isInteger(value) ||
    value < min ||
    value > max
  )
    throw new TerminalError(
      "INVALID_ARGUMENT",
      `${key} must be an integer in ${min}..${max}`,
    );
  return value;
}

export class Broker {
  private sessions = new Map<string, TerminalSession>();
  private links = new Map<string, Link>();
  private connections = new Set<Connection>();
  private server?: Server;
  private controlServer?: Server;
  private boundPaths = new Set<string>();
  private stopping = false;
  private idleTimer?: NodeJS.Timeout;
  private sweep?: NodeJS.Timeout;
  private notificationTimers = new Map<string, NodeJS.Timeout>();
  constructor(readonly config: BrokerConfig = getConfig()) {}
  async start(): Promise<void> {
    if (process.platform !== "darwin")
      throw new TerminalError("UNSUPPORTED_PLATFORM", "MVP requires macOS");
    privateDirectory(this.config.runtimeDir);
    privateDirectory(this.config.dataDir);
    await this.assertSocketAvailable();
    removeOwnedSocket(this.config.socketPath);
    removeOwnedSocket(this.config.controlSocketPath);
    await this.restore();
    this.controlServer = createServer((socket) => {
      socket.setEncoding("utf8");
      socket.setTimeout(5000, () => socket.destroy());
      let buffer = "";
      socket.on("error", () => {});
      socket.on("data", (data) => {
        buffer += data.toString("utf8");
        if (buffer.length > 16384) {
          socket.destroy();
          return;
        }
        const at = buffer.indexOf("\n");
        if (at < 0) return;
        try {
          const value = JSON.parse(buffer.slice(0, at));
          const session = this.sessions.get(value.session_id);
          const ok = session?.control(value) ?? false;
          socket.end(JSON.stringify({ ok }) + "\n");
        } catch {
          socket.destroy();
        }
      });
    });
    await this.listen(this.controlServer, this.config.controlSocketPath);
    this.server = createServer((socket) => this.accept(socket));
    await this.listen(this.server, this.config.socketPath);
    this.sweep = setInterval(() => {
      const now = Date.now();
      for (const link of this.links.values())
        if (link.expires < now) this.release(link.id);
      this.pruneExpiredSessions(now);
      this.checkIdle();
    }, 1000);
    this.checkIdle();
  }
  private async assertSocketAvailable() {
    try {
      const info = lstatSync(this.config.socketPath);
      if (!info.isSocket() || info.uid !== process.getuid?.())
        throw new TerminalError(
          "UNSAFE_SOCKET",
          "Runtime socket is not owned by current user",
        );
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === "ENOENT") return;
      throw e;
    }
    const live = await new Promise<boolean>((resolve) => {
      const socket = createConnection(this.config.socketPath);
      const timer = setTimeout(() => {
        socket.destroy();
        resolve(true);
      }, 1000);
      socket.once("connect", () => {
        clearTimeout(timer);
        socket.destroy();
        resolve(true);
      });
      socket.once("error", (error) => {
        clearTimeout(timer);
        resolve((error as NodeJS.ErrnoException).code !== "ECONNREFUSED");
      });
    });
    if (live)
      throw new TerminalError(
        "BROKER_ALREADY_RUNNING",
        "A process is already listening on the broker socket",
      );
  }
  private listen(server: Server, path: string): Promise<void> {
    return new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(path, () => {
        this.boundPaths.add(path);
        chmodSync(path, 0o600);
        resolve();
      });
    });
  }
  private async restore() {
    for (const name of readdirSync(this.config.dataDir)) {
      if (!/^[a-f0-9-]{36}$/.test(name)) continue;
      const directory = join(this.config.dataDir, name);
      try {
        const st = lstatSync(directory);
        if (
          !st.isDirectory() ||
          st.isSymbolicLink() ||
          st.uid !== process.getuid?.()
        )
          continue;
        const metadata = join(directory, "session.json");
        if (statSync(metadata).size > 2 * 1024 * 1024) continue;
        const record = JSON.parse(
          readFileSync(metadata, "utf8"),
        ) as SessionRecord;
        if (record.session_id !== name || !Array.isArray(record.commands))
          continue;
        if (
          ["EXITED", "LOST"].includes(record.state) &&
          Date.now() -
            new Date(record.exited_at ?? record.created_at).getTime() >
            this.config.retentionMs
        ) {
          rmSync(directory, { recursive: true });
          continue;
        }
        const wasActive = !["EXITED", "LOST"].includes(record.state);
        if (this.sessions.size >= this.config.maxRetainedSessions) {
          console.error("archive_capacity_exceeded", name);
          continue;
        }
        const session = new TerminalSession(this.config, record);
        this.register(session);
        if (
          wasActive &&
          processIdentity(record.shell_pid) === record.process_identity
        )
          await terminateProcesses(
            record.shell_pid,
            record.process_identity,
            record.tty,
            0,
          );
      } catch (error) {
        console.error("archive_restore_error", name, asError(error).message);
      }
    }
  }
  private register(session: TerminalSession) {
    this.sessions.set(session.record.session_id, session);
    session.on("changed", () => {
      this.scheduleNotification(session.record.session_id);
      this.enforceTotalBudget();
      this.checkIdle();
    });
  }
  private pruneExpiredSessions(now: number) {
    for (const session of this.sessions.values()) {
      if (
        !["EXITED", "LOST"].includes(session.record.state) ||
        now -
          new Date(
            session.record.exited_at ?? session.record.created_at,
          ).getTime() <=
          this.config.retentionMs
      )
        continue;
      for (const link of this.links.values())
        if (link.sessionId === session.record.session_id) this.release(link.id);
      session.closeStore();
      this.sessions.delete(session.record.session_id);
      try {
        rmSync(session.directory, { recursive: true });
      } catch (error) {
        console.error("archive_prune_error", asError(error).message);
      }
    }
  }
  private scheduleNotification(id: string) {
    if (this.notificationTimers.has(id)) return;
    this.notificationTimers.set(
      id,
      setTimeout(() => {
        this.notificationTimers.delete(id);
        for (const c of this.connections)
          this.send(c.socket, {
            notification: "session_changed",
            session_id: id,
          });
      }, 30),
    );
  }
  private enforceTotalBudget() {
    let bytes = [...this.sessions.values()].reduce(
      (n, s) => n + s.store.diskBytes,
      0,
    );
    if (bytes <= this.config.totalLogBytes) return;
    const ordered = [...this.sessions.values()].sort(
      (a, b) =>
        Number(!["EXITED", "LOST"].includes(a.record.state)) -
        Number(!["EXITED", "LOST"].includes(b.record.state)),
    );
    for (const s of ordered) {
      if (bytes <= this.config.totalLogBytes) break;
      const before = s.store.diskBytes;
      s.store.pruneTo(
        Math.max(0, before - (bytes - this.config.totalLogBytes)),
      );
      bytes -= before - s.store.diskBytes;
    }
  }
  private accept(socket: Socket) {
    if (this.connections.size >= 128 || this.stopping) {
      socket.destroy();
      return;
    }
    const c: Connection = { id: randomUUID(), socket, pending: new Map() };
    this.connections.add(c);
    this.checkIdle();
    socket.on("error", () => {});
    socket.setEncoding("utf8");
    let buffer = "";
    socket.on("data", (data) => {
      buffer += data;
      if (Buffer.byteLength(buffer) > 256 * 1024) {
        socket.destroy();
        return;
      }
      let at;
      while ((at = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, at);
        buffer = buffer.slice(at + 1);
        let request: WireRequest;
        try {
          request = JSON.parse(line);
          if (
            typeof request.id !== "string" ||
            typeof request.method !== "string" ||
            !request.params ||
            typeof request.params !== "object" ||
            Array.isArray(request.params)
          )
            throw new Error("Invalid wire request");
        } catch {
          socket.destroy();
          return;
        }
        if (request.method === "_cancel") {
          c.pending.get(String(request.params.request_id))?.controller.abort();
          continue;
        }
        if (c.pending.size >= 128) {
          this.send(socket, {
            id: request.id,
            error: {
              code: "REQUEST_LIMIT",
              message: "Too many in-flight requests",
            },
          });
          continue;
        }
        const pending = {
          controller: new AbortController(),
          linkId:
            typeof request.params.link_id === "string"
              ? request.params.link_id
              : undefined,
        };
        c.pending.set(request.id, pending);
        void this.dispatch(
          request.method,
          request.params,
          c,
          pending.controller.signal,
        )
          .then(
            (result) => this.send(socket, { id: request.id, result }),
            (error) => {
              const e = asError(error);
              this.send(socket, {
                id: request.id,
                error: { code: e.code, message: e.message, details: e.details },
              });
            },
          )
          .finally(() => c.pending.delete(request.id));
      }
    });
    socket.on("close", () => {
      this.connections.delete(c);
      for (const p of c.pending.values()) p.controller.abort();
      c.pending.clear();
      for (const link of this.links.values())
        if (link.owner === c.id && link.access === "control") {
          link.disconnectTimer = setTimeout(() => {
            if (link.owner === c.id) {
              link.access = "read";
              link.disconnectTimer = undefined;
            }
          }, this.config.disconnectGraceMs);
        }
      this.checkIdle();
    });
  }
  private send(socket: Socket, value: unknown) {
    if (socket.destroyed) return;
    if (socket.writableLength > 512 * 1024) {
      socket.destroy();
      return;
    }
    socket.write(JSON.stringify(value) + "\n");
  }
  private checkIdle() {
    const idle =
      !this.stopping &&
      this.connections.size === 0 &&
      ![...this.sessions.values()].some(
        (s) => !["EXITED", "LOST"].includes(s.record.state),
      );
    if (!idle) {
      clearTimeout(this.idleTimer);
      this.idleTimer = undefined;
      return;
    }
    if (!this.idleTimer)
      this.idleTimer = setTimeout(() => {
        this.idleTimer = undefined;
        void this.stop();
      }, this.config.idleExitMs);
  }
  private getLink(
    params: Record<string, unknown>,
    c: Connection,
    control = false,
  ): { link: Link; session: TerminalSession } {
    const id = string(params, "link_id", 128);
    const link = this.links.get(id);
    if (!link || link.expires < Date.now()) {
      if (link) this.release(id);
      throw new TerminalError(
        "LINK_EXPIRED",
        "Unknown or expired link; link the managed PID again",
      );
    }
    const session = this.sessions.get(link.sessionId)!;
    if (control && link.access !== "control")
      throw new TerminalError(
        "CONTROL_REQUIRED",
        "A control link is required; release current controller and link with access=control",
      );
    if (link.disconnectTimer) {
      clearTimeout(link.disconnectTimer);
      link.disconnectTimer = undefined;
    }
    link.owner = c.id;
    link.expires = Date.now() + this.config.linkTtlMs;
    return { link, session };
  }
  private addLink(session: TerminalSession, access: Access, c: Connection) {
    if (this.links.size >= 4096)
      throw new TerminalError("LINK_LIMIT", "Too many link handles");
    if (
      access === "control" &&
      [...this.links.values()].some(
        (l) =>
          l.sessionId === session.record.session_id &&
          l.access === "control" &&
          l.expires > Date.now(),
      )
    )
      throw new TerminalError(
        "CONTROL_HELD",
        "Another link currently controls this terminal; unlink it first",
      );
    const id = randomUUID();
    const link: Link = {
      id,
      sessionId: session.record.session_id,
      access,
      owner: c.id,
      expires: Date.now() + this.config.linkTtlMs,
    };
    this.links.set(id, link);
    return {
      ...session.snapshot(),
      link_id: id,
      access,
      link_expires_at: new Date(link.expires).toISOString(),
      resource_uri: `terminal://link/${id}/events`,
    };
  }
  private release(id: string) {
    const link = this.links.get(id);
    if (!link) return;
    clearTimeout(link.disconnectTimer);
    this.links.delete(id);
    for (const c of this.connections)
      for (const p of c.pending.values())
        if (p.linkId === id) p.controller.abort();
  }
  private async dispatch(
    method: string,
    p: Record<string, unknown>,
    c: Connection,
    signal: AbortSignal,
  ): Promise<unknown> {
    if (method === "broker_ping")
      return {
        protocol: "local-terminal-broker/1",
        version: "0.1.0",
        pid: process.pid,
      };
    if (method === "broker_status")
      return {
        pid: process.pid,
        socket_path: this.config.socketPath,
        clients: this.connections.size,
        sessions: [...this.sessions.values()].map((s) => s.snapshot()),
        links: this.links.size,
      };
    if (method === "broker_stop") {
      setTimeout(() => void this.stop(), 20);
      return { stopping: true };
    }
    if (this.stopping)
      throw new TerminalError("BROKER_STOPPING", "Broker is shutting down");
    if (method === "terminal_create") {
      if (
        [...this.sessions.values()].filter(
          (s) => !["EXITED", "LOST"].includes(s.record.state),
        ).length >= this.config.maxSessions
      )
        throw new TerminalError(
          "SESSION_LIMIT",
          "Maximum active terminal count reached",
        );
      this.pruneExpiredSessions(Date.now());
      if (this.sessions.size >= this.config.maxRetainedSessions)
        throw new TerminalError(
          "SESSION_ARCHIVE_LIMIT",
          "Retained-session limit reached; use a fresh data directory or raise LTMCP_MAX_RETAINED_SESSIONS",
        );
      const cols = integer(p, "cols", 80, 2, 1000),
        rows = integer(p, "rows", 24, 2, 1000);
      if (p.cwd !== undefined) string(p, "cwd", 4096);
      if (p.env !== undefined) {
        if (
          !p.env ||
          typeof p.env !== "object" ||
          Array.isArray(p.env) ||
          Object.entries(p.env).some(
            ([k, v]) =>
              !/^[_a-zA-Z][_a-zA-Z0-9]*$/.test(k) ||
              typeof v !== "string" ||
              v.includes("\0"),
          ) ||
          Buffer.byteLength(JSON.stringify(p.env)) > 32768
        )
          throw new TerminalError("INVALID_ARGUMENT", "Invalid env map");
      }
      const session = new TerminalSession(this.config);
      this.register(session);
      try {
        await session.start({ ...p, cols, rows });
        return this.addLink(session, "control", c);
      } catch (error) {
        await session.terminate(0);
        throw error;
      }
    }
    if (method === "terminal_link") {
      const pid = integer(p, "pid", 0, 1, 2147483647);
      const access = p.access ?? "read";
      if (access !== "read" && access !== "control")
        throw new TerminalError(
          "INVALID_ARGUMENT",
          "access must be read or control",
        );
      const session = [...this.sessions.values()].find(
        (s) => s.record.shell_pid === pid,
      );
      if (!session)
        throw new TerminalError(
          "NOT_MANAGED",
          "This PID is not a terminal managed by this broker",
        );
      const identity = processIdentity(pid);
      if (identity && identity !== session.record.process_identity)
        throw new TerminalError(
          "PID_REUSED",
          "The old shell PID is now owned by a different process",
        );
      if (!["EXITED", "LOST"].includes(session.record.state) && !identity)
        throw new TerminalError(
          "SESSION_LOST",
          "Managed shell process no longer exists",
        );
      if (
        access === "control" &&
        ["EXITED", "LOST"].includes(session.record.state)
      )
        throw new TerminalError(
          "SESSION_CLOSED",
          "Only read links can be created for an archived terminal",
        );
      return this.addLink(session, access, c);
    }
    if (method === "terminal_unlink") {
      const id = string(p, "link_id", 128);
      this.release(id);
      return { link_id: id, released: true };
    }
    const { link, session } = this.getLink(
      p,
      c,
      [
        "terminal_exec",
        "terminal_write",
        "terminal_resize",
        "terminal_terminate",
      ].includes(method),
    );
    switch (method) {
      case "terminal_exec":
        return session.exec(
          string(p, "command"),
          string(p, "request_key", 256),
          integer(p, "wait_ms", 2000, 0, 10000),
          signal,
        );
      case "terminal_read":
        return session.read(
          p.after_cursor === undefined
            ? undefined
            : string(p, "after_cursor", 512),
          integer(p, "max_bytes", 16384, 64, 65536),
          integer(p, "wait_ms", 0, 0, 20000),
          signal,
        );
      case "terminal_status":
        return {
          ...session.status(
            p.command_id === undefined
              ? undefined
              : string(p, "command_id", 128),
          ),
          link_id: link.id,
          access: link.access,
        };
      case "terminal_write": {
        if ((p.text !== undefined) === (p.keys !== undefined))
          throw new TerminalError(
            "INVALID_ARGUMENT",
            "Provide exactly one of text or keys",
          );
        let text: string;
        if (p.text !== undefined) {
          if (typeof p.text !== "string" || Buffer.byteLength(p.text) > 16384)
            throw new TerminalError(
              "INVALID_ARGUMENT",
              "text must be a string up to 16 KiB",
            );
          text = p.text;
        } else {
          if (
            !Array.isArray(p.keys) ||
            !p.keys.length ||
            p.keys.length > 128 ||
            p.keys.some((k) => typeof k !== "string" || KEYS[k] === undefined)
          )
            throw new TerminalError("INVALID_ARGUMENT", "Unsupported keys");
          text = p.keys.map((k) => KEYS[k as string]).join("");
        }
        session.write(text);
        return {
          session_id: session.record.session_id,
          bytes_written: Buffer.byteLength(text),
          delivered_to_pty: true,
        };
      }
      case "terminal_resize":
        session.resize(
          integer(p, "cols", 80, 2, 1000),
          integer(p, "rows", 24, 2, 1000),
        );
        return session.snapshot();
      case "terminal_terminate":
        return session.terminate(integer(p, "grace_ms", 3000, 0, 10000));
      default:
        throw new TerminalError(
          "UNKNOWN_METHOD",
          `Unknown broker method: ${method}`,
        );
    }
  }
  async stop(): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    clearInterval(this.sweep);
    clearTimeout(this.idleTimer);
    await Promise.all(
      [...this.sessions.values()].map((s) =>
        s
          .terminate(200)
          .catch((e) => console.error("terminate_error", asError(e).message)),
      ),
    );
    for (const timer of this.notificationTimers.values()) clearTimeout(timer);
    for (const link of this.links.values()) clearTimeout(link.disconnectTimer);
    for (const c of this.connections) c.socket.destroy();
    this.connections.clear();
    for (const s of this.sessions.values()) s.closeStore();
    await Promise.all(
      [this.server, this.controlServer]
        .filter(Boolean)
        .map(
          (server) =>
            new Promise<void>((resolve) => server!.close(() => resolve())),
        ),
    );
    for (const path of this.boundPaths) removeOwnedSocket(path);
    this.boundPaths.clear();
  }
}
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const index = process.argv.indexOf("--config");
  const config = getConfig(
    index >= 0 ? JSON.parse(process.argv[index + 1]!) : {},
  );
  const broker = new Broker(config);
  process.once("SIGTERM", () => void broker.stop());
  process.once("SIGINT", () => void broker.stop());
  broker.start().catch(async (error) => {
    console.error(asError(error));
    await broker.stop();
    process.exitCode = 1;
  });
}
