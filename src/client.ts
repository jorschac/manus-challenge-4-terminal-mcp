import { EventEmitter } from "node:events";
import { createConnection, type Socket } from "node:net";
import { spawn } from "node:child_process";
import {
  mkdirSync,
  readFileSync,
  rmSync,
  openSync,
  closeSync,
  lstatSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { getConfig, privateDirectory } from "./config.js";
import { TerminalError } from "./errors.js";
import { atomicJson, processIdentity, sleep } from "./system.js";
import type { BrokerConfig, WireResponse } from "./types.js";

export class BrokerClient extends EventEmitter {
  readonly config: BrokerConfig;
  private socket?: Socket;
  private connecting?: Promise<void>;
  private buffer = "";
  private closed = false;
  private pending = new Map<
    string,
    {
      resolve: (value: any) => void;
      reject: (error: Error) => void;
      clean: () => void;
    }
  >();
  constructor(config?: BrokerConfig) {
    super();
    this.config = config ?? getConfig();
  }
  async connect(): Promise<void> {
    if (this.socket && !this.socket.destroyed) return;
    if (this.connecting) return this.connecting;
    this.closed = false;
    this.connecting = this.ensureConnected();
    try {
      await this.connecting;
    } finally {
      this.connecting = undefined;
    }
  }
  private async open(): Promise<void> {
    const socket = createConnection(this.config.socketPath);
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        socket.destroy();
        reject(
          new TerminalError("CONNECT_TIMEOUT", "Broker connection timed out"),
        );
      }, 1000);
      socket.once("connect", () => {
        clearTimeout(timeout);
        resolve();
      });
      socket.once("error", (error) => {
        clearTimeout(timeout);
        reject(error);
      });
    });
    this.socket = socket;
    this.buffer = "";
    socket.setEncoding("utf8");
    socket.on("data", (data) => this.receive(String(data)));
    socket.on("error", () => {});
    socket.on("close", () => {
      if (this.socket === socket) this.socket = undefined;
      for (const item of this.pending.values()) {
        item.clean();
        item.reject(
          new TerminalError(
            "BROKER_DISCONNECTED",
            "Broker disconnected; do not blindly rerun a non-idempotent operation",
          ),
        );
      }
      this.pending.clear();
      this.emit("disconnect");
    });
    const ping = await this.send("broker_ping", {});
    if (ping.protocol !== "local-terminal-broker/1") {
      this.close();
      throw new TerminalError(
        "BROKER_VERSION_MISMATCH",
        "Socket is not a compatible terminal broker",
      );
    }
  }
  private async ensureConnected(): Promise<void> {
    privateDirectory(this.config.runtimeDir);
    privateDirectory(this.config.dataDir);
    try {
      await this.open();
      return;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (!["ENOENT", "ECONNREFUSED"].includes(code ?? "")) throw error;
    }
    const lock = join(this.config.runtimeDir, "start.lock");
    const deadline = Date.now() + 12000;
    let owned = false;
    while (Date.now() < deadline) {
      try {
        mkdirSync(lock, { mode: 0o700 });
        owned = true;
        atomicJson(join(lock, "owner.json"), {
          pid: process.pid,
          identity: processIdentity(process.pid),
        });
        break;
      } catch (error) {
        if (owned) {
          rmSync(lock, { recursive: true, force: true });
          throw error;
        }
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        const info = lstatSync(lock);
        if (
          !info.isDirectory() ||
          info.isSymbolicLink() ||
          info.uid !== process.getuid?.()
        )
          throw new TerminalError("UNSAFE_LOCK", "Unsafe broker startup lock");
        try {
          const owner = JSON.parse(
            readFileSync(join(lock, "owner.json"), "utf8"),
          );
          if (owner.identity && processIdentity(owner.pid) !== owner.identity) {
            rmSync(lock, { recursive: true });
            continue;
          }
        } catch {}
        try {
          await this.open();
          return;
        } catch (e) {
          if (
            !["ENOENT", "ECONNREFUSED"].includes(
              (e as NodeJS.ErrnoException).code ?? "",
            )
          )
            throw e;
        }
        await sleep(50);
      }
    }
    if (!owned)
      throw new TerminalError(
        "STARTUP_LOCK_TIMEOUT",
        "Broker startup lock timed out; inspect the private runtime directory",
      );
    try {
      try {
        await this.open();
        return;
      } catch (e) {
        if (
          !["ENOENT", "ECONNREFUSED"].includes(
            (e as NodeJS.ErrnoException).code ?? "",
          )
        )
          throw e;
      }
      const logPath = join(this.config.runtimeDir, "broker.log");
      try {
        const info = lstatSync(logPath);
        if (
          !info.isFile() ||
          info.isSymbolicLink() ||
          info.uid !== process.getuid?.()
        )
          throw new TerminalError("UNSAFE_LOG", "Unsafe broker log path");
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
      }
      const fd = openSync(logPath, "a", 0o600);
      const child = spawn(
        process.execPath,
        [
          fileURLToPath(new URL("./broker.js", import.meta.url)),
          "--config",
          JSON.stringify(this.config),
        ],
        { detached: true, stdio: ["ignore", fd, fd], env: process.env },
      );
      child.unref();
      closeSync(fd);
      let spawnError: Error | undefined;
      child.once("error", (error) => {
        spawnError = error;
      });
      while (Date.now() < deadline) {
        if (spawnError) throw spawnError;
        try {
          await this.open();
          return;
        } catch (e) {
          if (
            !["ENOENT", "ECONNREFUSED"].includes(
              (e as NodeJS.ErrnoException).code ?? "",
            )
          )
            throw e;
        }
        await sleep(50);
      }
      throw new TerminalError(
        "BROKER_START_TIMEOUT",
        `Broker did not start; see ${logPath}`,
      );
    } finally {
      rmSync(lock, { recursive: true, force: true });
    }
  }
  async request(
    method: string,
    params: Record<string, unknown> = {},
    signal?: AbortSignal,
  ): Promise<any> {
    await this.connect();
    return this.send(method, params, signal);
  }
  private send(
    method: string,
    params: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<any> {
    if (!this.socket || this.socket.destroyed)
      return Promise.reject(
        new TerminalError("BROKER_DISCONNECTED", "No broker connection"),
      );
    if (signal?.aborted)
      return Promise.reject(
        new TerminalError("CANCELLED", "Request cancelled"),
      );
    if (this.pending.size >= 128)
      return Promise.reject(
        new TerminalError("REQUEST_LIMIT", "Too many in-flight requests"),
      );
    const id = randomUUID();
    return new Promise((resolve, reject) => {
      const abort = () => {
        const item = this.pending.get(id);
        if (!item) return;
        this.pending.delete(id);
        item.clean();
        this.socket?.write(
          JSON.stringify({
            id: randomUUID(),
            method: "_cancel",
            params: { request_id: id },
          }) + "\n",
        );
        reject(
          new TerminalError("CANCELLED", "Wait cancelled; process continues"),
        );
      };
      const timeout = setTimeout(() => {
        abort();
      }, 45000);
      const clean = () => {
        clearTimeout(timeout);
        signal?.removeEventListener("abort", abort);
      };
      this.pending.set(id, { resolve, reject, clean });
      signal?.addEventListener("abort", abort, { once: true });
      this.socket!.write(
        JSON.stringify({ id, method, params }) + "\n",
        (error) => {
          if (error) {
            this.pending.delete(id);
            clean();
            reject(error);
          }
        },
      );
    });
  }
  private receive(chunk: string): void {
    this.buffer += chunk;
    if (Buffer.byteLength(this.buffer) > 1024 * 1024) {
      this.socket?.destroy();
      return;
    }
    let at: number;
    while ((at = this.buffer.indexOf("\n")) >= 0) {
      const line = this.buffer.slice(0, at);
      this.buffer = this.buffer.slice(at + 1);
      try {
        const value = JSON.parse(line);
        if (value.notification) {
          this.emit("notification", value);
          continue;
        }
        const response = value as WireResponse;
        const item = this.pending.get(response.id);
        if (!item) continue;
        this.pending.delete(response.id);
        item.clean();
        response.error
          ? item.reject(
              new TerminalError(
                response.error.code,
                response.error.message,
                response.error.details,
              ),
            )
          : item.resolve(response.result);
      } catch {
        this.socket?.destroy(new Error("Malformed broker message"));
      }
    }
  }
  close(): void {
    this.closed = true;
    this.socket?.end();
    this.socket?.destroy();
    this.socket = undefined;
  }
}
