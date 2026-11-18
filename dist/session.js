import { EventEmitter } from "node:events";
import { createHash, randomUUID } from "node:crypto";
import { spawn } from "node-pty";
import { execFileSync } from "node:child_process";
import { closeSync, constants, openSync, realpathSync, statSync, writeFileSync, writeSync, unlinkSync, } from "node:fs";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";
import { EventStore } from "./event-store.js";
import { TerminalError } from "./errors.js";
import { privateDirectory } from "./config.js";
import { atomicJson, processIdentity, quoteShell, sleep, terminateProcesses, } from "./system.js";
export class TerminalSession extends EventEmitter {
    config;
    store;
    directory;
    token = randomUUID();
    record;
    pty;
    fifoFd;
    pending = "";
    frames = new Map();
    active;
    readyResolve;
    readyReject;
    stopped = false;
    completionTimer;
    terminating;
    constructor(config, existing) {
        super();
        this.config = config;
        const id = existing?.session_id ?? randomUUID();
        this.directory = join(config.dataDir, id);
        privateDirectory(this.directory);
        this.store = new EventStore({
            directory: join(this.directory, "events"),
            sessionId: id,
            maxBytes: config.sessionLogBytes,
            cacheBytes: config.cacheBytes,
        });
        this.record = existing ?? {
            session_id: id,
            shell_pid: 0,
            process_identity: "",
            tty: "",
            cwd: "",
            cols: 80,
            rows: 24,
            state: "STARTING",
            created_at: new Date().toISOString(),
            commands: [],
        };
        this.store.on("append", () => this.emit("changed"));
        if (existing && !["EXITED", "LOST"].includes(existing.state)) {
            this.record.state = "LOST";
            for (const cmd of this.record.commands)
                if (cmd.state === "RUNNING" || cmd.state === "ACCEPTED") {
                    cmd.state = "UNKNOWN";
                    cmd.reason = "broker_restarted";
                }
            this.store.append("session_lost", {
                reason: "broker_restarted_live_pty_not_recoverable",
            });
            this.persist();
        }
    }
    persist() {
        atomicJson(join(this.directory, "session.json"), this.record);
    }
    snapshot() {
        return {
            ...this.record,
            commands: undefined,
            cursor: this.store.cursor,
            earliest_cursor: this.store.earliestCursor,
            storage_degraded: this.store.degraded,
            capabilities: {
                structured_exec: Boolean(this.pty) &&
                    !["UNMANAGED", "LOST", "EXITED"].includes(this.record.state),
                interactive_input: Boolean(this.pty),
                stdout_stderr_separate: false,
            },
        };
    }
    async start(params) {
        const cwd = realpathSync(typeof params.cwd === "string" ? params.cwd : homedir());
        if (!statSync(cwd).isDirectory())
            throw new TerminalError("INVALID_CWD", "cwd must be an existing directory");
        this.record.cwd = cwd;
        this.record.cols = Number(params.cols ?? 80);
        this.record.rows = Number(params.rows ?? 24);
        const fifo = join(this.directory, "commands.fifo");
        execFileSync("/usr/bin/mkfifo", ["-m", "600", fifo]);
        // Holding an O_RDWR endpoint prevents startup/idle EOF and blocking open races.
        this.fifoFd = openSync(fifo, constants.O_RDWR | constants.O_NONBLOCK);
        const report = `${quoteShell(process.execPath)} ${quoteShell(fileURLToPath(new URL("./reporter.js", import.meta.url)))} ${quoteShell(this.config.controlSocketPath)} ${quoteShell(this.token)}`;
        const qid = quoteShell(this.record.session_id);
        const bootstrap = join(this.directory, "bootstrap.zsh");
        const script = [
            "unsetopt MONITOR; setopt NO_BEEP; unsetopt ERR_EXIT ERR_RETURN;",
            "TRAPINT() { return 0; }",
            "/bin/stty -echo;",
            `exec 3< ${quoteShell(fifo)};`,
            `${report} ready ${qid} '' 0 "$PWD" "$(/usr/bin/tty)";`,
            "while true; do",
            '  IFS=" " read -r __lt_job __lt_nonce <&3 || continue;',
            '  [[ -n "$__lt_job" && -n "$__lt_nonce" ]] || continue;',
            '  builtin printf "\\036LTMCP:%s:S:%s\\037" "$__lt_nonce" "$__lt_job";',
            `  ${report} started ${qid} "$__lt_job" 0 "$PWD" '';`,
            `  builtin source ${quoteShell(this.directory)}/job-"$__lt_job".zsh;`,
            "  __lt_rc=$?;",
            '  builtin printf "\\036LTMCP:%s:E:%s\\037" "$__lt_nonce" "$__lt_job";',
            `  ${report} completed ${qid} "$__lt_job" "$__lt_rc" "$PWD" '';`,
            "done",
            "",
        ].join("\n");
        writeFileSync(bootstrap, script, { mode: 0o600 });
        const env = {
            HOME: homedir(),
            USER: process.env.USER ?? "",
            LOGNAME: process.env.LOGNAME ?? "",
            PATH: process.env.PATH ?? "/usr/bin:/bin:/usr/sbin:/sbin",
            LANG: process.env.LANG ?? "en_US.UTF-8",
            TERM: "xterm-256color",
            ...params.env,
        };
        const ready = new Promise((resolve, reject) => {
            this.readyResolve = resolve;
            this.readyReject = reject;
        });
        void ready.catch(() => { }); // startup can throw before the readiness await.
        this.pty = spawn("/bin/zsh", ["-f", "-i", bootstrap], {
            cwd,
            env,
            cols: this.record.cols,
            rows: this.record.rows,
            name: "xterm-256color",
        });
        this.record.shell_pid = this.pty.pid;
        this.pty.onData((data) => this.parse(data));
        this.pty.onExit(({ exitCode }) => this.onExit(exitCode));
        try {
            this.record.process_identity = processIdentity(this.pty.pid) ?? "";
        }
        catch (error) {
            // This is the just-created owned PTY, not an arbitrary PID lookup.
            this.pty.kill("SIGKILL");
            throw new TerminalError("PROCESS_CHECK_UNAVAILABLE", "Cannot inspect local process identity; launch the server in an execution context that permits /bin/ps", { cause: String(error) });
        }
        this.persist();
        let timer;
        try {
            await Promise.race([
                ready,
                new Promise((_, reject) => {
                    timer = setTimeout(() => reject(new TerminalError("BOOTSTRAP_TIMEOUT", "Managed shell did not become ready")), 7000);
                }),
            ]);
        }
        catch (error) {
            await this.terminate(0);
            throw error;
        }
        finally {
            clearTimeout(timer);
            this.readyResolve = undefined;
            this.readyReject = undefined;
        }
    }
    control(message) {
        if (message.token !== this.token)
            return false;
        const kind = message.kind;
        if (kind === "ready" && this.record.state === "STARTING") {
            this.record.tty = String(message.tty ?? "");
            this.record.cwd = String(message.cwd);
            this.record.state = "IDLE";
            this.store.append("session_ready", {
                shell_pid: this.record.shell_pid,
                tty: this.record.tty,
            });
            this.persist();
            this.readyResolve?.();
        }
        else if (this.active && message.command_id === this.active.command_id) {
            if (kind === "started") {
                this.active.state = "RUNNING";
                this.active.started_at = new Date().toISOString();
                this.persist();
            }
            if (kind === "completed") {
                this.active.completion_seen = true;
                this.active.exit_code = Number(message.exit_code);
                this.record.cwd = String(message.cwd);
                this.completeIfReady();
            }
        }
        return true;
    }
    parse(data) {
        this.pending += data;
        while (this.pending) {
            let found = -1;
            let frame = "";
            for (const key of this.frames.keys()) {
                const at = this.pending.indexOf(key);
                if (at >= 0 && (found < 0 || at < found)) {
                    found = at;
                    frame = key;
                }
            }
            if (found >= 0) {
                this.output(this.pending.slice(0, found));
                this.pending = this.pending.slice(found + frame.length);
                const callback = this.frames.get(frame);
                this.frames.delete(frame);
                callback();
                continue;
            }
            let keep = 0;
            for (const key of this.frames.keys())
                for (let n = 1; n < Math.min(key.length, this.pending.length + 1); n++)
                    if (this.pending.endsWith(key.slice(0, n)))
                        keep = Math.max(keep, n);
            this.output(this.pending.slice(0, this.pending.length - keep));
            this.pending = this.pending.slice(this.pending.length - keep);
            break;
        }
    }
    output(text) {
        if (!text)
            return;
        let chunk = "";
        let bytes = 0;
        for (const ch of text) {
            const size = Buffer.byteLength(ch);
            if (bytes + size > 2048) {
                this.store.append("output", {
                    text: chunk,
                    command_id: this.active?.command_id ?? null,
                });
                chunk = "";
                bytes = 0;
            }
            chunk += ch;
            bytes += size;
        }
        if (chunk)
            this.store.append("output", {
                text: chunk,
                command_id: this.active?.command_id ?? null,
            });
    }
    completeIfReady() {
        const cmd = this.active;
        if (!cmd?.completion_seen || !cmd.output_boundary_seen)
            return;
        cmd.state = "COMPLETED";
        cmd.completed_at = new Date().toISOString();
        this.store.append("command_completed", {
            command_id: cmd.command_id,
            exit_code: cmd.exit_code,
            cwd: this.record.cwd,
        });
        cmd.end_cursor = this.store.cursor;
        clearTimeout(this.completionTimer);
        this.removeCommandFile(cmd.command_id);
        this.active = undefined;
        if (this.record.state === "BUSY")
            this.record.state = "IDLE";
        this.persist();
        this.emit("changed");
    }
    removeCommandFile(id) {
        try {
            unlinkSync(join(this.directory, `job-${id}.zsh`));
        }
        catch (error) {
            if (error.code !== "ENOENT")
                console.error("command_cleanup_failed", id);
        }
    }
    markUnmanaged(reason) {
        if (this.record.state !== "BUSY")
            return;
        this.record.state = "UNMANAGED";
        if (this.active) {
            this.active.state = "UNKNOWN";
            this.active.reason = reason;
        }
        this.frames.clear();
        this.store.append("control_lost", {
            reason,
            command_id: this.active?.command_id,
        });
        this.persist();
        this.emit("changed");
    }
    checkControlHealth() {
        if (!this.pty || this.record.state !== "BUSY")
            return;
        try {
            const program = execFileSync("/bin/ps", ["-p", String(this.record.shell_pid), "-o", "comm="], { encoding: "utf8", timeout: 1000 }).trim();
            if (program && basename(program) !== "zsh")
                this.markUnmanaged("shell_replaced");
        }
        catch {
            /* onExit remains authoritative when the process has disappeared. */
        }
    }
    async exec(command, key, waitMs, signal) {
        this.checkControlHealth();
        const fingerprint = createHash("sha256").update(command).digest("hex");
        const old = this.record.commands.find((c) => c.request_key === key);
        if (old) {
            if (old.fingerprint !== fingerprint)
                throw new TerminalError("REQUEST_KEY_CONFLICT", "request_key was already used with a different command");
            await this.waitCommand(old, waitMs, signal);
            return this.commandResult(old);
        }
        if (this.record.state === "UNMANAGED")
            throw new TerminalError("SHELL_UNMANAGED", "Managed shell control protocol is unavailable; create a new session");
        if (this.record.state !== "IDLE")
            throw new TerminalError(this.record.state === "BUSY" ? "SESSION_BUSY" : "SESSION_CLOSED", `Cannot exec in state ${this.record.state}`);
        if (this.record.commands.length >= 1024)
            throw new TerminalError("COMMAND_LIMIT", "This session has reached 1024 command records; create a new session");
        const id = randomUUID();
        const nonce = randomUUID();
        const cmd = {
            command_id: id,
            request_key: key,
            fingerprint,
            state: "ACCEPTED",
            accepted_at: new Date().toISOString(),
            exit_code: null,
            start_cursor: this.store.cursor,
            completion_seen: false,
            output_boundary_seen: false,
        };
        this.record.commands.push(cmd);
        this.active = cmd;
        this.record.state = "BUSY";
        // The idempotency reservation is fsynced before delivery to the shell.
        this.persist();
        writeFileSync(join(this.directory, `job-${id}.zsh`), command + "\n", {
            mode: 0o600,
        });
        const start = `\x1eLTMCP:${nonce}:S:${id}\x1f`;
        const end = `\x1eLTMCP:${nonce}:E:${id}\x1f`;
        this.frames.set(start, () => {
            this.store.append("command_started", { command_id: id });
        });
        this.frames.set(end, () => {
            cmd.output_boundary_seen = true;
            this.completeIfReady();
            if (!cmd.completion_seen) {
                this.completionTimer = setTimeout(() => this.markUnmanaged("completion_control_timeout"), 4000);
                this.completionTimer.unref();
            }
        });
        try {
            writeSync(this.fifoFd, `${id} ${nonce}\n`);
        }
        catch (error) {
            cmd.state = "UNKNOWN";
            cmd.reason = "delivery_failed";
            this.record.state = "UNMANAGED";
            this.persist();
            throw error;
        }
        await this.waitCommand(cmd, waitMs, signal);
        return this.commandResult(cmd);
    }
    async waitCommand(cmd, waitMs, signal) {
        if (!waitMs || !["ACCEPTED", "RUNNING"].includes(cmd.state))
            return;
        await this.waitUntil(() => !["ACCEPTED", "RUNNING"].includes(cmd.state), waitMs, signal);
    }
    commandResult(cmd) {
        let read;
        let truncated = false;
        try {
            read = this.store.read(cmd.start_cursor, 65536);
        }
        catch (error) {
            if (!(error instanceof TerminalError) || error.code !== "CURSOR_EXPIRED")
                throw error;
            truncated = true;
            read = this.store.read(undefined, 65536);
        }
        // Keep all observed events for the returned cursor. Do not silently discard
        // unrelated/background events while advancing the session watermark.
        const events = read.events;
        return {
            session_id: this.record.session_id,
            state: this.record.state,
            command: { ...cmd },
            command_id: cmd.command_id,
            command_state: cmd.state,
            exit_code: cmd.exit_code,
            output: events
                .filter((e) => e.type === "output" && e.data.command_id === cmd.command_id)
                .map((e) => e.data.text)
                .join(""),
            events,
            next_cursor: read.next_cursor,
            earliest_cursor: read.earliest_cursor,
            has_more: read.has_more,
            output_truncated: truncated || read.has_more,
            storage_degraded: read.storage_degraded,
        };
    }
    async read(after, maxBytes, waitMs, signal) {
        let result = this.store.read(after, maxBytes);
        if (!result.events.length &&
            waitMs &&
            !["EXITED", "LOST"].includes(this.record.state)) {
            const cursor = result.next_cursor;
            await this.waitUntil(() => this.store.cursor !== cursor ||
                ["EXITED", "LOST"].includes(this.record.state), waitMs, signal);
            result = this.store.read(after, maxBytes);
        }
        return {
            ...result,
            session_id: this.record.session_id,
            state: this.record.state,
            eof: ["EXITED", "LOST"].includes(this.record.state) && !result.has_more,
        };
    }
    waitUntil(predicate, ms, signal) {
        return new Promise((resolve, reject) => {
            let timer;
            const done = (error) => {
                clearTimeout(timer);
                this.off("changed", check);
                signal?.removeEventListener("abort", abort);
                error ? reject(error) : resolve();
            };
            const check = () => {
                if (predicate())
                    done();
            };
            const abort = () => done(new TerminalError("CANCELLED", "Read/wait cancelled; terminal process is still running"));
            timer = setTimeout(() => done(), ms);
            this.on("changed", check);
            signal?.addEventListener("abort", abort, { once: true });
            if (signal?.aborted)
                abort();
            else
                check();
        });
    }
    write(text) {
        if (!this.pty || ["EXITED", "LOST", "CLOSING"].includes(this.record.state))
            throw new TerminalError("SESSION_CLOSED", "Terminal is no longer writable");
        if (!text)
            return;
        if (this.record.state === "IDLE")
            throw new TerminalError("SESSION_IDLE", "No foreground command is consuming input; use terminal_exec first");
        this.pty.write(text);
    }
    resize(cols, rows) {
        if (!this.pty || ["EXITED", "LOST", "CLOSING"].includes(this.record.state))
            throw new TerminalError("SESSION_CLOSED", "Terminal is no longer resizable");
        this.pty.resize(cols, rows);
        this.record.cols = cols;
        this.record.rows = rows;
        this.persist();
    }
    status(commandId) {
        this.checkControlHealth();
        const cmd = commandId
            ? this.record.commands.find((c) => c.command_id === commandId)
            : this.active;
        if (commandId && !cmd)
            throw new TerminalError("COMMAND_NOT_FOUND", "Command is not part of this session");
        return {
            ...this.snapshot(),
            command: cmd ? { ...cmd } : null,
            command_count: this.record.commands.length,
        };
    }
    async terminate(graceMs) {
        if (this.terminating)
            return this.terminating;
        if (!this.pty || ["EXITED", "LOST"].includes(this.record.state)) {
            if (this.record.state === "STARTING")
                this.onExit(-1);
            return {
                ...this.snapshot(),
                signaled_pids: [],
                killed_pids: [],
                remaining_pids: [],
            };
        }
        this.record.state = "CLOSING";
        this.persist();
        this.terminating = (async () => {
            const result = await terminateProcesses(this.record.shell_pid, this.record.process_identity, this.record.tty, graceMs);
            for (let n = 0; n < 20 && this.record.state === "CLOSING"; n++)
                await sleep(25);
            if (this.record.state === "CLOSING" && result.remaining_pids.length === 0)
                this.onExit(-1);
            return { ...this.snapshot(), ...result };
        })();
        return this.terminating;
    }
    onExit(exitCode) {
        if (this.stopped)
            return;
        this.stopped = true;
        this.readyReject?.(new TerminalError("SHELL_EXITED", "Shell exited before becoming ready"));
        clearTimeout(this.completionTimer);
        this.output(this.pending);
        this.pending = "";
        this.frames.clear();
        if (this.active && this.active.state !== "COMPLETED") {
            this.active.state = "UNKNOWN";
            this.active.reason ??= "shell_exited_without_completion";
            this.removeCommandFile(this.active.command_id);
        }
        this.active = undefined;
        this.record.state = "EXITED";
        this.record.shell_exit_code = exitCode;
        this.record.exited_at = new Date().toISOString();
        this.store.append("session_exit", { exit_code: exitCode });
        this.persist();
        if (this.fifoFd !== undefined) {
            closeSync(this.fifoFd);
            this.fifoFd = undefined;
        }
        this.pty = undefined;
        this.emit("changed");
    }
    closeStore() {
        this.store.close();
    }
}
//# sourceMappingURL=session.js.map