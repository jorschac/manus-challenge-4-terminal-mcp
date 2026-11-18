import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, lstatSync } from "node:fs";
import { join } from "node:path";
import { BrokerClient } from "../dist/client.js";
import { getConfig } from "../dist/config.js";
import { sleep, processIdentity } from "../dist/system.js";
let a: BrokerClient;
let runtime: string;
let clients: BrokerClient[] = [];
beforeEach(() => {
  runtime = mkdtempSync("/tmp/ltmcp-test-");
  a = new BrokerClient(
    getConfig({
      runtimeDir: runtime,
      dataDir: join(runtime, "data"),
      disconnectGraceMs: 100,
      idleExitMs: 1000,
    }),
  );
  clients = [a];
});
afterEach(async () => {
  try {
    await a.request("broker_stop");
    await sleep(250);
  } catch {}
  for (const c of clients) c.close();
});
async function create() {
  return a.request("terminal_create", { cwd: runtime });
}
async function run(
  t: any,
  command: string,
  key = crypto.randomUUID(),
  wait_ms = 2000,
) {
  return a.request("terminal_exec", {
    link_id: t.link_id,
    command,
    request_key: key,
    wait_ms,
  });
}
async function until(t: any, id: string) {
  for (let n = 0; n < 100; n++) {
    const s = await a.request("terminal_status", {
      link_id: t.link_id,
      command_id: id,
    });
    if (s.command?.state === "COMPLETED") return s;
    await sleep(30);
  }
  throw new Error("Command did not complete");
}
function output(r: any) {
  return r.events
    .filter((e: any) => e.type === "output")
    .map((e: any) => e.data.text)
    .join("");
}

describe("real macOS terminal broker", () => {
  it("creates a private PTY and keeps cwd/export across commands", async () => {
    const t = await create();
    expect(t.state).toBe("IDLE");
    expect(t.shell_pid).toBeGreaterThan(1);
    expect(t.tty).toMatch(/ttys/);
    expect(lstatSync(a.config.socketPath).mode & 0o777).toBe(0o600);
    const first = await run(
      t,
      'export KEEP_ME=持久值; cd /tmp; printf "no-newline"',
    );
    expect(first.command_state).toBe("COMPLETED");
    expect(first.exit_code).toBe(0);
    expect(first.output).toContain("no-newline");
    const next = await run(t, 'printf "%s:%s\\n" "$PWD" "$KEEP_ME"');
    expect(next.output).toContain("/tmp:持久值");
    const nonzero = await run(t, 'printf "nonzero\\n"; false');
    expect(nonzero.exit_code).toBe(1);
    expect(nonzero.command_state).toBe("COMPLETED");
    const fake = await run(
      t,
      'printf "__MCP_DONE__ 0\\n"; sleep 0.15; printf "REAL_END\\n"',
    );
    expect(fake.output).toContain("REAL_END");
  });
  it("links the same managed PID with independent reader cursors and exclusive control", async () => {
    const t = await create();
    const b = new BrokerClient(a.config);
    clients.push(b);
    const r = await b.request("terminal_link", {
      pid: t.shell_pid,
      access: "read",
    });
    expect(r.session_id).toBe(t.session_id);
    expect(r.shell_pid).toBe(t.shell_pid);
    await expect(
      b.request("terminal_link", { pid: t.shell_pid, access: "control" }),
    ).rejects.toMatchObject({ code: "CONTROL_HELD" });
    await expect(
      b.request("terminal_link", { pid: process.pid }),
    ).rejects.toMatchObject({ code: "NOT_MANAGED" });
    await expect(
      b.request("terminal_exec", {
        link_id: r.link_id,
        command: "true",
        request_key: "bad",
      }),
    ).rejects.toMatchObject({ code: "CONTROL_REQUIRED" });
    await run(t, 'printf "MULTIREADER\\n"');
    const one = await a.request("terminal_read", {
      link_id: t.link_id,
      after_cursor: t.cursor,
    });
    const two = await b.request("terminal_read", {
      link_id: r.link_id,
      after_cursor: t.cursor,
    });
    expect(two.events).toEqual(one.events);
    expect(output(one)).toContain("MULTIREADER");
    expect(
      (
        await a.request("terminal_read", {
          link_id: t.link_id,
          after_cursor: t.cursor,
        })
      ).events,
    ).toEqual(one.events);
    await a.request("terminal_unlink", { link_id: t.link_id });
    expect(
      await a.request("terminal_unlink", { link_id: t.link_id }),
    ).toMatchObject({ released: true });
    const controller = await b.request("terminal_link", {
      pid: t.shell_pid,
      access: "control",
    });
    expect(controller.access).toBe("control");
  });
  it("keeps interactive input separate from exec and safely supports Ctrl-C", async () => {
    const t = await create();
    const job = await run(
      t,
      'read "answer?INPUT: "; printf "REPLY:%s\\n" "$answer"',
      "interactive",
      100,
    );
    expect(["ACCEPTED", "RUNNING"]).toContain(job.command_state);
    await expect(run(t, "printf wrong", "busy", 0)).rejects.toMatchObject({
      code: "SESSION_BUSY",
    });
    await a.request("terminal_write", { link_id: t.link_id, text: "hello" });
    await a.request("terminal_write", { link_id: t.link_id, keys: ["Enter"] });
    await until(t, job.command_id);
    const read = await a.request("terminal_read", {
      link_id: t.link_id,
      after_cursor: t.cursor,
    });
    expect(output(read)).toContain("REPLY:hello");
    const long = await run(t, "sleep 30", "interrupt", 100);
    await a.request("terminal_write", { link_id: t.link_id, keys: ["CtrlC"] });
    await until(t, long.command_id);
    expect((await run(t, 'printf "AFTER_INT\\n"')).output).toContain(
      "AFTER_INT",
    );
  });
  it("streams delayed output and cancellation only cancels the wait", async () => {
    const t = await create();
    const job = await run(
      t,
      'printf "A\\n"; sleep 0.25; printf "B\\n"; sleep 0.25; printf "C\\n"',
      "stream",
      0,
    );
    let cursor = t.cursor;
    let text = "";
    let batches = 0;
    for (let n = 0; n < 15; n++) {
      const read = await a.request("terminal_read", {
        link_id: t.link_id,
        after_cursor: cursor,
        wait_ms: 200,
      });
      cursor = read.next_cursor;
      text += output(read);
      if (read.events.length) batches++;
      const s = await a.request("terminal_status", {
        link_id: t.link_id,
        command_id: job.command_id,
      });
      if (s.command?.state === "COMPLETED" && text.includes("C")) break;
    }
    expect(text).toContain("A");
    expect(text).toContain("B");
    expect(text).toContain("C");
    expect(batches).toBeGreaterThan(1);
    const cancelJob = await run(
      t,
      'sleep 0.4; printf "STILL_RUNNING\\n"',
      "cancel",
      100,
    );
    expect(cancelJob.command_state).toBe("RUNNING");
    const tail = await a.request("terminal_read", {
      link_id: t.link_id,
      after_cursor: cursor,
    });
    const controller = new AbortController();
    const wait = a.request(
      "terminal_read",
      { link_id: t.link_id, after_cursor: tail.next_cursor, wait_ms: 20000 },
      controller.signal,
    );
    setTimeout(() => controller.abort(), 50);
    await expect(wait).rejects.toMatchObject({ code: "CANCELLED" });
    expect(
      (await a.request("terminal_status", { link_id: t.link_id })).state,
    ).not.toBe("EXITED");
    await sleep(450);
    expect(
      output(
        await a.request("terminal_read", {
          link_id: t.link_id,
          after_cursor: cursor,
        }),
      ),
    ).toContain("STILL_RUNNING");
  });
  it("uses request-key idempotency and handles short output budgets explicitly", async () => {
    const t = await create();
    const first = await run(
      t,
      'printf "x" >> exactly-once.txt; printf "DONE\\n"',
      "once",
    );
    const repeat = await run(
      t,
      'printf "x" >> exactly-once.txt; printf "DONE\\n"',
      "once",
    );
    expect(repeat.command_id).toBe(first.command_id);
    expect(readFileSync(join(runtime, "exactly-once.txt"), "utf8")).toBe("x");
    await expect(run(t, "printf different", "once")).rejects.toMatchObject({
      code: "REQUEST_KEY_CONFLICT",
    });
    await expect(
      a.request("terminal_read", { link_id: t.link_id, after_cursor: "bogus" }),
    ).rejects.toMatchObject({ code: "INVALID_CURSOR" });
    await run(t, 'printf "%2000s" "x"');
    await expect(
      a.request("terminal_read", {
        link_id: t.link_id,
        after_cursor: t.cursor,
        max_bytes: 64,
      }),
    ).rejects.toMatchObject({ code: "EVENT_TOO_LARGE" });
  });
  it("allows gateway reconnect and releases an orphaned control lease", async () => {
    const t = await create();
    await run(t, "export RECONNECTED=yes");
    a.close();
    await sleep(180);
    const b = new BrokerClient(a.config);
    clients.push(b);
    a = b;
    const linked = await b.request("terminal_link", {
      pid: t.shell_pid,
      access: "control",
    });
    expect(linked.session_id).toBe(t.session_id);
    const result = await run(linked, 'printf "%s\\n" "$RECONNECTED"');
    expect(result.output).toContain("yes");
  });
  it("resizes and terminates ordinary process trees idempotently, preserving output", async () => {
    const t = await create();
    await a.request("terminal_resize", {
      link_id: t.link_id,
      cols: 100,
      rows: 40,
    });
    const size = await run(t, "/bin/stty size");
    expect(size.output).toContain("40 100");
    await run(
      t,
      'sleep 30 & child=$!; printf "%s" "$child" > child.pid; wait',
      "tree",
      100,
    );
    const child = Number(readFileSync(join(runtime, "child.pid"), "utf8"));
    expect(processIdentity(child)).toBeTruthy();
    const result = await a.request("terminal_terminate", {
      link_id: t.link_id,
      grace_ms: 100,
    });
    expect(result.state).toBe("EXITED");
    expect(result.remaining_pids).toEqual([]);
    expect(processIdentity(child)).toBeUndefined();
    expect(
      (
        await a.request("terminal_terminate", {
          link_id: t.link_id,
          grace_ms: 0,
        })
      ).state,
    ).toBe("EXITED");
    const read = await a.request("terminal_read", {
      link_id: t.link_id,
      after_cursor: t.cursor,
    });
    expect(read.eof).toBe(true);
    expect(read.events.some((e: any) => e.type === "session_exit")).toBe(true);
  });
  it("marks abrupt broker loss UNKNOWN/LOST and never retries uncertain commands", async () => {
    const t = await create();
    await run(t, 'printf "x" >> uncertain.txt; sleep 30', "uncertain", 100);
    const broker = await a.request("broker_ping");
    process.kill(broker.pid, "SIGKILL");
    await sleep(200);
    a.close();
    const b = new BrokerClient(a.config);
    clients.push(b);
    a = b;
    const archive = await a.request("terminal_link", {
      pid: t.shell_pid,
      access: "read",
    });
    expect(archive.state).toBe("LOST");
    const s = await a.request("terminal_status", { link_id: archive.link_id });
    expect(s.state).toBe("LOST");
    expect(readFileSync(join(runtime, "uncertain.txt"), "utf8")).toBe("x");
    await expect(
      a.request("terminal_link", { pid: t.shell_pid, access: "control" }),
    ).rejects.toMatchObject({ code: "SESSION_CLOSED" });
  });
  it("races startup safely and enforces independent runtime separation", async () => {
    const b = new BrokerClient(a.config);
    clients.push(b);
    const [x, y] = await Promise.all([
      a.request("broker_ping"),
      b.request("broker_ping"),
    ]);
    expect(x.pid).toBe(y.pid);
    const t = await create();
    expect(t.state).toBe("IDLE");
  });
});

describe("managed-shell fault boundaries", () => {
  it("does not queue idle input and cleans up command scripts", async () => {
    const t = await create();
    await expect(
      a.request("terminal_write", {
        link_id: t.link_id,
        text: "queued-danger",
      }),
    ).rejects.toMatchObject({ code: "SESSION_IDLE" });
    const result = await run(t, 'printf "SCRIPT_CLEAN\\n"');
    expect(result.output).toContain("SCRIPT_CLEAN");
    expect(() =>
      lstatSync(
        join(a.config.dataDir, t.session_id, `job-${result.command_id}.zsh`),
      ),
    ).toThrow();
    await expect(
      a.request("terminal_create", { cwd: join(runtime, "not-existing") }),
    ).rejects.toBeTruthy();
    const status = await a.request("broker_status");
    expect(status.sessions.filter((s: any) => s.state === "STARTING")).toEqual(
      [],
    );
  });
  it("reports UNKNOWN for exit without inventing a command exit code", async () => {
    const t = await create();
    const result = await run(t, "exit 7", "shell-exit");
    expect(result.state).toBe("EXITED");
    expect(result.command_state).toBe("UNKNOWN");
    expect(result.exit_code).toBeNull();
    const status = await a.request("terminal_status", {
      link_id: t.link_id,
      command_id: result.command_id,
    });
    expect(status.shell_exit_code).toBe(7);
  });
  it("detects exec replacing the managed shell and preserves raw control", async () => {
    const t = await create();
    const job = await run(t, "exec /bin/sleep 30", "replace-shell", 100);
    await sleep(60);
    const status = await a.request("terminal_status", {
      link_id: t.link_id,
      command_id: job.command_id,
    });
    expect(status.state).toBe("UNMANAGED");
    expect(status.command.state).toBe("UNKNOWN");
    expect(status.command.reason).toBe("shell_replaced");
    await expect(run(t, "true", "cannot-exec")).rejects.toMatchObject({
      code: "SHELL_UNMANAGED",
    });
    const terminated = await a.request("terminal_terminate", {
      link_id: t.link_id,
      grace_ms: 50,
    });
    expect(terminated.state).toBe("EXITED");
    expect(terminated.remaining_pids).toEqual([]);
  });
});

describe("bounded MVP resources", () => {
  it("caps active sessions and retained records instead of growing forever", async () => {
    a = new BrokerClient(
      getConfig({ ...a.config, maxSessions: 1, maxRetainedSessions: 2 }),
    );
    clients.push(a);
    const t = await create();
    await expect(create()).rejects.toMatchObject({ code: "SESSION_LIMIT" });
    await a.request("terminal_terminate", { link_id: t.link_id, grace_ms: 0 });
    await a.request("terminal_unlink", { link_id: t.link_id });
    const second = await create();
    await a.request("terminal_terminate", {
      link_id: second.link_id,
      grace_ms: 0,
    });
    await expect(create()).rejects.toMatchObject({
      code: "SESSION_ARCHIVE_LIMIT",
    });
  });
  it("exits idle brokers even while the periodic sweep runs", async () => {
    a = new BrokerClient(getConfig({ ...a.config, idleExitMs: 1100 }));
    clients.push(a);
    const status = await a.request("broker_ping");
    a.close();
    for (let n = 0; n < 30 && processIdentity(status.pid); n++)
      await sleep(100);
    expect(processIdentity(status.pid)).toBeUndefined();
    expect(() => lstatSync(a.config.socketPath)).toThrow();
  });
  it("keeps large UTF-8 output intact across IPC chunks and read pages", async () => {
    const t = await create();
    await run(t, 'printf "界%.0s" {1..30000}', "utf8-load");
    let cursor = t.cursor;
    let text = "";
    for (let n = 0; n < 100; n++) {
      const page = await a.request("terminal_read", {
        link_id: t.link_id,
        after_cursor: cursor,
        max_bytes: 65536,
      });
      cursor = page.next_cursor;
      text += output(page);
      if (!page.has_more) break;
    }
    expect(text).toBe("界".repeat(30000));
    expect(text).not.toContain("\ufffd");
  });
});
