import { execFileSync } from "node:child_process";
import {
  openSync,
  closeSync,
  writeFileSync,
  fsyncSync,
  renameSync,
  lstatSync,
  unlinkSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { TerminalError } from "./errors.js";

export const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));
export function atomicJson(path: string, value: unknown): void {
  const temp = `${path}.${randomUUID()}.tmp`;
  const fd = openSync(temp, "wx", 0o600);
  try {
    writeFileSync(fd, JSON.stringify(value));
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(temp, path);
}
export function removeOwnedSocket(path: string): void {
  try {
    const st = lstatSync(path);
    if (!st.isSocket() || st.uid !== process.getuid?.() || st.isSymbolicLink())
      throw new TerminalError(
        "UNSAFE_SOCKET",
        `Refusing to remove non-owned socket: ${path}`,
      );
    unlinkSync(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}
export interface ProcessInfo {
  pid: number;
  ppid: number;
  pgid: number;
  uid: number;
  tty: string;
  identity: string;
}
export function processTable(): ProcessInfo[] {
  const text = execFileSync(
    "/bin/ps",
    ["-axo", "pid=,ppid=,pgid=,uid=,lstart=,tty="],
    { encoding: "utf8", env: { ...process.env, LC_ALL: "C" }, timeout: 3000 },
  );
  const rows: ProcessInfo[] = [];
  for (const line of text.split("\n")) {
    const m = line
      .trim()
      .match(
        /^(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\w+\s+\w+\s+\d+\s+[\d:]+\s+\d+)\s+(\S+)$/,
      );
    if (m)
      rows.push({
        pid: +m[1]!,
        ppid: +m[2]!,
        pgid: +m[3]!,
        uid: +m[4]!,
        identity: `${m[4]}:${m[5]}`,
        tty: m[6]!,
      });
  }
  return rows;
}
export function processIdentity(pid: number): string | undefined {
  return processTable().find((p) => p.pid === pid)?.identity;
}
export function signalVerified(
  target: ProcessInfo,
  signal: NodeJS.Signals,
): boolean {
  const now = processTable().find((p) => p.pid === target.pid);
  if (
    !now ||
    now.uid !== process.getuid?.() ||
    now.identity !== target.identity
  )
    return false;
  try {
    process.kill(target.pid, signal);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}
export function managedProcesses(
  pid: number,
  identity: string,
  tty: string,
): ProcessInfo[] {
  const rows = processTable();
  const root = rows.find((p) => p.pid === pid);
  const uid = process.getuid?.();
  // Once the root is gone a TTY name may be recycled. Only previously captured
  // PID identities may be signalled; never discover new targets from that name.
  if (!root || root.identity !== identity || root.uid !== uid) return [];
  const owned = new Set<number>([pid]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const p of rows)
      if (p.uid === uid && owned.has(p.ppid) && !owned.has(p.pid)) {
        owned.add(p.pid);
        changed = true;
      }
  }
  const normalized = tty.replace("/dev/", "");
  return rows.filter(
    (p) =>
      p.uid === uid &&
      (owned.has(p.pid) ||
        (normalized && normalized !== "??" && p.tty === normalized)),
  );
}
export async function terminateProcesses(
  pid: number,
  identity: string,
  tty: string,
  graceMs: number,
) {
  const targets = new Map<number, ProcessInfo>();
  const capture = () => {
    for (const p of managedProcesses(pid, identity, tty)) targets.set(p.pid, p);
  };
  capture();
  const signaled: number[] = [];
  for (const p of [...targets.values()].sort(
    (a, b) => (a.pid === pid ? 1 : 0) - (b.pid === pid ? 1 : 0),
  )) {
    if (signalVerified(p, "SIGTERM")) signaled.push(p.pid);
  }
  const deadline = Date.now() + graceMs;
  while (Date.now() < deadline) {
    capture();
    const live = processTable();
    if (
      ![...targets.values()].some((p) =>
        live.some((q) => q.pid === p.pid && q.identity === p.identity),
      )
    )
      break;
    await sleep(Math.min(50, Math.max(0, deadline - Date.now())));
  }
  capture();
  const killed: number[] = [];
  for (const p of targets.values())
    if (signalVerified(p, "SIGKILL")) killed.push(p.pid);
  await sleep(50);
  const rows = processTable();
  return {
    signaled_pids: signaled,
    killed_pids: killed,
    remaining_pids: [...targets.values()]
      .filter((p) =>
        rows.some((q) => q.pid === p.pid && q.identity === p.identity),
      )
      .map((p) => p.pid),
  };
}
export function quoteShell(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}
