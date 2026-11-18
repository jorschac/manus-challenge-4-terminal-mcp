import { afterEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";
import { createHmac } from "node:crypto";
import { EventStore } from "../src/event-store.js";
import { TerminalError } from "../src/errors.js";
import type { StoreOptions, TerminalEvent } from "../src/types.js";

const directories: string[] = [];
const stores: EventStore[] = [];
function directory(): string {
  const result = fs.mkdtempSync("/tmp/event-store-test-");
  directories.push(result);
  return result;
}
function create(overrides: Partial<StoreOptions> = {}): EventStore {
  const store = new EventStore({
    directory: directory(),
    sessionId: "session-a",
    maxBytes: 1024 * 1024,
    cacheBytes: 4096,
    ...overrides,
  });
  stores.push(store);
  return store;
}
function errorFrom(operation: () => unknown): TerminalError {
  try {
    operation();
  } catch (error) {
    expect(error).toBeInstanceOf(TerminalError);
    return error as TerminalError;
  }
  throw new Error("Expected a TerminalError");
}
function signedCursor(dir: string, seq: number): string {
  const manifest = JSON.parse(
    fs.readFileSync(path.join(dir, "manifest.json"), "utf8"),
  );
  const payload = Buffer.from(JSON.stringify([manifest.nonce, seq])).toString(
    "base64url",
  );
  return `v1.${payload}.${createHmac("sha256", manifest.secret).update(payload).digest("base64url")}`;
}

afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.close();
  for (const dir of directories.splice(0))
    fs.rmSync(dir, { recursive: true, force: true });
});

describe("EventStore", () => {
  it("appends, emits immutable snapshots, and replays from an initial cursor", () => {
    const store = create();
    const initial = store.cursor;
    const emitted: TerminalEvent[] = [];
    store.on("append", (event) => {
      emitted.push(event);
      event.data.text = "listener changed";
    });
    const data = { text: "hello" };
    const event = store.append("output", data);
    data.text = "caller changed";
    event.data.text = "return changed";
    const read = store.read(initial);
    expect(read.events).toHaveLength(1);
    expect(read.events[0]).toMatchObject({
      seq: 1,
      type: "output",
      data: { text: "hello" },
    });
    expect(emitted).toHaveLength(1);
    expect(read.events[0]!.time).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    read.events[0]!.data.text = "reader changed";
    expect(store.read(initial).events[0]!.data.text).toBe("hello");
    expect(store.read(read.next_cursor).events).toEqual([]);
    expect(read.next_cursor).toBe(store.cursor);
    expect(read.storage_degraded).toBe(false);
  });

  it("supports independent readers and repeatedly reusing the same cursor", () => {
    const store = create();
    const start = store.cursor;
    const first = store.append("output", { text: "one" });
    const one = store.cursor;
    store.append("output", { text: "two" });
    const firstRead = store.read(start);
    expect(store.read(start)).toEqual(firstRead);
    expect(firstRead.events.map((event) => event.seq)).toEqual([
      first.seq,
      first.seq + 1,
    ]);
    expect(store.read(one).events.map((event) => event.data.text)).toEqual([
      "two",
    ]);
    expect(store.read().events).toEqual(firstRead.events);
  });

  it("rotates segments at the configured size and applies private filesystem modes", () => {
    const dir = directory();
    const store = create({ directory: dir, segmentBytes: 512 });
    for (let index = 0; index < 12; index++)
      store.append("output", { text: "x".repeat(200), index });
    const logs = fs.readdirSync(dir).filter((file) => file.endsWith(".jsonl"));
    expect(logs.length).toBeGreaterThan(1);
    expect(fs.statSync(dir).mode & 0o777).toBe(0o700);
    for (const file of fs.readdirSync(dir))
      expect(fs.statSync(path.join(dir, file)).mode & 0o777).toBe(0o600);
    expect(store.diskBytes).toBe(
      logs.reduce(
        (sum, file) => sum + fs.statSync(path.join(dir, file)).size,
        0,
      ),
    );
    expect(store.read().events).toHaveLength(12);
  });

  it("defaults to 64 KiB segments without a per-event permanent index", () => {
    const dir = directory();
    const store = create({ directory: dir, cacheBytes: 1000 });
    for (let index = 0; index < 90; index++)
      store.append("output", { text: "x".repeat(1800), index });
    const files = fs.readdirSync(dir).filter((file) => file.endsWith(".jsonl"));
    expect(files.length).toBeGreaterThan(1);
    for (const file of files)
      expect(fs.statSync(path.join(dir, file)).size).toBeLessThanOrEqual(
        64 * 1024,
      );
    const internals = store as unknown as {
      cache: unknown[];
      cachedBytes: number;
      segments: unknown[];
    };
    expect(internals.cache).toHaveLength(1); // one oversize latest record is the explicit exception
    expect(internals.cachedBytes).toBeLessThanOrEqual(64 * 1024);
    expect(internals.segments.length).toBe(files.length);
    const result = store.read(undefined, 1024 * 1024);
    expect(result.events).toHaveLength(90); // replay comes from disk, not just the small cache
  });

  it("preserves cursor identity and records on clean restart and never reuses sequences", () => {
    const dir = directory();
    const first = create({ directory: dir });
    const start = first.cursor;
    first.append("output", { text: "first" });
    const tail = first.append("output", { text: "second" });
    const tailCursor = first.cursor;
    first.close();
    const second = create({ directory: dir });
    expect(second.read(start).events.map((event) => event.data.text)).toEqual([
      "first",
      "second",
    ]);
    expect(second.read(tailCursor).events).toEqual([]);
    expect(second.cursor).toBe(tailCursor);
    const next = second.append("output", { text: "third" });
    expect(next.seq).toBeGreaterThan(tail.seq);
    expect(second.read(tailCursor).events).toEqual([next]);
  });

  it("compacts the final segment and expires evicted cursors with an actionable boundary", () => {
    const store = create({ segmentBytes: 64 * 1024 });
    const initial = store.cursor;
    for (let index = 0; index < 10; index++)
      store.append("output", { text: "x".repeat(200), index });
    store.pruneTo(700);
    expect(store.diskBytes).toBeLessThanOrEqual(700);
    const error = errorFrom(() => store.read(initial));
    expect(error.code).toBe("CURSOR_EXPIRED");
    expect(error.details).toMatchObject({
      earliest_cursor: store.earliestCursor,
    });
    const retained = store.read(store.earliestCursor).events;
    expect(retained.at(-1)!.data.index).toBe(9);
    expect(retained.length).toBeLessThan(10);
    expect(errorFrom(() => store.read(initial)).code).toBe("CURSOR_EXPIRED");
  });

  it("retains the latest record even at a zero retention limit, and explicitly signals overflow", () => {
    const store = create();
    store.append("output", { text: "old" });
    const latest = store.append("output", { text: "latest" });
    const overflow = vi.fn();
    store.on("retention_overflow", overflow);
    store.pruneTo(0);
    expect(store.read().events).toEqual([latest]);
    expect(store.diskBytes).toBeGreaterThan(0);
    expect(overflow).toHaveBeenCalledWith(
      expect.objectContaining({ max_bytes: 0, newest_record_retained: true }),
    );
    store.append("output", { text: "new latest" });
    expect(store.read().events.map((event) => event.data.text)).toEqual([
      "new latest",
    ]);
  });

  it("prunes full segments, survives restart, and does not resurrect evicted cached events", () => {
    const dir = directory();
    const first = create({
      directory: dir,
      segmentBytes: 400,
      maxBytes: 800,
      cacheBytes: 8192,
    });
    const initial = first.cursor;
    for (let index = 0; index < 25; index++)
      first.append("output", { text: "x".repeat(180), index });
    expect(first.diskBytes).toBeLessThanOrEqual(800);
    const retained = first.read().events;
    expect(retained.at(-1)!.data.index).toBe(24);
    expect(errorFrom(() => first.read(initial)).code).toBe("CURSOR_EXPIRED");
    const earliest = first.earliestCursor;
    first.close();
    const second = create({ directory: dir, segmentBytes: 400, maxBytes: 800 });
    expect(second.earliestCursor).toBe(earliest);
    expect(second.read(earliest).events).toEqual(retained);
    expect(errorFrom(() => second.read(initial)).code).toBe("CURSOR_EXPIRED");
  });

  it("rejects foreign, malformed, altered, and future cursors without consuming history", () => {
    const dir = directory();
    const store = create({ directory: dir });
    const foreign = create({ sessionId: "session-b" });
    store.append("output", { text: "data" });
    expect(errorFrom(() => store.read(foreign.cursor)).code).toBe(
      "FOREIGN_CURSOR",
    );
    for (const invalid of [
      "",
      "not-a-cursor",
      "v1.e30.fake",
      store.cursor + "x",
    ]) {
      expect(errorFrom(() => store.read(invalid)).code).toBe("INVALID_CURSOR");
    }
    const future = signedCursor(dir, 500);
    expect(errorFrom(() => store.read(future)).code).toBe("CURSOR_FUTURE");
    expect(store.read().events).toHaveLength(1);
  });

  it("rejects reuse of a session directory for a foreign session", () => {
    const dir = directory();
    const first = create({ directory: dir });
    first.close();
    expect(
      errorFrom(() => create({ directory: dir, sessionId: "other" })).code,
    ).toBe("FOREIGN_SESSION");
  });

  it("enforces an exact UTF-8 serialized array budget and paginates 2 KiB output chunks", () => {
    const store = create();
    const start = store.cursor;
    const one = store.append("output", { text: "x".repeat(2048) });
    store.append("output", { text: "终".repeat(600) });
    const budget = Buffer.byteLength(JSON.stringify([one]));
    const firstPage = store.read(start, budget);
    expect(firstPage.events).toEqual([one]);
    expect(firstPage.has_more).toBe(true);
    expect(
      Buffer.byteLength(JSON.stringify(firstPage.events)),
    ).toBeLessThanOrEqual(budget);
    const secondPage = store.read(firstPage.next_cursor, budget);
    expect(secondPage.events).toHaveLength(1);
    expect(secondPage.has_more).toBe(false);
    expect(
      Buffer.byteLength(JSON.stringify(secondPage.events)),
    ).toBeLessThanOrEqual(budget);
    expect(store.read(start, budget)).toEqual(firstPage);
  });

  it("fails actionably instead of looping if the next event exceeds the read budget", () => {
    const store = create();
    const start = store.cursor;
    const event = store.append("output", { text: "🙂".repeat(1000) });
    const required = Buffer.byteLength(JSON.stringify([event]));
    const error = errorFrom(() => store.read(start, required - 1));
    expect(error.code).toBe("EVENT_TOO_LARGE");
    expect(error.details).toMatchObject({
      minimum_bytes: required,
      after_cursor: start,
    });
    expect(store.read(start, required).events).toEqual([event]);
  });

  it("caps individual records and rejects invalid arguments without consuming sequence numbers", () => {
    const store = create();
    const cursor = store.cursor;
    expect(
      errorFrom(() => store.append("output", { text: "x".repeat(64 * 1024) }))
        .code,
    ).toBe("EVENT_TOO_LARGE");
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(errorFrom(() => store.append("output", cyclic)).code).toBe(
      "INVALID_ARGUMENT",
    );
    for (const budget of [-1, 0, 1, NaN, Infinity, 1.5]) {
      expect(errorFrom(() => store.read(undefined, budget)).code).toBe(
        "INVALID_ARGUMENT",
      );
    }
    expect(errorFrom(() => store.pruneTo(-1)).code).toBe("INVALID_ARGUMENT");
    expect(store.cursor).toBe(cursor);
    expect(store.append("output", { text: "okay" }).seq).toBe(1);
  });

  it("handles short writes rather than silently persisting truncated records", () => {
    const store = create();
    const actual = fs.writeSync;
    vi.spyOn(fs, "writeSync").mockImplementation(((
      fd: number,
      buffer: Buffer,
      offset: number,
      length: number,
      position: number,
    ) =>
      actual(
        fd,
        buffer,
        offset,
        Math.min(length, 7),
        position,
      )) as typeof fs.writeSync);
    const event = store.append("output", { text: "x".repeat(2000) });
    expect(store.read().events).toEqual([event]);
    expect(store.degraded).toBe(false);
  });

  it("degrades on write failure, emits gap/storage information, and bounds memory", () => {
    const store = create({ cacheBytes: 1024 });
    const oldCursor = store.cursor;
    for (let index = 0; index < 8; index++)
      store.append("output", { text: "x".repeat(700), index });
    const beforeBytes = store.diskBytes;
    const emitted: TerminalEvent[] = [];
    store.on("append", (event) => emitted.push(event));
    vi.spyOn(fs, "writeSync").mockImplementationOnce(() => {
      throw new Error("injected ENOSPC");
    });
    const event = store.append("output", { text: "memory only" });
    expect(store.degraded).toBe(true);
    expect(emitted.map((item) => item.type)).toEqual([
      "gap",
      "storage_degraded",
      "output",
    ]);
    expect(emitted.at(-1)).toEqual(event);
    expect(store.diskBytes).toBe(beforeBytes);
    expect(errorFrom(() => store.read(oldCursor)).details).toMatchObject({
      earliest_cursor: store.earliestCursor,
    });
    expect(store.read().storage_degraded).toBe(true);
    const memoryCursor = store.cursor;
    for (let index = 0; index < 100; index++)
      store.append("output", { text: "x".repeat(700), index });
    expect(errorFrom(() => store.read(memoryCursor)).code).toBe(
      "CURSOR_EXPIRED",
    );
    const internal = store as unknown as {
      cachedBytes: number;
      cache: unknown[];
    };
    expect(internal.cachedBytes).toBeLessThanOrEqual(1024);
    expect(internal.cache.length).toBeLessThan(3);
    expect(store.read().events.at(-1)!.data.index).toBe(99);
    expect(store.diskBytes).toBe(beforeBytes);
  });

  it("detects a real inaccessible-directory fault without silently losing history", () => {
    const dir = directory();
    const moved = directory();
    fs.rmdirSync(moved);
    const store = create({ directory: dir, cacheBytes: 512 });
    const initial = store.cursor;
    for (let index = 0; index < 10; index++)
      store.append("output", { text: "x".repeat(300), index });
    fs.renameSync(dir, moved);
    const event = store.append("output", { text: "after directory moved" });
    expect(store.degraded).toBe(true);
    expect(store.read().events.at(-1)).toEqual(event);
    expect(errorFrom(() => store.read(initial)).code).toBe("CURSOR_EXPIRED");
    store.close();
    fs.renameSync(moved, dir);
  });

  it("degrades on a disk-read failure and gives expired readers the new earliest cursor", () => {
    const store = create({ cacheBytes: 600 });
    const initial = store.cursor;
    for (let index = 0; index < 8; index++)
      store.append("output", { text: "x".repeat(350), index });
    vi.spyOn(fs, "readSync").mockImplementationOnce(() => {
      throw new Error("injected EIO");
    });
    const error = errorFrom(() => store.read(initial));
    expect(error.code).toBe("CURSOR_EXPIRED");
    expect(error.details).toMatchObject({
      earliest_cursor: store.earliestCursor,
      storage_degraded: true,
    });
    expect(store.degraded).toBe(true);
    expect(store.read().storage_degraded).toBe(true);
  });

  it("reports the uncertain non-durable tail on restart and never reuses its sequence numbers", () => {
    const dir = directory();
    const first = create({ directory: dir, cacheBytes: 8192 });
    const initial = first.cursor;
    first.append("output", { text: "durable" });
    vi.spyOn(fs, "writeSync").mockImplementationOnce(() => {
      throw new Error("injected ENOSPC");
    });
    const memoryEvent = first.append("output", { text: "lost on restart" });
    const memoryCursor = first.cursor;
    first.close();
    vi.restoreAllMocks();
    const second = create({ directory: dir, cacheBytes: 8192 });
    const replay = second.read(initial);
    expect(replay.events.some((event) => event.data.text === "durable")).toBe(
      true,
    );
    expect(
      replay.events.some((event) => event.data.text === "lost on restart"),
    ).toBe(false);
    const gap = replay.events.find((event) => event.type === "gap");
    expect(gap?.data.reason).toBe("unclean_restart");
    expect(
      second.read(memoryCursor).events.some((event) => event.type === "gap"),
    ).toBe(true);
    expect(second.append("output", { text: "new" }).seq).toBeGreaterThan(
      memoryEvent.seq,
    );
    expect(second.degraded).toBe(false);
  });

  it("recovers an incomplete final line, while rejecting complete corrupt JSON records", () => {
    const dir = directory();
    const first = create({ directory: dir });
    const event = first.append("output", { text: "durable" });
    first.close();
    const file = path.join(
      dir,
      fs.readdirSync(dir).find((file) => file.endsWith(".jsonl"))!,
    );
    fs.appendFileSync(file, '{"seq":');
    const second = create({ directory: dir });
    expect(second.read().events).toEqual([event]);
    second.close();
    fs.appendFileSync(file, "broken-json\n");
    expect(errorFrom(() => create({ directory: dir })).code).toBe(
      "STORAGE_CORRUPT",
    );
  });

  it("fails explicitly if even initial durable sequence reservation is impossible", () => {
    const dir = directory();
    vi.spyOn(fs, "renameSync").mockImplementationOnce(() => {
      throw new Error("injected EACCES");
    });
    expect(errorFrom(() => create({ directory: dir })).code).toBe(
      "STORAGE_UNAVAILABLE",
    );
  });

  it("degrades if retention cannot delete a segment, without silently returning incomplete disk history", () => {
    const store = create({ segmentBytes: 400, cacheBytes: 600 });
    const initial = store.cursor;
    for (let index = 0; index < 10; index++)
      store.append("output", { text: "x".repeat(250), index });
    const emitted: TerminalEvent[] = [];
    store.on("append", (event) => emitted.push(event));
    vi.spyOn(fs, "unlinkSync").mockImplementationOnce(() => {
      throw new Error("injected EACCES");
    });
    store.pruneTo(0);
    expect(store.degraded).toBe(true);
    expect(emitted.map((event) => event.type)).toEqual([
      "gap",
      "storage_degraded",
    ]);
    expect(errorFrom(() => store.read(initial)).code).toBe("CURSOR_EXPIRED");
    expect(store.read().storage_degraded).toBe(true);
  });

  it("rejects invalid UTF-8 on replay rather than miscounting the serialized read budget", () => {
    const dir = directory();
    const first = create({ directory: dir });
    first.append("output", { text: "valid" });
    first.close();
    const file = path.join(
      dir,
      fs.readdirSync(dir).find((file) => file.endsWith(".jsonl"))!,
    );
    fs.appendFileSync(
      file,
      Buffer.concat([
        Buffer.from('{"seq":2,"time":"now","type":"output","data":{"text":"'),
        Buffer.from([0xff]),
        Buffer.from('"}}\n'),
      ]),
    );
    expect(errorFrom(() => create({ directory: dir })).code).toBe(
      "STORAGE_CORRUPT",
    );
  });

  it("closes idempotently and rejects use after closing", () => {
    const store = create();
    store.close();
    store.close();
    expect(errorFrom(() => store.append("output", {})).code).toBe(
      "STORE_CLOSED",
    );
    expect(errorFrom(() => store.read()).code).toBe("STORE_CLOSED");
    expect(errorFrom(() => store.pruneTo(100)).code).toBe("STORE_CLOSED");
  });
});
