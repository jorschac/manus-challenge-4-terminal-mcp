import { EventEmitter } from "node:events";
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { TerminalError } from "./errors.js";
const DEFAULT_SEGMENT_BYTES = 64 * 1024;
const MAX_RECORD_BYTES = 64 * 1024;
const RESERVATION = 1_048_576;
const READ_CHUNK_BYTES = 16 * 1024;
/**
 * One synchronous writer per session directory. Readers have independent cursors.
 * diskBytes counts JSONL bytes, not the small manifest. The newest record is kept
 * even when a retention/cache budget is smaller than it (at most 64 KiB).
 * pruneTo emits `retention_overflow` when that unavoidable exception applies.
 * No automatic recovery from degraded mode: reopen after fixing the disk.
 */
export class EventStore extends EventEmitter {
    directory;
    manifestFile;
    segmentLimit;
    cacheLimit;
    retentionLimit;
    manifest;
    segments = [];
    cache = [];
    cachedBytes = 0;
    logBytes = 0;
    lastSeq = 0;
    nextSeq = 1;
    isDegraded = false;
    isClosed = false;
    constructor(options) {
        super();
        this.validateBytes(options.maxBytes, "maxBytes", true);
        this.validateBytes(options.cacheBytes, "cacheBytes", true);
        this.validateBytes(options.segmentBytes ?? DEFAULT_SEGMENT_BYTES, "segmentBytes");
        if (!options.sessionId || !options.directory) {
            throw new TerminalError("INVALID_ARGUMENT", "directory and sessionId are required");
        }
        this.directory = path.resolve(options.directory);
        this.manifestFile = path.join(this.directory, "manifest.json");
        this.segmentLimit = options.segmentBytes ?? DEFAULT_SEGMENT_BYTES;
        this.cacheLimit = options.cacheBytes;
        this.retentionLimit = options.maxBytes;
        let previous;
        try {
            fs.mkdirSync(this.directory, { recursive: true, mode: 0o700 });
            fs.chmodSync(this.directory, 0o700);
            if (fs.existsSync(this.manifestFile)) {
                previous = this.parseManifest(fs.readFileSync(this.manifestFile, "utf8"), options.sessionId);
            }
            else if (fs
                .readdirSync(this.directory)
                .some((file) => /^segment-\d+\.jsonl$/.test(file))) {
                throw new TerminalError("STORAGE_CORRUPT", "Logs exist without their cursor/sequence manifest");
            }
            this.manifest = previous ?? {
                version: 1,
                sessionId: options.sessionId,
                nonce: randomBytes(16).toString("hex"),
                secret: randomBytes(32).toString("hex"),
                reservedUntil: 0,
                lastSeq: 0,
                clean: true,
            };
            this.replay();
            const durableLast = this.segments.at(-1)?.last ?? 0;
            this.lastSeq = Math.max(durableLast, this.manifest.lastSeq);
            this.nextSeq = this.manifest.reservedUntil + 1;
            // Reservation is durable BEFORE accepting any events. A disk outage cannot
            // cause acknowledged memory-only sequence numbers to be reused on restart.
            this.reserve();
            if (previous && (!previous.clean || previous.lastSeq > durableLast)) {
                this.lastSeq = Math.max(this.lastSeq, previous.reservedUntil);
                this.append("gap", {
                    reason: "unclean_restart",
                    from_seq: durableLast + 1,
                    through_seq: previous.reservedUntil,
                    may_include_unissued_sequences: true,
                    message: "The prior non-durable tail cannot be recovered; unused reserved sequences are also skipped.",
                });
            }
            this.pruneTo(this.retentionLimit);
        }
        catch (error) {
            if (error instanceof TerminalError)
                throw error;
            // Without an initial durable reservation, safe memory-only acceptance is
            // impossible. Fail explicitly rather than risk sequence reuse on restart.
            throw new TerminalError("STORAGE_UNAVAILABLE", "Cannot initialize a durable event store", {
                reason: this.reason(error),
                directory: this.directory,
            });
        }
    }
    get cursor() {
        return this.encodeCursor(this.lastSeq);
    }
    get earliestCursor() {
        return this.encodeCursor(this.earliestSequence());
    }
    get diskBytes() {
        return this.logBytes;
    }
    get degraded() {
        return this.isDegraded;
    }
    append(type, data) {
        this.assertOpen();
        if (!type ||
            typeof type !== "string" ||
            !data ||
            typeof data !== "object" ||
            Array.isArray(data)) {
            throw new TerminalError("INVALID_ARGUMENT", "An event needs a nonempty type and an object data value");
        }
        // Validate serialization before consuming a sequence or changing storage.
        let snapshot;
        try {
            snapshot = JSON.parse(JSON.stringify(data));
            if (!snapshot || typeof snapshot !== "object" || Array.isArray(snapshot))
                throw new Error("not an object");
        }
        catch (error) {
            throw new TerminalError("INVALID_ARGUMENT", "Event data must be JSON serializable", { reason: this.reason(error) });
        }
        this.checkRecordSize(this.makeRecord(type, snapshot, this.nextSeq));
        this.ensureReservation(3); // failure signals use two extra reserved sequences
        if (!this.isDegraded) {
            const record = this.makeRecord(type, snapshot, this.nextSeq);
            try {
                this.persist(record);
            }
            catch (error) {
                this.enterDegraded(error, "disk_write_failed");
            }
            if (!this.isDegraded) {
                this.nextSeq++;
                this.accept(record);
                this.pruneTo(this.retentionLimit);
                return JSON.parse(record.json);
            }
        }
        const record = this.makeRecord(type, snapshot, this.nextSeq);
        this.nextSeq++;
        this.accept(record);
        return JSON.parse(record.json);
    }
    read(afterCursor, maxBytes = DEFAULT_SEGMENT_BYTES) {
        this.assertOpen();
        this.validateBytes(maxBytes, "maxBytes");
        if (maxBytes < 2) {
            throw new TerminalError("INVALID_ARGUMENT", "maxBytes must fit the serialized empty events array", { minimum_bytes: 2 });
        }
        const after = afterCursor === undefined
            ? this.earliestSequence()
            : this.decodeCursor(afterCursor);
        this.checkExpired(after);
        let events = [];
        let used = 2; // exact UTF-8 size of the serialized events array, including []
        let last = after;
        let more = false;
        const consume = (record) => {
            if (record.seq <= after)
                return true;
            const required = record.bytes - 1 + (events.length ? 1 : 0);
            if (used + required > maxBytes) {
                if (!events.length) {
                    throw new TerminalError("EVENT_TOO_LARGE", "Increase maxBytes to read the next event", {
                        minimum_bytes: record.bytes + 1,
                        max_bytes: maxBytes,
                        after_cursor: this.encodeCursor(after),
                        event_cursor: this.encodeCursor(record.seq),
                    });
                }
                more = true;
                return false;
            }
            events.push(JSON.parse(record.json));
            used += required;
            last = record.seq;
            return true;
        };
        const fromCache = this.isDegraded || after >= (this.cache[0]?.seq ?? Infinity) - 1;
        if (fromCache) {
            for (const record of this.cache)
                if (!consume(record))
                    break;
        }
        else {
            try {
                for (const segment of this.segments) {
                    if (segment.last <= after)
                        continue;
                    if (!this.scan(segment.file, segment.bytes, consume))
                        break;
                }
            }
            catch (error) {
                if (error instanceof TerminalError && error.code === "EVENT_TOO_LARGE")
                    throw error;
                this.enterDegraded(error, "disk_read_failed");
                this.checkExpired(after);
                events = [];
                used = 2;
                last = after;
                more = false;
                for (const record of this.cache)
                    if (!consume(record))
                        break;
            }
        }
        return {
            events,
            next_cursor: this.encodeCursor(more ? last : this.lastSeq),
            earliest_cursor: this.earliestCursor,
            has_more: more,
            storage_degraded: this.isDegraded,
        };
    }
    pruneTo(maxBytes) {
        this.assertOpen();
        this.validateBytes(maxBytes, "maxBytes", true);
        this.retentionLimit = maxBytes;
        if (this.isDegraded)
            return;
        try {
            while (this.logBytes > maxBytes && this.segments.length > 1) {
                const oldest = this.segments[0];
                fs.unlinkSync(oldest.file);
                this.syncDirectory();
                this.segments.shift();
                this.logBytes -= oldest.bytes;
            }
            const segment = this.segments[0];
            if (segment && segment.bytes > maxBytes) {
                // Compact only the last segment; the rolling suffix is bounded by the
                // requested retention limit plus one capped record, never a full index.
                const tail = [];
                let bytes = 0;
                this.scan(segment.file, segment.bytes, (record) => {
                    tail.push(record);
                    bytes += record.bytes;
                    while (tail.length > 1 && bytes > maxBytes)
                        bytes -= tail.shift().bytes;
                    return true;
                });
                if (bytes < segment.bytes) {
                    this.atomicWrite(segment.file, tail.map((record) => record.json + "\n").join(""));
                    this.logBytes += bytes - segment.bytes;
                    segment.bytes = bytes;
                    segment.first = tail[0].seq;
                    segment.last = tail.at(-1).seq;
                }
                if (bytes > maxBytes) {
                    this.emit("retention_overflow", {
                        max_bytes: maxBytes,
                        retained_bytes: bytes,
                        newest_record_retained: true,
                    });
                }
            }
            // Healthy cache must not make already-pruned disk events readable again.
            const earliest = this.segments[0]?.first ?? this.lastSeq + 1;
            while (this.cache.length && this.cache[0].seq < earliest)
                this.cachedBytes -= this.cache.shift().bytes;
        }
        catch (error) {
            this.enterDegraded(error, "disk_prune_failed");
        }
    }
    close() {
        if (this.isClosed)
            return;
        if (!this.isDegraded) {
            try {
                this.saveManifest({
                    ...this.manifest,
                    clean: true,
                    lastSeq: this.lastSeq,
                });
            }
            catch (error) {
                this.enterDegraded(error, "disk_close_failed");
            }
        }
        this.isClosed = true;
    }
    makeRecord(type, data, seq) {
        const json = JSON.stringify({
            seq,
            time: new Date().toISOString(),
            type,
            data,
        });
        const record = { seq, json, bytes: Buffer.byteLength(json) + 1 };
        this.checkRecordSize(record);
        return record;
    }
    checkRecordSize(record) {
        if (record.bytes > MAX_RECORD_BYTES) {
            throw new TerminalError("EVENT_TOO_LARGE", "Split large event data into smaller events", {
                maximum_bytes: MAX_RECORD_BYTES,
                actual_bytes: record.bytes,
            });
        }
    }
    accept(record) {
        this.lastSeq = record.seq;
        this.addCache(record);
        this.emit("append", JSON.parse(record.json));
    }
    addCache(record) {
        this.cache.push(record);
        this.cachedBytes += record.bytes;
        while (this.cache.length > 1 && this.cachedBytes > this.cacheLimit)
            this.cachedBytes -= this.cache.shift().bytes;
    }
    persist(record) {
        let segment = this.segments.at(-1);
        let newSegment = false;
        if (!segment || segment.bytes + record.bytes > this.segmentLimit) {
            segment = {
                file: path.join(this.directory, `segment-${String(record.seq).padStart(16, "0")}.jsonl`),
                bytes: 0,
                first: record.seq,
                last: record.seq,
            };
            newSegment = true;
        }
        const fd = fs.openSync(segment.file, newSegment ? "wx" : "r+", 0o600);
        try {
            fs.fchmodSync(fd, 0o600);
            this.writeAll(fd, Buffer.from(record.json + "\n"), segment.bytes);
            fs.fsyncSync(fd);
            if (newSegment)
                this.syncDirectory();
        }
        catch (error) {
            // Ignore partial writes even when rollback itself fails. Scans use only
            // acknowledged byte lengths; replay reports a gap for an unclean tail.
            try {
                fs.ftruncateSync(fd, segment.bytes);
            }
            catch {
                /* disk unavailable */
            }
            throw error;
        }
        finally {
            fs.closeSync(fd);
        }
        if (newSegment)
            this.segments.push(segment);
        segment.bytes += record.bytes;
        segment.last = record.seq;
        this.logBytes += record.bytes;
    }
    enterDegraded(error, reason) {
        if (this.isDegraded)
            return;
        this.isDegraded = true;
        const durableThrough = this.segments.at(-1)?.last ?? 0;
        const omittedThrough = (this.cache[0]?.seq ?? this.lastSeq + 1) - 1;
        // Two already-reserved control records, also subject to the bounded cache.
        const gap = this.makeRecord("gap", {
            reason,
            durable_through_seq: durableThrough,
            unavailable_through_seq: omittedThrough,
            durability_lost: true,
            message: "Reads now use bounded memory; older cursors may have expired.",
        }, this.nextSeq++);
        this.accept(gap);
        const info = this.makeRecord("storage_degraded", {
            reason: this.reason(error).slice(0, 2048),
            mode: "bounded_memory",
            cache_bytes: this.cacheLimit,
            maximum_record_bytes: MAX_RECORD_BYTES,
            restart_required: true,
        }, this.nextSeq++);
        this.accept(info);
    }
    earliestSequence() {
        const first = this.isDegraded
            ? this.cache[0]?.seq
            : this.segments[0]?.first;
        return first === undefined ? this.lastSeq : first - 1;
    }
    checkExpired(after) {
        if (after < this.earliestSequence()) {
            throw new TerminalError("CURSOR_EXPIRED", "The requested history has been evicted or become unavailable", {
                earliest_cursor: this.earliestCursor,
                storage_degraded: this.isDegraded,
            });
        }
    }
    encodeCursor(seq) {
        const payload = Buffer.from(JSON.stringify([this.manifest.nonce, seq])).toString("base64url");
        const signature = createHmac("sha256", this.manifest.secret)
            .update(payload)
            .digest("base64url");
        return `v1.${payload}.${signature}`;
    }
    decodeCursor(cursor) {
        if (typeof cursor !== "string" || cursor.length > 512)
            throw new TerminalError("INVALID_CURSOR", "Malformed cursor");
        const parts = cursor.split(".");
        if (parts.length !== 3 ||
            parts[0] !== "v1" ||
            !/^[A-Za-z0-9_-]+$/.test(parts[1])) {
            throw new TerminalError("INVALID_CURSOR", "Malformed cursor");
        }
        let decoded;
        try {
            decoded = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
        }
        catch {
            throw new TerminalError("INVALID_CURSOR", "Malformed cursor payload");
        }
        if (!Array.isArray(decoded) ||
            decoded.length !== 2 ||
            typeof decoded[0] !== "string" ||
            !Number.isSafeInteger(decoded[1]) ||
            decoded[1] < 0) {
            throw new TerminalError("INVALID_CURSOR", "Invalid cursor sequence");
        }
        if (decoded[0] !== this.manifest.nonce)
            throw new TerminalError("FOREIGN_CURSOR", "Cursor belongs to another session");
        const expected = createHmac("sha256", this.manifest.secret)
            .update(parts[1])
            .digest("base64url");
        const actual = parts[2];
        if (!/^[A-Za-z0-9_-]+$/.test(actual) ||
            actual.length !== expected.length ||
            !timingSafeEqual(Buffer.from(actual), Buffer.from(expected))) {
            throw new TerminalError("INVALID_CURSOR", "Invalid cursor signature");
        }
        const seq = decoded[1];
        if (seq > this.lastSeq)
            throw new TerminalError("CURSOR_FUTURE", "Cursor is ahead of this store", { latest_cursor: this.cursor });
        return seq;
    }
    replay() {
        const names = fs
            .readdirSync(this.directory)
            .filter((name) => /^segment-\d{16}\.jsonl$/.test(name))
            .sort();
        let previous = 0;
        for (const name of names) {
            const file = path.join(this.directory, name);
            fs.chmodSync(file, 0o600);
            const size = fs.statSync(file).size;
            let first = 0;
            let last = 0;
            let bytes = 0;
            this.scan(file, size, (record) => {
                if (record.seq <= previous ||
                    record.seq > this.manifest.reservedUntil) {
                    throw new TerminalError("STORAGE_CORRUPT", "Log sequences are not ordered or reserved", { file });
                }
                first ||= record.seq;
                last = record.seq;
                previous = last;
                bytes += record.bytes;
                this.addCache(record);
                return true;
            }, true);
            // Only an incomplete final line is recoverable. Complete invalid records
            // fail explicitly; the unclean-restart gap covers a discarded partial tail.
            if (bytes !== size)
                fs.truncateSync(file, bytes);
            if (!bytes) {
                fs.unlinkSync(file);
                continue;
            }
            this.segments.push({ file, bytes, first, last });
            this.logBytes += bytes;
        }
    }
    /** Chunked scanner: memory is bounded even for a large configured segment. */
    scan(file, limit, visit, allowPartial = false) {
        const fd = fs.openSync(file, "r");
        const buffer = Buffer.alloc(READ_CHUNK_BYTES);
        let position = 0;
        let pending = Buffer.alloc(0);
        try {
            while (position < limit) {
                const count = fs.readSync(fd, buffer, 0, Math.min(buffer.length, limit - position), position);
                if (!count)
                    throw new TerminalError("STORAGE_CORRUPT", "Log was truncated while reading", { file });
                position += count;
                pending = Buffer.concat([pending, buffer.subarray(0, count)]);
                let newline;
                while ((newline = pending.indexOf(10)) !== -1) {
                    const line = pending.subarray(0, newline);
                    if (line.length + 1 > MAX_RECORD_BYTES)
                        throw new TerminalError("STORAGE_CORRUPT", "Oversized log record", {
                            file,
                        });
                    const json = line.toString("utf8");
                    if (!line.equals(Buffer.from(json)))
                        throw new TerminalError("STORAGE_CORRUPT", "Invalid UTF-8 log record", { file });
                    let event;
                    try {
                        event = JSON.parse(json);
                    }
                    catch {
                        throw new TerminalError("STORAGE_CORRUPT", "Invalid JSONL record", {
                            file,
                        });
                    }
                    if (!event ||
                        !Number.isSafeInteger(event.seq) ||
                        event.seq <= 0 ||
                        typeof event.time !== "string" ||
                        typeof event.type !== "string" ||
                        !event.data ||
                        typeof event.data !== "object" ||
                        Array.isArray(event.data)) {
                        throw new TerminalError("STORAGE_CORRUPT", "Invalid event record", {
                            file,
                        });
                    }
                    if (!visit({ seq: event.seq, json, bytes: line.length + 1 }))
                        return false;
                    pending = pending.subarray(newline + 1);
                }
                if (pending.length >= MAX_RECORD_BYTES)
                    throw new TerminalError("STORAGE_CORRUPT", "Oversized/incomplete log record", { file });
            }
            if (pending.length && !allowPartial)
                throw new TerminalError("STORAGE_CORRUPT", "Incomplete log record", {
                    file,
                });
            return true;
        }
        finally {
            fs.closeSync(fd);
        }
    }
    reserve() {
        const reservedUntil = this.nextSeq + RESERVATION - 1;
        if (!Number.isSafeInteger(reservedUntil))
            throw new TerminalError("SEQUENCE_EXHAUSTED", "Event sequence space exhausted");
        this.saveManifest({
            ...this.manifest,
            reservedUntil,
            lastSeq: this.lastSeq,
            clean: false,
        });
    }
    ensureReservation(count) {
        if (this.nextSeq + count - 1 <= this.manifest.reservedUntil)
            return;
        try {
            this.reserve();
        }
        catch (error) {
            throw new TerminalError("STORAGE_UNAVAILABLE", "Durable sequence reservation exhausted; repair storage before appending", {
                reason: this.reason(error),
                latest_cursor: this.cursor,
            });
        }
    }
    saveManifest(manifest) {
        this.atomicWrite(this.manifestFile, JSON.stringify(manifest) + "\n");
        this.manifest = manifest;
    }
    atomicWrite(file, content) {
        const temporary = file + ".tmp";
        let fd;
        try {
            fd = fs.openSync(temporary, "w", 0o600);
            fs.fchmodSync(fd, 0o600);
            this.writeAll(fd, Buffer.from(content), 0);
            fs.fsyncSync(fd);
            fs.closeSync(fd);
            fd = undefined;
            fs.renameSync(temporary, file);
            this.syncDirectory();
        }
        finally {
            if (fd !== undefined)
                fs.closeSync(fd);
            try {
                fs.unlinkSync(temporary);
            }
            catch {
                /* already renamed or inaccessible */
            }
        }
    }
    syncDirectory() {
        const fd = fs.openSync(this.directory, "r");
        try {
            fs.fsyncSync(fd);
        }
        finally {
            fs.closeSync(fd);
        }
    }
    writeAll(fd, buffer, position) {
        let written = 0;
        while (written < buffer.length) {
            const count = fs.writeSync(fd, buffer, written, buffer.length - written, position + written);
            if (count <= 0)
                throw new Error("Storage write made no progress");
            written += count;
        }
    }
    parseManifest(json, sessionId) {
        let value;
        try {
            value = JSON.parse(json);
        }
        catch {
            throw new TerminalError("STORAGE_CORRUPT", "Invalid manifest JSON");
        }
        if (!value ||
            value.version !== 1 ||
            typeof value.sessionId !== "string" ||
            typeof value.nonce !== "string" ||
            !/^[a-f0-9]{32}$/.test(value.nonce) ||
            typeof value.secret !== "string" ||
            !/^[a-f0-9]{64}$/.test(value.secret) ||
            !Number.isSafeInteger(value.reservedUntil) ||
            value.reservedUntil < 0 ||
            !Number.isSafeInteger(value.lastSeq) ||
            value.lastSeq < 0 ||
            value.lastSeq > value.reservedUntil ||
            typeof value.clean !== "boolean")
            throw new TerminalError("STORAGE_CORRUPT", "Invalid manifest fields");
        if (value.sessionId !== sessionId)
            throw new TerminalError("FOREIGN_SESSION", "Directory belongs to another session");
        return value;
    }
    validateBytes(value, name, allowZero = false) {
        if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)) {
            throw new TerminalError("INVALID_ARGUMENT", `${name} must be a ${allowZero ? "nonnegative" : "positive"} safe integer`);
        }
    }
    assertOpen() {
        if (this.isClosed)
            throw new TerminalError("STORE_CLOSED", "Event store is closed");
    }
    reason(error) {
        return error instanceof Error ? error.message : String(error);
    }
}
//# sourceMappingURL=event-store.js.map