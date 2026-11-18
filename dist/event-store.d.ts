import { EventEmitter } from "node:events";
import type { EventRead, StoreOptions, TerminalEvent } from "./types.js";
/**
 * One synchronous writer per session directory. Readers have independent cursors.
 * diskBytes counts JSONL bytes, not the small manifest. The newest record is kept
 * even when a retention/cache budget is smaller than it (at most 64 KiB).
 * pruneTo emits `retention_overflow` when that unavoidable exception applies.
 * No automatic recovery from degraded mode: reopen after fixing the disk.
 */
export declare class EventStore extends EventEmitter {
    private readonly directory;
    private readonly manifestFile;
    private readonly segmentLimit;
    private readonly cacheLimit;
    private retentionLimit;
    private manifest;
    private segments;
    private cache;
    private cachedBytes;
    private logBytes;
    private lastSeq;
    private nextSeq;
    private isDegraded;
    private isClosed;
    constructor(options: StoreOptions);
    get cursor(): string;
    get earliestCursor(): string;
    get diskBytes(): number;
    get degraded(): boolean;
    append(type: string, data: Record<string, unknown>): TerminalEvent;
    read(afterCursor?: string, maxBytes?: number): EventRead;
    pruneTo(maxBytes: number): void;
    close(): void;
    private makeRecord;
    private checkRecordSize;
    private accept;
    private addCache;
    private persist;
    private enterDegraded;
    private earliestSequence;
    private checkExpired;
    private encodeCursor;
    private decodeCursor;
    private replay;
    /** Chunked scanner: memory is bounded even for a large configured segment. */
    private scan;
    private reserve;
    private ensureReservation;
    private saveManifest;
    private atomicWrite;
    private syncDirectory;
    private writeAll;
    private parseManifest;
    private validateBytes;
    private assertOpen;
    private reason;
}
