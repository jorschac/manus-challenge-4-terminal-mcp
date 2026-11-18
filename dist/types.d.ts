export type SessionState = "STARTING" | "IDLE" | "BUSY" | "UNMANAGED" | "CLOSING" | "EXITED" | "LOST";
export type CommandState = "ACCEPTED" | "RUNNING" | "COMPLETED" | "UNKNOWN";
export type Access = "read" | "control";
export interface TerminalEvent {
    seq: number;
    time: string;
    type: string;
    data: Record<string, unknown>;
}
export interface EventRead {
    events: TerminalEvent[];
    next_cursor: string;
    earliest_cursor: string;
    has_more: boolean;
    storage_degraded: boolean;
}
export interface CommandRecord {
    command_id: string;
    request_key: string;
    fingerprint: string;
    state: CommandState;
    accepted_at: string;
    started_at?: string;
    completed_at?: string;
    exit_code: number | null;
    start_cursor: string;
    end_cursor?: string;
    output_boundary_seen: boolean;
    completion_seen: boolean;
    reason?: string;
}
export interface SessionRecord {
    session_id: string;
    shell_pid: number;
    process_identity: string;
    tty: string;
    cwd: string;
    cols: number;
    rows: number;
    state: SessionState;
    created_at: string;
    exited_at?: string;
    shell_exit_code?: number;
    commands: CommandRecord[];
}
export interface BrokerConfig {
    runtimeDir: string;
    dataDir: string;
    socketPath: string;
    controlSocketPath: string;
    maxSessions: number;
    cacheBytes: number;
    sessionLogBytes: number;
    totalLogBytes: number;
    maxRetainedSessions: number;
    retentionMs: number;
    disconnectGraceMs: number;
    linkTtlMs: number;
    idleExitMs: number;
}
export interface StoreOptions {
    directory: string;
    sessionId: string;
    maxBytes: number;
    cacheBytes: number;
    segmentBytes?: number;
}
export interface WireRequest {
    id: string;
    method: string;
    params: Record<string, unknown>;
}
export interface WireResponse {
    id: string;
    result?: unknown;
    error?: {
        code: string;
        message: string;
        details?: unknown;
    };
}
export interface WireNotification {
    notification: "session_changed";
    session_id: string;
}
