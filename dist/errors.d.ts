export declare class TerminalError extends Error {
    readonly code: string;
    readonly details?: unknown | undefined;
    constructor(code: string, message: string, details?: unknown | undefined);
}
export declare function asError(error: unknown): TerminalError;
