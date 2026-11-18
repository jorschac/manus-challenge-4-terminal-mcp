export class TerminalError extends Error {
    code;
    details;
    constructor(code, message, details) {
        super(message);
        this.code = code;
        this.details = details;
        this.name = "TerminalError";
    }
}
export function asError(error) {
    return error instanceof TerminalError
        ? error
        : new TerminalError("INTERNAL_ERROR", error instanceof Error ? error.message : String(error));
}
//# sourceMappingURL=errors.js.map