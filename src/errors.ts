export class TerminalError extends Error {
  constructor(
    public readonly code: string,
    message: string,
    public readonly details?: unknown,
  ) {
    super(message);
    this.name = "TerminalError";
  }
}
export function asError(error: unknown): TerminalError {
  return error instanceof TerminalError
    ? error
    : new TerminalError(
        "INTERNAL_ERROR",
        error instanceof Error ? error.message : String(error),
      );
}
