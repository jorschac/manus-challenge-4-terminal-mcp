export declare const sleep: (ms: number) => Promise<void>;
export declare function atomicJson(path: string, value: unknown): void;
export declare function removeOwnedSocket(path: string): void;
export interface ProcessInfo {
    pid: number;
    ppid: number;
    pgid: number;
    uid: number;
    tty: string;
    identity: string;
}
export declare function processTable(): ProcessInfo[];
export declare function processIdentity(pid: number): string | undefined;
export declare function signalVerified(target: ProcessInfo, signal: NodeJS.Signals): boolean;
export declare function managedProcesses(pid: number, identity: string, tty: string): ProcessInfo[];
export declare function terminateProcesses(pid: number, identity: string, tty: string, graceMs: number): Promise<{
    signaled_pids: number[];
    killed_pids: number[];
    remaining_pids: number[];
}>;
export declare function quoteShell(value: string): string;
