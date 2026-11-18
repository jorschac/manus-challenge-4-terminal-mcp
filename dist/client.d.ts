import { EventEmitter } from "node:events";
import type { BrokerConfig } from "./types.js";
export declare class BrokerClient extends EventEmitter {
    readonly config: BrokerConfig;
    private socket?;
    private connecting?;
    private buffer;
    private closed;
    private pending;
    constructor(config?: BrokerConfig);
    connect(): Promise<void>;
    private open;
    private ensureConnected;
    request(method: string, params?: Record<string, unknown>, signal?: AbortSignal): Promise<any>;
    private send;
    private receive;
    close(): void;
}
