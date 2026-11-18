#!/usr/bin/env node
import { McpServer, type McpRequestContext } from "@modelcontextprotocol/server";
import { type StdioServerHandle } from "@modelcontextprotocol/server/stdio";
import { z } from "zod";
import type { BrokerConfig } from "./types.js";
/** These same schemas validate calls and supply the advertised MCP input schemas. */
export declare const toolSchemas: {
    readonly terminal_create: z.ZodObject<{
        cwd: z.ZodDefault<z.ZodString>;
        cols: z.ZodDefault<z.ZodNumber>;
        rows: z.ZodDefault<z.ZodNumber>;
        env: z.ZodOptional<z.ZodRecord<z.ZodString, z.ZodString>>;
    }, z.core.$strict>;
    readonly terminal_link: z.ZodObject<{
        pid: z.ZodNumber;
        access: z.ZodDefault<z.ZodEnum<{
            read: "read";
            control: "control";
        }>>;
    }, z.core.$strict>;
    readonly terminal_exec: z.ZodObject<{
        link_id: z.ZodString;
        command: z.ZodString;
        request_key: z.ZodString;
        wait_ms: z.ZodDefault<z.ZodNumber>;
    }, z.core.$strict>;
    readonly terminal_read: z.ZodObject<{
        link_id: z.ZodString;
        after_cursor: z.ZodOptional<z.ZodString>;
        max_bytes: z.ZodDefault<z.ZodNumber>;
        wait_ms: z.ZodDefault<z.ZodNumber>;
    }, z.core.$strict>;
    readonly terminal_write: z.ZodObject<{
        link_id: z.ZodString;
        text: z.ZodOptional<z.ZodString>;
        keys: z.ZodOptional<z.ZodArray<z.ZodEnum<{
            Enter: "Enter";
            Tab: "Tab";
            ArrowUp: "ArrowUp";
            ArrowDown: "ArrowDown";
            ArrowRight: "ArrowRight";
            ArrowLeft: "ArrowLeft";
            CtrlC: "CtrlC";
            CtrlD: "CtrlD";
            Escape: "Escape";
            Backspace: "Backspace";
        }>>>;
    }, z.core.$strict>;
    readonly terminal_status: z.ZodObject<{
        link_id: z.ZodString;
        command_id: z.ZodOptional<z.ZodString>;
    }, z.core.$strict>;
    readonly terminal_terminate: z.ZodObject<{
        link_id: z.ZodString;
        grace_ms: z.ZodDefault<z.ZodNumber>;
    }, z.core.$strict>;
    readonly terminal_unlink: z.ZodObject<{
        link_id: z.ZodString;
    }, z.core.$strict>;
    readonly terminal_resize: z.ZodObject<{
        link_id: z.ZodString;
        cols: z.ZodNumber;
        rows: z.ZodNumber;
    }, z.core.$strict>;
};
/** One SDK factory instance owns one IPC client, never the broker or the sessions. */
export declare function createTerminalMcpServer(context: Pick<McpRequestContext, "era">, config?: BrokerConfig): McpServer;
/** Official v2 factory entry: omitted legacy option intentionally keeps dual-era serving. */
export declare function startStdioGateway(config?: BrokerConfig, exitOnClose?: boolean): StdioServerHandle;
