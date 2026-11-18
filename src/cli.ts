#!/usr/bin/env node
import { BrokerClient } from "./client.js";
import { asError } from "./errors.js";
const command = process.argv[2] ?? "status";
if (!["status", "stop"].includes(command)) {
  console.error("Usage: node dist/cli.js [status|stop]");
  process.exitCode = 2;
} else {
  const client = new BrokerClient();
  try {
    console.log(
      JSON.stringify(
        await client.request(
          command === "stop" ? "broker_stop" : "broker_status",
        ),
        null,
        2,
      ),
    );
  } catch (error) {
    console.error(JSON.stringify(asError(error)));
    process.exitCode = 1;
  } finally {
    client.close();
  }
}
