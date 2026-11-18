import { createConnection } from "node:net";
const [socketPath, token, kind, session_id, command_id, exitCode, cwd, tty] = process.argv.slice(2);
const socket = createConnection(socketPath);
const timeout = setTimeout(() => {
    socket.destroy();
    process.exitCode = 1;
}, 3000);
socket.on("connect", () => socket.write(JSON.stringify({
    token,
    kind,
    session_id,
    command_id,
    exit_code: Number(exitCode),
    cwd,
    tty,
}) + "\n"));
socket.on("data", () => {
    clearTimeout(timeout);
    socket.end();
});
socket.on("error", () => {
    clearTimeout(timeout);
    process.exitCode = 1;
});
//# sourceMappingURL=reporter.js.map