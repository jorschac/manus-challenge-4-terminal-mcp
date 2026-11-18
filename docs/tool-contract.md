# Terminal MCP 工具契约

`local-terminal-mcp` 是本地 macOS PTY broker 的 MCP stdio 网关。**所有核心能力都只需 Tools**；Resources 是可选的事件快照和更新提示，不是伪装的实时字节流。网关没有自定义终端 MCP notification，也不依赖客户端支持 Resources 才能执行、读取或控制会话。

## SDK 与连接生命周期

实现按本机 `@modelcontextprotocol/server`、`@modelcontextprotocol/client` **2.4.0** 及 `zod` **4.6.7** 的 README、`.d.mts` 声明和 stdio 实现核对 API。

服务端使用 `serveStdio(context => createTerminalMcpServer(context, config))`，不传 `legacy: 'reject'`。官方入口默认支持双代：2025-era 的 `initialize` 连接与 2026-07-28-era 的逐请求 envelope 连接各自使用同一工厂定义，并为连接选择、固定一个实例。工厂收到的 `context.era` 为 `legacy` 或 `modern`。不要用手写 JSON-RPC 握手代替它。

SDK 客户端默认 `versionNegotiation.mode: 'legacy'`；现代连接可使用 `{ mode: { pin: '2026-07-28' } }`。`mode: 'auto'` 的官方 stdio 传输还会运行临时探测子进程。网关仅在真正的终端请求上懒连接 IPC，协议探测不会创建 broker 或 shell。

每个网关实例拥有自己的 `BrokerClient`。网关 stdin EOF、传输关闭、SIGINT 或 SIGTERM 只关闭网关和 IPC 连接，**不调用 `terminal_terminate` 或 `broker_stop`**，不杀 PTY、shell 或已接受的命令。broker 拥有会话；客户端可重连并通过已知 shell PID 重新获取 link。不得把 `disconnectGraceMs` 当作终止 PTY 的超时。

stdout 严格属于官方 SDK，仅输出 MCP JSON-RPC。启动错误、协议错误和可选诊断都写 stderr。

## 通用输入与结果

工具名与 broker 方法名相同，参数经严格 Zod object 校验，不做数字/布尔隐式转换，不接收未知字段。`link_id`、`request_key` 和显式 `command_id` 是非空字符串；它们是 opaque 标识，不能交换使用。`after_cursor` 是 broker 发出的 opaque cursor，不应解析、拼接、猜测或当作全局整数。

所有正常工具结果同时包含相同业务对象的 `structuredContent` 与 JSON text：

```json
{
  "content": [{ "type": "text", "text": "{\"session_id\":\"...\",\"link_id\":\"...\"}" }],
  "structuredContent": { "session_id": "...", "link_id": "..." },
  "isError": false
}
```

网关不裁剪或重新推断 broker 返回字段。broker 返回的 `session_id`、`link_id`、`resource_uri`、命令状态、事件与降级标志均保留。官方 SDK 的 `Server.projectCallToolResult` 负责所选代际的结果投影。

业务 `TerminalError` 转成 `isError: true`，业务结构为：

```json
{
  "error": {
    "code": "REQUEST_KEY_CONFLICT",
    "message": "request_key was already used with a different command",
    "details": {}
  }
}
```

`details` 仅在存在时出现。结构同样放入 `structuredContent` 和 JSON text。参数校验失败是 `INVALID_PARAMS`，包含 Zod issues；未知工具为 `UNKNOWN_TOOL`；非业务异常经 `asError` 转为 `INTERNAL_ERROR`。为避免高层 SDK 默认将校验错误转成非 JSON text，网关注册工具定义后用官方 `Server.setRequestHandler('tools/call', ...)` 统一校验和返回结构，仍通过 SDK 做结果投影。

**非零 `exit_code` 不是 MCP 工具错误。** `false`、程序退出 1 或测试失败属于命令业务结果，仍是 `isError: false`。`exit_code: null` 不意味着成功：应读取 `command_state`、`command.state`、完成边界与状态。没有收到可信完成信号时不能猜测完成或退出码。

## 九个工具

| 工具 | 输入 | 默认值 / 校验 | 语义 |
| --- | --- | --- | --- |
| `terminal_create` | `cwd?`, `cols?`, `rows?`, `env?` | `cwd=HOME`；`cols=80`；`rows=24`；尺寸均为整数 `2..1000`；`env` 为 string → string record | 创建 broker-owned 的持久 PTY shell，取得控制 link。 |
| `terminal_link` | `pid`, `access?` | `pid` 必须为正整数；`access` 为 `read` / `control`，默认 `read` | 只链接到本 Broker 已托管的 shell PID；任意外部 PID 返回 NOT_MANAGED，不提供外部观察视图。 |
| `terminal_exec` | `link_id`, `command`, `request_key`, `wait_ms?` | `command` 与 `request_key` 必需且非空；等待整数 `0..10000`，默认 `2000` | 控制 link 提交命令；等待仅限制本次响应等待时间。请求键保证同一会话中的重试不重复执行。 |
| `terminal_read` | `link_id`, `after_cursor?`, `max_bytes?`, `wait_ms?` | 字节预算整数 `64..65536`，默认 `16384`；等待整数 `0..20000`，默认 `0` | 按序读取持久事件，可 bounded long-poll。 |
| `terminal_write` | `link_id`, **恰一**：`text` 或 `keys` | `text` 至多 `16384` UTF-8 bytes；`keys` 必须非空且每项来自下述枚举 | 控制 link 向运行中的前台程序写输入；空闲状态非空输入返回 SESSION_IDLE，不偷偷留给下一条命令。空 text 是合法无操作输入。 |
| `terminal_status` | `link_id`, `command_id?` | 显式 command ID 为非空字符串 | 返回会话状态及可选命令记录。 |
| `terminal_terminate` | `link_id`, `grace_ms?` | 整数 `0..10000`，默认 `3000` | 明确终止有控制权限的 broker-owned 终端，宽限后由 broker 升级终止。 |
| `terminal_unlink` | `link_id` | 非空字符串 | 释放此 link，不终止持久会话。 |
| `terminal_resize` | `link_id`, `cols`, `rows` | 两个尺寸必需，整数 `2..1000` | 修改受控 PTY 大小。 |

支持的 keys 为 `Enter`、`Tab`、`ArrowUp`、`ArrowDown`、`ArrowLeft`、`ArrowRight`、`CtrlC`、`CtrlD`、`Escape`、`Backspace`。例如：

```json
{ "link_id": "...", "text": "hello" }
{ "link_id": "...", "keys": ["Enter"] }
```

不能同时传 `text` 与 `keys`，也不能两者都不传。UTF-8 字节限制不是 JS string length：6000 个 `界` 已超过 16 KiB。工具 schema 描述注明恰一规则，执行时还用 Zod refinement 校验恰一和字节长度；客户端不能只靠 JSON Schema 的 `maxLength` 推断字节合法性。

## 执行与读取流程

建议流程是 `create/link → exec → status/read → terminate（确实需要时）→ unlink`。exec 之后保留 `session_id`、`shell_pid`、`link_id`、`command_id` 和读取 cursor。若响应超时或连接中断，用同一 `request_key` 和同一 `command` 重试，不生成新键重复执行。相同键配不同命令会返回 `REQUEST_KEY_CONFLICT`。

`terminal_read` 返回至少：

- `events`：broker 定义的有序事件数组；每项含 `seq`、`time`、`type` 与 `data`；PTY 输出不保证 stdout/stderr 分流。
- `next_cursor`：下一次读取的 `after_cursor`。
- `earliest_cursor`：当前保留窗口最早可读位置。
- `has_more`：当前分页后是否还有保留事件。
- `storage_degraded`：持久化/存储降级提示，必须保留并展示，不能宣称无损。
- `eof`、`state`：会话终态及本次读取的 EOF 状态；必须结合分页判断，不把暂时无输出当作 EOF。

若 `has_more: true`，继续以 `next_cursor` 读取，不靠 sleep 假装已经读完。日志保留窗口之外的 cursor 可能产生 `CURSOR_EXPIRED`；按业务错误处理并明确说明历史不再完整。高流量输出可能超过单次字节预算，不能把单次 exec 的 output 或单页 read 当成全部历史。

会话状态包括 `STARTING`、`IDLE`、`BUSY`、`UNMANAGED`、`CLOSING`、`EXITED`、`LOST`；命令状态包括 `ACCEPTED`、`RUNNING`、`COMPLETED`、`UNKNOWN`。这些状态来自 broker，不由 MCP 层根据等待时间推算。交互输入、TUI 或不再遵循管理协议的 shell 可能影响可结构化执行能力，应检查 broker 的 capabilities 与状态。

## 取消不等于终止

SDK handler 上取消信号的实际位置是 `ctx.mcpReq.signal`。网关仅将此信号传入 `terminal_read` 和 `terminal_exec` 的 broker 请求；取消停止读取或等待，已接受命令继续执行。其他改变状态的操作不由该信号触发额外动作。网关绝不把 `notifications/cancelled` 翻译成 `CtrlC`、`CtrlD`、`terminal_terminate`、unlink 或进程信号。

真正需要中断时必须明确调用 `terminal_write` 的 `keys: ["CtrlC"]`；真正需要关闭会话时必须明确调用 `terminal_terminate`。两者都是有副作用的业务操作，并不等价。

## 可选 Resources 与标准订阅

canonical resource URI：

```text
terminal://link/<link_id>/events
```

资源模板为 `terminal://link/{link_id}/events{?after_cursor,max_bytes}`。`resources/list` 仅列出当前网关已知 link 的 canonical URI，不枚举未经授权的全部 broker 会话。`resources/templates/list` 提供模板。

资源读支持可选 query `after_cursor`、`max_bytes`，映射到 `terminal_read`，默认 `wait_ms=0`：

```text
terminal://link/<link_id>/events?after_cursor=<percent-encoded-cursor>&max_bytes=16384
```

返回单个 `application/json` resource content，text 是 broker read 结果的 JSON。query 不是新的终端或新的订阅身份；不支持 resource 流式字节输出。参数范围与 read tool 相同；不支持未知/重复 query、fragment 或非 terminal/link URI。资源错误按 MCP protocol error 返回，并携带 `terminal_error.code` 业务信息；需要统一工具错误结构的客户端可始终只调用 `terminal_read`。

本机 SDK v2.4.0 的 URI 模板反向匹配在只提供第二个可选 query（例如只有 `max_bytes`）时不匹配。网关仍通过 `registerResource` 公布标准模板与资源列表，但用官方 `Server.setRequestHandler('resources/read', ...)` 严格解析 URI，保证两项都真正可选、可交换 query 顺序，并拒绝未知/重复参数。

订阅仅使用不带 query 的 canonical URI：

- **旧代**：标准 `resources/subscribe` / `resources/unsubscribe`。网关先通过 `terminal_status` 验证订阅 link；只对已订阅的 canonical URI 发更新提示。
- **现代 2026-07-28**：官方 `client.listen({ resourceSubscriptions: [uri] })`，返回 `McpSubscription`；`subscription.close()` 关闭订阅。SDK `serveStdio` 接管标准 `subscriptions/listen` 的确认、筛选、subscription-id envelope 与取消，不再伪造旧代 unsolicited 通知或自己实现协议流。

broker IPC 的 `session_changed` 在网关内根据已知 `session_id → link` 映射转换为标准资源更新。SDK v2.4.0 的实际 API 是 **`server.server.sendResourceUpdated({ uri })`**，不是 `McpServer.notifyResourceUpdated`；代码中的本地 `notifyResourceUpdated` 回调调用这个官方方法。现代 SDK 根据已确认的 canonical URI 订阅筛选、封装发送；旧代网关显式维护订阅集合。`terminal_unlink` 成功会移除此网关 link 的通知映射。

更新通知只是“可能有新事件”的提示，不包含虚构流、不承诺一通知一输出、不保证补齐掉线期间的所有通知。收到更新后仍用 `terminal_read` 或 resource snapshot 配 cursor 查询真实持久事件。无需订阅也能完全使用九个工具。

## 验证与安全边界

`test/mcp.test.ts` 使用官方 client 2.4.0 的 legacy 与 pinned-modern 两种连接，启动 `node dist/mcp.js`。每种测试使用 `fs.mkdtemp('<TEST_TMP_PATH> 的独立 runtime/data，子进程注入 `LTMCP_RUNTIME_DIR` / `LTMCP_DATA_DIR`，并在 finally 关闭自己客户端、调用自己的 broker `broker_stop`、移除临时目录。

覆盖：九工具发现、各工具错误参数、UTF-8 限制、create/exec/read、请求键重试与冲突、非零退出码、只读链接拒绝 exec、resize、resource query 读与未知/重复 query 拒绝、标准资源更新与退订、取消 read 不杀会话、取消 exec 等待不杀已接受命令、网关 stdin EOF 及时退出、断连后长命令继续和同 PID 重新链接、显式 terminate/unlink。清理不仅等待 IPC socket 消失，还验证本测试 broker PID 已退出。协议解析错误也会失败，避免 stdout 混入非 JSON-RPC。

本地终端工具拥有用户权限，能够运行用户指定的任意命令；MCP 本身不是沙箱或批准系统。`control` 必须明确请求并由 broker 执行权限判断。网关不更改 Cursor 配置，不扩大外部 PID 的控制权限，不删除业务错误信息，也不自动清理用户会话。
