# Local Terminal MCP — 第4题 MVP

交付记录：[任务完成报告 PDF](docs/任务完成报告.pdf)、[完整40项测试日志](evidence/tests/40-tests.log)、[Vitest JSON结果](evidence/tests/vitest-results.json)、[Cursor脱敏调用记录](evidence/cursor/acceptance.json)。

此服务提供通用 shell 执行能力，不是安全沙箱。只应接入明确信任的本地 Agent；源码公开不表示可以无认证暴露到公网。

本项目在 macOS 上提供真实 PTY 终端能力，不是一次性 shell 命令包装。MCP stdio 网关通过本地 Unix socket 调用独立 Broker；Broker 保持 shell、交互进程和有序日志，客户端断开不会自动终止它们。

## 在这台 Mac 上开始

需要 macOS 和 Node.js 22.12+。工程已安装依赖并生成 `dist/`。进入工程后可直接运行 `npm run demo`：示例会创建真实 PTY，执行命令，按 PID 重新 link，写入交互输入，然后终止会话和测试 Broker。

```bash
cd '/workspace/local-terminal-mcp'
npm run demo
```

重新安装或从源码包恢复时：

```bash
npm ci
npm run build
npm test
```

`npm ci` 的 postinstall 只修复本工程 `node-pty` macOS spawn-helper 的执行位。这是实测发现的预编译安装问题，不修改系统权限。若需要编译原生依赖，可能需要 Xcode Command Line Tools。

运行测试必须允许创建本地 PTY、执行 `/bin/ps` 并对测试子进程发送信号。测试只创建自己专用的 `<TEST_TMP_PATH> 目录和进程；受限 Agent 沙箱可能拒绝这些操作。普通本机终端可运行，不需要 sudo；错误 `PROCESS_CHECK_UNAVAILABLE` 或 `EPERM` 表示宿主执行边界受限，不要为了测试给整个应用全盘权限。

## 接入 Cursor

工程附带项目级验证配置生成器，不会覆盖 `~/.cursor/mcp.json`：

```bash
node scripts/prepare-cursor.mjs
cd examples/cursor-smoke
cursor-agent mcp enable challenge4-local-terminal
cursor-agent mcp list-tools challenge4-local-terminal
```

本机当前 Cursor CLI 的 `mcp enable` 必须从实际配置工作目录执行；仅传 `--workspace` 的实测行为不一致。这里明确 `cd` 到独立示例目录。本机的 `agent` 命令不是 Cursor，务必使用上面的绝对路径。

生成的 `.cursor/mcp.json` 使用 Node 和 `dist/mcp.js` 的真实绝对路径。以后可以把这条服务器定义合并到自己的 Cursor 项目配置中，不要直接覆盖已有 `mcpServers`。

```json
{
  "mcpServers": {
    "local-terminal": {
      "type": "stdio",
      "command": "<node 的绝对路径>",
      "args": ["<本项目 dist/mcp.js 的绝对路径>"]
    }
  }
}
```

MCP 无登录模块。Cursor 自身启动模型 Agent 仍需要其有效登录；`mcp list-tools` 成功不等于端到端交互通过。本项目现在已经由真实 Cursor Agent 完成 7 步验收，实际调用全部 9 种工具，共 15 次调用；关键输出是 `MCP_CURSOR_OK` 和 `ECHO:cursor-ok`，终止后无残留测试进程。

无交互 `-p` 模式需要独立的工具执行权限；只做 `mcp enable` 不够。本次使用项目级 `.cursor/cli.json`，精确允许本题 9 个 MCP 工具，并禁止内置 Shell、Write、WebFetch。可按以下步骤复现；`cp -n` 不覆盖已存在的权限文件，已有文件应手工合并所需规则。

```bash
# 从工程根目录开始，先按前述步骤生成并启用服务器
cp -n docs/cursor-cli-permissions.json examples/cursor-smoke/.cursor/cli.json
cd examples/cursor-smoke
cursor-agent -p --trust "$(cat ../../docs/cursor-acceptance-prompt.txt)"
```

若明确授权更宽的无提示执行，Cursor 对应开关是 `--force`，别名 `--yolo`；它允许未被显式 deny 的操作，并非无视所有拒绝规则。本次项目级授权已经使验收通过，没有需要再用强制开关重跑。CLI 权限与 Cursor IDE 的 `permissions.json` 是不同机制，见 [Cursor 官方 CLI 权限说明](https://cursor.com/docs/cli/reference/permissions)。

## 9 个原子工具与推荐调用方式

工具是 `terminal_create`、`terminal_link`、`terminal_exec`、`terminal_read`、`terminal_write`、`terminal_status`、`terminal_terminate`、`terminal_unlink` 和 `terminal_resize`。完整字段、返回值和 Resource 订阅约定在 [工具契约](docs/tool-contract.md)。

先创建受控 shell，保存返回的 `session_id`、`shell_pid`、`link_id` 和 `cursor`。也可以由其他本地客户端凭这个 shell PID 重新 link。PID 必须已由同一个 Broker 托管；不接管 Terminal.app、iTerm2 或其他进程的任意现有终端。

```json
{"name":"terminal_create","arguments":{"cwd":"/tmp","cols":80,"rows":24}}
{"name":"terminal_exec","arguments":{"link_id":"<返回的控制 link>","command":"export KEEP=yes; printf '%s\\n' \"$KEEP\"","request_key":"unique-command-1","wait_ms":2000}}
```

`exec` 在同一个 `/bin/zsh -f -i` 中执行，`cd` 和 `export` 可以延续。shell 有私有 FIFO 命令队列，命令脚本通过 source 执行；TTY stdin 留给前台交互程序。没有 GUI 窗口、登录 shell 配置或普通 shell 提示符，因此不要用 `write` 在空闲 shell 中输入新命令。

当 exec 返回运行中，保留 `command_id`，用 read 读取提示，再 write 内容和按键。非空输入在 IDLE 会返回 `SESSION_IDLE`，不偷偷排队给下一条命令。忙碌时的新 exec 返回 `SESSION_BUSY`。

```json
{"name":"terminal_exec","arguments":{"link_id":"<控制 link>","command":"read \"answer?INPUT: \"; printf 'REPLY:%s\\n' \"$answer\"","request_key":"unique-interactive-1","wait_ms":100}}
{"name":"terminal_read","arguments":{"link_id":"<控制 link>","after_cursor":"<上次 cursor>","wait_ms":100}}
{"name":"terminal_write","arguments":{"link_id":"<控制 link>","text":"hello"}}
{"name":"terminal_write","arguments":{"link_id":"<控制 link>","keys":["Enter"]}}
{"name":"terminal_status","arguments":{"link_id":"<控制 link>","command_id":"<exec 返回的 command_id>"}}
```

同一会话只允许一个控制 link，可以有多个独立读者。`unlink` 只释放关联，不杀进程；`terminate` 明确结束受管会话，并保留输出。Ctrl-C 是前台中断输入，不等于终止整个会话。

```json
{"name":"terminal_terminate","arguments":{"link_id":"<控制 link>","grace_ms":3000}}
{"name":"terminal_unlink","arguments":{"link_id":"<link>"}}
```

## 输出、流式读取与可靠结果

退出码由私有 IPC 回报，前台输出边界由随机 PTY 帧定位，不匹配提示符或普通输出中的“完成”字样。`COMPLETED` 需要控制回报和输出边界均已到达。`exit` 或替换 shell 的 `exec` 可以导致 `UNKNOWN`；`exit_code: null` 不是成功。

PTY 合并 stdout/stderr。日志包含带序号的 output 与生命周期事件，支持 UTF-8、ANSI、无换行输出和游标重放。background job 的字符来源无法可靠逐一归属；`output` 只是该前台命令关联输出，完整观察流以 `events` 为准。exec 的单次输出有界，`output_truncated`、`has_more` 或 `CURSOR_EXPIRED` 必须向用户说明。

只支持 Tools 的客户端可以通过 `terminal_read` 有界长轮询，持续更新 `after_cursor=next_cursor`；多读者游标互不干扰。支持 Resources 的客户端可监听 canonical URI `terminal://link/<link_id>/events`。2025 代使用 `resources/subscribe`，2026-07-28 代使用 `subscriptions/listen`。标准资源更新通知只是唤醒信号，输出内容仍按游标增量获取，不宣称无限 Tool 返回或自定义原始字节推送。

MCP 请求取消只取消 read/exec 的等待，已接受命令继续运行。重试 exec 时必须保留同一个 `request_key` 和命令内容；服务预先持久化请求键，避免盲目重复执行。不承诺 Broker 崩溃窗口下的 exactly-once。

## Broker 运行与配置

默认 runtime 是 `~/Library/Application Support/local-terminal-mcp`，data 位于其 `data/`。创建第一个终端操作时自动启动 Broker；只探测 MCP 工具不会创建终端或 Broker。Unix socket 路径太长时，使用较短的私有目录。

```bash
export LTMCP_RUNTIME_DIR="$HOME/.local-terminal-mcp"
export LTMCP_DATA_DIR="$HOME/.local-terminal-mcp/data"
node dist/cli.js status
node dist/cli.js stop
```

`status` 当前会在 Broker 不存在时启动一个空 Broker 来查询；它在无人连接、没有活跃会话时默认 60 秒退出。`stop` 是本地管理命令，会结束这个 Broker 的全部受管会话；不是暴露给 Agent 的 Tool。若客户端使用自定义 runtime/data，这两个管理命令也必须设置相同环境变量。

默认最多 16 个活跃终端，每个会话最多 1024 条命令，每个 Broker 最多保留 64 个会话记录；保留会话上限把退出会话的缓存也算在内，防止无限增长。达到 64 条返回 `SESSION_ARCHIVE_LIMIT`，可换独立 data 目录，或通过 `LTMCP_MAX_RETAINED_SESSIONS` 明确提高上限。MVP 尚未实现独立历史索引和按需加载海量归档；把上限调高会相应增加内存需求。

日志每会话默认 100 MiB，总 JSONL 预算 1 GiB，尾缓存每会话 4 MiB，退出日志最多保留 7 天，并定期清理。预算不包括 metadata 或 Broker 诊断文件；最后一个最大 64 KiB 的事件允许最小保留超额。每页 read 默认 16 KiB，上限 64 KiB；如果预算装不下下一个事件，返回 `EVENT_TOO_LARGE` 及需要的字节数。游标过期明确报错，不悄悄丢历史。

磁盘故障切换有界内存并给出 `storage_degraded` / gap。恢复写盘需要修复后重新打开 Store；连序号预留都无法安全建立或预留耗尽时拒绝继续接收，不能承诺无限故障期间无损记录。普通文本输出会留下私有日志；输入正文不另外写审计日志，但程序回显仍可能包含秘密。命令临时脚本在完成或退出后清理。

## 安全与限制

本服务只有本机 stdio 与私有 Unix socket，不开放 HTTP、不实现用户登录、不提升 root 权限。文件夹 `0700`，socket 与日志 `0600`。同 UID 客户端属于同一个信任域，不提供 Agent 间安全隔离；不可猜测 handle 不是登录系统。

任意 shell 命令拥有服务进程的 OS 权限。cwd 限制不是沙箱，命令可以自行访问其他有权限的文件或联网；Agent 获得日志后可能将其发往模型服务。不要把服务作为无认证 HTTP endpoint 暴露到公网。

网关断线/重启后可以继续 link 原 PTY。Broker 崩溃后只恢复落盘记录，live PTY 标记 LOST，不虚报恢复。进程信号先校验 UID 和启动身份；根进程消失后不按可能已复用的 TTY 名字发现新目标。主动脱离会话、setsid 或重新托管的程序不提供容器式强收容保证。

不支持任意外部 PID 接管、独立 Docker 式并发 exec、任意二进制流、完整终端屏幕渲染、Windows/Linux、launchd 自启或公网桥接。Manus 公开自定义 MCP 入口要求 HTTPS，不能直接承诺这套本地 stdio 服务可用；若桌面版有本地自定义入口，可另外验证。

## 工程结构

`src/broker.ts` 负责生命周期和控制权；`src/session.ts` 负责真实 PTY 与受控 shell；`src/client.ts` 负责 IPC 和 Broker 启动竞态；`src/event-store.ts` 负责有界日志、签名游标与回放；`src/mcp.ts` 负责官方 SDK 双代 stdio 和 Resources；`src/system.ts` 负责进程身份与信号检查。`test/` 覆盖真实 PTY、Broker、日志故障及两代 MCP 客户端。
