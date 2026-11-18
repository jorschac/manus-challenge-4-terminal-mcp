# 第4题 MVP 测试报告

**版本 0.1.0｜2026-11-18｜Manus AI**

本地终端 MCP MVP 已实现并构建。最终自动化结果为 **40/40 通过，3/3 测试文件通过**，完整运行耗时 **28.77 秒**。这些测试使用真实 macOS PTY 和进程，不是替换成单次 shell 包装或模拟输出。

Cursor 模型 Agent 的端到端验收已于 **13:50 补测通过**：实际调用全部 9 种工具，共 15 次 MCP 调用，7 个验收步骤全部完成，耗时 104.821 秒。调用轨迹与结果已解析核对，不只依据模型的“成功”总结。Manus 接入本地自定义 stdio 的入口尚未实测，也未部署公网 HTTPS 服务。

## 交付工程与边界

工程位于 `/workspace/local-terminal-mcp`，包含 TypeScript 源码、锁定依赖、编译产物、3 组测试、Cursor 项目级配置生成器和工具契约。已实现 create、link、exec、read、write、status、terminate、unlink、resize 共 9 个工具。

服务是同一 macOS 用户下的本地进程。没有登录模块，没有 HTTP/TCP 监听，也没有提升 root 权限。MCP 网关与独立 Broker 分离；真实 PTY 由 Broker 持有。PID link 只关联本服务已托管的 shell，未托管 PID 明确拒绝。

exec 在空闲受控 zsh 中执行，工作目录与 export 延续；交互输入通过 write 发送给运行中的前台程序。空闲状态不把输入偷偷排队给下一条命令。标准资源订阅发送更新通知，内容再按游标增量读取；Tools-only 客户端可以使用有界长轮询。

## 本机与锁定依赖

测试环境为 macOS 26.3、Node.js 22.23.1。MCP Server/Client SDK 2.4.0，node-pty 1.1.0，Zod 4.6.7，TypeScript 5.9.3，Vitest 5.1.0。Cursor CLI 为 `cursor-agent`，版本 `2026.02.13-41ac335`。

默认执行沙箱会拒绝 `/bin/ps`。真实系统集成在允许普通 macOS 进程检查、PTY 和信号的本机执行上下文中完成，不使用 sudo。原生 spawn-helper 安装后的执行位缺失已纳入本工程 postinstall 修复，不修改系统文件。

## 实际测试覆盖

### 真实 Broker 与 PTY：15 项通过

验证了创建真实 TTY，同一个 shell 的 cwd/export 延续，以及正常、非零、无换行和 Unicode 输出。普通输出中的伪完成标记不会让命令提前结束。

验证了客户端 B 根据 A 返回的 PID 关联到相同 session ID 和 shell PID；只读 link 不能执行或写入，控制权不被抢占，多读者游标互不影响，重复游标可以重放相同事件。未托管 PID 明确拒绝。

验证了交互提示 → 文本写入 → Enter → 回应的完整流程；忙碌时的新 exec 被拒绝，Ctrl-C 后受控 shell 可继续执行。分时输出被多个批次读取；取消长轮询不会杀进程。

验证了 request-key 重试不重复执行，冲突明确报错。网关断线后控制租约释放，但原 shell 和环境仍存活，可重新 link。

验证了 resize 的实际终端尺寸、普通进程树终止、无残留受管 PID、重复 terminate 幂等，以及退出后日志仍可读。Broker 被 SIGKILL 后，重启保留落盘日志并报告 LOST/UNKNOWN，不复活或盲目重跑不确定命令。

补充验证了空闲输入拒绝、命令脚本清理、无效 cwd 不留下永久 STARTING 会话；`exit 7` 报告未知命令结果而不编造退出码，shell 的实际退出码单独保留。用 `exec /bin/sleep` 替换 shell 后检测为 UNMANAGED，结构化 exec 被拒绝，但仍能终止。

资源测试覆盖活跃终端与总保留会话上限、周期扫描不妨碍空闲 Broker 自动退出，以及 **30,000 个中文字符**在跨 IPC chunk 和多页 read 后逐字完整，无替换字符损坏。

### MCP 协议集成：2 个端到端场景通过

分别使用 SDK legacy 初始化模式和固定 `2026-07-28` modern 模式启动真实 stdio 子进程。每个场景都覆盖完整 9 工具发现及实际调用，不只检查 tools/list。

覆盖资源快照、两项可选 query 的读取、标准订阅与退订、无效 URI、参数校验、UTF-8 写入限额、非零退出、幂等冲突、只读权限、resize 和真实交互输入。检测网关 stdout 没有非协议内容或自定义终端通知。

还覆盖 read/exec 等待取消后进程继续、stdin EOF 后网关及时退出、原 shell 和长命令继续存活、第二个网关重新 link，以及最终 terminate/unlink。两代测试均在 finally 停止自己创建的 Broker。

### 有界事件存储：23 项通过

覆盖默认 64 KiB 分段、私有权限、签名游标、foreign/篡改/future cursor 拒绝、重复读取、多读者、重启、淘汰和精确 UTF-8 页预算。没有永久逐事件索引或无限内存缓存。

故障覆盖短写、磁盘写入/读取/删除失败、目录移动、损坏记录、降级后重启及 gap。丢失历史通过 CURSOR_EXPIRED 和降级标志显式报告，不静默跳过。

## Cursor CLI 的真实结果

只在工程的独立 `examples/cursor-smoke/.cursor/mcp.json` 配置本题服务器，并只批准 `challenge4-local-terminal`。没有覆盖用户的全局 MCP 配置，也没有使用全局自动批准参数。

从该目录运行以下命令，实际返回 9 个工具名称和参数：

```bash
cursor-agent mcp list-tools challenge4-local-terminal
```

本版本 `mcp enable` 对 `--workspace` 的行为不一致，必须从实际配置目录执行。该兼容细节已写入 README。

首次模型运行被 Cursor 认证阻塞。用户重新登录后，模型可以运行，但无交互模式仍拒绝 terminal_create。随后仅在独立工作区新增 `.cursor/cli.json`，用 `Mcp(challenge4-local-terminal:tool)` 精确允许 9 种工具，同时拒绝内置 Shell、Write 和 WebFetch。本次没有改全局配置，也没有使用 `--force`、`--yolo` 或 `--approve-mcps`。

实际 Cursor 会话为 `<CURSOR_SESSION>`，受管终端会话为 `<TERMINAL_SESSION>`，shell PID 为 `<SHELL_PID>`。create 取得真实 PTY；exec 返回 `MCP_CURSOR_OK`，可信退出码为 0；按此 PID 创建只读 link，返回相同 session ID；resize 返回 100×40。

交互 exec 返回 RUNNING 和 `INPUT: `，随后模型通过两次 write 发送 `cursor-ok` 和 Enter。status 返回 COMPLETED/exit_code=0，增量 read 取得 `ECHO:cursor-ok`。terminate 返回 EXITED、remaining_pids=[]，退出日志仍可读且 eof=true；控制和只读 link 均成功 unlink。

共计 15 次完成的 MCP 调用：create 1、exec 2、read 4、link 1、resize 1、write 2、status 1、terminate 1、unlink 2。轨迹没有内置 shell 或其他 MCP 执行调用；MCP schema 发现属于客户端适配层。已单独核实测试 shell 不再存在，独立 runtime 没有活跃会话、links=0，并停止测试 Broker。

不含模型思考内容的逐次参数和结果保存在同目录 `Cursor验收记录.json`。因此 Cursor 的端到端验收现在为 **通过**，不是仍待验证。用户提到的对应强制开关是 `--force`，别名 `--yolo`；已有项目授权足以完成本轮，无需为了重跑而重复创建会话。

## MVP 的明确限制

最多 16 个活跃会话，默认最多保留 64 个会话记录；每会话最多 1024 条命令。日志默认每会话 100 MiB，总 JSONL 预算 1 GiB，尾缓存 4 MiB，退出日志最多保留 7 天。总保留量到顶会明确拒绝新建，不无限占用内存；调高上限会增加内存需求。

JSONL 预算不包括 metadata 和 Broker 诊断文件。为了保留至少一条记录，单个最大 64 KiB 事件可能造成最小超额。磁盘故障的有界降级不是无限无损保障，无法安全预留序号时会拒绝继续接收。此 MVP 尚未对长期海量归档、磁盘物理断电或所有极端竞态做压力认证。

PID 身份使用同 UID、启动时间和当前进程关系核实；`ps` 启动时间是秒级，不能宣称提供恶意同 UID 环境中的不可攻击身份认证。进程检查和发信号之间仍存在 OS 级竞态。同 UID 受信任调用、普通前台和子进程场景是本次验收边界。

Broker 崩溃后的 live PTY、主动脱离或重新托管的程序不提供恢复/强收容保证。对任意外部终端、Docker 式并发进程、完整屏幕渲染、二进制流、公网桥接和 launchd 自启均未实现。服务无 GUI 窗口，可通过支持 MCP 的本地客户端操作。

## 复现与验收

在工程目录运行：

```bash
npm ci
npm run build
npm test
npm run demo
```

使用文档中的项目级 Cursor 配置和 CLI 权限示例即可复现无交互验收。JSON 测试结果和原始过程日志保留在本题 `.work/terminal-mcp-challenge4`；交付包不包含 node_modules、用户凭据、模型思考内容或测试 runtime 日志。
