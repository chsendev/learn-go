# 手搓 HTTP 库（myhttp）—— 实现路线图

> 目标：基于 Go 标准库的 `net`（TCP）从零实现一个 HTTP 库，借此吃透 `net/http` 的核心原理。
> 原则：**每个里程碑都能编译、能运行、能验证**。先让它跑起来，再让它正确，最后让它优雅。

---

## 学习地图：我们要覆盖的核心原理

```
服务端                                客户端
─────────────────────              ─────────────────────
M1  Accept 循环 + 一连接一 goroutine   M9  Dial + 收发
M2  Handler 接口 + 函数适配器          M10 连接池 + readLoop/writeLoop 分离
M3  ServeMux 路由                     M11 超时 / 重试 / 取消
M4  HTTP 报文编解码（含 chunked）
M5  keep-alive + 多层超时             跨领域
M6  中间件（洋葱模型）                 M0  HTTP 报文格式认知
M7  panic 恢复 + ConnState            M12 进阶：TLS / HTTP2 概念
M8  优雅关机 + context 传播
```

每个里程碑下面都标了 **【对应 net/http】** 和 **【核心原理】**，做完回头读对应源码，理解会翻倍。

---

## M0 · 准备：搞懂 HTTP/1.1 报文长什么样

**不写代码，先建立认知。** 用 `nc` 手动发一个 HTTP 请求感受报文结构：

```bash
printf 'GET /hello HTTP/1.1\r\nHost: example.com\r\n\r\n' | nc example.com 80
```

要彻底记住的报文结构（关键是 `\r\n` 和空行）：

```
请求：
GET /path?q=1 HTTP/1.1\r\n        ← 请求行：方法 路径 版本
Host: example.com\r\n             ← header
Content-Length: 5\r\n
\r\n                              ← 空行：header 结束
hello                            ← body

响应：
HTTP/1.1 200 OK\r\n               ← 状态行
Content-Type: text/plain\r\n
Content-Length: 5\r\n
\r\n
world
```

**产出**：能在纸上默写出请求/响应的字节结构。
**【核心原理】** HTTP 是文本协议；`\r\n` 分隔行，空行分隔 header 与 body；body 长度靠 `Content-Length` 或 `chunked` 界定。

---

## M1 · 最小可用服务端：能响应一个写死的 200

**目标**：`curl localhost:8080/` 能拿到 "hello"。

**要实现什么**：
1. `Server.ListenAndServe()`：`net.Listen("tcp", addr)` 拿到 Listener。
2. `for { conn, _ := ln.Accept(); go srv.serveConn(conn) }`：接受连接，每条开一个 goroutine。
3. `serveConn`：用 `bufio.NewReader(conn)` 读取——
   - 读请求行（`ReadString('\n')`），切出 method / path / proto。
   - 循环读 header 行，直到空行。
4. 往 `conn` 写死一个合法响应：`HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello`。

**【对应 net/http】** `Server.Serve`（server.go:3404 的 `for{Accept;go c.serve()}`）、`conn.serve`（server.go:1897）。
**【核心原理】**
- ⭐ **「一连接一 goroutine」模型**——整个 Go 高并发服务端的灵魂。
- `bufio.Reader` 缓冲读取的意义。
- 手动解析报文，体会「为什么需要一个 Request 结构体」。

**验收**：`curl -v localhost:8080/` 返回 200 和 hello。

---

## M2 · 抽象 Request / ResponseWriter / Handler

**目标**：把 M1 里写死的逻辑抽象成可扩展的接口，用户能自定义处理逻辑。

**要实现什么**：
1. `type Request struct { Method, Path, Proto string; Header map[string][]string; Body io.Reader; ... }`，写一个 `readRequest(*bufio.Reader) (*Request, error)`。
2. `type ResponseWriter interface { Header() Header; WriteHeader(int); Write([]byte) (int, error) }`。
3. 实现一个具体的 `response` 结构体实现 `ResponseWriter`：
   - 缓冲 header，第一次 `Write` 或 `WriteHeader` 时把状态行 + header 刷到 conn（**强制 header→status→body 顺序**）。
4. `type Handler interface { ServeHTTP(ResponseWriter, *Request) }`。
5. `type HandlerFunc func(ResponseWriter, *Request)`，给它加 `ServeHTTP` 方法调用自己。

**【对应 net/http】** `Handler`（server.go:88）、`HandlerFunc`（server.go:2285 那 3 行）、`ResponseWriter`、`response`（server.go:426）。
**【核心原理】**
- ⭐ **单方法接口 + 函数适配器**：`HandlerFunc` 让"函数"变成"接口实现"。
- ⭐ **ResponseWriter 的状态机约束**：为什么 header 必须在 body 之前写。
- 延迟写头（buffer header until first Write）。

**验收**：用户能写 `srv.Handle(myHandler)`，自定义返回内容。

---

## M3 · 路由：ServeMux

**目标**：不同路径 / 方法分发到不同 Handler。

**要实现什么**：
1. `type ServeMux struct { m map[string]Handler }`，本身实现 `Handler` 接口（`ServeHTTP` 里查表分发）。
2. `mux.Handle(pattern, handler)` / `mux.HandleFunc(pattern, fn)`。
3. 匹配规则（从简到繁）：
   - 先做精确匹配 `/users`。
   - 再做前缀匹配 `/static/`（以 `/` 结尾表示子树）。
   - 进阶：支持 `GET /user/{id}` 这种方法 + 路径参数（模仿 Go 1.22）。

**【对应 net/http】** `ServeMux`（server.go:2575）、`ServeMux.ServeHTTP`（2814）、`ServeMux.Handler`（2647）。
**【核心原理】**
- ⭐ **mux 自己也是一个 Handler**——理解"路由器只是一个特殊的处理器"，这是洋葱模型的基础。
- 优先级路由匹配（精确 > 通配 > 前缀）。

**验收**：`/a` 和 `/b` 返回不同内容；不存在的路径返回 404。

---

## M4 · 报文编解码完善：Content-Length 与 chunked

**目标**：正确读请求 body、正确写响应 body，支持两种长度界定方式。

**要实现什么**：
1. **读 body**：
   - 有 `Content-Length` → 用 `io.LimitReader` 精确读 N 字节。
   - `Transfer-Encoding: chunked` → 实现 chunked 解码（读 `十六进制长度\r\n` + 数据，直到 0 块）。
2. **写 body**：
   - 用户给了固定内容 → 自动算 `Content-Length`。
   - 长度未知（流式）→ 用 chunked 编码输出。
3. 处理 `GET`/`HEAD` 无 body、`Connection: close` 等边界。

**【对应 net/http】** `transferReader` / `chunkedReader` / `chunkedWriter`（transfer.go、internal/chunked.go）、`readResponse`。
**【核心原理】**
- ⭐ **body 长度的两种界定方式**——这是 HTTP/1.1 的核心难点，理解它就理解了"流如何在一条连接上分包"。
- `io.Reader`/`io.Writer` 的组合（LimitReader、装饰器模式）。

**验收**：能正确接收 POST 的 JSON body；能流式输出（chunked）一个大响应。

---

## M5 · keep-alive 与多层超时

**目标**：一条连接处理多个请求；防止慢连接拖死服务。

**要实现什么**：
1. **keep-alive 循环**：`serveConn` 改成 `for { req := readRequest(); handler.ServeHTTP(); if shouldClose(req) break }`——一条连接循环处理多个请求。
2. 根据 `Connection: close` 头 / HTTP 版本决定是否关闭。
3. **超时**（用 `conn.SetReadDeadline` / `SetWriteDeadline` 实现）：
   - `ReadHeaderTimeout`：读 header 的超时。
   - `ReadTimeout`：读完整个请求。
   - `WriteTimeout`：写响应。
   - `IdleTimeout`：keep-alive 空闲等待下一个请求的超时。

**【对应 net/http】** `conn.serve` 的循环、`Server` 超时字段（server.go:2964）。
**【核心原理】**
- ⭐ **keep-alive 协议**：一条 TCP 串行处理多请求（呼应之前讨论的"复用但串行"）。
- ⭐ **多层超时**：为什么超时不是一个值而是四个（防 Slowloris 攻击）。
- `SetReadDeadline` 与阻塞读的配合。

**验收**：`curl` 默认复用连接发两个请求只建一次 TCP（用 `-v` 看 `Re-using existing connection`）；慢客户端被超时踢掉。

---

## M6 · 中间件（洋葱模型）

**目标**：支持日志、鉴权、panic 恢复等可插拔的横切逻辑。

**要实现什么**：
1. 定义 `type Middleware func(Handler) Handler`。
2. 实现几个示范中间件：
   - `Logging`：记录 method/path/耗时/状态码。
   - `Recover`：`defer recover()` 防止业务 panic 拖垮连接。
   - `Auth`：检查 header，不通过返回 401。
3. 实现 `Chain(h, mws...)` 把多个中间件按顺序套上。

**【对应 net/http】** `serverHandler.ServeHTTP`（server.go:3302 这个"最小中间件"）、社区中间件惯例。
**【核心原理】**
- ⭐ **洋葱模型 / 装饰器模式**：`func(Handler) Handler` 是所有 Web 框架的根。
- 中间件顺序与"前置/后置"逻辑。

**验收**：请求经过日志中间件打印一行；handler 里故意 panic 不会让整个 server 挂。

---

## M7 · panic 恢复与连接状态管理

**目标**：单个请求出错不影响其他；可观测连接生命周期。

**要实现什么**：
1. 在 `serveConn` 顶层加 `defer recover()`（即使没用 Recover 中间件，连接级也要兜底）。
2. `type ConnState int`（New / Active / Idle / Closed）+ `Server.ConnState func(net.Conn, ConnState)` 回调钩子。
3. 在连接状态变化时触发回调。

**【对应 net/http】** `conn.serve` 的 defer recover、`Server.ConnState`、`ConnState` 类型。
**【核心原理】**
- ⭐ **每个独立工作单元加 recover**——工业级服务的基本要求。
- ⭐ **钩子点设计**：库给关键路径留扩展点（监控/追踪从这接入）。
- 连接状态机。

**验收**：注册 ConnState 回调能打印出连接 New→Active→Idle→Closed 的流转。

---

## M8 · 优雅关机与 context 传播

**目标**：收到退出信号时，不再接新请求，等存量请求处理完再退出。

**要实现什么**：
1. `Server.Shutdown(ctx)`：
   - 关闭 Listener（停止 Accept）。
   - 关闭所有空闲连接。
   - 等待活跃连接处理完（用 `sync.WaitGroup` 跟踪），最多等到 `ctx` 超时。
2. 给每个 `Request` 挂上 `context.Context`（`req.Context()`），连接关闭 / shutdown 时 cancel，让业务能感知。
3. 跟踪活跃连接集合（`map[*conn]struct{}` + mutex）。

**【对应 net/http】** `Server.Shutdown`、`Server.trackConn`、`BaseContext`/`ConnContext`。
**【核心原理】**
- ⭐ **优雅关机三段式**：停止接收 → 等存量完成 → 超时强退（适用于所有服务）。
- ⭐ **context 取消传播**：请求级的取消信号如何贯穿整个调用链。

**验收**：`Ctrl+C` 后正在处理的请求能完成，新请求被拒，进程在请求结束后干净退出。

> **至此服务端完成。建议在这停下，对照 server.go 通读一遍，会非常通透。**

---

## M9 · 最小客户端：Dial + 收发

**目标**：`client.Get("http://localhost:8080/")` 拿到响应。

**要实现什么**：
1. `net.Dial("tcp", host)` 建连。
2. 把 `Request` 序列化成 HTTP 报文写到 conn（请求行 + header + body）。
3. 用 `bufio.Reader` 读响应：解析状态行、header、body（复用 M4 的 body 解码逻辑）。
4. `type Response struct { StatusCode int; Header Header; Body io.ReadCloser }`。

**【对应 net/http】** `Client.Get`/`Do`（client.go）、`Request.write`、`ReadResponse`。
**【核心原理】**
- ⭐ **客户端与服务端的对称性**：服务端 read 请求 / write 响应，客户端反过来。
- 报文序列化（与 M4 解码互为逆操作）。

**验收**：自己的 client 能请求自己的 server，跑通端到端。

---

## M10 · Transport：连接池 + readLoop/writeLoop 分离 ⭐ 最硬核

**目标**：连接复用，且支持"边写请求边读响应"的全双工。

**要实现什么**：
1. `type persistConn`：一条可复用连接，建连后启动两个 goroutine：
   - `writeLoop`：从 `writech` 取请求，序列化写到 conn。
   - `readLoop`：`bufio.Peek(1)` 阻塞等响应 / 探测连接死活，读到后解析响应回传。
2. `roundTrip`：通过 channel 把请求派给 writeLoop / readLoop，自己 `select` 等结果。
3. **连接池**：`map[connKey][]*persistConn`，按 host 分组：
   - 用完放回池（LIFO）。
   - 取用前健康检查（是否 broken / 超时）。
   - `getConn`：先查池，没有再 Dial。

**【对应 net/http】** `persistConn.roundTrip`（transport.go:2813）、`readLoop`/`writeLoop`（2291/2649）、`getConn`/`queueForIdleConn`/`queueForDial`（1174/1594）。
**【核心原理】**（这一步是整个项目的精华）
- ⭐ **CSP：用 channel 解耦 goroutine**（writech/reqch/result 当邮箱）。
- ⭐ **readLoop/writeLoop 分离**：为什么写和读要并发（服务端可能边读 body 边回响应）。
- ⭐ **`bufio.Peek(1)` 的双重语义**：等数据 + 探测连接关闭，且不消费字节。
- ⭐ **连接池**：LIFO、按 host 分组、健康检查、配额。

**验收**：客户端对同一 host 发多个请求只建少量 TCP 连接（加日志数 Dial 次数验证复用）。

---

## M11 · 客户端的超时、取消与安全重试

**目标**：请求可超时、可取消、空闲连接失效时能安全重试。

**要实现什么**：
1. `Client.Timeout` + `context`：超时 / 主动取消时中断 roundTrip。
2. `select` 同时处理：响应到达 / 写错误 / 超时 / context 取消 / 连接关闭（模仿 `persistConn.roundTrip` 主循环）。
3. **安全重试**：从池里取的连接刚发现已被对端关闭（且还没写出任何字节、请求幂等、body 可重读）→ 重试一次。需要把 body 包成可 rewind。

**【对应 net/http】** `roundTrip` 重试循环（transport.go:673）、`shouldRetryRequest`、`setupRewindBody`。
**【核心原理】**
- ⭐ **多路 select 处理多种退出条件**。
- ⭐ **安全重试的前提**：幂等 + body 可重放 + 尚未写出（呼应之前讨论的 keep-alive 竞态窗口）。
- ⭐ **客户端/服务端 IdleTimeout 配置的相互关系**。

**验收**：超时能正确返回 timeout 错误；故意让 server 关闭空闲连接，client 能静默重试成功。

---

## M12 · 进阶（可选，按兴趣）

- **TLS**：把 `net.Dial` / `net.Listen` 换成 `tls.Dial` / `tls.Listen`，体会 HTTPS 只是套了层 TLS。
- **HTTP/2 概念验证**：理解 frame / stream / 多路复用（不必完整实现，读懂 `x/net/http2` 的设计即可）。
- **httptest 式测试工具**：实现一个内存版的 server/client 对接，体会可测试性设计。
- **连接级 metrics / trace 钩子**：模仿 `httptrace`。

**【核心原理】** HTTPS 的本质、HTTP/2 多路复用（呼应之前关于 stream / 保序 / 队头阻塞的讨论）。

---

## 建议的推进节奏

| 阶段 | 里程碑 | 产出 | 难度 |
|------|--------|------|------|
| 第一周 | M0–M3 | 能路由分发的服务端 | ★★ |
| 第二周 | M4–M6 | 完整、带中间件的服务端 | ★★★ |
| 第三周 | M7–M9 | 可优雅关机的服务端 + 最小客户端 | ★★★ |
| 第四周 | M10–M11 | 带连接池的客户端（精华） | ★★★★★ |
| 按兴趣 | M12 | TLS / HTTP2 认知 | ★★★ |

## 每个里程碑的工作法（重要）

1. **先自己实现**，卡住了再去翻对应的 net/http 源码。
2. **实现完再读源码**，对比"标准库多考虑了哪些边界"——这一步收获最大。
3. **每步都写个小测试或 curl 验证**，保持可运行。
4. 不追求完整，**追求理解**。比如 chunked 只要能跑通典型用例即可，不必处理所有 RFC 边界。

---

## 目录结构建议

```
myhttp/
├── server.go      # Server, serveConn, keep-alive 循环 (M1,M5,M7,M8)
├── handler.go     # Handler, HandlerFunc, ServeMux (M2,M3)
├── request.go     # Request, readRequest (M2)
├── response.go    # ResponseWriter, response (M2)
├── transfer.go    # Content-Length / chunked 编解码 (M4)
├── middleware.go  # Logging / Recover / Auth (M6)
├── client.go      # Client, Response, Do (M9)
├── transport.go   # persistConn, readLoop/writeLoop, 连接池 (M10,M11)
└── example/       # 可运行的 demo
```

> 完成度自检：做完 M11，回头读一遍 `transport.go` 的 `roundTrip`——如果你能说出标准库每一处比你多做的考量，说明你真的吃透了。
