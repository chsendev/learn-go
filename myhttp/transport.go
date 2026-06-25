package myhttp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// ─── M10 Transport：连接池 + readLoop/writeLoop 分离 ──────────────────
//
// 这一层负责"一次 HTTP 事务"的全部底层细节：
//   - 建/取连接（连接池）
//   - 把 Request 写到 wire
//   - 从 wire 读回 Response
//
// 关键设计（与 net/http 对齐）：
//
//  1. 一条物理连接 = 一个 *persistConn，建立时启动两个常驻 goroutine：
//       writeLoop —— 从 writech 取请求，序列化写出去
//       readLoop  —— 用 bufio.Peek(1) 阻塞等待 / 探测连接死活，读到就解析
//     上层 RoundTrip 不直接碰 conn，而是通过 channel 与这两个 loop 通信。
//
//  2. 为什么必须分两个 goroutine？
//     HTTP/1.1 的请求/响应虽然在单条连接上是串行的，但 readLoop 还承担着
//     一个独立职责：在连接空闲时阻塞 Peek(1)，用来感知"对端是否主动关了
//     这条连接"——一旦 Peek 返回 EOF 就把连接标记为 broken。
//     如果读写挤在同一个 goroutine 里，就没法既"写请求"又"探活"了。
//
//  3. 连接池：map[connectMethod][]*persistConn，按 host:port 分组，LIFO。
//     用完先调 isBroken() 健康检查，再放回。

// ─── 配置 ────────────────────────────────────────────────────────────

// Transport 是 RoundTripper 的实现，管理底层 TCP 连接的复用。
//
// 零值可用；并发安全。
type Transport struct {
	// DialTimeout TCP 握手超时（0 表示不限制）。
	DialTimeout time.Duration

	// MaxIdleConnsPerHost 每个 host 最多保留多少条空闲连接。0 表示用默认值。
	MaxIdleConnsPerHost int

	mu       sync.Mutex
	idleConn map[connectMethod][]*persistConn // 空闲连接池（LIFO）
	closed   bool
}

// DefaultTransport 全局默认 Transport，类似 net/http.DefaultTransport。
var DefaultTransport = &Transport{
	DialTimeout:         5 * time.Second,
	MaxIdleConnsPerHost: 2,
}

// connectMethod 是连接池的 key：scheme + host:port。
//
// 抽象成单独类型是为了将来扩展（比如加 proxy、TLS 配置时 key 会变复杂）。
type connectMethod struct {
	scheme string // "http"
	addr   string // "host:port"
}

func (cm connectMethod) String() string { return cm.scheme + "://" + cm.addr }

// ─── RoundTrip：对外入口 ──────────────────────────────────────────────

// RoundTrip 执行一次 HTTP 事务：取/建连接 → 发请求 → 收响应。
//
// 这里是 M11 的重试外壳：
//   - 同一个请求最多尝试 maxRetries 次；
//   - 仅当满足"可安全重试"的所有条件时才会真的进入下一轮（详见 shouldRetryRequest）；
//   - 重试前会调 req.GetBody() 重新生成 body，避免 body 已被部分消费。
//
// 注意：它只负责"一来一回"，不处理重定向、cookie、超时这些高层语义
// （那是 Client 的事，呼应 net/http 中 Client vs Transport 的分工）。
func (t *Transport) RoundTrip(req *Request) (*Response, error) {
	if req.URL == nil {
		return nil, errors.New("myhttp: Request.URL is nil")
	}
	cm, err := connectMethodForRequest(req)
	if err != nil {
		return nil, err
	}

	// 重试循环。
	// 上限设小（2）：在 keep-alive 池子的"竞态窗口"里捡一次漏就够了，
	// 真的网络坏掉就别死磕，让调用方自己决定。
	const maxRetries = 2
	for attempt := 0; ; attempt++ {
		// 每轮重试前要先检查 ctx：超时/取消后不应该再发新请求
		if err := req.Context().Err(); err != nil {
			return nil, err
		}

		// 1) 拿一条可用连接，并知道它是不是池里复用来的
		pc, reused, err := t.getConn(cm)
		if err != nil {
			return nil, err
		}

		// 2) 真正发请求 / 收响应
		resp, err := pc.roundTrip(req)
		if err == nil {
			return resp, nil
		}

		// 3) 失败：判断要不要重试
		if attempt+1 >= maxRetries || !shouldRetryRequest(req, reused, pc, err) {
			return nil, err
		}

		// 4) 重新准备 body
		if req.GetBody != nil {
			newBody, gerr := req.GetBody()
			if gerr != nil {
				return nil, err // 拿不到新 body，原错误返回
			}
			req.Body = newBody
		}
		// 继续 for 下一轮，会重新 getConn（之前那条已经在 roundTrip 内 close 了）
	}
}

// shouldRetryRequest 判定"上一轮失败后是否允许重试"。
//
// ⭐ 这是 M11 的精髓：keep-alive 复用连接时存在一个无法消除的竞态窗口——
// 我们从池里拿到一条连接的瞬间，对端可能已经主动 FIN 了，但我们还来不及感知。
// 只要本次还没有任何字节真正被对端 ACK / 进入业务处理，重试这一次就是安全的。
//
// 必须同时满足：
//
//  1. 连接是从池里复用来的（新建连接失败说明根源就坏了，重试无意义）。
//  2. 本次还没写出任何字节（nwrite==0）—— 否则服务端可能已经开始处理，
//     重试会造成"重复请求"，对非幂等方法是致命的。
//  3. 请求方法幂等，或调用方提供了 GetBody（即 body 可重放）。
func shouldRetryRequest(req *Request, reused bool, pc *persistConn, err error) bool {
	if !reused {
		return false
	}
	// 任何字节都没真正写出去：可以重试
	if pc.bytesWritten() > 0 {
		return false
	}
	// context 已被取消/超时：别再重试
	if req.Context().Err() != nil {
		return false
	}
	// 幂等方法（无副作用），重试天然安全
	if isIdempotent(req.Method) {
		return true
	}
	// 非幂等方法仅当 body 可重放时才考虑（其实标准库这里更严格，
	// 默认不重试 POST；我们这里跟标准库一致：非幂等就不重试）
	_ = err
	return false
}

func isIdempotent(method string) bool {
	switch method {
	case "GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE":
		return true
	}
	return false
}

// connectMethodForRequest 从 Request 算出连接池的 key。
func connectMethodForRequest(req *Request) (connectMethod, error) {
	u := req.URL
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" {
		// 本项目暂不实现 TLS（M12 才考虑）
		return connectMethod{}, fmt.Errorf("myhttp: unsupported scheme %q", u.Scheme)
	}
	addr := u.Host
	if !strings.Contains(addr, ":") {
		addr += ":80"
	}
	return connectMethod{scheme: scheme, addr: addr}, nil
}

// ─── 连接池：取与放 ──────────────────────────────────────────────────

// getConn 先从池里取，没有可用的就新建。
// 返回的 reused=true 表示这条连接是从池里复用来的——这个信息上层判断"能否安全重试"要用。
func (t *Transport) getConn(cm connectMethod) (pc *persistConn, reused bool, err error) {
	// 1) 尝试从池里复用
	if pc := t.tryIdleConn(cm); pc != nil {
		return pc, true, nil
	}
	// 2) 池里没有 → Dial 新连接
	pc, err = t.dialConn(cm)
	return pc, false, err
}

// tryIdleConn 从池里弹出一条还能用的连接（LIFO）。
//
// LIFO 是个细节但重要：最近放回的连接"温度"最高，TCP 拥塞窗口大、
// 对端没断连的概率也大。FIFO 反而容易把冷连接派出去。
func (t *Transport) tryIdleConn(cm connectMethod) *persistConn {
	t.mu.Lock()
	defer t.mu.Unlock()

	conns := t.idleConn[cm]
	for len(conns) > 0 {
		// 弹栈顶
		pc := conns[len(conns)-1]
		conns = conns[:len(conns)-1]
		if pc.isBroken() {
			// 已损坏（对端关了 / readLoop 退出），扔掉继续找
			pc.close()
			continue
		}
		t.idleConn[cm] = conns
		return pc
	}
	if len(conns) == 0 {
		delete(t.idleConn, cm)
	}
	return nil
}

// putIdleConn 把用完的连接放回池子。
//
// 调用时机：roundTrip 读完响应 body 之后（由 bodyEOFSignal 触发）。
func (t *Transport) putIdleConn(pc *persistConn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || pc.isBroken() {
		return false
	}

	if t.idleConn == nil {
		t.idleConn = make(map[connectMethod][]*persistConn)
	}
	max := t.MaxIdleConnsPerHost
	if max == 0 {
		max = 2
	}
	conns := t.idleConn[pc.cm]
	if len(conns) >= max {
		// 池满，关掉多余的（不要无限堆积）
		return false
	}
	t.idleConn[pc.cm] = append(conns, pc)
	return true
}

// dialConn 建一条新连接，并启动 read/write 两个 loop。
func (t *Transport) dialConn(cm connectMethod) (*persistConn, error) {
	d := net.Dialer{Timeout: t.DialTimeout}
	conn, err := d.Dial("tcp", cm.addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cm.addr, err)
	}

	pc := &persistConn{
		t:       t,
		cm:      cm,
		conn:    conn,
		br:      bufio.NewReader(conn),
		bw:      bufio.NewWriter(conn),
		reqch:   make(chan requestAndChan, 1),
		writech: make(chan writeReq, 1),
		closech: make(chan struct{}),
	}
	go pc.readLoop()
	go pc.writeLoop()
	return pc, nil
}

// CloseIdleConnections 关闭池里所有空闲连接（活跃连接不受影响）。
func (t *Transport) CloseIdleConnections() {
	t.mu.Lock()
	conns := t.idleConn
	t.idleConn = nil
	t.mu.Unlock()
	for _, list := range conns {
		for _, pc := range list {
			pc.close()
		}
	}
}

// ─── persistConn：一条可复用连接 + 两个 goroutine ────────────────────

// persistConn 表示一条仍可使用的物理连接。
type persistConn struct {
	t    *Transport
	cm   connectMethod
	conn net.Conn
	br   *bufio.Reader // 包了 conn，readLoop 用
	bw   *bufio.Writer // 包了 conn，writeLoop 用

	// reqch：roundTrip 把请求交给 readLoop（让它知道该读响应了）
	reqch chan requestAndChan
	// writech：roundTrip 把请求交给 writeLoop（让它写到 wire）
	writech chan writeReq

	closech   chan struct{} // 关闭时 close(此 channel)，所有 loop 收到信号退出
	closeOnce sync.Once

	mu     sync.Mutex
	broken bool // 已发生不可恢复错误，禁止复用
	// nwrite 记录"自这条连接建立以来，writeLoop 成功 Flush 出去过多少个请求"。
	// M11 用它判断"本次失败时是否已经把请求字节真正写出"——
	// 没写出就可以安全重试（典型场景：池里复用的连接其实已被对端关掉）。
	nwrite int64
}

// requestAndChan 是 roundTrip → readLoop 的"工单"：
// 告诉 readLoop "我已经发了一个请求，等会儿响应回来请通过 ch 回传给我"。
type requestAndChan struct {
	req *Request
	ch  chan responseAndError
}

type responseAndError struct {
	resp *Response
	err  error
}

// writeReq 是 roundTrip → writeLoop 的工单。
type writeReq struct {
	req     *Request
	resultc chan error // 写完后把结果发回来
}

func (pc *persistConn) isBroken() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.broken
}

func (pc *persistConn) markBroken() {
	pc.mu.Lock()
	pc.broken = true
	pc.mu.Unlock()
}

// bytesWritten 返回这条连接迄今成功 Flush 过的请求次数。
// 用于 M11 的"是否已写出字节"判定。
func (pc *persistConn) bytesWritten() int64 {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.nwrite
}

func (pc *persistConn) close() {
	pc.closeOnce.Do(func() {
		pc.markBroken()
		close(pc.closech)
		_ = pc.conn.Close()
	})
}

// ─── roundTrip：一次请求/响应的协调中心 ───────────────────────────────

// roundTrip 把请求派给 writeLoop / readLoop，等响应回来。
//
// 这里是整个 Transport 的灵魂：通过 select 把多种结果统一处理。
// M11 在这里多加了一路 ctx.Done()——超时/取消时立即关闭连接，
// 让 readLoop/writeLoop 也跟着退出，避免 goroutine 泄漏。
func (pc *persistConn) roundTrip(req *Request) (*Response, error) {
	writeErrCh := make(chan error, 1)
	respCh := make(chan responseAndError, 1)

	// 1) 通知 writeLoop 把请求写出去
	pc.writech <- writeReq{req: req, resultc: writeErrCh}
	// 2) 通知 readLoop 该读响应了
	pc.reqch <- requestAndChan{req: req, ch: respCh}

	ctx := req.Context()
	ctxDoneCh := ctx.Done() // 缓存一下，nil ctx 时是 nil channel，select 永远不会命中

	// 3) 多路等待：写错误 / 响应到达 / 连接关闭 / ctx 取消
	for {
		select {
		case err := <-writeErrCh:
			if err != nil {
				pc.markBroken()
				pc.close()
				return nil, fmt.Errorf("write request: %w", err)
			}
			// 写成功了，继续等响应（不 return，进入下一轮 select）
			writeErrCh = nil // 防止下次 select 又命中这个零值 channel

		case re := <-respCh:
			if re.err != nil {
				pc.markBroken()
				pc.close()
				return nil, re.err
			}
			return re.resp, nil

		case <-pc.closech:
			return nil, errors.New("myhttp: connection closed before response")

		case <-ctxDoneCh:
			// ctx 取消 / 超时：主动把连接关掉，readLoop/writeLoop 会感知到
			// 然后通过上面两个分支退出 select。但我们这里直接返回 ctx.Err()，
			// 不再等它们——因为对调用方来说"已经超时了"就是终局。
			//
			// 这条连接已经不能复用了（写到一半 / 读到一半都可能）。
			pc.markBroken()
			pc.close()
			return nil, ctx.Err()
		}
	}
}

// ─── writeLoop：常驻 goroutine，串行把请求写到 wire ──────────────────

func (pc *persistConn) writeLoop() {
	for {
		select {
		case wr := <-pc.writech:
			err := writeRequest(pc.bw, wr.req, wr.req.Host, wr.req.URL.RequestURI())
			if err == nil {
				err = pc.bw.Flush()
			}
			if err == nil {
				// 这一次的字节是真的进 wire 了，记账。
				// 用于 M11 的安全重试判定：bytesWritten>0 表示对端可能已开始处理，不可重试。
				pc.mu.Lock()
				pc.nwrite++
				pc.mu.Unlock()
			}
			wr.resultc <- err
			if err != nil {
				// 写失败：让 readLoop 也尽快退出
				pc.close()
				return
			}
		case <-pc.closech:
			return
		}
	}
}

// ─── readLoop：常驻 goroutine，串行解析响应 ──────────────────────────

func (pc *persistConn) readLoop() {
	// readLoop 退出 = 这条连接不能再用了
	defer pc.close()

	alive := true
	for alive {
		// ⭐ Peek(1) 的双重语义：
		//   - 阻塞等待对端的下一个字节（响应到达）
		//   - 如果对端关闭了连接，会立刻返回 EOF（探活）
		//   且 Peek 不消费字节，后续 readResponse 还能从头解析。
		_, err := pc.br.Peek(1)

		// 等一个"已经发出请求、正在等响应"的工单
		var rc requestAndChan
		select {
		case rc = <-pc.reqch:
			// 拿到了，下面真正解析响应
		case <-pc.closech:
			return
		}

		if err != nil {
			// 连接断了 / 对端关了：把错误回传给 roundTrip
			rc.ch <- responseAndError{err: fmt.Errorf("read response: %w", err)}
			return
		}

		// 解析响应
		resp, err := readResponse(pc.br, pc.conn)
		if err != nil {
			rc.ch <- responseAndError{err: fmt.Errorf("read response: %w", err)}
			return
		}

		// 把 body 包一层：等用户读完 / Close 时，再决定要不要把连接放回池子
		waitForBodyRead := make(chan bool, 1)
		resp.Body = &bodyEOFSignal{
			body: resp.Body,
			fn: func(err error) error {
				// err == nil 说明读到了 EOF 且没出错 → 可复用
				alive := err == nil && !rc.req.Close && !pc.isBroken()
				waitForBodyRead <- alive
				return nil
			},
		}

		rc.ch <- responseAndError{resp: resp}

		// 等用户读完 body（或主动 Close）
		select {
		case alive = <-waitForBodyRead:
			if alive {
				if !pc.t.putIdleConn(pc) {
					alive = false
				}
			}
		case <-pc.closech:
			return
		}
	}
}

// ─── bodyEOFSignal：在 body 关闭/读完时回调 ──────────────────────────
//
// 这是连接池能"自动放回"的关键：用户看到的 resp.Body 是这个 wrapper，
// 它把读到 EOF 或 Close() 当成"请求完成"的信号，通知 readLoop。

type bodyEOFSignal struct {
	body     io.ReadCloser
	mu       sync.Mutex
	closed   bool
	rerr     error                 // Read 时出现的最后一个错误
	fn       func(err error) error // 完成回调（只会被调一次）
	earlyErr error                 // Close 早于 EOF 时记下来
}

func (es *bodyEOFSignal) Read(p []byte) (n int, err error) {
	n, err = es.body.Read(p)
	if err != nil {
		es.mu.Lock()
		defer es.mu.Unlock()
		if es.rerr == nil {
			es.rerr = err
		}
		es.condfn(err)
	}
	return
}

func (es *bodyEOFSignal) Close() error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.closed {
		return nil
	}
	es.closed = true
	err := es.body.Close()
	// 如果之前没读到 EOF 就 Close，认为连接不能复用
	if es.rerr == nil {
		es.rerr = errors.New("body closed before EOF")
	}
	es.condfn(es.rerr)
	return err
}

// condfn 确保 fn 只被触发一次。
func (es *bodyEOFSignal) condfn(err error) {
	if es.fn == nil {
		return
	}
	if err == io.EOF {
		es.fn(nil) // 正常读完 → 可复用
	} else {
		es.fn(err) // 异常 → 不可复用
	}
	es.fn = nil
}
