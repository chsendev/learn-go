package myhttp

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type HttpServer struct {
	handler Handler
	address string

	// 多层超时（0 表示不超时）
	ReadHeaderTimeout time.Duration // 读完请求行+header 的超时
	ReadTimeout       time.Duration // 读完整个请求（含 body）的超时，从请求开始计时
	WriteTimeout      time.Duration // 写响应的超时
	IdleTimeout       time.Duration // keep-alive 空闲等待下一个请求的超时

	ConnState func(net.Conn, ConnState)

	inShutdown atomic.Bool
	listener   net.Listener
	mu         sync.Mutex
	connState  map[net.Conn]ConnState // 跟踪每个连接的状态
}

func NewHttpServer(address string, handler Handler) *HttpServer {
	return &HttpServer{
		address: address,
		handler: handler,
	}
}

func Handle(pattern string, handler Handler) {
	defaultServeMux.Handle(pattern, handler)
}

func HandleFunc(pattern string, fn HandlerFunc) {
	defaultServeMux.HandleFunc(pattern, fn)
}

func (s *HttpServer) Shutdown(ctx context.Context) error {
	s.inShutdown.Store(true)

	// ① 关 listener，不再接受新连接
	if s.listener != nil {
		s.listener.Close()
	}

	// ② 轮询等待所有连接退出（模仿 net/http）
	pollInterval := time.Millisecond
	timer := time.NewTimer(pollInterval)
	defer timer.Stop()

	for {
		if s.closeIdleConns() {
			return nil // 所有连接都优雅退出了
		}
		select {
		case <-ctx.Done():
			// 超时：只返回 error，不强制关闭活跃连接（模仿 net/http）
			s.mu.Lock()
			active := len(s.connState)
			s.mu.Unlock()
			log.Printf("Shutdown timeout, %d connections still active", active)
			return ctx.Err()
		case <-timer.C:
			// 指数退避
			pollInterval *= 2
			if pollInterval > 500*time.Millisecond {
				pollInterval = 500 * time.Millisecond
			}
			timer.Reset(pollInterval)
		}
	}
}

// closeIdleConns 关闭所有空闲连接，返回 true 表示所有连接都已退出
func (s *HttpServer) closeIdleConns() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 关闭所有空闲连接
	for conn, state := range s.connState {
		if state == StateIdle {
			conn.Close()
			delete(s.connState, conn)
		}
	}

	// 返回 true 表示没有连接了
	return len(s.connState) == 0
}

func (s *HttpServer) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = ln
	s.connState = make(map[net.Conn]ConnState)

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.inShutdown.Load() {
				return nil // 关机导致的关闭，不算错误
			}
			return err
		}
		log.Println("新连接:", conn.RemoteAddr())
		go serveConn(s, conn)
	}
}

func (s *HttpServer) onConnState(conn net.Conn, state ConnState) {
	if s.ConnState != nil {
		s.ConnState(conn, state)
	}
}

func recoverTool() func() {
	return func() {
		r := recover()
		if r != nil {
			log.Println(r)
		}
	}
}

type Request struct {
	Method string
	Path   string
	Proto  string
	Header map[string][]string
	Body   io.Reader

	// ─── M10 客户端侧新增字段 ─────────────────────────────────────
	// 这些字段服务端读请求时用不到，仅在客户端构造请求时填充。
	// 之所以放这里而不是另起类型，是为了贴近 net/http：标准库的
	// Request 同时承担"服务端解析出来的"和"客户端要发出去的"两种角色。

	URL  *url.URL // 客户端请求的目标 URL（含 scheme/host/path/query）
	Host string   // 写到报文里的 Host 头；默认取 URL.Host
	// Close=true 时报文会带 "Connection: close"，且响应读完后连接不放回池子
	Close bool

	// ─── M11 取消 / 超时 / 重试 ───────────────────────────────────
	// ctx 是请求级 context。零值时按 context.Background() 处理。
	// 取消 / 超时都通过它传播：Transport.roundTrip 的 select 会盯着 ctx.Done()。
	ctx context.Context

	// GetBody 用于在安全重试时重新生成一份 body。
	// 由 newRequest 在能识别 body 类型（*bytes.Reader、*strings.Reader、*bytes.Buffer、nil）时自动填好。
	// 用户自己构造 Request 时如果不填，则该请求不可重试（保守策略，避免 body 被部分消费后无法回放）。
	GetBody func() (io.ReadCloser, error)
}

// Context 返回请求绑定的 context。nil 时返回 context.Background()。
func (r *Request) Context() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

// WithContext 返回 r 的浅拷贝，绑定新的 ctx。
// 与 net/http 一样：原 Request 不被修改，便于在调用链里"派生"。
func (r *Request) WithContext(ctx context.Context) *Request {
	if ctx == nil {
		panic("nil ctx")
	}
	r2 := new(Request)
	*r2 = *r
	r2.ctx = ctx
	return r2
}

type Header map[string][]string

type ResponseWriter interface {
	Write(p []byte) (n int, err error)
	Header() Header
	WriteHeader(statusCode int)
}

type response struct {
	writer     *bufio.Writer
	buf        bytes.Buffer
	header     Header
	statusCode int
}

func newResponse(writer *bufio.Writer) *response {
	return &response{header: make(Header), writer: writer}
}

func (r *response) Write(b []byte) (int, error) {
	return r.buf.Write(b)
}

func (r *response) Header() Header {
	return r.header
}

func (r *response) WriteHeader(statusCode int) {
	r.statusCode = statusCode
}

func (r *response) flush() error {
	r.header["Content-Length"] = []string{strconv.Itoa(r.buf.Len())}

	sb := strings.Builder{}
	if r.statusCode == 0 {
		r.statusCode = 200
	}
	sb.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", r.statusCode, statusText(r.statusCode)))
	for k, v := range r.header {
		sb.WriteString(fmt.Sprintf("%s: %s\r\n", k, strings.Join(v, ",")))
	}
	sb.WriteString("\r\n")
	sb.Write(r.buf.Bytes())

	r.writer.Write([]byte(sb.String()))
	return r.writer.Flush()
}

// statusText 把状态码映射成响应行里的文本。
var statusTexts = map[int]string{
	200: "OK",
	201: "Created",
	204: "No Content",
	301: "Moved Permanently",
	302: "Found",
	400: "Bad Request",
	401: "Unauthorized",
	403: "Forbidden",
	404: "Not Found",
	405: "Method Not Allowed",
	500: "Internal Server Error",
	501: "Not Implemented",
	502: "Bad Gateway",
	503: "Service Unavailable",
}

func statusText(code int) string {
	if t, ok := statusTexts[code]; ok {
		return t
	}
	return "Unknown"
}

type Handler interface {
	ServeHTTP(ResponseWriter, *Request)
}

type HandlerFunc func(ResponseWriter, *Request)

func (h HandlerFunc) ServeHTTP(write ResponseWriter, req *Request) {
	h(write, req)
}

type ConnState int

const (
	StateNew ConnState = iota
	StateActivate
	StateIdle
	StateClosed
)

func serveConn(s *HttpServer, conn net.Conn) {
	defer recoverTool()
	defer conn.Close()

	s.trackConn(conn, StateNew)
	defer s.untrackConn(conn)
	defer s.onConnState(conn, StateClosed)

	s.onConnState(conn, StateNew)
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		s.setState(conn, StateIdle)
		s.onConnState(conn, StateIdle)

		if s.inShutdown.Load() {
			return
		}

		// ─── 阶段 0：IdleTimeout，等下一个请求的第一个字节 ───
		// 用 Peek(1) 阻塞等数据到来，同时能感知对端关闭。
		if s.IdleTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(s.IdleTimeout))
		} else {
			conn.SetReadDeadline(time.Time{}) // 清掉之前的 deadline
		}
		if _, err := r.Peek(1); err != nil {
			// 空闲超时 / 对端关闭 / EOF，都从这里退出（属于正常路径）
			log.Println("空闲超时 / 对端关闭 / EOF")
			return
		}
		s.setState(conn, StateActivate)
		s.onConnState(conn, StateActivate)

		// 客户端开始发请求了，记录请求起始时间，
		// 后面 ReadHeaderTimeout 和 ReadTimeout 都从这里计算。
		start := time.Now()

		// ─── 阶段 1：ReadHeaderTimeout，读请求行 + header ───
		if s.ReadHeaderTimeout > 0 {
			conn.SetReadDeadline(start.Add(s.ReadHeaderTimeout))
		}
		req, err := readRequestHead(r)
		if err != nil {
			log.Println("read head:", err)
			return
		}

		// ─── 阶段 2：ReadTimeout，读完整个请求（含 body） ───
		// 注意：deadline 也是从 start 算，所以 ReadTimeout 含义是"整个请求总耗时上限"。
		if s.ReadTimeout > 0 {
			conn.SetReadDeadline(start.Add(s.ReadTimeout))
		}
		if err := readRequestBody(r, req); err != nil {
			log.Println("read body:", err)
			return
		}

		log.Println("请求行：", req.Method, req.Path, req.Proto)
		log.Println("请求头：")
		for k, v := range req.Header {
			log.Println(k, v)
		}

		// ─── handler 处理（期间不读不写网络，清掉 read deadline，避免误伤） ───
		conn.SetReadDeadline(time.Time{})

		rsp := newResponse(w)
		if s.handler == nil {
			defaultServeMux.ServeHTTP(rsp, req)
		} else {
			s.handler.ServeHTTP(rsp, req)
		}

		// ─── 阶段 3：WriteTimeout，写响应 ───
		if s.WriteTimeout > 0 {
			conn.SetWriteDeadline(time.Now().Add(s.WriteTimeout))
		}
		if err := rsp.flush(); err != nil {
			log.Println("write response:", err)
			return
		}
		// 写完清掉 write deadline，准备进入下一轮 idle。
		conn.SetWriteDeadline(time.Time{})

		if shouldClose(req) {
			break
		}
	}
}

func (s *HttpServer) trackConn(conn net.Conn, state ConnState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connState[conn] = state
}

func (s *HttpServer) untrackConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.connState, conn)
}

func (s *HttpServer) setState(conn net.Conn, state ConnState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.connState[conn]; ok {
		s.connState[conn] = state
	}
}

// shouldClose 按 HTTP/1.1 语义判断：
//   - 显式 "Connection: close"  → 关
//   - HTTP/1.1 默认 keep-alive  → 不关
//   - HTTP/1.0 默认 close，除非显式 "Connection: keep-alive" → 不关
func shouldClose(req *Request) bool {
	conn := ""
	if v := req.Header["Connection"]; len(v) > 0 {
		conn = strings.ToLower(strings.TrimSpace(v[0]))
	}
	if conn == "close" {
		return true
	}
	if req.Proto == "HTTP/1.1" {
		return false
	}
	// HTTP/1.0 及以下
	return conn != "keep-alive"
}

// readRequestHead 只读请求行 + header，不读 body。
// 拆出来是为了：① 让 ReadHeaderTimeout 精准覆盖 header 阶段；
//
//	② 给 M4 chunked / 未来客户端复用做准备。
func readRequestHead(r *bufio.Reader) (*Request, error) {
	req := &Request{Header: make(Header)}

	// 1) 请求行
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid request line: %q", line)
	}
	req.Method, req.Path, req.Proto = parts[0], parts[1], parts[2]

	// 2) header，循环到空行
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == "\r\n" {
			break
		}
		kv := strings.SplitN(line, ": ", 2)
		if len(kv) == 2 {
			k := kv[0] // 注：这里没做规范化，后面查 Header 时要注意大小写
			v := strings.TrimRight(kv[1], "\r\n")
			req.Header[k] = append(req.Header[k], v)
		}
	}
	return req, nil
}

// readRequestBody 根据 header 决定怎么读 body，并填到 req.Body。
// 目前只支持 Content-Length；chunked 暂未支持，遇到直接报错避免污染下一个请求。
func readRequestBody(r *bufio.Reader, req *Request) error {
	if te := req.Header["Transfer-Encoding"]; len(te) > 0 && strings.Contains(strings.ToLower(te[0]), "chunked") {
		return fmt.Errorf("chunked transfer-encoding not supported yet")
	}

	var body []byte
	if cl := req.Header["Content-Length"]; len(cl) > 0 {
		n, err := strconv.Atoi(cl[0])
		if err != nil {
			return fmt.Errorf("invalid Content-Length: %v", err)
		}
		if n > 0 {
			body = make([]byte, n)
			if _, err := io.ReadFull(r, body); err != nil {
				return err
			}
		}
	}
	req.Body = bytes.NewReader(body)
	return nil
}

type ServeMux struct {
	m map[string]Handler
}

var defaultServeMux = NewServeMux()

func NewServeMux() *ServeMux {
	return &ServeMux{m: make(map[string]Handler)}
}

func (mux *ServeMux) ServeHTTP(writer ResponseWriter, req *Request) {
	pattern := strings.TrimRight(strings.Split(req.Path, "?")[0], "/")
	if h, ok := mux.m[pattern]; !ok {
		log.Println("no match handle")
		writer.WriteHeader(404)
		writer.Write([]byte("Not found"))
	} else {
		h.ServeHTTP(writer, req)
	}
}

func (mux *ServeMux) Handle(pattern string, handler Handler) {
	mux.m[strings.TrimRight(pattern, "/")] = handler
}

func (mux *ServeMux) HandleFunc(pattern string, fn HandlerFunc) {
	mux.m[strings.TrimRight(pattern, "/")] = fn
}

type Middleware func(Handler) Handler

func Logging(next Handler) Handler {
	return HandlerFunc(func(writer ResponseWriter, req *Request) {
		start := time.Now()

		next.ServeHTTP(writer, req)

		log.Printf("%s %s (%v)",
			req.Method, req.Path, time.Since(start))
	})
}

// Recover 中间件：防止 handler panic 拖垮连接
func Recover(next Handler) Handler {
	return HandlerFunc(func(w ResponseWriter, req *Request) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("panic recovered: %v", r)
				w.WriteHeader(500)
				w.Write([]byte("Internal Server Error"))
			}
		}()
		next.ServeHTTP(w, req)
	})
}

func Chain(h Handler, mws ...Middleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
