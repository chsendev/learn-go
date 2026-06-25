package myhttp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ─── 客户端高层 API ──────────────────────────────────────────────────
//
// 分层（与 net/http 对齐）：
//
//   Client      ← 高层语义：URL 解析、便捷方法、（未来的）重定向 / 超时 / cookie
//      ↓ 调用
//   Transport   ← 底层 IO：连接池、Dial、readLoop/writeLoop
//      ↓ 操作
//   persistConn ← 一条物理 TCP
//
// Client 自己不再碰 net.Dial，所有 IO 交给 Transport（M10）。

// RoundTripper 是 Transport 的抽象：执行"一次请求一次响应"。
//
// 抽象这个接口是为了将来可以注入 mock Transport 做测试（M12 提到的 httptest）。
type RoundTripper interface {
	RoundTrip(*Request) (*Response, error)
}

// Client 最小 HTTP 客户端。零值可用（会使用 DefaultTransport）。
type Client struct {
	// Transport 为 nil 时使用 DefaultTransport。
	Transport RoundTripper

	// Timeout 为整个请求设置上限（含连接获取、写请求、读响应、读 body）。
	// 0 表示不限制。内部实现是用 context.WithTimeout 包一层 req.Context()。
	//
	// 注意（与 net/http 一致的取舍）：超时触发时正在读的 body 也会被 cancel，
	// 用户拿到的 err 是 context.DeadlineExceeded。
	Timeout time.Duration
}

// transport 返回当前生效的 RoundTripper。
func (c *Client) transport() RoundTripper {
	if c.Transport != nil {
		return c.Transport
	}
	return DefaultTransport
}

// Response 表示一次 HTTP 响应。
//
// 调用方读完后必须 Close Body，否则连接无法放回连接池（会泄漏）。
type Response struct {
	Proto      string // e.g. "HTTP/1.1"
	StatusCode int    // e.g. 200
	Status     string // e.g. "OK"
	Header     Header
	Body       io.ReadCloser
}

// Get 是 Client.Do 的便捷封装。
func (c *Client) Get(rawurl string) (*Response, error) {
	req, err := newRequest("GET", rawurl, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Post 发起一个 POST 请求。body 可为 nil。
func (c *Client) Post(rawurl, contentType string, body io.Reader) (*Response, error) {
	req, err := newRequest("POST", rawurl, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header["Content-Type"] = []string{contentType}
	}
	return c.Do(req)
}

// Do 执行一次请求。底层调用 Transport.RoundTrip。
//
// 处理顺序：
//  1. 字段默认值（Method/Proto/Header/Host）
//  2. 应用 Client.Timeout（用 context.WithTimeout 派生）
//  3. 调用 Transport.RoundTrip
//  4. 包一层 body：body 关闭时取消 timeout context（避免 goroutine 泄漏）
func (c *Client) Do(req *Request) (*Response, error) {
	if req.URL == nil {
		return nil, errors.New("myhttp: Request.URL is nil")
	}
	if req.Method == "" {
		req.Method = "GET"
	}
	if req.Proto == "" {
		req.Proto = "HTTP/1.1"
	}
	if req.Header == nil {
		req.Header = make(Header)
	}
	if req.Host == "" {
		req.Host = req.URL.Host
	}

	// 应用 Timeout：派生一个会自动取消的 ctx。
	// 这里有个细节：cancel 不能在 Do 返回时立刻调，必须等用户读完/关闭 body 之后。
	// 否则 ctx 一被取消，body 还没读完就会被打断。
	var cancel context.CancelFunc
	if c.Timeout > 0 {
		ctx, c2 := context.WithTimeout(req.Context(), c.Timeout)
		cancel = c2
		req = req.WithContext(ctx)
	}

	resp, err := c.transport().RoundTrip(req)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, err
	}
	// 成功路径：把 cancel 挂到 body 上，body Close 时再触发，保证 ctx 一定会被释放
	if cancel != nil {
		resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
	}
	return resp, nil
}

// cancelOnClose 是给 Response.Body 套的一层，
// 让用户 Close body 时顺带 cancel 掉 Client.Timeout 派生的 ctx。
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   bool
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	if !c.once {
		c.once = true
		c.cancel()
	}
	return err
}

// ─── 内部辅助 ─────────────────────────────────────────────────────

// newRequest 构造一个客户端用的 Request，把 URL 拆分填好。
//
// M11：对已知"可重放"的 body 类型自动生成 GetBody，
// 让 GET/PUT/DELETE 等幂等请求在 keep-alive 竞态窗口里可以被安全重试。
// 不能识别的类型（如任意 io.Reader）则 GetBody=nil，等同"不可重试"。
func newRequest(method, rawurl string, body io.Reader) (*Request, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("missing host in url: %q", rawurl)
	}

	req := &Request{
		Method: method,
		Path:   u.RequestURI(), // 形如 "/a?x=1"
		Proto:  "HTTP/1.1",
		Header: make(Header),
		URL:    u,
		Host:   u.Host,
	}

	// 根据 body 的具体类型生成可重放快照 + GetBody。
	switch v := body.(type) {
	case nil:
		req.Body = strings.NewReader("")
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("")), nil
		}
	case *bytes.Buffer:
		// 快照成 []byte，避免 v 被 Read 后无法重放
		buf := v.Bytes()
		req.Body = bytes.NewReader(buf)
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(buf)), nil
		}
	case *bytes.Reader:
		// *bytes.Reader 自己支持 Seek，但更直接：拍一份 []byte
		// （v.Len() 此时是"剩余字节数"，把它读出来快照）
		buf, _ := io.ReadAll(v)
		req.Body = bytes.NewReader(buf)
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(buf)), nil
		}
	case *strings.Reader:
		s, _ := io.ReadAll(v)
		req.Body = bytes.NewReader(s)
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(s)), nil
		}
	default:
		// 任意 io.Reader：保守起见不生成 GetBody（不可重试）
		req.Body = body
	}
	return req, nil
}

// writeRequest 把 Request 序列化成 HTTP/1.1 报文写到 bw。
//
// 注意：调用方负责 Flush（writeLoop 里在这之后会主动 Flush）。
func writeRequest(bw *bufio.Writer, req *Request, host, reqPath string) error {
	// 1) 请求行
	if _, err := fmt.Fprintf(bw, "%s %s %s\r\n", req.Method, reqPath, req.Proto); err != nil {
		return err
	}
	// 2) Host
	if _, err := fmt.Fprintf(bw, "Host: %s\r\n", host); err != nil {
		return err
	}
	// 3) Body 长度（简化：全部读到内存）
	var bodyBytes []byte
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return err
		}
		bodyBytes = b
	}
	if len(bodyBytes) > 0 {
		if _, err := fmt.Fprintf(bw, "Content-Length: %d\r\n", len(bodyBytes)); err != nil {
			return err
		}
	}
	// 4) 用户自定义 header（去掉与上面重复的）
	for k, vs := range req.Header {
		if strings.EqualFold(k, "Host") ||
			strings.EqualFold(k, "Content-Length") ||
			strings.EqualFold(k, "Connection") {
			continue
		}
		for _, v := range vs {
			if _, err := fmt.Fprintf(bw, "%s: %s\r\n", k, v); err != nil {
				return err
			}
		}
	}
	// 5) Connection 头：默认 keep-alive；req.Close=true 才发 close
	if req.Close {
		if _, err := bw.WriteString("Connection: close\r\n"); err != nil {
			return err
		}
	}
	// 6) 空行 + body
	if _, err := bw.WriteString("\r\n"); err != nil {
		return err
	}
	if len(bodyBytes) > 0 {
		if _, err := bw.Write(bodyBytes); err != nil {
			return err
		}
	}
	return nil
}

// readResponse 解析 HTTP 响应。
//
// 与服务端 readRequestHead 对称：状态行 → header → body。
// body 根据 Content-Length / Connection: close 决定边界。
func readResponse(br *bufio.Reader, conn net.Conn) (*Response, error) {
	// 1) 状态行：HTTP/1.1 200 OK
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid status line: %q", line)
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid status code %q: %w", parts[1], err)
	}
	rsp := &Response{
		Proto:      parts[0],
		StatusCode: code,
		Status:     parts[2],
		Header:     make(Header),
	}
	// 2) header 直到空行
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == "\r\n" {
			break
		}
		kv := strings.SplitN(line, ": ", 2)
		if len(kv) == 2 {
			k := kv[0]
			v := strings.TrimRight(kv[1], "\r\n")
			rsp.Header[k] = append(rsp.Header[k], v)
		}
	}
	// 3) body：按 Content-Length 限长；否则读到 EOF
	var body io.Reader
	connClose := false
	for _, v := range rsp.Header["Connection"] {
		if strings.EqualFold(strings.TrimSpace(v), "close") {
			connClose = true
		}
	}
	if cl := rsp.Header["Content-Length"]; len(cl) > 0 {
		n, err := strconv.Atoi(cl[0])
		if err != nil {
			return nil, fmt.Errorf("invalid Content-Length: %w", err)
		}
		body = io.LimitReader(br, int64(n))
	} else {
		// 没 Content-Length：读到 EOF（说明服务端是 Connection: close 模式）
		body = br
		connClose = true
	}
	rsp.Body = &bodyReader{r: body, conn: conn, mustClose: connClose}
	return rsp, nil
}

// bodyReader 是底层 body 读取器。
//
// 注意：M9 时这里负责关 conn；M10 之后连接由 Transport 管，所以 Close
// 不再无脑关 conn——只在 mustClose（Connection: close）时关。
// 实际放回连接池的逻辑由外层 bodyEOFSignal 完成。
type bodyReader struct {
	r         io.Reader
	conn      net.Conn
	mustClose bool
}

func (b *bodyReader) Read(p []byte) (int, error) { return b.r.Read(p) }
func (b *bodyReader) Close() error {
	if b.mustClose && b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

// ─── 包级便捷函数 ────────────────────────────────────────────────────

// DefaultClient 全局默认客户端。
var DefaultClient = &Client{}

// Get 包级便捷函数。
func Get(rawurl string) (*Response, error) { return DefaultClient.Get(rawurl) }

// Post 包级便捷函数。
func Post(rawurl, contentType string, body io.Reader) (*Response, error) {
	return DefaultClient.Post(rawurl, contentType, body)
}
