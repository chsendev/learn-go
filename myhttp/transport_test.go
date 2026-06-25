package myhttp

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// startTestServer 起一个最小的 myhttp 服务端，返回 addr 和关闭函数。
//
// 不走 ListenAndServe（它会无条件打日志），而是手动 Accept → serveConn。
func startTestServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &HttpServer{
		handler: HandlerFunc(func(w ResponseWriter, r *Request) {
			w.Header()["Content-Type"] = []string{"text/plain"}
			io.WriteString(w, "ok:"+r.Path)
		}),
		connState: make(map[net.Conn]ConnState),
	}
	s.listener = ln
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveConn(s, conn)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

// TestTransport_BasicGet 端到端跑一遍 GET，验证基本正确性。
func TestTransport_BasicGet(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	resp, err := (&Client{}).Get("http://" + addr + "/hello")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok:/hello" {
		t.Fatalf("body=%q", body)
	}
}

// TestTransport_ConnectionReuse 验证 M10 的核心目标：连接池能复用 TCP。
//
// 思路：对同一 host 串行发 N 个请求，期望池子里最终只剩 1 条空闲连接 ——
// 说明所有请求都在复用这一条物理连接。
func TestTransport_ConnectionReuse(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	tr := &Transport{
		DialTimeout:         2 * time.Second,
		MaxIdleConnsPerHost: 2,
	}
	client := &Client{Transport: tr}

	const N = 5
	for i := 0; i < N; i++ {
		u := "http://" + addr + "/p" + itoa(i)
		resp, err := client.Get(u)
		if err != nil {
			t.Fatalf("req %d: %v", i, err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("req %d read: %v", i, err)
		}
		resp.Body.Close()
		if !strings.HasPrefix(string(body), "ok:") {
			t.Fatalf("req %d: unexpected body %q", i, body)
		}
		// 等一下让 readLoop 把连接放回池子（异步）
		time.Sleep(10 * time.Millisecond)
	}

	tr.mu.Lock()
	totalIdle := 0
	for _, list := range tr.idleConn {
		totalIdle += len(list)
	}
	tr.mu.Unlock()

	if totalIdle != 1 {
		t.Fatalf("expect 1 idle conn after %d requests (reuse failed), got %d",
			N, totalIdle)
	}
}

// TestTransport_CloseIdleConnections 验证手动关闭池子能清空。
func TestTransport_CloseIdleConnections(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	tr := &Transport{}
	client := &Client{Transport: tr}

	resp, err := client.Get("http://" + addr + "/x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	time.Sleep(10 * time.Millisecond)

	tr.mu.Lock()
	idleBefore := 0
	for _, l := range tr.idleConn {
		idleBefore += len(l)
	}
	tr.mu.Unlock()
	if idleBefore != 1 {
		t.Fatalf("expect 1 idle before close, got %d", idleBefore)
	}

	tr.CloseIdleConnections()

	tr.mu.Lock()
	idleAfter := 0
	for _, l := range tr.idleConn {
		idleAfter += len(l)
	}
	tr.mu.Unlock()
	if idleAfter != 0 {
		t.Fatalf("expect 0 idle after close, got %d", idleAfter)
	}
}

// itoa: 测试用的小工具，避免 import strconv。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[n:])
}

// ─── M11 测试：超时 / 取消 / 安全重试 ───────────────────────────────

// TestClient_Timeout 验证 Client.Timeout 能把"卡住的服务端"超时掉。
//
// 思路：服务端 accept 后什么也不回（不发 status line），客户端在 Peek 里挂着；
// 超时一到，roundTrip 应该通过 ctx.Done() 分支退出，返回 deadline exceeded。
func TestClient_Timeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		// 接受连接但不响应，让 client 一直读不到东西
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// 保持连接开着，等 client 超时
			go func(c net.Conn) {
				time.Sleep(2 * time.Second)
				c.Close()
			}(c)
		}
	}()

	client := &Client{Timeout: 200 * time.Millisecond}
	start := time.Now()
	_, err = client.Get("http://" + ln.Addr().String() + "/")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expect timeout error, got nil")
	}
	// err 是 context.DeadlineExceeded 或包了它的 error
	if !strings.Contains(err.Error(), "deadline exceeded") &&
		!strings.Contains(err.Error(), "context deadline") {
		t.Fatalf("expect deadline error, got: %v", err)
	}
	// 真的是被超时打断的（而不是其他原因立刻失败）
	if elapsed < 150*time.Millisecond || elapsed > 1*time.Second {
		t.Fatalf("unexpected elapsed: %v", elapsed)
	}
	t.Logf("timed out as expected, err=%v, elapsed=%v", err, elapsed)
}

// TestClient_ContextCancel 验证调用方主动 cancel context 也能中断请求。
func TestClient_ContextCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				time.Sleep(2 * time.Second)
				c.Close()
			}(c)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	// 100ms 后主动 cancel
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	req, err := newRequest("GET", "http://"+ln.Addr().String()+"/", nil)
	if err != nil {
		t.Fatalf("newRequest: %v", err)
	}
	req = req.WithContext(ctx)

	start := time.Now()
	_, err = (&Client{}).Do(req)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expect cancel error, got nil")
	}
	if !strings.Contains(err.Error(), "canceled") &&
		!strings.Contains(err.Error(), "context") {
		t.Fatalf("expect cancel error, got: %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("cancel did not take effect promptly, elapsed=%v", elapsed)
	}
	t.Logf("canceled as expected, err=%v, elapsed=%v", err, elapsed)
}

// TestTransport_RetryOnDeadIdleConn 验证 M11 的核心：
// keep-alive 池里的连接被对端关掉时，幂等请求能静默重试到一条新连接成功。
//
// 方法：直接构造一条"看起来空闲但底层已经死"的 persistConn 塞进池子。
// 用 net.Pipe 拿一对内存连接，把 server 端立刻 Close，client 端塞池子——
// getConn 会优先取池里这条死连接 → writeLoop 写 EPIPE → roundTrip 返回错误 →
// 重试逻辑判定可重试（reused=true, nwrite=0, GET 幂等）→ 重新 getConn → Dial 新连接 → 成功。
func TestTransport_RetryOnDeadIdleConn(t *testing.T) {
	// 启动一个真实能响应的目标服务端
	var hits int32
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				atomic.AddInt32(&hits, 1)
				// 极简：读到 \r\n\r\n 就回固定响应
				buf := make([]byte, 4096)
				total := []byte{}
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					total = append(total, buf[:n]...)
					if strings.Contains(string(total), "\r\n\r\n") {
						break
					}
				}
				io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
			}(c)
		}
	}()

	tr := &Transport{DialTimeout: 2 * time.Second}
	client := &Client{Transport: tr}
	addr := ln.Addr().String()

	// 制造一条"对端已 Close 的内存连接"塞进池子
	deadClient, deadServer := net.Pipe()
	deadServer.Close() // 写 deadClient 会立即报错

	u, _ := url.Parse("http://" + addr + "/")
	cm, _ := connectMethodForRequest(&Request{URL: u})

	dead := &persistConn{
		t:       tr,
		cm:      cm,
		conn:    deadClient,
		br:      bufio.NewReader(deadClient),
		bw:      bufio.NewWriter(deadClient),
		reqch:   make(chan requestAndChan, 1),
		writech: make(chan writeReq, 1),
		closech: make(chan struct{}),
	}
	go dead.readLoop()
	go dead.writeLoop()

	// 强制塞进池子（绕过 isBroken 检查）
	tr.mu.Lock()
	if tr.idleConn == nil {
		tr.idleConn = make(map[connectMethod][]*persistConn)
	}
	tr.idleConn[cm] = append(tr.idleConn[cm], dead)
	tr.mu.Unlock()

	// 发请求：取出 dead → 写失败 → 重试 → Dial 新连接 → 200
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("expect retry success, got err=%v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "hello" {
		t.Fatalf("body=%q", body)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("expect exactly 1 real server hit (only the retry), got %d", got)
	}
	t.Logf("retry success: real server hits=%d", atomic.LoadInt32(&hits))
}

// TestTransport_NoRetryForNonIdempotent 验证非幂等方法（POST）即使遇到
// "池里连接死了"也不会重试——避免"重复扣款"这种致命场景。
//
// （注：newRequest 给 *strings.Reader body 会自动填 GetBody，
//
//	但 isIdempotent("POST")=false，shouldRetryRequest 也会拒绝重试。）
func TestTransport_NoRetryForNonIdempotent(t *testing.T) {
	tr := &Transport{}

	deadClient, deadServer := net.Pipe()
	deadServer.Close()

	// 池子里塞死连接（host 随便指一个；反正不会真发出去）
	cm := connectMethod{scheme: "http", addr: "127.0.0.1:1"}
	dead := &persistConn{
		t:       tr,
		cm:      cm,
		conn:    deadClient,
		br:      bufio.NewReader(deadClient),
		bw:      bufio.NewWriter(deadClient),
		reqch:   make(chan requestAndChan, 1),
		writech: make(chan writeReq, 1),
		closech: make(chan struct{}),
	}
	go dead.readLoop()
	go dead.writeLoop()

	tr.mu.Lock()
	tr.idleConn = map[connectMethod][]*persistConn{cm: {dead}}
	tr.mu.Unlock()

	req, err := newRequest("POST", "http://"+cm.addr+"/", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("newRequest: %v", err)
	}
	client := &Client{Transport: tr}
	_, err = client.Do(req)
	if err == nil {
		t.Fatal("expect POST to fail without retry, got nil")
	}
	t.Logf("POST correctly failed without retry: %v", err)
}
