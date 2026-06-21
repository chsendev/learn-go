package myhttp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

type HttpServer struct {
	handler Handler
	address string
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

func (s *HttpServer) ListenAndServe() {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		log.Fatal(err)
	}

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		log.Println("新连接:", conn.RemoteAddr())
		go serveConn(s, conn)
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
	//"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"
	r.header["Content-Length"] = []string{strconv.Itoa(r.buf.Len())}

	sb := strings.Builder{}
	if r.statusCode == 0 {
		r.statusCode = 200
	}
	sb.WriteString(fmt.Sprintf("HTTP/1.1 %d OK\r\n", r.statusCode))
	for k, v := range r.header {
		sb.WriteString(fmt.Sprintf("%s: %s\r\n", k, strings.Join(v, ",")))
	}
	sb.WriteString("\r\n")
	sb.Write(r.buf.Bytes())

	r.writer.Write([]byte(sb.String()))
	return r.writer.Flush()
}

type Handler interface {
	ServeHTTP(ResponseWriter, *Request)
}

type HandlerFunc func(ResponseWriter, *Request)

func (h HandlerFunc) ServeHTTP(write ResponseWriter, req *Request) {
	h(write, req)
}

func serveConn(s *HttpServer, conn net.Conn) {
	defer recoverTool()
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		req, err := readRequest(r)
		if err != nil {
			log.Println(err)
			return
		}

		log.Println("请求行：", req.Method, req.Path, req.Proto)
		log.Println("请求头：")
		for k, v := range req.Header {
			log.Println(k, v)
		}
		log.Println("请求体：", req.Body)

		rsp := newResponse(w)
		if s.handler == nil {
			defaultServeMux.ServeHTTP(rsp, req)
		} else {
			s.handler.ServeHTTP(rsp, req)
		}
		rsp.flush()

		if shouldClose(req) {
			break
		}
	}
}

func shouldClose(req *Request) bool {
	if req.Proto == "HTTP/1.1" || (len(req.Header["connection"]) > 0 && req.Header["connection"][0] != "keep-alive") {
		return false
	} else {
		return true
	}
}

func readRequest(r *bufio.Reader) (*Request, error) {
	req := &Request{}
	req.Header = make(map[string][]string)

	// 1) 读请求行（行式：以 \r\n 结尾）
	line, err := r.ReadString('\n')
	if err != nil {
		log.Println("read request line:", err)
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 3 {
		log.Println("invalid request line:", line)
		return nil, err
	}
	req.Method, req.Path, req.Proto = parts[0], parts[1], parts[2]

	// 2) 读 header（行式：循环读直到空行 \r\n）
	for {
		line, err = r.ReadString('\n')
		if err != nil {
			log.Println("read header:", err)
			return nil, err
		}
		if line == "\r\n" { // 空行：header 结束
			break
		}
		kv := strings.SplitN(line, ": ", 2)
		if len(kv) == 2 {
			k := kv[0]
			v := strings.TrimRight(kv[1], "\r\n")
			req.Header[k] = append(req.Header[k], v)
		}
	}

	// 3) 读 body（流式：长度由 Content-Length 决定，不能按行读）
	var body []byte
	if cl := req.Header["Content-Length"]; len(cl) > 0 {
		n, err := strconv.Atoi(cl[0])
		if err == nil && n > 0 {
			body = make([]byte, n)
			if _, err := io.ReadFull(r, body); err != nil {
				log.Println("read body:", err)
				return nil, err
			}
		}
	}
	req.Body = bytes.NewReader(body)

	return req, nil
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
