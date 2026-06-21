package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

// 这是一个最小的 TCP echo 服务端。
// 对照 net/http 源码：http.Server 的核心就是下面这个
//   for { conn := ln.Accept(); go handle(conn) }
// 模型，只不过 http 在 handle 里多做了「解析 HTTP 报文 + 路由」。
func main() {
	// 1) Listen：在 tcp 端口上监听，返回一个 Listener。
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()
	log.Println("tcp server listening on :9000")

	// 2) 核心循环：不断 Accept 新连接，每来一个就开一个 goroutine 处理。
	//    这就是 Go 网络编程的「一连接一 goroutine」模型。
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		// 交给独立 goroutine，主循环立刻回去接下一个连接。
		go handleConn(conn)
	}
}

// handleConn 处理单条连接：按行读取，原样回写（echo）。
func handleConn(conn net.Conn) {
	// 连接用完一定要关，否则文件描述符泄漏。
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	log.Printf("new connection from %s", remote)

	// 给连接包一个带缓冲的 reader，方便按行读。
	reader := bufio.NewReader(conn)

	for {
		// 可选：设置读超时，防止连接一直占用。
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// ReadString 一直读到 '\n'（含）。这就是我们自定义的「分包协议」。
		line, err := reader.ReadString('\n')
		if err != nil {
			// 客户端关闭或超时，结束这条连接。
			log.Printf("connection %s closed: %v", remote, err)
			return
		}

		msg := strings.TrimSpace(line)
		log.Printf("recv from %s: %q", remote, msg)

		// 约定：客户端发 "bye" 就主动断开。
		if msg == "bye" {
			fmt.Fprintln(conn, "bye~")
			return
		}

		// 原样回写（加个前缀以示区分）。
		fmt.Fprintf(conn, "echo: %s\n", msg)
	}
}
