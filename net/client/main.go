package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
)

// 这是配套的 TCP 客户端。
// 对照 net/http：http.Client 底层也是用 net.Dial 建连，
// 只不过它把连接放进连接池复用，并在上面收发 HTTP 报文。
func main() {
	// 1) Dial：主动连接服务端，返回一个 net.Conn。
	//    DialTimeout 可以给连接阶段设超时。
	conn, err := net.DialTimeout("tcp", "127.0.0.1:9000", 5*time.Second)
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	log.Printf("connected to %s", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	// 要发送的几条消息，最后一条 "bye" 让服务端断开。
	messages := []string{"hello", "net package", "你好", "bye"}

	for _, msg := range messages {
		// 2) 写：直接往 conn 里写字节即可。注意要带 '\n'，
		//    因为服务端用 ReadString('\n') 来分包。
		if _, err := fmt.Fprintf(conn, "%s\n", msg); err != nil {
			log.Fatalf("write failed: %v", err)
		}

		// 3) 读：读取服务端返回的一行。
		resp, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("read failed: %v", err)
			return
		}
		fmt.Printf("server reply: %s", resp)

		time.Sleep(300 * time.Millisecond) // 仅为演示，便于观察
	}
}
