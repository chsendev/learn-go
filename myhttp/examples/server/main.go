package main

import (
	"learn/myhttp"
	"log"
	"net"
)

//type myHandler struct {
//}
//
//func (m *myHandler) ServeHTTP(writer myhttp.ResponseWriter, req *myhttp.Request) {
//	writer.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"))
//	writer.Flush()
//}

func main() {
	s := myhttp.NewHttpServer(":9090", nil)

	a := myhttp.HandlerFunc(func(writer myhttp.ResponseWriter, request *myhttp.Request) {
		log.Println(request)
		writer.Write([]byte("/a ok"))
	})

	s.ConnState = func(conn net.Conn, state myhttp.ConnState) {
		log.Printf("连接 %s → %d", conn.RemoteAddr(), state)
	}

	myhttp.Handle("/a", myhttp.Chain(a, myhttp.Logging, myhttp.Recover))

	myhttp.HandleFunc("/b", func(writer myhttp.ResponseWriter, request *myhttp.Request) {
		log.Println(request)
		writer.Write([]byte("/b ok"))
	})

	s.ListenAndServe()
}
