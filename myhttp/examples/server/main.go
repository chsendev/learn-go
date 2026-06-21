package main

import (
	"learn/myhttp"
	"log"
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

	myhttp.HandleFunc("/a", func(writer myhttp.ResponseWriter, request *myhttp.Request) {
		log.Println(request)
		writer.Write([]byte("/a ok"))
	})

	myhttp.HandleFunc("/b", func(writer myhttp.ResponseWriter, request *myhttp.Request) {
		log.Println(request)
		writer.Write([]byte("/b ok"))
	})

	s.ListenAndServe()
}
