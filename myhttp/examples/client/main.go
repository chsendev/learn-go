package main

import (
	"fmt"
	"io"
	"learn/myhttp"
	"log"
	"strings"
)

func main() {
	// ─── 示例 1：GET ─────────────────────────────────────
	log.Println("─── GET /a ───")
	rsp, err := myhttp.Get("http://127.0.0.1:9090/a")
	if err != nil {
		log.Fatal(err)
	}
	dump(rsp)

	// ─── 示例 2：POST ────────────────────────────────────
	log.Println("─── POST /b ───")
	rsp2, err := myhttp.Post("http://127.0.0.1:9090/b", "text/plain",
		strings.NewReader("hello from myhttp client"))
	if err != nil {
		log.Fatal(err)
	}
	dump(rsp2)
}

func dump(rsp *myhttp.Response) {
	defer rsp.Body.Close()
	fmt.Printf("Status: %d %s\n", rsp.StatusCode, rsp.Status)
	fmt.Println("Headers:")
	for k, v := range rsp.Header {
		fmt.Printf("  %s: %s\n", k, v)
	}
	body, _ := io.ReadAll(rsp.Body)
	fmt.Printf("Body: %s\n\n", body)
}
