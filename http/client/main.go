package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const baseURL = "http://localhost:8080"

// User 与服务端保持一致，用于 JSON 编解码。
type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	// 1) 最简单的 GET 请求：http.Get 用的是默认 client。
	simpleGet()

	// 2) 带自定义 Client（超时）和自定义 Header 的请求。
	getWithHeader()

	// 3) POST 一个 JSON 请求体，并解析返回的 JSON。
	postJSON()
}

// simpleGet 演示最基础的 GET。
func simpleGet() {
	resp, err := http.Get(baseURL + "/hello")
	if err != nil {
		log.Fatalf("GET /hello failed: %v", err)
	}
	// 一定要关闭 Body，否则连接无法复用，会造成泄漏。
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("[simpleGet] status=%d body=%s", resp.StatusCode, body)
}

// getWithHeader 演示用 http.Client + http.NewRequest 自定义请求。
func getWithHeader() {
	// 自定义 Client，设置整体超时（连接 + 读取）。
	client := &http.Client{Timeout: 5 * time.Second}

	// NewRequest 可以精细控制方法、URL、Body、Header。
	req, err := http.NewRequest(http.MethodGet, baseURL+"/headers", nil)
	if err != nil {
		log.Fatalf("build request failed: %v", err)
	}
	// 添加自定义请求头。
	req.Header.Set("X-Token", "my-secret-token")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("GET /headers failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("[getWithHeader] X-Server=%s body=%s",
		resp.Header.Get("X-Server"), body)
}

// postJSON 演示发送 JSON 请求体并解析响应。
func postJSON() {
	newUser := User{Name: "Carol", Age: 30}

	// 把结构体序列化成 JSON 字节。
	payload, err := json.Marshal(newUser)
	if err != nil {
		log.Fatalf("marshal failed: %v", err)
	}

	// http.Post 直接传 Content-Type 和 body（io.Reader）。
	resp, err := http.Post(
		baseURL+"/user",
		"application/json",
		bytes.NewReader(payload),
	)
	if err != nil {
		log.Fatalf("POST /user failed: %v", err)
	}
	defer resp.Body.Close()

	// 解析返回的 JSON。
	var created User
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		log.Fatalf("decode response failed: %v", err)
	}
	fmt.Printf("[postJSON] status=%d created=%+v\n", resp.StatusCode, created)
}
