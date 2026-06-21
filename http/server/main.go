package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// User 是一个示例数据结构，用于演示 JSON 的编解码。
type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

// 内存里的假数据，模拟数据库。
var users = []User{
	{ID: 1, Name: "Alice", Age: 20},
	{ID: 2, Name: "Bob", Age: 25},
}

func main() {
	// http.NewServeMux 创建一个路由器（多路复用器）。
	// 相比直接用 http.HandleFunc（全局默认 mux），显式创建 mux 更清晰、可控。
	mux := http.NewServeMux()

	// 1) 最基础的处理函数：返回纯文本。
	mux.HandleFunc("/hello", helloHandler)

	// 2) 读取 query 参数：GET /greet?name=xxx
	mux.HandleFunc("/greet", greetHandler)

	// 3) 返回 JSON：GET /users 列表
	mux.HandleFunc("/users", usersHandler)

	// 4) 根据请求方法做不同处理：GET / POST /user
	mux.HandleFunc("/user", userHandler)

	// 5) 演示读取请求头、设置响应头。
	mux.HandleFunc("/headers", headersHandler)

	http.HandleFunc("/hello", helloHandler)

	// Server 结构体可以配置超时等参数，比直接 http.ListenAndServe 更适合生产。
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Println("server listening on http://localhost:8080")
	// ListenAndServe 会阻塞，返回非 nil error 时打印并退出。
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// helloHandler 返回一段纯文本。
func helloHandler(w http.ResponseWriter, r *http.Request) {
	// w.Write 会默认以 200 状态码返回。
	fmt.Fprintln(w, "hello, net/http!")
}

// greetHandler 演示如何读取 URL query 参数。
func greetHandler(w http.ResponseWriter, r *http.Request) {
	// r.URL.Query() 返回所有 query 参数，Get 取第一个值。
	name := r.URL.Query().Get("name")
	if name == "" {
		name = "stranger"
	}
	w.Write()
	fmt.Fprintf(w, "hello, %s!\n", name)
}

// usersHandler 把切片序列化成 JSON 返回。
func usersHandler(w http.ResponseWriter, r *http.Request) {
	// 设置响应头，告诉客户端返回的是 JSON。
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// json.NewEncoder 直接把数据写入 w，避免中间 buffer。
	if err := json.NewEncoder(w).Encode(users); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// userHandler 根据请求方法分别处理：GET 查询、POST 新增。
func userHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// 返回第一个用户作为示例。
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(users[0])

	case http.MethodPost:
		// 解析请求体里的 JSON。
		var u User
		if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
		u.ID = len(users) + 1
		users = append(users, u)

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusCreated) // 201
		_ = json.NewEncoder(w).Encode(u)

	default:
		// 其他方法返回 405。
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// headersHandler 演示读取请求头与设置响应头。
func headersHandler(w http.ResponseWriter, r *http.Request) {
	// 读取客户端发来的自定义请求头。
	token := r.Header.Get("X-Token")

	// 设置自定义响应头。
	w.Header().Set("X-Server", "learn-net-http")
	fmt.Fprintf(w, "received X-Token: %q\n", token)
}
