// soloprobe 是 TRAE SOLO llm_utils_chat 通道的原始 wire 探针。
//
// 目的：把一次真实聊天请求的 HTTP 状态、全部响应头、以及 SSE 每一行（带相对
// 时间戳与行间间隔）原样打出来，用于确认上游是否在流里下发排队/等待类事件
// （troubleshooting「等很久」）。
//
//	cd plugins/trae && go run ./cmd/soloprobe <auth.json> [model] [prompt] [timeoutSec]
//
// 只读凭据：不做 RefreshToken、不回写 auth 文件。
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/mmqz/cpa-multi-plugins/plugins/trae/auth"
	"github.com/mmqz/cpa-multi-plugins/plugins/trae/upstream"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: soloprobe <auth.json> [model] [prompt] [timeoutSec]")
		os.Exit(2)
	}
	authFile := os.Args[1]
	model := "deepseek-v4.1-flash"
	if len(os.Args) > 2 {
		model = os.Args[2]
	}
	prompt := "只回复两个字：收到"
	if len(os.Args) > 3 {
		prompt = os.Args[3]
	}
	timeoutSec := 300
	if len(os.Args) > 4 {
		fmt.Sscanf(os.Args[4], "%d", &timeoutSec)
	}

	raw, err := os.ReadFile(authFile)
	if err != nil {
		fmt.Println("read auth:", err)
		os.Exit(1)
	}
	a, err := auth.Parse(raw)
	if err != nil {
		fmt.Println("parse auth:", err)
		os.Exit(1)
	}
	fmt.Printf("auth: uid=%s variant=%q machine=%s device=%s\n", a.UID, a.Variant, a.MachineID, a.DeviceID)

	body, _ := json.Marshal(map[string]any{
		"model":    model,
		"stream":   true,
		"messages": []any{map[string]any{"role": "user", "content": prompt}},
	})
	prepared := upstream.PrepareBodyResolved(body, a.Variant, model)
	fmt.Printf("outbound: url=%s%s model=%q function=%q bytes=%d\n",
		upstream.AgentHost, upstream.EpChat, model, upstream.FunctionFor(a.Variant), len(prepared))

	req, err := http.NewRequest(http.MethodPost, upstream.AgentHost+upstream.EpChat, bytes.NewReader(prepared))
	if err != nil {
		fmt.Println("newreq:", err)
		os.Exit(1)
	}
	upstream.SOLOHeaders(req, a, true)

	hc := &http.Client{Timeout: time.Duration(timeoutSec) * time.Second}
	start := time.Now()
	resp, err := hc.Do(req)
	if err != nil {
		fmt.Printf("transport error after %dms: %v\n", time.Since(start).Milliseconds(), err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	fmt.Printf("=== status=%d t=%dms ===\n", resp.StatusCode, time.Since(start).Milliseconds())
	for k, v := range resp.Header {
		fmt.Printf("H %s: %v\n", k, v)
	}

	br := bufio.NewReaderSize(resp.Body, 1<<16)
	last := start
	for {
		line, rerr := br.ReadString('\n')
		if line != "" {
			now := time.Now()
			fmt.Printf("[%8dms +%5dms] %s", now.Sub(start).Milliseconds(), now.Sub(last).Milliseconds(), line)
			last = now
		}
		if rerr != nil {
			if rerr != io.EOF {
				fmt.Println("read err:", rerr)
			}
			break
		}
	}
	fmt.Printf("=== EOF t=%dms ===\n", time.Since(start).Milliseconds())
}
