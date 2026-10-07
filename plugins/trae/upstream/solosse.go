// solosse.go SOLO 自定义 SSE 解析 → OpenAI SSE（流式转换 + 非流式聚合）。
//
// SOLO 事件序列（SPEC §4.6，实测）：
//
//	id:1
//	event:metadata
//	data:{"model":"","session_id":"...","prompt_completion_id":0,...}
//
//	id:2
//	event:timing_cost
//	data:{"name":"llm_raw_chat_v2",...}
//
//	event:output                          ← ×N，核心内容
//	data:{"response":"<content 增量>",
//	      "reasoning_content":"<思考链增量>",
//	      "tool_calls":<null 或工具调用>}
//
//	event:extra_info                       ← 含 reasoning_content 完整版
//	event:token_usage
//	data:{"prompt_tokens":21,"completion_tokens":142,"total_tokens":163,"reasoning_tokens":135}
//
//	event:done
//	data:{"finish_reason":"stop"}
package upstream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// SOLOEvent 单条 SOLO SSE 事件（归一化）。
type SOLOEvent struct {
	Event        string          // metadata | timing_cost | output | extra_info | token_usage | done | error | queue_begin | request_wait_in_queue | progress_notice
	Response     string          // output: content 增量
	Reasoning    string          // output: 思考链增量
	ToolCalls    json.RawMessage // output: 工具调用（null 或对象/数组）
	Usage        map[string]any  // token_usage
	FinishReason string          // done
	ErrorCode    int64           // error
	ErrorMessage string          // error

	// v0.12.84 排队观测。上游在真正出 token 之前会持续下发这三类事件
	// （2026-09-29 实测 soloprobe，CN 账号 + deepseek-v4.1-flash）：
	//
	//	event:progress_notice        data:"Processing_1790673459"   ← 裸 JSON 字符串
	//	event:queue_begin            data:{"queue_id":"..","queue_type":1,"is_async":false,"request_uuid":".."}
	//	event:request_wait_in_queue  data:{"position":560,"message":"Too many current requests. Your queue position is 560. Please wait for a while.","queue_id":"..","request_uuid":".."}
	//
	// request_wait_in_queue 约每秒一条，position 从大到小递减（实测 560→550→546→537）。
	// 此前这些事件在 Stream/Aggregate/convertSOLOStreamToOpenAI 里被静默丢弃，
	// 调用方只表现为"卡住不动"，没有任何排队提示。
	Notice      string // progress_notice: 裸字符串内容（"Processing_<unix>" / ";Processing"）
	QueueID     string // queue_begin / request_wait_in_queue: 本次排队的队列票据
	QueueType   int64  // queue_begin: 队列类型（1..6，见客户端 QueueType 枚举）
	IsAsync     bool   // queue_begin / request_wait_in_queue: 上游是否异步处理
	RequestUUID string // 本次请求身份，用于把同一请求的排队段串起来
	Position    int64  // request_wait_in_queue: 当前排队位次
	HasPosition bool   // request_wait_in_queue: position 字段存在（0 也是合法位次）
	Message     string // request_wait_in_queue: 上游原文提示
}

// EventObserver 接收每个解析成功的事件，由调用方决定关心哪些（排队观测关心
// queue_begin / request_wait_in_queue / progress_notice 以及首个 output）。
// 可为 nil。回调在读取流的 goroutine 上同步执行，必须是非阻塞的。
type EventObserver func(*SOLOEvent)

// IsQueueEvent 报告事件是否属于排队观测关心的三类。
func (e *SOLOEvent) IsQueueEvent() bool {
	switch e.Event {
	case "queue_begin", "request_wait_in_queue", "progress_notice":
		return true
	}
	return false
}

// SOLOStreamError 上游 SSE 流内的业务错误（event:error）。非流式聚合时返回，
// 调用方可据此分类冷却账号并轮转。
type SOLOStreamError struct {
	Code int64
	Msg  string
}

func (e *SOLOStreamError) Error() string {
	return fmt.Sprintf("solo error code=%d msg=%s", e.Code, e.Msg)
}

// soloPlanLimitCodes 流内业务码 → plan_limit 语义集合。
// v0.12.48: 补 4008 —— 上游对「该账号没有这个模型的配额」发 HTTP 200 +
// 流内 code=4008 "Your requests have exceeded the quota"（dsh-router-traework
// 2026-09-08 三账号实测：面板还有 200 积分的号也中招，这个配额与积分余额
// 是两回事）。此前 4008 归 ErrClient（60s 短冷却、不累计禁用），坏号留在
// 池里反复撞墙。对齐 dsh-router-traework PLAN_LIMIT_CODES（2026-09-15）。
var soloPlanLimitCodes = map[int64]struct{}{1005: {}, 4008: {}}

// soloModelMismatchCodes 流内业务码 → 模型/通道不匹配语义集合。
// v0.12.79 (issue #9): 4001 除「输入过大」文案外都是"模型在当前 function
// 通道不可用"（trae2api-more IsModelConfigMismatch 同语义；文案泛化为
// "We're sorry, the param is invalid"，无法再细分）。请求级失败：换任何账号
// 结果相同，不冷却、不累计 errCount —— 否则客户端选到死模型（已由
// FetchModels 名单过滤，兜底仍会漏网）会把健康账号累计冷却。
var soloModelMismatchCodes = map[int64]struct{}{4001: {}}

// IsModelMismatchCode reports whether an in-stream biz code means the model
// is not available on the current chat lane (request-level failure).
func IsModelMismatchCode(code int64) bool {
	_, ok := soloModelMismatchCodes[code]
	return ok
}

// Kind 将 SSE 流内错误分类。1005/4008 → ErrPlanLimit；非过大文案的 4001 →
// ErrModelUnavailable（请求级，不冷却）；其余归 ErrClient。
func (e *SOLOStreamError) Kind() ErrKind {
	if _, ok := soloPlanLimitCodes[e.Code]; ok {
		return ErrPlanLimit
	}
	// v0.12.50: 流内过大文案 → 请求级问题，不冷却账号。
	// （顺序敏感：4001 + "prompt is too long…" 仍是 ErrInputTooLarge。）
	if MsgIndicatesInputTooLarge(e.Msg) {
		return ErrInputTooLarge
	}
	if _, ok := soloModelMismatchCodes[e.Code]; ok {
		return ErrModelUnavailable
	}
	return ErrClient
}

// ParseSOLOLine 解析一条事件（eventName 为 event 行值，dataLine 为 data 行值）。
func ParseSOLOLine(eventName, dataLine string) (*SOLOEvent, error) {
	ev := &SOLOEvent{Event: strings.TrimSpace(eventName)}
	if dataLine == "" {
		return ev, nil
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(dataLine), &raw); err != nil {
		// progress_notice 的 data 是裸 JSON 字符串（"Processing_1790673459"），
		// 不是对象。解析失败时退化为字符串而不是丢掉整行——否则排队期的心跳
		// 完全不可观测。其他事件的字符串 data 同样走这条路。
		var s string
		if serr := json.Unmarshal([]byte(dataLine), &s); serr == nil {
			ev.Notice = s
			return ev, nil
		}
		return nil, err
	}
	switch ev.Event {
	case "output":
		if v, ok := raw["response"].(string); ok {
			ev.Response = v
		}
		if v, ok := raw["reasoning_content"].(string); ok {
			ev.Reasoning = v
		}
		if tc, ok := raw["tool_calls"]; ok {
			ev.ToolCalls, _ = json.Marshal(tc)
		}
	case "token_usage":
		ev.Usage = raw
	case "done":
		if v, ok := raw["finish_reason"].(string); ok {
			ev.FinishReason = v
		}
	case "error":
		if v, ok := raw["code"].(float64); ok {
			ev.ErrorCode = int64(v)
		}
		if v, ok := raw["message"].(string); ok {
			ev.ErrorMessage = v
		}
	case "queue_begin", "request_wait_in_queue":
		if v, ok := raw["queue_id"].(string); ok {
			ev.QueueID = v
		}
		if v, ok := raw["request_uuid"].(string); ok {
			ev.RequestUUID = v
		}
		if v, ok := raw["queue_type"].(float64); ok {
			ev.QueueType = int64(v)
		}
		if v, ok := raw["is_async"].(bool); ok {
			ev.IsAsync = v
		}
		if v, ok := raw["position"].(float64); ok {
			ev.Position = int64(v)
			ev.HasPosition = true
		}
		if v, ok := raw["message"].(string); ok {
			ev.Message = v
		}
	}
	return ev, nil
}

// sseState 维护一行 SSE 的 event/data 跨行累积。
type sseState struct {
	event string
	data  strings.Builder
}

// reset 清空状态（事件边界触发）。
func (s *sseState) reset() {
	s.event = ""
	s.data.Reset()
}

// scanLine 处理一行；返回该行触发的事件（事件边界时解析并返回）。
func scanLine(st *sseState, line string) *SOLOEvent {
	switch {
	case line == "":
		if st.event == "" {
			st.reset()
			return nil
		}
		ev, err := ParseSOLOLine(st.event, st.data.String())
		st.reset()
		if err != nil {
			return nil
		}
		return ev
	case strings.HasPrefix(line, "event:"):
		st.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
	case strings.HasPrefix(line, "data:"):
		st.data.WriteString(strings.TrimPrefix(line, "data:"))
	case strings.HasPrefix(line, ":"):
		// 注释行忽略
	}
	return nil
}

// Aggregate 读取完整 SOLO SSE，聚合 response + reasoning + tool_calls + usage，
// 产出单个 OpenAI chat.completion（非流式）。
func Aggregate(r io.Reader) (map[string]any, error) {
	return aggregateWith(r, nil)
}

// AggregateWithObserver 同 Aggregate，额外把每个事件交给 obs（可为 nil）。
// 非流式路径也需要它：上游的排队事件在聚合模式下同样会出现，没有观测点就
// 只能看到"这个请求花了 90 秒"，看不到其中 85 秒在排队。
func AggregateWithObserver(r io.Reader, obs EventObserver) (map[string]any, error) {
	return aggregateWith(r, obs)
}

func aggregateWith(r io.Reader, obs EventObserver) (map[string]any, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	var (
		id           string
		content      strings.Builder
		reasoning    strings.Builder
		finishReason = "stop"
		usage        map[string]any
		toolCalls    = map[int]map[string]any{}
		toolOrder    []int
		upstreamErr  error
	)
	st := &sseState{}
	for {
		line, err := br.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		if ev := scanLine(st, strings.TrimRight(line, "\r\n")); ev != nil {
			if obs != nil {
				obs(ev)
			}
			switch ev.Event {
			case "metadata":
				if id == "" && ev.Usage != nil {
					// metadata 无 id 可用，保留为空，末尾补 chatcmpl。
				}
			case "output":
				content.WriteString(ev.Response)
				reasoning.WriteString(ev.Reasoning)
				mergeToolCallJSON(toolCalls, &toolOrder, ev.ToolCalls)
			case "token_usage":
				usage = ev.Usage
			case "done":
				if ev.FinishReason != "" {
					finishReason = ev.FinishReason
				}
			case "error":
				upstreamErr = &SOLOStreamError{Code: ev.ErrorCode, Msg: ev.ErrorMessage}
			}
		}
		if err == io.EOF {
			break
		}
	}
	if upstreamErr != nil {
		return nil, upstreamErr
	}
	if id == "" {
		id = fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	}
	message := map[string]any{
		"role":    "assistant",
		"content": content.String(),
	}
	if reasoning.Len() > 0 {
		message["reasoning_content"] = reasoning.String()
	}
	if len(toolOrder) > 0 {
		sortInts(toolOrder)
		calls := make([]map[string]any, 0, len(toolOrder))
		for _, idx := range toolOrder {
			calls = append(calls, toolCalls[idx])
		}
		message["tool_calls"] = calls
	}
	resp := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   "",
		"choices": []any{
			map[string]any{
				"index":         0,
				"message":       message,
				"finish_reason": finishReason,
			},
		},
	}
	if usage != nil {
		resp["usage"] = usage
	}
	return resp, nil
}

// mergeToolCallJSON 把 SOLO output.tool_calls（json.RawMessage，可能 null/对象/数组）
// 合并进 toolCalls（按 index）。
func mergeToolCallJSON(toolCalls map[int]map[string]any, toolOrder *[]int, raw json.RawMessage) {
	if len(raw) == 0 || string(raw) == "null" {
		return
	}
	var arr []map[string]any
	if err := json.Unmarshal(raw, &arr); err != nil {
		var one map[string]any
		if json.Unmarshal(raw, &one) != nil {
			return
		}
		arr = []map[string]any{one}
	}
	for _, call := range arr {
		if call == nil {
			continue
		}
		idx := 0
		if v, ok := call["index"].(float64); ok {
			idx = int(v)
		}
		merged, seen := toolCalls[idx]
		if !seen {
			merged = map[string]any{"index": idx}
			toolCalls[idx] = merged
			*toolOrder = append(*toolOrder, idx)
		}
		mergeToolCallDelta(merged, call)
	}
}

// mergeToolCallDelta 把流式 tool_call 片段合并到累计对象：
// id/type/function.name 直覆盖，function.arguments 拼接。
// 上游 SOLO 用 `function_call` 字段（实测），OpenAI 标准用 `function`；两者都兼容。
func mergeToolCallDelta(merged, delta map[string]any) {
	if v, ok := delta["id"].(string); ok && v != "" {
		merged["id"] = v
	}
	if v, ok := delta["type"].(string); ok && v != "" {
		merged["type"] = v
	}
	df, _ := delta["function"].(map[string]any)
	if df == nil {
		df, _ = delta["function_call"].(map[string]any) // SOLO 专属字段名
	}
	if df == nil {
		return
	}
	// 清理 SOLO 专属字段,只保留标准 OpenAI function 结构(name/arguments)
	delete(df, "namespace")
	delete(df, "partial_arguments")
	mf, _ := merged["function"].(map[string]any)
	if mf == nil {
		mf = map[string]any{}
		merged["function"] = mf
	}
	if v, ok := df["name"].(string); ok && v != "" {
		mf["name"] = v
	}
	if v, ok := df["arguments"].(string); ok && v != "" {
		if prev, _ := mf["arguments"].(string); prev != "" {
			mf["arguments"] = prev + v
		} else {
			mf["arguments"] = v
		}
	}
}

// sortInts 升序排序（避免引 sort 包只为三行）。
func sortInts(a []int) {
	for i := 0; i < len(a)-1; i++ {
		for j := i + 1; j < len(a); j++ {
			if a[j] < a[i] {
				a[i], a[j] = a[j], a[i]
			}
		}
	}
}

// Stream 流式转换：SOLO SSE → OpenAI SSE chunk，每 chunk flush，保证至少一个 [DONE]。
// 调用方必须先设置过 status 200；本函数自设 SSE headers。
func Stream(w http.ResponseWriter, r io.Reader) error {
	return streamOpts(w, r, nil)
}

// StreamWithError 同 Stream，额外在遇到上游 event:error 时回调 onErr（非 nil），
// 供调用方冷却账号/记录日志；错误信息同时注入 SSE 事件流。
func StreamWithError(w http.ResponseWriter, r io.Reader, onErr func(*SOLOStreamError)) error {
	return streamOpts(w, r, onErr)
}

// StreamWithObserver 同 StreamWithError，额外把每个事件交给 obs（可为 nil）。
func StreamWithObserver(w http.ResponseWriter, r io.Reader, onErr func(*SOLOStreamError), obs EventObserver) error {
	return streamOptsObs(w, r, onErr, obs)
}

// streamOpts Stream 的可选参数版本。
func streamOpts(w http.ResponseWriter, r io.Reader, onErr func(*SOLOStreamError)) error {
	return streamOptsObs(w, r, onErr, nil)
}

func streamOptsObs(w http.ResponseWriter, r io.Reader, onErr func(*SOLOStreamError), obs EventObserver) error {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	fl, _ := w.(http.Flusher)

	br := bufio.NewReaderSize(r, 64*1024)
	id := fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	var pendingUsage map[string]any
	sawDone := false
	st := &sseState{}
	writeChunk := func(delta map[string]any, finish string) error {
		chunk := map[string]any{
			"id":      id,
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   "",
			"choices": []any{
				map[string]any{
					"index": 0,
					"delta": delta,
				},
			},
		}
		choice := chunk["choices"].([]any)[0].(map[string]any)
		if finish != "" {
			choice["finish_reason"] = finish
		}
		if pendingUsage != nil {
			chunk["usage"] = pendingUsage
			pendingUsage = nil
		}
		raw, _ := json.Marshal(chunk)
		if _, err := io.WriteString(w, "data: "+string(raw)+"\n\n"); err != nil {
			return err
		}
		if fl != nil {
			fl.Flush()
		}
		return nil
	}
	writeDONE := func() error {
		if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
			return err
		}
		if fl != nil {
			fl.Flush()
		}
		return nil
	}

	for {
		line, err := br.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		if ev := scanLine(st, strings.TrimRight(line, "\r\n")); ev != nil {
			if obs != nil {
				obs(ev)
			}
			switch ev.Event {
			case "output":
				delta := map[string]any{}
				if ev.Response != "" {
					delta["content"] = ev.Response
				}
				if ev.Reasoning != "" {
					delta["reasoning_content"] = ev.Reasoning
				}
				if len(ev.ToolCalls) > 0 && string(ev.ToolCalls) != "null" {
					var tc []map[string]any
					if err := json.Unmarshal(ev.ToolCalls, &tc); err == nil {
						// SOLO 上游 tool_call 条目用 `function_call` 字段 → 转成 OpenAI 的 `function`
						for _, call := range tc {
							if fc, ok := call["function_call"].(map[string]any); ok {
								call["function"] = fc
								delete(call, "function_call")
							}
							// 清理 SOLO 专属字段,只保留标准 OpenAI function 结构(name/arguments)
							if fn, ok := call["function"].(map[string]any); ok {
								delete(fn, "namespace")
								delete(fn, "partial_arguments")
							}
						}
						delta["tool_calls"] = tc
					}
				}
				if len(delta) > 0 {
					if err := writeChunk(delta, ""); err != nil {
						return err
					}
				}
			case "token_usage":
				pendingUsage = ev.Usage
			case "done":
				if err := writeChunk(map[string]any{}, ev.FinishReason); err != nil {
					return err
				}
				if err := writeDONE(); err != nil {
					return err
				}
				sawDone = true
			case "error":
				// 上游业务错误：回调 + 写一条 error 事件 + [DONE]。
				se := &SOLOStreamError{Code: ev.ErrorCode, Msg: ev.ErrorMessage}
				if onErr != nil {
					onErr(se)
				}
				msg := fmt.Sprintf("solo error code=%d msg=%s", ev.ErrorCode, ev.ErrorMessage)
				if _, err := io.WriteString(w, "event: error\n"+"data: "+jsonEscape(msg)+"\n\n"); err != nil {
					return err
				}
				if err := writeDONE(); err != nil {
					return err
				}
				sawDone = true
			}
		}
		if err == io.EOF {
			break
		}
	}
	if !sawDone {
		// 幂等兜底：上游中断（无 done）仍写 [DONE]。
		return writeDONE()
	}
	return nil
}

func jsonEscape(s string) string {
	raw, _ := json.Marshal(s)
	return string(raw)
}
