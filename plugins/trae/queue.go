// queue.go TRAE SOLO 排队观测。
//
// 背景（v0.12.84，2026-09-29 soloprobe 实测）：上游 llm_utils_chat 在真正出
// token 之前，会先下发一串"还没轮到你"的事件：
//
//	event:progress_notice        data:"Processing_1790673459"      ← 约 1–2 秒一次心跳
//	event:queue_begin            data:{"queue_id":"..","queue_type":1,"is_async":false,"request_uuid":".."}
//	event:request_wait_in_queue  data:{"position":560,"message":"Too many current requests. Your queue position is 560. Please wait for a while.","queue_id":"..","request_uuid":".."}
//
// request_wait_in_queue 约每秒一条，position 从大到小递减（实测 560→550→546→537）。
// 也就是说"等很久"不是卡死，而是排队，而且位次一直是可以看到的。
//
// 此前这些事件在 solosse.go / sse_convert.go 的 switch 里没有分支，被静默丢弃，
// 调用方只能看到"这个请求花了 90 秒"。本文件把它们聚合成一个可查询的快照，由
// 管理接口 GET /v0/management/plugins/trae/queue 暴露给 DSH 侧插件。
//
// 并发模型：queueWatcher 的 ep 字段只允许在 soloQueue.mu 保护下读写；watcher
// 自身不再加锁，避免两把锁的交叉。
package main

import (
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/mmqz/cpa-multi-plugins/plugins/trae/upstream"
)

const (
	// queueRecentKeep 快照里保留的最近排队记录条数。
	queueRecentKeep = 20
	// queueRecentTTL 最近记录在快照里的保留时长。
	queueRecentTTL = 30 * time.Minute
	// queueStaleAfter 活跃条目多久没有任何上游更新就判定为死条目（客户端断开、
	// 进程被杀等导致 finish 没跑到的兜底）。
	queueStaleAfter = 5 * time.Minute
	// queueMaxWait 超过这个等待时长仍未出 token 的排队记录在快照里标记为
	// 上游长时间未推进（仅用于展示，不做任何自动动作）。
	queueMaxWait = 30 * time.Minute
)

// queueRequest 标识一次聊天请求的观测身份。同一个账号+模型可能有并发请求，
// 所以只是兜底标识；能拿到 request_uuid 时以它为准。
type queueRequest struct {
	UID      string
	Nickname string
	Variant  string
	Model    string
}

func (q queueRequest) fallbackKey() string {
	return q.UID + "|" + q.Variant + "|" + q.Model
}

// queueEpisode 一次请求的排队片段。字段只在 soloQueue.mu 下读写。
type queueEpisode struct {
	queueRequest

	// registryKey 该条目在 registry 里的键，创建后不再变化。
	registryKey string

	QueueID     string
	QueueType   int64
	RequestUUID string
	IsAsync     bool
	Message     string

	Position    int64
	HasPosition bool
	MaxPosition int64 // 见过的最大位次（刚进队时）
	MinPosition int64 // 见过的最小位次（最靠前时）

	// Phase queued（还在排队）| processing（排到了、已开始产出）。
	Phase string

	StartedAt  time.Time // 首次见到 queue_begin / request_wait_in_queue
	UpdatedAt  time.Time // 最后一次上游更新
	FinishedAt time.Time
	Outcome    string // started | error | canceled
}

// activeJSON 渲染排队中的条目。
func (e *queueEpisode) activeJSON(now time.Time) map[string]any {
	queuedMS := now.Sub(e.StartedAt).Milliseconds()
	if queuedMS < 0 {
		queuedMS = 0
	}
	updatedMS := now.Sub(e.UpdatedAt).Milliseconds()
	if updatedMS < 0 {
		updatedMS = 0
	}
	return map[string]any{
		"uid":          e.UID,
		"nickname":     e.Nickname,
		"variant":      e.Variant,
		"model":        e.Model,
		"queue_id":     e.QueueID,
		"queue_type":   e.QueueType,
		"request_uuid": e.RequestUUID,
		"is_async":     e.IsAsync,
		"position":     e.Position,
		"has_position": e.HasPosition,
		"max_position": e.MaxPosition,
		"min_position": e.MinPosition,
		"message":      e.Message,
		"phase":        e.Phase,
		"queued_ms":    queuedMS,
		"updated_ms":   updatedMS,
		"stalled":      queuedMS > queueMaxWait.Milliseconds() && e.Phase == "queued",
	}
}

// recentJSON 渲染已结束的条目。
func (e *queueEpisode) recentJSON() map[string]any {
	waitedMS := e.FinishedAt.Sub(e.StartedAt).Milliseconds()
	if waitedMS < 0 {
		waitedMS = 0
	}
	return map[string]any{
		"uid":          e.UID,
		"nickname":     e.Nickname,
		"variant":      e.Variant,
		"model":        e.Model,
		"request_uuid": e.RequestUUID,
		"max_position": e.MaxPosition,
		"min_position": e.MinPosition,
		"waited_ms":    waitedMS,
		"finished_at":  e.FinishedAt.UnixMilli(),
		"outcome":      e.Outcome,
	}
}

// queueRegistry 活跃 + 最近排队记录。
type queueRegistry struct {
	mu     sync.Mutex
	active map[string]*queueEpisode
	recent []*queueEpisode
}

var soloQueue = &queueRegistry{active: map[string]*queueEpisode{}}

// attachLocked 把一个刚创建的条目登记为活跃。调用方必须持有 r.mu。
func (r *queueRegistry) attachLocked(e *queueEpisode) {
	r.active[e.registryKey] = e
}

// queueWatcher 观测一次执行调用（execute 或 execute_stream 的一次请求）。
//
// 生命周期：调用方在拿到上游流之后 newQueueWatcher，把 Observe 作为
// upstream.EventObserver 交给转换函数，并在流结束（含错误路径）时调用 Finish。
type queueWatcher struct {
	req queueRequest
	// ep 只在 soloQueue.mu 下访问。
	ep *queueEpisode
	// seq 给没有 request_uuid 的事件提供互不冲突的兜底键。
	seq int64
}

var queueWatcherSeq int64

func newQueueWatcher(req queueRequest) *queueWatcher {
	soloQueue.mu.Lock()
	queueWatcherSeq++
	seq := queueWatcherSeq
	soloQueue.mu.Unlock()
	return &queueWatcher{req: req, seq: seq}
}

// Observe 可以直接作为 upstream.EventObserver 传入。
func (w *queueWatcher) Observe(ev *upstream.SOLOEvent) {
	if ev == nil {
		return
	}
	soloQueue.mu.Lock()
	defer soloQueue.mu.Unlock()
	w.observeLocked(ev, time.Now())
}

func (w *queueWatcher) observeLocked(ev *upstream.SOLOEvent, now time.Time) {
	switch ev.Event {
	case "queue_begin", "request_wait_in_queue":
		if w.ep == nil {
			key := ""
			if ev.RequestUUID != "" {
				key = "uuid:" + ev.RequestUUID
			} else {
				key = "req:" + w.req.fallbackKey() + "#" + strconv.FormatInt(w.seq, 36)
			}
			w.ep = &queueEpisode{
				queueRequest: w.req,
				registryKey:  key,
				Phase:        "queued",
				QueueType:    ev.QueueType,
				IsAsync:      ev.IsAsync,
				RequestUUID:  ev.RequestUUID,
				QueueID:      ev.QueueID,
				Message:      ev.Message,
				StartedAt:    now,
				UpdatedAt:    now,
			}
			if ev.HasPosition {
				w.ep.Position = ev.Position
				w.ep.HasPosition = true
				w.ep.MaxPosition = ev.Position
				w.ep.MinPosition = ev.Position
			}
			soloQueue.attachLocked(w.ep)
			return
		}
		// 后续更新：票据/位次都可能变（位次递减、message 跟着位次重写）。
		if ev.QueueID != "" {
			w.ep.QueueID = ev.QueueID
		}
		if ev.RequestUUID != "" {
			w.ep.RequestUUID = ev.RequestUUID
		}
		if ev.QueueType != 0 {
			w.ep.QueueType = ev.QueueType
		}
		if ev.IsAsync {
			w.ep.IsAsync = true
		}
		if ev.Message != "" {
			w.ep.Message = ev.Message
		}
		if ev.HasPosition {
			w.ep.Position = ev.Position
			w.ep.HasPosition = true
			if ev.Position > w.ep.MaxPosition {
				w.ep.MaxPosition = ev.Position
			}
			if w.ep.MinPosition == 0 || ev.Position < w.ep.MinPosition {
				w.ep.MinPosition = ev.Position
			}
		}
		w.ep.UpdatedAt = now
	case "progress_notice":
		// 心跳本身不代表排队（未排队时上游也会发），但如果这个请求已经进过队，
		// 用它把 UpdatedAt 顶住，避免排队期间被 queueStaleAfter 误判为死条目。
		if w.ep != nil {
			w.ep.UpdatedAt = now
		}
	case "output":
		// 排到了：第一个 output 就是出队标志。
		if w.ep != nil && w.ep.Phase == "queued" {
			w.ep.Phase = "processing"
			w.ep.UpdatedAt = now
		}
	}
}

// Finish 结束一次观测。faulted=true 表示这一路以上游错误收场。
// 从未进过队的请求不产生任何记录（recent 只放真正排过队的请求）。
func (w *queueWatcher) Finish(faulted bool) {
	soloQueue.mu.Lock()
	defer soloQueue.mu.Unlock()
	ep := w.ep
	if ep == nil {
		return
	}
	w.ep = nil
	ep.FinishedAt = time.Now()
	switch {
	case faulted:
		ep.Outcome = "error"
	case ep.Phase == "processing":
		ep.Outcome = "started"
	default:
		// 没等到 output 就结束：客户端断开或上下文被取消。
		ep.Outcome = "canceled"
	}
	delete(soloQueue.active, ep.registryKey)
	soloQueue.recent = append([]*queueEpisode{ep}, soloQueue.recent...)
	if len(soloQueue.recent) > queueRecentKeep {
		soloQueue.recent = soloQueue.recent[:queueRecentKeep]
	}
}

// Snapshot 产出管理接口的 JSON 文档。
func (r *queueRegistry) Snapshot() map[string]any {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	// 兜底淘汰：finish 没跑到（进程内 panic、宿主强杀 goroutine）的活跃条目
	// 不能永远挂在快照里。
	for k, ep := range r.active {
		if now.Sub(ep.UpdatedAt) > queueStaleAfter {
			delete(r.active, k)
		}
	}
	kept := r.recent[:0]
	for _, ep := range r.recent {
		if now.Sub(ep.FinishedAt) <= queueRecentTTL {
			kept = append(kept, ep)
		}
	}
	r.recent = kept

	active := make([]*queueEpisode, 0, len(r.active))
	for _, ep := range r.active {
		active = append(active, ep)
	}
	sort.Slice(active, func(i, j int) bool { return active[i].StartedAt.Before(active[j].StartedAt) })

	activeOut := make([]map[string]any, 0, len(active))
	for _, ep := range active {
		activeOut = append(activeOut, ep.activeJSON(now))
	}
	recentOut := make([]map[string]any, 0, len(r.recent))
	for _, ep := range r.recent {
		recentOut = append(recentOut, ep.recentJSON())
	}

	return map[string]any{
		"ok":     true,
		"now":    now.Format(time.RFC3339),
		"active": activeOut,
		"recent": recentOut,
	}
}

// handleQueueQuery 是 GET /plugins/trae/queue 的处理器。
func handleQueueQuery() map[string]any {
	return soloQueue.Snapshot()
}
