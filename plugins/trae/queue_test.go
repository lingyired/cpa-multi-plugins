package main

import (
	"strings"
	"testing"
	"time"

	"github.com/mmqz/cpa-multi-plugins/plugins/trae/upstream"
)

// queueSampleSSE 是 2026-09-29 soloprobe 实测抓到的开场序列（截断到前几条
// request_wait_in_queue），逐字节照抄 wire 形状，包含 progress_notice 的裸
// 字符串 data。
const queueSampleSSE = "event:progress_notice\n" +
	"data:\"Processing_1790673459\"\n\n" +
	"event:progress_notice\n" +
	"data:\";Processing\"\n\n" +
	"event:queue_begin\n" +
	"data:{\"queue_id\":\"5zyI415F\",\"queue_type\":1,\"is_async\":false,\"request_uuid\":\"16ddbd82-69fa-4061-95fd-b5f8e2859d14\"}\n\n" +
	"event:request_wait_in_queue\n" +
	"data:{\"position\":560,\"message\":\"Too many current requests. Your queue position is 560. Please wait for a while.\",\"queue_id\":\"5zyI415F\",\"request_uuid\":\"16ddbd82-69fa-4061-95fd-b5f8e2859d14\"}\n\n" +
	"event:request_wait_in_queue\n" +
	"data:{\"position\":550,\"message\":\"Too many current requests. Your queue position is 550. Please wait for a while.\",\"queue_id\":\"5zyI415F\",\"request_uuid\":\"16ddbd82-69fa-4061-95fd-b5f8e2859d14\"}\n\n" +
	"event:output\n" +
	"data:{\"response\":\"收到\",\"reasoning_content\":\"\",\"tool_calls\":null}\n\n" +
	"event:done\n" +
	"data:{\"finish_reason\":\"stop\"}\n\n"

// TestParseSOLOLineQueueEvents 锁死排队事件的字段映射，含 progress_notice 的
// 裸字符串 data（旧实现会因 json.Unmarshal 到 map 失败而整行丢弃）。
func TestParseSOLOLineQueueEvents(t *testing.T) {
	ev, err := upstream.ParseSOLOLine("progress_notice", `"Processing_1790673459"`)
	if err != nil {
		t.Fatalf("progress_notice should not error, got %v", err)
	}
	if ev.Notice != "Processing_1790673459" {
		t.Errorf("Notice=%q want %q", ev.Notice, "Processing_1790673459")
	}

	ev, err = upstream.ParseSOLOLine("queue_begin",
		`{"queue_id":"5zyI415F","queue_type":1,"is_async":false,"request_uuid":"uuid-1"}`)
	if err != nil {
		t.Fatalf("queue_begin: %v", err)
	}
	if ev.QueueID != "5zyI415F" || ev.QueueType != 1 || ev.RequestUUID != "uuid-1" {
		t.Errorf("queue_begin parse: %+v", ev)
	}
	if ev.HasPosition {
		t.Error("queue_begin must not claim a position")
	}

	ev, err = upstream.ParseSOLOLine("request_wait_in_queue",
		`{"position":537,"message":"queued","queue_id":"5zyI415F","request_uuid":"uuid-1"}`)
	if err != nil {
		t.Fatalf("request_wait_in_queue: %v", err)
	}
	if !ev.HasPosition || ev.Position != 537 || ev.Message != "queued" {
		t.Errorf("request_wait_in_queue parse: %+v", ev)
	}
	if !ev.IsQueueEvent() {
		t.Error("request_wait_in_queue must report IsQueueEvent")
	}
	if (&upstream.SOLOEvent{Event: "output"}).IsQueueEvent() {
		t.Error("output must not report IsQueueEvent")
	}
}

// TestAggregateWithObserverSeesQueueEvents 确认非流式（聚合）路径也能观测排队：
// 此前 Aggregate 只在 switch 里认 output/token_usage/done/error。
func TestAggregateWithObserverSeesQueueEvents(t *testing.T) {
	var seen []string
	var positions []int64
	_, err := upstream.AggregateWithObserver(strings.NewReader(queueSampleSSE), func(ev *upstream.SOLOEvent) {
		seen = append(seen, ev.Event)
		if ev.HasPosition {
			positions = append(positions, ev.Position)
		}
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	joined := strings.Join(seen, ",")
	for _, want := range []string{"progress_notice", "queue_begin", "request_wait_in_queue", "output", "done"} {
		if !strings.Contains(joined, want) {
			t.Errorf("observer missed %q; saw %s", want, joined)
		}
	}
	if len(positions) != 2 || positions[0] != 560 || positions[1] != 550 {
		t.Errorf("positions=%v want [560 550]", positions)
	}
}

// newTestWatcher 造一个隔离的 watcher（不污染全局 recent 断言之外的场景：
// 测试里对同一 watcher 取快照，不依赖全局具体条数）。
func newTestWatcher() *queueWatcher {
	return newQueueWatcher(queueRequest{
		UID: "4053245665740660", Nickname: "lingyirEd", Variant: "cn", Model: "deepseek-v4.1-flash",
	})
}

// findActive 在快照里按 registryKey 之外的可见字段找条目。
func findActive(snap map[string]any, uid string) map[string]any {
	rows, _ := snap["active"].([]map[string]any)
	for _, row := range rows {
		if row["uid"] == uid {
			return row
		}
	}
	return nil
}

// TestQueueWatcherTracksPositionAndFinish 覆盖一次完整排队：位次递减、
// max/min 收敛、首个 output 把 phase 推到 processing、Finish 落到 recent。
func TestQueueWatcherTracksPositionAndFinish(t *testing.T) {
	w := newTestWatcher()
	w.Observe(&upstream.SOLOEvent{Event: "queue_begin", QueueID: "q1", QueueType: 1, RequestUUID: "uuid-test-1"})
	w.Observe(&upstream.SOLOEvent{Event: "request_wait_in_queue", Position: 560, HasPosition: true, Message: "pos 560", RequestUUID: "uuid-test-1"})
	w.Observe(&upstream.SOLOEvent{Event: "request_wait_in_queue", Position: 546, HasPosition: true, Message: "pos 546", RequestUUID: "uuid-test-1"})

	row := findActive(soloQueue.Snapshot(), "4053245665740660")
	if row == nil {
		t.Fatal("queued request missing from active snapshot")
	}
	if row["position"] != int64(546) {
		t.Errorf("position=%v want 546 (latest wins)", row["position"])
	}
	if row["max_position"] != int64(560) || row["min_position"] != int64(546) {
		t.Errorf("max/min=%v/%v want 560/546", row["max_position"], row["min_position"])
	}
	if row["phase"] != "queued" {
		t.Errorf("phase=%v want queued", row["phase"])
	}
	if row["nickname"] != "lingyirEd" {
		t.Errorf("nickname=%v", row["nickname"])
	}

	// 首个 output = 出队。
	w.Observe(&upstream.SOLOEvent{Event: "output", Response: "收到"})
	if row := findActive(soloQueue.Snapshot(), "4053245665740660"); row == nil || row["phase"] != "processing" {
		t.Fatalf("after output phase=%v want processing", row)
	}

	w.Finish(false)
	if row := findActive(soloQueue.Snapshot(), "4053245665740660"); row != nil {
		t.Errorf("finished episode still active: %v", row)
	}
	snap := soloQueue.Snapshot()
	recent, _ := snap["recent"].([]map[string]any)
	var found map[string]any
	for _, r := range recent {
		if r["uid"] == "4053245665740660" && r["max_position"] == int64(560) {
			found = r
			break
		}
	}
	if found == nil {
		t.Fatal("finished episode missing from recent")
	}
	if found["outcome"] != "started" {
		t.Errorf("outcome=%v want started", found["outcome"])
	}
}

// TestQueueWatcherIgnoresRequestsThatNeverQueued 是一个明确的产品决定：
// 没排过队的请求不进 recent，否则快照会被普通请求刷满，看不出排队历史。
func TestQueueWatcherIgnoresRequestsThatNeverQueued(t *testing.T) {
	w := newQueueWatcher(queueRequest{UID: "never-queued", Variant: "cn", Model: "m"})
	w.Observe(&upstream.SOLOEvent{Event: "progress_notice", Notice: "Processing_1"})
	w.Observe(&upstream.SOLOEvent{Event: "output", Response: "hi"})
	w.Finish(false)

	snap := soloQueue.Snapshot()
	recent, _ := snap["recent"].([]map[string]any)
	for _, r := range recent {
		if r["uid"] == "never-queued" {
			t.Fatal("a request that never queued must not appear in recent")
		}
	}
	if findActive(snap, "never-queued") != nil {
		t.Fatal("a request that never queued must not appear in active")
	}
}

// TestQueueWatcherOutcomeMapping 锁死 Finish 的结果映射。
func TestQueueWatcherOutcomeMapping(t *testing.T) {
	cases := []struct {
		name    string
		faulted bool
		events  []upstream.SOLOEvent
		want    string
	}{
		{"error", true, []upstream.SOLOEvent{{Event: "queue_begin", RequestUUID: "u-err"}}, "error"},
		{"started", false, []upstream.SOLOEvent{{Event: "queue_begin", RequestUUID: "u-ok"}, {Event: "output"}}, "started"},
		{"canceled", false, []upstream.SOLOEvent{{Event: "request_wait_in_queue", Position: 12, HasPosition: true, RequestUUID: "u-cancel"}}, "canceled"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newQueueWatcher(queueRequest{UID: "outcome-" + c.name, Variant: "cn", Model: "m"})
			for i := range c.events {
				ev := c.events[i]
				w.Observe(&ev)
			}
			w.Finish(c.faulted)
			snap := soloQueue.Snapshot()
			recent, _ := snap["recent"].([]map[string]any)
			for _, r := range recent {
				if r["uid"] == "outcome-"+c.name {
					if r["outcome"] != c.want {
						t.Fatalf("outcome=%v want %v", r["outcome"], c.want)
					}
					return
				}
			}
			t.Fatalf("episode %s missing from recent", c.name)
		})
	}
}

// TestQueueWatcherStaleEviction 保证 Finish 没跑到的条目不会永远挂在 active 里。
func TestQueueWatcherStaleEviction(t *testing.T) {
	w := newTestWatcher()
	w.Observe(&upstream.SOLOEvent{Event: "queue_begin", RequestUUID: "uuid-stale"})

	soloQueue.mu.Lock()
	for _, ep := range soloQueue.active {
		if ep.RequestUUID == "uuid-stale" {
			ep.UpdatedAt = time.Now().Add(-2 * queueStaleAfter)
		}
	}
	soloQueue.mu.Unlock()

	if row := findActive(soloQueue.Snapshot(), "4053245665740660"); row != nil {
		// 同一 uid 可能有别的测试残留，只断言这条 stale 记录被清掉。
		soloQueue.mu.Lock()
		_, still := soloQueue.active["uuid:uuid-stale"]
		soloQueue.mu.Unlock()
		if still {
			t.Fatalf("stale episode survived eviction: %v", row)
		}
	}
	w.Finish(false)
}

// TestConvertSOLOStreamToOpenAIForwardsQueueEvents 确认流式转换器也会把排队
// 事件交给 observer —— 这是 DSH 侧能实时看到位次的唯一来源。
func TestConvertSOLOStreamToOpenAIForwardsQueueEvents(t *testing.T) {
	var got []*upstream.SOLOEvent
	ch := convertSOLOStreamToOpenAI(strings.NewReader(queueSampleSSE), "m1", nil, func(ev *upstream.SOLOEvent) {
		got = append(got, ev)
	})
	for range ch {
	}
	var positions []int64
	var sawBegin bool
	for _, ev := range got {
		switch ev.Event {
		case "queue_begin":
			sawBegin = true
		case "request_wait_in_queue":
			positions = append(positions, ev.Position)
		}
	}
	if !sawBegin {
		t.Error("observer never saw queue_begin")
	}
	if len(positions) != 2 || positions[0] != 560 || positions[1] != 550 {
		t.Errorf("positions=%v want [560 550]", positions)
	}
}
