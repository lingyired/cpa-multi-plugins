package main

// v0.9.36 degraded-completion guard.
//
// The DeepSeek model behind WorkBuddy emits its tool call as DSML text. When
// the gateway loses that call, only the closing tags survive inside
// reasoning_content and the stream still terminates with finish_reason=stop.
// Every client then reports a completed turn with no answer and no tool call
// (field evidence 2026-10-07: 23 of the 25 leaked-DSML turns on this machine
// ended the client's turn that way, and the owner had to reply to resume).
//
// These tests pin the contract on all three upstream read paths - the async
// pump, the synchronous chunk collector, and the non-stream fold - plus the
// load-bearing error wording.

import (
	"bufio"
	"strings"
	"testing"
	"time"
)

const (
	// The real shape: reasoning text ends with leaked DSML closing tags, then
	// an empty terminal chunk claims a clean stop. The fullwidth bars are
	// written as Go escapes so this fixture stays plain ASCII.
	degradedReasoningFrame = "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"reasoning_content\":\"Let me read lines 1674-1730.\\n\\uff5c\\uff5cDSML\\uff5c\\uff5c parameter>\"}}]}"
	degradedStopFrame      = "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}"
	answeredStopFrame      = "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}"
	toolCallStopFrame      = "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"read\",\"arguments\":\"{\\\"path\\\":\\\"/a\\\"}\"}}]},\"finish_reason\":\"stop\"}]}"
	lengthStopFrame        = "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}"
	roleOnlyFrame          = "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}"
	emptyCallShellFrame    = "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"function_call\":null}}]}"
)

func TestCompletionCoverageDegraded(t *testing.T) {
	cases := []struct {
		name   string
		frames []string
		want   bool
	}{
		{"reasoning then stop", []string{degradedReasoningFrame, degradedStopFrame}, true},
		{"role-only then stop", []string{roleOnlyFrame, degradedStopFrame}, true},
		{"empty function_call shell then stop", []string{emptyCallShellFrame, degradedStopFrame}, true},
		{"answer then stop", []string{answeredStopFrame}, false},
		{"tool call then stop", []string{toolCallStopFrame}, false},
		{"reasoning then length", []string{degradedReasoningFrame, lengthStopFrame}, false},
		{"no terminal finish", []string{degradedReasoningFrame}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			coverage := &completionCoverage{}
			for _, frame := range tc.frames {
				coverage.observe([]byte(stripDataPrefix(frame)))
			}
			if got := coverage.degraded(); got != tc.want {
				t.Fatalf("degraded() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPumpStreamFramesFailsDegradedCompletion(t *testing.T) {
	stream := degradedReasoningFrame + "\n" + degradedStopFrame + "\n"
	scanner := bufio.NewScanner(strings.NewReader(stream))
	rec := &streamRecorder{}
	seen, handled := pumpStreamFrames(scanner, rec, nil, false, &sseUsageCollector{}, "gpt", "gpt", "uid-1", time.Now(), nil)
	if !seen {
		t.Fatal("the reasoning frame must count as payload")
	}
	if !handled {
		t.Fatal("the guard must report the terminal state")
	}
	chunks, errs := rec.snapshot()
	if len(errs) != 1 || !strings.Contains(errs[0], "degraded_completion") {
		t.Fatalf("want one degraded_completion error, got %q", errs)
	}
	if len(chunks) != 1 {
		t.Fatalf("only the reasoning chunk may reach the client, got %q", chunks)
	}
	if strings.Contains(chunks[0], "finish_reason") {
		t.Fatalf("the terminal stop chunk must not be forwarded: %q", chunks[0])
	}
}

func TestPumpStreamFramesKeepsAnsweredCompletion(t *testing.T) {
	scanner := bufio.NewScanner(strings.NewReader(answeredStopFrame + "\n"))
	rec := &streamRecorder{}
	_, handled := pumpStreamFrames(scanner, rec, nil, false, &sseUsageCollector{}, "gpt", "gpt", "uid-1", time.Now(), nil)
	if handled {
		t.Fatal("an answered completion must not be reported as a handled failure")
	}
	chunks, errs := rec.snapshot()
	if len(errs) != 0 {
		t.Fatalf("an answered completion must not error: %q", errs)
	}
	if len(chunks) != 1 || !strings.Contains(chunks[0], "done") {
		t.Fatalf("the answer chunk must reach the client: %q", chunks)
	}
}

func TestAggregateSSEWithCollectorFailsDegradedCompletion(t *testing.T) {
	stream := degradedReasoningFrame + "\n" + degradedStopFrame + "\n"
	_, err := aggregateSSEWithCollector(strings.NewReader(stream), false, &sseUsageCollector{})
	if err == nil || !strings.Contains(err.Error(), "degraded_completion") {
		t.Fatalf("want degraded_completion, got %v", err)
	}
}

func TestAggregateCompletionFailsDegradedCompletion(t *testing.T) {
	stream := degradedReasoningFrame + "\n" + degradedStopFrame + "\n"
	out, err := aggregateCompletion(strings.NewReader(stream), "m")
	if err == nil || !strings.Contains(err.Error(), "degraded_completion") {
		t.Fatalf("want degraded_completion, got %v", err)
	}
	if out != nil {
		t.Fatalf("no payload may survive a degraded completion: %s", out)
	}
}

func TestAggregateCompletionKeepsToolCallCompletion(t *testing.T) {
	out, err := aggregateCompletion(strings.NewReader(toolCallStopFrame+"\ndata: [DONE]\n\n"), "m")
	if err != nil {
		t.Fatalf("a tool-call completion must not be flagged: %v", err)
	}
	if !strings.Contains(string(out), "call_1") {
		t.Fatalf("the tool call must survive: %s", out)
	}
}

func TestAggregateCompletionKeepsLengthStop(t *testing.T) {
	out, err := aggregateCompletion(strings.NewReader(lengthStopFrame+"\n"), "m")
	if err != nil {
		t.Fatalf("finish_reason=length must stay a normal completion: %v", err)
	}
	if !strings.Contains(string(out), "\"finish_reason\":\"length\"") {
		t.Fatalf("length must surface unchanged: %s", out)
	}
}

func TestDegradedCompletionErrorStaysLifecycleClassified(t *testing.T) {
	message := degradedCompletionError().Error()
	if !strings.HasPrefix(message, "degraded_completion:") {
		t.Fatalf("plugin-side key missing: %q", message)
	}
	// CPA only skips credential cooling for a status-less error whose text
	// carries connection-lifecycle phrasing (conductor_cooldown.go).
	if !strings.Contains(strings.ToLower(message), "unexpected eof") {
		t.Fatalf("CPA would cool a healthy credential on: %q", message)
	}
}
