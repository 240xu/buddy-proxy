package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wnddd839/codebuddy-proxy/internal/accounts"
	"github.com/wnddd839/codebuddy-proxy/internal/config"
	"github.com/wnddd839/codebuddy-proxy/internal/systemprompt"
)

func TestIsChannelBlocked(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"CodeBuddy chat completion failed with 400: request illegal (code 11128)", true},
		{"unapproved channel", true},
		{"UNAPPROVED CHANNEL", true},
		{"model rate limit (code 6004)", false},
		{"prompt is too long (code 11115)", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isChannelBlocked(errors.New(c.msg)); got != c.want {
			t.Fatalf("isChannelBlocked(%q)=%v want %v", c.msg, got, c.want)
		}
	}
	if isChannelBlocked(nil) {
		t.Fatal("nil 应返回 false")
	}
}

func TestSystemPromptModeDrivesKeepSystem(t *testing.T) {
	svc := New(config.Config{Site: "domestic"}, nil)
	t.Cleanup(func() { _ = svc.Close() })

	svc.SetBehaviorSwitches(BehaviorSwitches{SystemPromptMode: config.SystemPromptModeFold})
	if svc.chatOptionsFromAccount(accounts.Account{Site: "domestic"}, CompleteOptions{}).KeepSystem {
		t.Fatal("fold 模式 KeepSystem 应为 false")
	}
	svc.SetBehaviorSwitches(BehaviorSwitches{SystemPromptMode: config.SystemPromptModeKeep})
	if !svc.chatOptionsFromAccount(accounts.Account{Site: "domestic"}, CompleteOptions{}).KeepSystem {
		t.Fatal("keep 模式 KeepSystem 应为 true")
	}
	svc.SetBehaviorSwitches(BehaviorSwitches{SystemPromptMode: config.SystemPromptModeAuto})
	if !svc.chatOptionsFromAccount(accounts.Account{Site: "domestic"}, CompleteOptions{}).KeepSystem {
		t.Fatal("auto 模式首试 KeepSystem 应为 true")
	}
	// 已降级过的请求必须强制 fold，否则会在 11128 上无限递归。
	svc.SetBehaviorSwitches(BehaviorSwitches{SystemPromptMode: config.SystemPromptModeAuto})
	if svc.chatOptionsFromAccount(accounts.Account{Site: "domestic"}, CompleteOptions{FoldedRetry: true}).KeepSystem {
		t.Fatal("FoldedRetry 必须强制 fold")
	}
}

func TestSystemPromptModeSurvivesOtherSwitchUpdates(t *testing.T) {
	svc := New(config.Config{Site: "domestic"}, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetBehaviorSwitches(BehaviorSwitches{SystemPromptMode: config.SystemPromptModeKeep})
	// 其它开关整批下发时不带 SystemPromptMode，不能把模式清回 auto。
	svc.SetBehaviorSwitches(BehaviorSwitches{QuotaFloorCredits: 12})
	if got := svc.Config().SystemPromptMode; got != config.SystemPromptModeKeep {
		t.Fatalf("模式被意外重置：%q", got)
	}
}

func TestRecordSystemPromptEventNeverStoresPlaintext(t *testing.T) {
	dir := t.TempDir()
	svc := New(config.Config{Site: "domestic", AccountsPath: filepath.Join(dir, "accounts.json")}, nil)
	t.Cleanup(func() { _ = svc.Close() })

	const secret = "ZQ7X-SECRET-MARKER"
	svc.RecordSystemPromptEvent(systemPromptEventFixture(secret))

	recent := svc.SystemPromptJournal.Recent(10)
	if len(recent) != 1 {
		t.Fatalf("应记录 1 条，实际 %d", len(recent))
	}
	blob := recent[0].Fingerprint + " " + recent[0].ErrorCode + " " + recent[0].Model
	if strings.Contains(blob, secret) {
		t.Fatalf("事件里不得出现 system 原文：%q", blob)
	}
	// 落盘文件同样不能有原文。
	matches, _ := filepath.Glob(filepath.Join(dir, "proxy-systemprompt-*.jsonl"))
	if len(matches) != 1 {
		t.Fatalf("应写 1 个按天文件，实际 %v", matches)
	}
	raw, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatalf("落盘文件里出现 system 原文：%s", raw)
	}
}

// blockOnceTransport 第一次 chat 调用返回 11128，之后返回正常 SSE。
// neverBlock=true 时永远返回正常 SSE（用于验证不重试的路径）。
type blockOnceTransport struct {
	mu         sync.Mutex
	chats      int
	bodies     []string
	neverBlock bool
}

func (t *blockOnceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	header := make(http.Header)
	path := req.URL.Path
	if strings.Contains(path, "get-user-resource") {
		header.Set("Content-Type", "application/json")
		return &http.Response{
			StatusCode: http.StatusOK, Status: "OK", Header: header,
			Body: io.NopCloser(strings.NewReader(`{"code":1,"msg":"no fixture"}`)), Request: req,
		}, nil
	}
	if strings.Contains(path, "get-dosage-notify") {
		header.Set("Content-Type", "application/json")
		return &http.Response{
			StatusCode: http.StatusOK, Status: "OK", Header: header,
			Body: io.NopCloser(strings.NewReader(`{"code":0,"data":{"dosageNotifyCode":0}}`)), Request: req,
		}, nil
	}
	var raw []byte
	if req.Body != nil {
		raw, _ = io.ReadAll(req.Body)
	}
	t.mu.Lock()
	t.chats++
	n := t.chats
	neverBlock := t.neverBlock
	t.bodies = append(t.bodies, string(raw))
	t.mu.Unlock()
	if n == 1 && !neverBlock {
		header.Set("Content-Type", "application/json")
		return &http.Response{
			StatusCode: http.StatusBadRequest, Status: "Bad Request", Header: header,
			Body: io.NopCloser(strings.NewReader(`{"error":{"message":"request illegal (code 11128)","code":11128}}`)), Request: req,
		}, nil
	}
	header.Set("Content-Type", "text/event-stream")
	return &http.Response{
		StatusCode: http.StatusOK, Status: "OK", Header: header,
		Body: io.NopCloser(strings.NewReader(
			"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")), Request: req,
	}, nil
}

func (t *blockOnceTransport) snapshotBodies() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string{}, t.bodies...)
}

func TestAutoModeDowngradesOn11128(t *testing.T) {
	dir := t.TempDir()
	svc := New(config.Config{
		Site:             "domestic",
		AccountsPath:     filepath.Join(dir, "accounts.json"),
		UsagePath:        filepath.Join(dir, "proxy-usage.json"),
		SystemPromptMode: config.SystemPromptModeAuto,
	}, nil)
	t.Cleanup(func() { _ = svc.Close() })

	if _, _, err := svc.Pool.Upsert(accounts.CreateAccount(accounts.Account{
		Label: "a", Site: "domestic", BearerToken: "token-a", Enabled: true,
		AuthStatus: accounts.AuthStatus{UserID: "u-a"},
	})); err != nil {
		t.Fatal(err)
	}
	transport := &blockOnceTransport{}
	svc.Provider.HTTP = &http.Client{Transport: transport}

	const clientSystem = "You are Claude Code, Anthropic's official CLI for Claude."
	outcome := &SystemPromptOutcome{}
	_, err := svc.CompleteFromPool(context.Background(), CompleteOptions{
		Model: "auto",
		Messages: []map[string]any{
			{"role": "system", "content": clientSystem},
			{"role": "user", "content": "hi"},
		},
		SystemOutcome: outcome,
	})
	if err != nil {
		t.Fatalf("降级后应成功，实际 %v", err)
	}

	bodies := transport.snapshotBodies()
	if len(bodies) != 2 {
		t.Fatalf("应打 2 次上游（keep 失败 + fold 重试），实际 %d", len(bodies))
	}
	if !strings.Contains(bodies[0], "You are Claude Code") {
		t.Fatalf("第一次应原样带 system，实际 body=%s", bodies[0])
	}
	folded := chatBodyMessages(t, bodies[1])
	if len(folded) == 0 || folded[0]["role"] != "system" {
		t.Fatalf("第二次首条应是 system，实际 %v", folded)
	}
	if folded[0]["content"] == clientSystem {
		t.Fatalf("第二次不应再原样带客户端 system：%v", folded[0])
	}
	if !strings.Contains(bodies[1], "client_instructions") {
		t.Fatalf("第二次应把 system 折进 client_instructions，实际 body=%s", bodies[1])
	}

	if outcome.Form != "fold" || !outcome.Degraded {
		t.Fatalf("outcome 应记录 fold+degraded，实际 %+v", outcome)
	}

	events := svc.SystemPromptJournal.Recent(10)
	if len(events) != 2 {
		t.Fatalf("应记 2 条事件（keep/11128 + fold/200），实际 %d：%+v", len(events), events)
	}
	// Recent 最新在前。
	if events[0].Form != "fold" || events[0].Status != http.StatusOK || events[0].Degraded {
		t.Fatalf("最新一条应是 fold/200，实际 %+v", events[0])
	}
	if events[1].Form != "keep" || !events[1].Degraded || events[1].ErrorCode != "11128" {
		t.Fatalf("较早一条应是 keep/11128/degraded，实际 %+v", events[1])
	}
	if events[0].Fingerprint == "" || events[0].Fingerprint != events[1].Fingerprint {
		t.Fatalf("两条事件指纹应一致且非空：%q vs %q", events[0].Fingerprint, events[1].Fingerprint)
	}
	if events[0].SystemLen != len(clientSystem) {
		t.Fatalf("SystemLen=%d want %d", events[0].SystemLen, len(clientSystem))
	}
}

func TestKeepModeDoesNotRetryOn11128(t *testing.T) {
	dir := t.TempDir()
	svc := New(config.Config{
		Site:             "domestic",
		AccountsPath:     filepath.Join(dir, "accounts.json"),
		SystemPromptMode: config.SystemPromptModeKeep,
	}, nil)
	t.Cleanup(func() { _ = svc.Close() })

	if _, _, err := svc.Pool.Upsert(accounts.CreateAccount(accounts.Account{
		Label: "a", Site: "domestic", BearerToken: "token-a", Enabled: true,
		AuthStatus: accounts.AuthStatus{UserID: "u-a"},
	})); err != nil {
		t.Fatal(err)
	}
	transport := &blockOnceTransport{}
	svc.Provider.HTTP = &http.Client{Transport: transport}

	_, err := svc.CompleteFromPool(context.Background(), CompleteOptions{
		Model: "auto",
		Messages: []map[string]any{
			{"role": "system", "content": "You are Claude Code, Anthropic's official CLI for Claude."},
			{"role": "user", "content": "hi"},
		},
	})
	if err == nil {
		t.Fatal("keep 模式应把 11128 原样返回")
	}
	if got := len(transport.snapshotBodies()); got != 1 {
		t.Fatalf("keep 模式不得重试，实际打了 %d 次", got)
	}
}

func TestFoldModeNeverSendsClientSystem(t *testing.T) {
	dir := t.TempDir()
	svc := New(config.Config{
		Site:             "domestic",
		AccountsPath:     filepath.Join(dir, "accounts.json"),
		SystemPromptMode: config.SystemPromptModeFold,
	}, nil)
	t.Cleanup(func() { _ = svc.Close() })

	if _, _, err := svc.Pool.Upsert(accounts.CreateAccount(accounts.Account{
		Label: "a", Site: "domestic", BearerToken: "token-a", Enabled: true,
		AuthStatus: accounts.AuthStatus{UserID: "u-a"},
	})); err != nil {
		t.Fatal(err)
	}
	transport := &blockOnceTransport{neverBlock: true}
	svc.Provider.HTTP = &http.Client{Transport: transport}

	_, err := svc.CompleteFromPool(context.Background(), CompleteOptions{
		Model: "auto",
		Messages: []map[string]any{
			{"role": "system", "content": "SYSTEM-RAW-TEXT"},
			{"role": "user", "content": "hi"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	bodies := transport.snapshotBodies()
	if len(bodies) != 1 {
		t.Fatalf("应只打 1 次，实际 %d", len(bodies))
	}
	if strings.Contains(bodies[0], `"role":"system","content":"SYSTEM-RAW-TEXT"`) {
		t.Fatalf("fold 模式不应把客户端 system 原样发出：%s", bodies[0])
	}
	if !strings.Contains(bodies[0], "client_instructions") {
		t.Fatalf("fold 模式应折叠到 <client_instructions>：%s", bodies[0])
	}
}

// systemPromptEventFixture 构造一条带「原文」标记的事件，用于隐私断言。
func systemPromptEventFixture(secret string) systemprompt.Event {
	return systemprompt.Event{
		At:          time.Now().UnixMilli(),
		RequestID:   "req-fixture",
		Form:        "keep",
		Mode:        "auto",
		Fingerprint: systemprompt.Fingerprint("prefix " + secret + " suffix"),
		SystemLen:   len("prefix " + secret + " suffix"),
		Status:      http.StatusBadGateway,
		ErrorCode:   "11128",
		Model:       "auto",
	}
}

// chatBodyMessages 解析上游 chat 请求体里的 messages。
func chatBodyMessages(t *testing.T, body string) []map[string]any {
	t.Helper()
	var payload struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("请求体不是 JSON：%v\n%s", err, body)
	}
	return payload.Messages
}
