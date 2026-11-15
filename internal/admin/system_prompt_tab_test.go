package admin

import (
	"strings"
	"testing"
)

func TestSystemPromptTabRendered(t *testing.T) {
	page := PageHTML()
	for _, want := range []string{
		`data-tab="tab-system-prompt"`,
		`id="tab-system-prompt"`,
		"System Prompt",
		`id="spModeSeg"`,
		`id="spRealtimeRows"`,
		`id="spHistoryRows"`,
		`id="spDateSelect"`,
		`data-mode="auto"`,
		`data-mode="keep"`,
		`data-mode="fold"`,
	} {
		if !strings.Contains(page, want) {
			t.Fatalf("页面缺少 %q", want)
		}
	}
}

func TestUsageTabIndexShiftedTo07(t *testing.T) {
	page := PageHTML()
	if !strings.Contains(page, `07 /</span> 用量与明细`) {
		t.Fatal("用量 tab 应顺延为 07")
	}
	if !strings.Contains(page, `06 /</span> System Prompt`) {
		t.Fatal("06 / 应分配给 System Prompt")
	}
}

func TestSystemPromptTabIsTopLevel(t *testing.T) {
	page := PageHTML()
	// 必须是独立一级 tab，而不是塞进 05 模型与路由里。
	models := strings.Index(page, `id="tab-models"`)
	sp := strings.Index(page, `id="tab-system-prompt"`)
	usage := strings.Index(page, `id="tab-usage"`)
	if models < 0 || sp < 0 || usage < 0 {
		t.Fatalf("panel 缺失 models=%d sp=%d usage=%d", models, sp, usage)
	}
	if !(models < sp && sp < usage) {
		t.Fatalf("顺序应为 models < system-prompt < usage，实际 %d/%d/%d", models, sp, usage)
	}
}

func TestSystemPromptTabScriptWiring(t *testing.T) {
	page := PageHTML()
	for _, want := range []string{
		"function refreshSystemPrompt(",
		"function spSwitchMode(",
		"function refreshSpHistory(",
		"function spResultLabel(",
		"/direct-admin/api/system-prompt/events",
		"/direct-admin/api/system-prompt",
		"visibilityState",
	} {
		if !strings.Contains(page, want) {
			t.Fatalf("脚本缺少 %q", want)
		}
	}
}

func TestUsageTableHasSystemColumn(t *testing.T) {
	page := PageHTML()
	if !strings.Contains(page, "<th>System</th>") {
		t.Fatal("用量明细表应新增 System 列")
	}
	// 新增一列后 colspan 必须从 11 跟上，否则空态会错位。
	if strings.Contains(page, `colspan="11"`) {
		t.Fatal("用量表 colspan 仍为 11，加列后应为 12")
	}
	if !strings.Contains(page, "systemForm") {
		t.Fatal("用量表应渲染 systemForm")
	}
}

func TestSystemPromptModeDescriptionsAreConcrete(t *testing.T) {
	page := PageHTML()
	// 「原样」既不是整个 body 原样，也不是使用端原样，指代不明 —— 不得再出现。
	if strings.Contains(page, "原样") {
		t.Fatal("模式说明不得再使用含糊的「原样」")
	}
	for _, want := range []string{
		"先直传，被拒再改写",
		"直传 system",
		"改写 system",
		"只决定",
		"role: system",
		"11128",
	} {
		if !strings.Contains(page, want) {
			t.Fatalf("模式说明缺少 %q", want)
		}
	}
	// 技术名仍必须保留（API / 日志 / .env 用它）。
	for _, want := range []string{`data-mode="auto"`, `data-mode="keep"`, `data-mode="fold"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("缺少技术名 %q", want)
		}
	}
}
