package systemprompt

import (
	"sync"
	"time"
)

const defaultJournalCapacity = 500

// Stats 是近 24 小时的聚合视图。
type Stats struct {
	Total    int `json:"total"`
	Keep     int `json:"keep"`
	Fold     int `json:"fold"`
	Degraded int `json:"degraded"`
	Blocked  int `json:"blocked"`
	OK       int `json:"ok"`
	Failed   int `json:"failed"`
}

// Journal 是保存最近 N 条事件的环形缓冲，供面板实时读取。
// 全部方法对 nil 接收者安全。
type Journal struct {
	mu       sync.RWMutex
	capacity int
	entries  []Event
}

// NewJournal 创建容量为 capacity 的事件流；capacity<=0 时用 500。
func NewJournal(capacity int) *Journal {
	if capacity <= 0 {
		capacity = defaultJournalCapacity
	}
	return &Journal{capacity: capacity, entries: make([]Event, 0, capacity)}
}

// Record 追加一条事件，超出容量时丢弃最旧的。
func (j *Journal) Record(e Event) {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.entries) >= j.capacity {
		copy(j.entries, j.entries[len(j.entries)-j.capacity+1:])
		j.entries = j.entries[:j.capacity-1]
	}
	j.entries = append(j.entries, e)
}

// Recent 返回最新在前的至多 limit 条事件副本。
func (j *Journal) Recent(limit int) []Event {
	if j == nil || limit <= 0 {
		return nil
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	n := limit
	if n > len(j.entries) {
		n = len(j.entries)
	}
	out := make([]Event, 0, n)
	for i := len(j.entries) - 1; i >= len(j.entries)-n; i-- {
		out = append(out, j.entries[i])
	}
	return out
}

// Stats24h 统计 now 往前 24 小时内的事件。
func (j *Journal) Stats24h(now time.Time) Stats {
	if j == nil {
		return Stats{}
	}
	cutoff := now.Add(-24 * time.Hour).UnixMilli()
	j.mu.RLock()
	defer j.mu.RUnlock()
	var s Stats
	for _, e := range j.entries {
		if e.At < cutoff {
			continue
		}
		s.Total++
		switch e.Form {
		case "keep":
			s.Keep++
		case "fold":
			s.Fold++
		}
		if e.Degraded {
			s.Degraded++
		}
		if e.ErrorCode == "11128" {
			s.Blocked++
		}
		if e.IsProblem() {
			s.Failed++
		} else {
			s.OK++
		}
	}
	return s
}

// Clear 清空内存事件流（不动磁盘历史）。
func (j *Journal) Clear() {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = j.entries[:0]
}
