package systemprompt

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const storeFilePrefix = "proxy-systemprompt-"
const storeFileSuffix = ".jsonl"
const storeDateLayout = "20060102"

// Store 按天把事件追加到 jsonl 文件。
// dir 为空时退化为纯内存 store（测试与未配置场景）。
type Store struct {
	mu        sync.Mutex
	dir       string
	retention time.Duration
	mem       []Event
	memDates  map[string]struct{}
}

// OpenStore 打开（或创建）dir 下的按天事件存储。
// retention<=0 时用 7 天。
func OpenStore(dir string, retention time.Duration) (*Store, error) {
	if retention <= 0 {
		retention = 7 * 24 * time.Hour
	}
	s := &Store{dir: dir, retention: retention}
	if dir == "" {
		s.memDates = make(map[string]struct{})
		return s, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("systemprompt: 创建事件目录 %s 失败: %w", dir, err)
	}
	return s, nil
}

// Close 目前无资源需要释放，保留以便调用方统一 defer。
func (s *Store) Close() error { return nil }

func (s *Store) pathFor(date string) string {
	return filepath.Join(s.dir, storeFilePrefix+date+storeFileSuffix)
}

func eventDate(e Event) string {
	return time.UnixMilli(e.At).In(time.Local).Format(storeDateLayout)
}

// Append 写入一条事件。单次 Write 写完整行，并发下不会出现半行交错。
func (s *Store) Append(e Event) error {
	if s == nil {
		return nil
	}
	date := eventDate(e)
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("systemprompt: 序列化事件失败: %w", err)
	}
	line = append(line, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dir == "" {
		s.mem = append(s.mem, e)
		if s.memDates == nil {
			s.memDates = make(map[string]struct{})
		}
		s.memDates[date] = struct{}{}
		return nil
	}

	f, err := os.OpenFile(s.pathFor(date), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("systemprompt: 打开事件文件失败: %w", err)
	}
	defer f.Close()
	// 一次 Write 一整行：O_APPEND 下单次写是原子的，避免高并发行交错。
	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("systemprompt: 写入事件失败: %w", err)
	}
	return nil
}

// Dates 返回升序的日期列表（20060102）。
func (s *Store) Dates() []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dir == "" {
		out := make([]string, 0, len(s.memDates))
		for d := range s.memDates {
			out = append(out, d)
		}
		sort.Strings(out)
		return out
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || !strings.HasPrefix(name, storeFilePrefix) || !strings.HasSuffix(name, storeFileSuffix) {
			continue
		}
		date := strings.TrimSuffix(strings.TrimPrefix(name, storeFilePrefix), storeFileSuffix)
		if _, err := time.ParseInLocation(storeDateLayout, date, time.Local); err != nil {
			continue
		}
		out = append(out, date)
	}
	sort.Strings(out)
	return out
}

// Read 返回指定日期的事件页（最新在前）与当日总条数。
func (s *Store) Read(date string, offset, limit int) ([]Event, int, error) {
	if s == nil {
		return nil, 0, nil
	}
	date = strings.TrimSpace(date)
	if _, err := time.ParseInLocation(storeDateLayout, date, time.Local); err != nil {
		return nil, 0, fmt.Errorf("systemprompt: 非法日期 %q", date)
	}

	s.mu.Lock()
	// 内存 store 无文件，直接读 mem 切片；磁盘 store 在锁外读文件更宽松，
	// 但保持简单：锁内读，保证与 Append 的可见性一致。
	defer s.mu.Unlock()

	var all []Event
	if s.dir == "" {
		for _, e := range s.mem {
			if eventDate(e) == date {
				all = append(all, e)
			}
		}
	} else {
		events, err := s.readFileEventsLocked(date)
		if err != nil {
			return nil, 0, err
		}
		all = events
	}

	total := len(all)
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = total
	}
	// 反序：最新在前。
	out := make([]Event, 0, limit)
	for i := total - 1 - offset; i >= 0 && len(out) < limit; i-- {
		out = append(out, all[i])
	}
	return out, total, nil
}

func (s *Store) readFileEventsLocked(date string) ([]Event, error) {
	raw, err := os.ReadFile(s.pathFor(date))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("systemprompt: 读取事件文件失败: %w", err)
	}
	var out []Event
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			// 坏行跳过：半行/手工编辑都不该让整页读不出来。
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// Prune 删除超过 retention 的按天文件。
func (s *Store) Prune(now time.Time) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dir == "" {
		cutoffDate := now.Add(-s.retention).Format(storeDateLayout)
		kept := s.mem[:0]
		dates := make(map[string]struct{})
		for _, e := range s.mem {
			d := eventDate(e)
			if d < cutoffDate {
				continue
			}
			kept = append(kept, e)
			dates[d] = struct{}{}
		}
		s.mem = kept
		s.memDates = dates
		return nil
	}

	cutoff := now.Add(-s.retention)
	for _, date := range s.datesLocked() {
		day, err := time.ParseInLocation(storeDateLayout, date, time.Local)
		if err != nil {
			continue
		}
		// 以「该日结束时刻」判过期，避免把当天整天误删。
		if day.Add(24 * time.Hour).After(cutoff) {
			continue
		}
		if err := os.Remove(s.pathFor(date)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("systemprompt: 删除过期事件文件 %s 失败: %w", date, err)
		}
	}
	return nil
}

func (s *Store) datesLocked() []string {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || !strings.HasPrefix(name, storeFilePrefix) || !strings.HasSuffix(name, storeFileSuffix) {
			continue
		}
		date := strings.TrimSuffix(strings.TrimPrefix(name, storeFilePrefix), storeFileSuffix)
		if _, err := time.ParseInLocation(storeDateLayout, date, time.Local); err != nil {
			continue
		}
		out = append(out, date)
	}
	sort.Strings(out)
	return out
}
