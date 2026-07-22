package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
)

// panicLookupSource 对指定 key 抛 panic，验证 ingest 主循环 recover（J2），
// 不再因用户 TableSource.Lookup bug 崩整进程。
type panicLookupSource struct {
	name    string
	boomKey string
	good    map[string]any
}

func (p panicLookupSource) Name() string { return p.name }
func (p panicLookupSource) Init() error  { return nil }
func (p panicLookupSource) Close() error { return nil }
func (p panicLookupSource) Lookup(key any) (map[string]any, bool) {
	vals, _ := key.([]any)
	var k any
	if len(vals) > 0 {
		k = vals[0]
	}
	if s, ok := k.(string); ok && s == p.boomKey {
		panic("boom: simulated user Lookup panic")
	}
	if s, ok := k.(string); ok && s == "ok" {
		return p.good, true
	}
	return nil, false
}

func TestJ2_PanicInLookupRecovered(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err := ssql.RegisterTableSource(panicLookupSource{
		name: "meta", boomKey: "boom", good: map[string]any{"location": "plantA"},
	}); err != nil {
		t.Fatalf("RegisterTableSource: %v", err)
	}

	var mu sync.Mutex
	var got []map[string]any
	ssql.AddSink(func(rows []map[string]any) {
		mu.Lock()
		got = append(got, rows...)
		mu.Unlock()
	})

	// 先发会触发 Lookup panic 的行，再发正常行：修复前 panic 直接崩进程；
	// 修复后 recover 记日志，正常行仍应到达 sink。
	ssql.Emit(map[string]any{"deviceId": "boom"})
	ssql.Emit(map[string]any{"deviceId": "ok"})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("panic in Lookup crashed/blocked the stream; valid row never reached sink")
	}
	if got[0]["location"] != "plantA" {
		t.Errorf("got %v, want location=plantA", got[0])
	}
}
