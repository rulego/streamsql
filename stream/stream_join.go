/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stream

import (
	"container/list"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/metrics"
	"github.com/rulego/streamsql/types"
)

// 流-流 JOIN（WITHIN 窗口化 interval join）状态引擎。设计见
// docs/STREAM_STREAM_JOIN_DESIGN.md：N 条流 = N-1 个二元 joinStageRuntime
// 左深级联；每级双侧缓冲、保留期回收、sweeper（内嵌于本级 processor 的 select）、
// Stop 按级联序 Flush。
//
// 统一时间模型：每行 ts = TsProp 提取的事件时间（存在则必取），否则到达
// 时刻（processing-time）。匹配谓词 |L.ts − R.ts| ≤ WITHIN；行保留期 = 本侧
// maxSeenTs − row.ts ≤ WITHIN。迟到（超保留期）行丢弃并计数，不静默。

const (
	// defaultJoinMaxKeys 每侧缓冲 key 数上限默认值（对齐 analytic/cep 的 10000）。
	defaultJoinMaxKeys = 10000
	// joinSweepMinInterval sweeper ticker 下限，防极小 WITHIN 产生过密 ticker。
	joinSweepMinInterval = 50 * time.Millisecond
)

// joinMsg 是 JOIN 输入通道的消息：raw 流行（explicit=false，ts 到达时提取）或上级
// 复合行（explicit=true，ts 为上级算好的 max(两匹配行 ts)，仅 event-time 有意义）。
type joinMsg struct {
	row      map[string]any
	ts       int64
	explicit bool
}

// joinRow 是缓冲中的一行：事件时间戳、LEFT 已匹配标记（防重复补发 NULL）、原始数据。
type joinRow struct {
	ts      int64
	matched bool
	data    map[string]any
}

// keyBuf 是一个归一键下的窗内行集合；el 指向 LRU 链表节点（淘汰序）。
type keyBuf struct {
	key  string
	rows []*joinRow
	el   *list.Element
}

// joinSide 是一侧的缓冲状态：归一键 → 窗内行 + LRU + 行数闸 + 本侧最简水位。
// 单 goroutine（本级 processor）所有，无锁。
type joinSide struct {
	bufs        map[string]*keyBuf
	lru         *list.List
	rows        int
	maxTs       int64 // 本侧 maxSeenTs（最简水位）
	sweptTs     int64 // 上次懒清理扫过时的水位（门控，见 process）
	lastArrival int64 // 最后到行墙钟（ns），idle 判定用
}

func newJoinSide() joinSide {
	return joinSide{bufs: make(map[string]*keyBuf), lru: list.New()}
}

func (s *joinSide) reset() {
	s.bufs = make(map[string]*keyBuf)
	s.lru = list.New()
	s.rows = 0
	s.sweptTs = 0
}

// joinStageRuntime 是级联中的一级二元 JOIN runner（对标 cepRunner 的角色）。
type joinStageRuntime struct {
	idx         int // 1-based 级序（指标前缀 join_stage{idx}_*）
	cfg         types.StreamJoinStage
	sourceAlias string // FROM 别名（仅级 1 的输出行需要暴露）
	left        joinSide
	right       joinSide
	leftIn      <-chan joinMsg
	rightIn     <-chan joinMsg
	emit        func(row map[string]any, ts int64) // 输出回调：中间级=下级入口，末级=直连尾巴

	eventTime   bool
	tsProp      string
	idleTimeout time.Duration
	maxKeys     int
	maxRows     int
	log         logger.Logger
	now         func() int64 // 墙钟（ns），可注入；processing-time 戳与 idle 判定用

	// 指标（级前缀）
	mMatches      *metrics.Counter
	mLeftTimeout  *metrics.Counter
	mLateDropped  *metrics.Counter
	mInputDropped *metrics.Counter
	mRowsDropped  *metrics.Counter
	mKeysEvicted  *metrics.Counter
	gBuffered     *metrics.Gauge
	gLeftWm       *metrics.Gauge
	gRightWm      *metrics.Gauge

	// 10s 节流告警（照抄 reportGroupEvictions 模式）
	lastLateWarn  int64 // unix sec
	lastLateSeen  int64
	lastRowsWarn  int64
	lastRowsSeen  int64
	lastKeysWarn  int64
	lastKeysSeen  int64
	lastInputWarn int64
	lastInputSeen int64
}

// joinStageParams 构造参数（pipeline 接线与测试直调共用）。
type joinStageParams struct {
	idx         int
	cfg         types.StreamJoinStage
	sourceAlias string
	eventTime   bool
	tsProp      string
	idleTimeout time.Duration
	maxKeys     int
	maxRows     int
	reg         *metrics.Registry
	log         logger.Logger
	now         func() int64
}

func newJoinStageRuntime(p joinStageParams) *joinStageRuntime {
	if p.maxKeys <= 0 {
		p.maxKeys = defaultJoinMaxKeys
	}
	if p.log == nil {
		p.log = logger.GetDefault()
	}
	if p.now == nil {
		p.now = func() int64 { return time.Now().UnixNano() }
	}
	if p.reg == nil {
		p.reg = metrics.NewRegistry()
	}
	prefix := fmt.Sprintf("join_stage%d_", p.idx)
	rt := &joinStageRuntime{
		idx:           p.idx,
		cfg:           p.cfg,
		sourceAlias:   p.sourceAlias,
		left:          newJoinSide(),
		right:         newJoinSide(),
		eventTime:     p.eventTime,
		tsProp:        p.tsProp,
		idleTimeout:   p.idleTimeout,
		maxKeys:       p.maxKeys,
		maxRows:       p.maxRows,
		log:           p.log,
		now:           p.now,
		mMatches:      p.reg.Counter(prefix + "matches_emitted"),
		mLeftTimeout:  p.reg.Counter(prefix + "left_timeout_emitted"),
		mLateDropped:  p.reg.Counter(prefix + "late_dropped"),
		mInputDropped: p.reg.Counter(prefix + "input_dropped"),
		mRowsDropped:  p.reg.Counter(prefix + "rows_dropped"),
		mKeysEvicted:  p.reg.Counter(prefix + "keys_evicted"),
		gBuffered:     p.reg.Gauge(prefix + "buffered_rows"),
		gLeftWm:       p.reg.Gauge(prefix + "left_watermark"),
		gRightWm:      p.reg.Gauge(prefix + "right_watermark"),
	}
	return rt
}

func (r *joinStageRuntime) withinNs() int64 { return r.cfg.Within.Nanoseconds() }

// run 是本级 processor 主循环：select 两条入口 + sweeper ticker + done。
// 状态由本 goroutine 独占（sweeper 内嵌，免锁、无 Stop 窗口竞态）。
func (r *joinStageRuntime) run(done <-chan struct{}) {
	interval := r.cfg.Within / 2
	if interval < joinSweepMinInterval {
		interval = joinSweepMinInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case msg := <-r.leftIn:
			r.processLeftMsg(msg)
		case msg := <-r.rightIn:
			r.processRightMsg(msg)
		case <-t.C:
			r.sweepTick()
		case <-done:
			return
		}
	}
}

// processLeftMsg / processRightMsg 处理一条左/右入口消息（测试可直调）。
func (r *joinStageRuntime) processLeftMsg(msg joinMsg)  { r.process(&r.left, &r.right, msg) }
func (r *joinStageRuntime) processRightMsg(msg joinMsg) { r.process(&r.right, &r.left, msg) }

// process 处理一条到行：取 ts → 更新本侧水位 → 迟到判定 → 懒清理本侧 →
// 入缓冲（maxRows/maxKeys 闸）→ 扫对侧同 key 邻近行逐对产出。
func (r *joinStageRuntime) process(side *joinSide, other *joinSide, msg joinMsg) {
	ts, ok := r.rowTs(msg)
	if !ok {
		r.noteLateDropped(1)
		return
	}
	if ts > side.maxTs {
		side.maxTs = ts
	}
	side.lastArrival = r.now()
	// 迟到（事件时间）：到行时已超本侧保留期 → 丢弃（可观测，不静默）。
	if side.maxTs-ts > r.withinNs() {
		r.noteLateDropped(1)
		return
	}
	// 懒清理：回收本侧超保留期行（LEFT 未匹配的先补发 NULL，单一出口）。
	// 门控：水位自上次清扫后再推进 ≥ WITHIN/4 才全量扫——逐行全扫在水位停滞的
	// 高吞吐场景是 O(n²)；时延兜底由 sweeper 的 within/2 ticker 提供，语义不变。
	if side.maxTs-side.sweptTs >= r.withinNs()/4 {
		r.expireSide(side, side == &r.left)
		side.sweptTs = side.maxTs
	}

	key := r.keyFor(side, msg.row)
	ar := &joinRow{ts: ts, data: msg.row}
	if !r.insert(side, key, ar) {
		return // maxRows 丢行（已计数）
	}
	r.scanAndEmit(other, key, ar, side == &r.left)
	r.updateGauges()
}

// rowTs 提取行的 ts：event-time 显式 ts（上级复合行）或 TsProp 字段（数值归一 ns），
// 否则到达墙钟。TsProp 缺失返回 ok=false（调用方丢行计数）。
func (r *joinStageRuntime) rowTs(msg joinMsg) (int64, bool) {
	if !r.eventTime {
		return r.now(), true
	}
	if msg.explicit {
		return msg.ts, true
	}
	v, ok := streamFieldValue(msg.row, r.tsProp)
	if !ok || v == nil {
		return 0, false
	}
	return normalizeJoinTs(toJoinInt64(v)), true
}

// keyFor 按 ON 对提取归一键：左侧行用 StreamField（级 ≥2 可为 "b.k" 前缀路径），
// 右侧行用 TableField。键编码复用流表 JOIN 的数值归一（1/1.0/"1" 语义）。
func (r *joinStageRuntime) keyFor(side *joinSide, row map[string]any) string {
	vals := make([]any, len(r.cfg.OnPairs))
	for i := range r.cfg.OnPairs {
		f := r.cfg.OnPairs[i].TableField
		if side == &r.left {
			f = r.cfg.OnPairs[i].StreamField
		}
		v, _ := streamFieldValue(row, f)
		vals[i] = v
	}
	return encodeKey(vals)
}

// insert 入缓冲（先过 maxKeys LRU / maxRows 闸）。返回 false = 该行被丢弃（已计数）。
func (r *joinStageRuntime) insert(side *joinSide, key string, row *joinRow) bool {
	if kb := side.bufs[key]; kb != nil {
		if r.maxRows > 0 && side.rows >= r.maxRows {
			r.noteRowsDropped(1)
			return false
		}
		kb.rows = append(kb.rows, row)
		side.rows++
		side.lru.MoveToFront(kb.el)
		return true
	}
	// 新 key：先 LRU 淘汰到上限内。
	if r.maxKeys > 0 && len(side.bufs) >= r.maxKeys {
		r.evictLRU(side)
	}
	if r.maxRows > 0 && side.rows >= r.maxRows {
		r.noteRowsDropped(1)
		return false
	}
	kb := &keyBuf{key: key}
	kb.rows = append(kb.rows, row)
	kb.el = side.lru.PushFront(kb)
	side.bufs[key] = kb
	side.rows++
	return true
}

// evictLRU 淘汰最久未用的 key（连同其行）。淘汰含 pending LEFT 行时不补发 NULL
// （与聚合 LRU 同语义），计指标 + 10s 节流告警。
func (r *joinStageRuntime) evictLRU(side *joinSide) {
	oldest := side.lru.Back()
	if oldest == nil {
		return
	}
	kb := oldest.Value.(*keyBuf)
	side.lru.Remove(oldest)
	delete(side.bufs, kb.key)
	side.rows -= len(kb.rows)
	r.mKeysEvicted.Inc()
	pending := 0
	isLeft := side == &r.left
	if isLeft && r.cfg.JoinType == "LEFT" {
		for _, row := range kb.rows {
			if !row.matched {
				pending++
			}
		}
	}
	now := time.Now().Unix()
	if last := atomic.LoadInt64(&r.lastKeysWarn); now-last >= 10 && atomic.CompareAndSwapInt64(&r.lastKeysWarn, last, now) {
		sideLbl := "right"
		if isLeft {
			sideLbl = "left"
		}
		r.log.Warn("stream JOIN key cap exceeded on stage %d (%s side): evicted key %q with %d row(s) (total keys evicted %d, pending LEFT rows discarded %d); raise WithJoinMaxKeys above the peak active-key count.",
			r.idx, sideLbl, kb.key, len(kb.rows), r.mKeysEvicted.Value(), pending)
	}
}

// scanAndEmit 扫对侧同 key 且 |ts 差| ≤ WITHIN 的行，逐对产出。行保留在缓冲中
// （窗内可重复匹配），不因匹配删除。已过其本侧保留期的候选行不参与匹配
// （与 expireSide 同一谓词）：它们的出口是驱逐 / LEFT 补 NULL，保证"过期即
// 不再匹配"与声明的保留期语义严格一致——否则懒清理的滑动缝隙会让同一输入
// 在"匹配"与"补 NULL"之间随墙钟竞速漂移。
func (r *joinStageRuntime) scanAndEmit(other *joinSide, key string, ar *joinRow, arrivingLeft bool) {
	kb := other.bufs[key]
	if kb == nil {
		return
	}
	other.lru.MoveToFront(kb.el) // 命中访问提升 LRU
	within := r.withinNs()
	for _, m := range kb.rows {
		if other.maxTs-m.ts > within {
			continue // 已过保留期：等 expireSide 驱逐（LEFT 未匹配先补 NULL）
		}
		diff := m.ts - ar.ts
		if diff < 0 {
			diff = -diff
		}
		if diff > within {
			continue
		}
		if arrivingLeft {
			r.emitMatch(ar, m)
		} else {
			r.emitMatch(m, ar)
		}
	}
}

// emitMatch 产出一对匹配：合并行（与流表 JOIN 同构：左字段平铺 + 右行挂别名，
// 级 1 另暴露 FROM 别名）；LEFT 标记左行已匹配；事件时间模式的输出 ts = max(两行 ts)
// （复合事件发生时刻 = 其最晚组成行）。
func (r *joinStageRuntime) emitMatch(L, R *joinRow) {
	working := buildJoinWorkingRow(L.data, R.data, r.cfg.RightAlias, r.sourceAlias, r.idx)
	if r.cfg.JoinType == "LEFT" {
		L.matched = true
	}
	ts := int64(0)
	if r.eventTime {
		ts = L.ts
		if R.ts > ts {
			ts = R.ts
		}
	}
	r.mMatches.Inc()
	if r.emit != nil {
		r.emit(working, ts)
	}
}

// emitLeftNull 为超窗未匹配的 LEFT 行补发 NULL 行（空右表行挂别名）。
// 唯一出口：expireSide / flushLeft，两处都随行出缓冲，保证只补一次。
func (r *joinStageRuntime) emitLeftNull(L *joinRow) {
	working := buildJoinWorkingRow(L.data, emptyMetadataRow, r.cfg.RightAlias, r.sourceAlias, r.idx)
	r.mLeftTimeout.Inc()
	ts := int64(0)
	if r.eventTime {
		ts = L.ts
	}
	if r.emit != nil {
		r.emit(working, ts)
	}
}

// expireSide 回收 side 上超保留期的行（maxSeenTs − row.ts > WITHIN）。
// 左侧 LEFT 且未匹配的行先补发 NULL 再驱逐。
func (r *joinStageRuntime) expireSide(side *joinSide, isLeftSide bool) {
	within := r.withinNs()
	for el := side.lru.Front(); el != nil; {
		next := el.Next()
		kb := el.Value.(*keyBuf)
		kept := kb.rows[:0]
		for _, row := range kb.rows {
			if side.maxTs-row.ts > within {
				side.rows--
				if isLeftSide && r.cfg.JoinType == "LEFT" && !row.matched {
					r.emitLeftNull(row)
				}
				continue
			}
			kept = append(kept, row)
		}
		if len(kept) == 0 {
			side.lru.Remove(el)
			delete(side.bufs, kb.key)
		} else {
			kb.rows = kept
		}
		el = next
	}
}

// sweepTick 是 sweeper 周期（processor 内嵌，不独立起 goroutine）：
//   - processing-time：墙钟即时间域，双侧水位推进到墙钟（否则停流后 LEFT NULL 永不触发）；
//   - event-time + IdleTimeout>0：空闲超时侧水位推进到墙钟（复用窗口 IdleTimeout 语义，
//     权衡：恢复后的迟到行会被丢，文档已写明）；
//   - 然后统一按保留期回收双侧。
func (r *joinStageRuntime) sweepTick() {
	now := r.now()
	sides := []*joinSide{&r.left, &r.right}
	for _, side := range sides {
		if r.eventTime {
			if r.idleTimeout <= 0 || side.lastArrival <= 0 {
				continue
			}
			if now-side.lastArrival >= r.idleTimeout.Nanoseconds() && now > side.maxTs {
				side.maxTs = now
			}
		} else if now > side.maxTs {
			side.maxTs = now
		}
	}
	r.expireSide(&r.left, true)
	r.expireSide(&r.right, false)
	r.updateGauges()
}

// flushLeft Stop-Flush：全部未匹配 LEFT pending 补发 NULL 后清空双侧缓冲。
// 仅在所有 processor goroutine join 后由 Stop 单线程调用（经 pipeline 级联序执行）。
func (r *joinStageRuntime) flushLeft() {
	if r.cfg.JoinType == "LEFT" {
		for el := r.left.lru.Front(); el != nil; el = el.Next() {
			kb := el.Value.(*keyBuf)
			for _, row := range kb.rows {
				if !row.matched {
					r.emitLeftNull(row)
				}
			}
		}
	}
	r.left.reset()
	r.right.reset()
	r.updateGauges()
}

func (r *joinStageRuntime) updateGauges() {
	r.gBuffered.Set(int64(r.left.rows + r.right.rows))
	r.gLeftWm.Set(r.left.maxTs)
	r.gRightWm.Set(r.right.maxTs)
}

// ---- 可观测：每类丢弃计数 + 10s 节流告警（照抄 reportGroupEvictions 模式） ----

func (r *joinStageRuntime) noteLateDropped(n int64) {
	r.mLateDropped.IncBy(n)
	now := time.Now().Unix()
	if last := atomic.LoadInt64(&r.lastLateWarn); now-last >= 10 && atomic.CompareAndSwapInt64(&r.lastLateWarn, last, now) {
		r.log.Warn("stream JOIN stage %d dropped %d late row(s) (total %d): event time older than the side watermark minus WITHIN, or TIMESTAMP field missing. Edge clock-skew scenarios should keep WITHIN slack.",
			r.idx, n, r.mLateDropped.Value())
	}
}

func (r *joinStageRuntime) noteRowsDropped(n int64) {
	r.mRowsDropped.IncBy(n)
	now := time.Now().Unix()
	if last := atomic.LoadInt64(&r.lastRowsWarn); now-last >= 10 && atomic.CompareAndSwapInt64(&r.lastRowsWarn, last, now) {
		r.log.Warn("stream JOIN stage %d row-buffer cap exceeded: dropped %d new row(s) (total %d). Raise WithJoinMaxRows above the peak (input rate × WITHIN) or shorten WITHIN.",
			r.idx, n, r.mRowsDropped.Value())
	}
}

// noteInputDropped 输入 chan 溢出丢行（公开输入与级间内部 chan 共用）。
func (r *joinStageRuntime) noteInputDropped(n int64) {
	r.mInputDropped.IncBy(n)
	now := time.Now().Unix()
	if last := atomic.LoadInt64(&r.lastInputWarn); now-last >= 10 && atomic.CompareAndSwapInt64(&r.lastInputWarn, last, now) {
		r.log.Warn("stream JOIN stage %d input channel full: dropped %d row(s) (total %d). The consumer is lagging; raise BufferConfig.DataChannelSize or use the block overflow strategy.",
			r.idx, n, r.mInputDropped.Value())
	}
}

// buildJoinWorkingRow 构造匹配输出行（与流表 JOIN 的 enrichJoin 同构）：
// 左行字段平铺 + 右行挂别名；级 1 且有 FROM 别名时暴露 working[sourceAlias]=左行，
// 使 "s.x"/"v.y" 两类引用都能解析。级 ≥2 的左行本身已是复合行，拷贝即保留既有别名。
func buildJoinWorkingRow(leftData, rightData map[string]any, rightAlias, sourceAlias string, stageIdx int) map[string]any {
	working := make(map[string]any, len(leftData)+2)
	for k, v := range leftData {
		working[k] = v
	}
	if stageIdx == 1 && sourceAlias != "" {
		working[sourceAlias] = leftData
	}
	working[rightAlias] = rightData
	return working
}

// normalizeJoinTs 把事件时间戳归一化为纳秒（自动判单位：ns/μs/ms/s epoch），
// 与 cep.normalizeTs 同款启发式（含溢出钳制）。小值（<1e9，相对序号）原样比较，
// WITHIN 需同单位。
func normalizeJoinTs(v int64) int64 {
	const maxI64 = int64(1<<63 - 1)
	switch {
	case v <= 0:
		return 0
	case v >= 1e18:
		return v
	case v >= 1e15: // 微秒
		if v > maxI64/1000 {
			return maxI64
		}
		return v * 1000
	case v >= 1e12: // 毫秒
		if v > maxI64/1000000 {
			return maxI64
		}
		return v * 1000000
	case v >= 1e9: // 秒
		if v > maxI64/1000000000 {
			return maxI64
		}
		return v * 1000000000
	}
	return v
}

func toJoinInt64(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int64:
		return x
	case int32:
		return int64(x)
	case uint:
		return int64(x)
	case uint64:
		return int64(x)
	case uint32:
		return int64(x)
	case float64:
		return int64(x)
	case float32:
		return int64(x)
	case time.Time:
		return x.UnixNano()
	}
	return 0
}

// ---- joinPipeline：输入 chan 集合 + 级联接线 + Flush ----

// joinPipeline 持有流-流 JOIN 的全部接线：每流一条输入 chan（EmitTo 按名投递）、
// N-1 级 runner、末级直连尾巴。Flush 在 Stop 末尾单线程执行（flushMode 置位后，
// 级间输出改走内联直调，不再经 chan——此时 processor 已全部退出）。
type joinPipeline struct {
	inputs     map[string]chan joinMsg
	inputNames []string
	// stageFor 把输入流名映射到接收它的级（FROM 流 → 级 1 左入口；JOIN 流 →
	// 对应级的右入口），用于输入溢出计数的归属。
	stageFor map[string]*joinStageRuntime
	stages   []*joinStageRuntime
	// internal[i] 是级 i → 级 i+1 的级间 chan（i 从 0 起，共 len(stages)-1 条）。
	internal  []chan joinMsg
	eventTime bool
	done      <-chan struct{} // 流级 done：block 溢出策略的退出闸

	overflowStrategy string
	blockTimeout     time.Duration

	tailAsync func(row map[string]any) // 末级输出：直连尾巴（异步）
	tailSync  func(row map[string]any) // Flush 期末级输出：内联派发

	flushMode bool
	log       logger.Logger
}

// initJoinPipeline 构造期接线（StreamFactory 调用）：每流一条输入 chan（容量沿
// BufferConfig.DataChannelSize）、N-1 级 runner、级间 chan、末级直连尾巴。
func (s *Stream) initJoinPipeline() error {
	sj := s.config.StreamJoin
	chanCap := s.config.PerformanceConfig.BufferConfig.DataChannelSize
	if chanCap < 0 {
		chanCap = 0
	}
	p := &joinPipeline{
		inputs:           make(map[string]chan joinMsg, len(sj.Inputs)),
		inputNames:       sj.Inputs,
		stageFor:         make(map[string]*joinStageRuntime, len(sj.Inputs)),
		eventTime:        sj.TsProp != "",
		done:             s.done,
		overflowStrategy: s.overflowStrategy,
		blockTimeout:     s.blockingTimeout,
		tailAsync:        s.emitJoinRowAsync,
		tailSync:         s.emitJoinRowSync,
		log:              s.log,
	}
	if p.overflowStrategy == types.OverflowStrategyExpand {
		p.overflowStrategy = types.OverflowStrategyDrop
		s.log.Warn("overflow strategy \"expand\" does not apply to stream JOIN inputs (no dynamic channel growth); using drop semantics")
	}
	for _, name := range sj.Inputs {
		if _, dup := p.inputs[name]; dup {
			return fmt.Errorf("duplicate input stream name %q (FROM and JOIN sources must be distinct)", name)
		}
		p.inputs[name] = make(chan joinMsg, chanCap)
	}
	for i := range sj.Stages {
		rt := newJoinStageRuntime(joinStageParams{
			idx:         i + 1,
			cfg:         sj.Stages[i],
			sourceAlias: s.config.SourceAlias,
			eventTime:   p.eventTime,
			tsProp:      sj.TsProp,
			idleTimeout: sj.IdleTimeout,
			maxKeys:     s.config.JoinMaxKeys,
			maxRows:     s.config.JoinMaxRows,
			reg:         s.metricsRegistry,
			log:         s.log,
		})
		rt.leftIn = p.inputs[sj.Inputs[0]]
		rt.rightIn = p.inputs[sj.Stages[i].RightName]
		p.stages = append(p.stages, rt)
	}
	if len(p.stages) > 1 {
		p.internal = make([]chan joinMsg, len(p.stages)-1)
		for i := 1; i < len(p.stages); i++ {
			ch := make(chan joinMsg, chanCap)
			p.internal[i-1] = ch
			p.stages[i].leftIn = ch
		}
	}
	p.stageFor[sj.Inputs[0]] = p.stages[0]
	for i := range p.stages {
		p.stageFor[sj.Stages[i].RightName] = p.stages[i]
	}
	p.wireCascade()
	s.join = p
	return nil
}

// sendJoinMsg 向一条 JOIN 输入 chan 投递。drop：非阻塞 + 计数；block：阻塞至送达
// 或 blockTimeout（≤0 时无限阻塞，可被 done 解除）再丢；expand 不适用于 JOIN 输入
// （无动态扩容），构造期已降级为 drop 并告警。flushMode 下调用方保证走内联路径，
// 不会进到这里。
func (p *joinPipeline) sendJoinMsg(stage *joinStageRuntime, ch chan joinMsg, msg joinMsg) {
	if p.overflowStrategy == types.OverflowStrategyBlock {
		if p.blockTimeout <= 0 {
			select {
			case ch <- msg:
			case <-p.done:
			}
			return
		}
		t := time.NewTimer(p.blockTimeout)
		defer t.Stop()
		select {
		case ch <- msg:
			return
		case <-p.done:
			return
		case <-t.C:
		}
		stage.noteInputDropped(1)
		return
	}
	select {
	case ch <- msg:
	default:
		stage.noteInputDropped(1)
	}
}

// emitToName 按流名投递一行（Emit/EmitTo 入口）。
func (p *joinPipeline) emitToName(name string, row map[string]any) error {
	ch, ok := p.inputs[name]
	if !ok {
		return fmt.Errorf("unknown input stream %q for stream JOIN (known inputs: %v)", name, p.inputNames)
	}
	p.sendJoinMsg(p.stageFor[name], ch, joinMsg{row: row})
	return nil
}

// wireCascade 组装级联：级 i 的输出回调——中间级投递到级间 chan（flushMode 时
// 内联直调下级左入口），末级走直连尾巴。
func (p *joinPipeline) wireCascade() {
	for i, st := range p.stages {
		i, st := i, st
		if i+1 < len(p.stages) {
			down := p.stages[i+1]
			ch := p.internal[i]
			st.emit = func(row map[string]any, ts int64) {
				msg := joinMsg{row: row, ts: ts, explicit: p.eventTime}
				if p.flushMode {
					// Stop-Flush 期：processor 已退出，单线程内联流入下级
					//（上级 pending 先流入下级再由下级 Flush）。
					down.processLeftMsg(msg)
					return
				}
				p.sendJoinMsg(down, ch, msg)
			}
		} else {
			st.emit = func(row map[string]any, ts int64) {
				if p.flushMode {
					p.tailSync(row)
				} else {
					p.tailAsync(row)
				}
			}
		}
	}
}

// flush 按级联序 Flush：先上级后下级——上级 pending LEFT 补发的
// NULL 行先（内联）流入下级参与匹配/挂起，再由下级 Flush 收尾。末级输出内联派发。
func (p *joinPipeline) flush() {
	p.flushMode = true
	for _, st := range p.stages {
		st.flushLeft()
	}
}

// statsSnapshot 汇出全部级的 JOIN 指标（GetStats 合并用）。
func (p *joinPipeline) statsSnapshot() map[string]int64 {
	out := make(map[string]int64, len(p.stages)*9)
	for _, st := range p.stages {
		prefix := fmt.Sprintf("join_stage%d_", st.idx)
		out[prefix+"matches_emitted"] = st.mMatches.Value()
		out[prefix+"left_timeout_emitted"] = st.mLeftTimeout.Value()
		out[prefix+"late_dropped"] = st.mLateDropped.Value()
		out[prefix+"input_dropped"] = st.mInputDropped.Value()
		out[prefix+"rows_dropped"] = st.mRowsDropped.Value()
		out[prefix+"keys_evicted"] = st.mKeysEvicted.Value()
		out[prefix+"buffered_rows"] = st.gBuffered.Value()
		out[prefix+"left_watermark"] = st.gLeftWm.Value()
		out[prefix+"right_watermark"] = st.gRightWm.Value()
	}
	return out
}
