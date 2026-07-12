package stream

import (
	"strconv"
	"strings"

	"github.com/rulego/streamsql/cep"
	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
)

// cepRunner 是 Stream 持有的 CEP 引擎适配器：把分区键算好喂给 cep.Engine，
// 输出行直发 sink/metrics。分区键构造复用分析函数的 typeKey/resolvePartitionField。
type cepRunner struct {
	engine      *cep.Engine
	partitionBy []string
}

func newCepRunner(spec *types.MatchRecognizeSpec, maxPartitions int, log logger.Logger) (*cepRunner, error) {
	eng, err := cep.NewEngine(spec)
	if err != nil {
		return nil, err
	}
	if maxPartitions > 0 {
		eng.SetMaxPartitions(maxPartitions)
	}
	eng.SetLogger(log) // per-engine 诊断日志器（避免包级共享）
	return &cepRunner{engine: eng, partitionBy: spec.PartitionBy}, nil
}

// partitionKey 按 PARTITION BY 字段拼接分区键（与 analyticFieldEngine.partitionKey 同构）。
func (c *cepRunner) partitionKey(row map[string]any) string {
	if len(c.partitionBy) == 0 {
		return ""
	}
	var sb strings.Builder
	var lbuf [4]byte
	for _, k := range c.partitionBy {
		tk := typeKey(resolvePartitionField(row, k))
		lstr := strconv.AppendInt(lbuf[:0], int64(len(tk)), 10)
		sb.Write(lstr)
		sb.WriteByte(':')
		sb.WriteString(tk)
		sb.WriteByte('|')
	}
	return sb.String()
}
