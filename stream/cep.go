package stream

import (
	"strconv"
	"strings"

	"github.com/rulego/streamsql/cep"
	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
)

// cepRunner is a CEP engine adapter owned by Stream: it calculates partition keys and feeds them to cep.Engine,
// Output lines are sent directly to sink/metrics. The partition key constructs the typeKey/resolvePartitionField of the reuse analysis function.
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
	eng.SetLogger(log) // per-engine diagnostic logger (to avoid packet-level sharing)
	return &cepRunner{engine: eng, partitionBy: spec.PartitionBy}, nil
}

// partitionKey is concatenated by the PARTITION BY field (isomorphic to analyticFieldEngine.partitionKey).
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

// Start the CEP engine's WITHIN active expired sweeper.
func (c *cepRunner) Start() { c.engine.Start() }

// Stop: Stop the CEP engine and join the sweeper.
func (c *cepRunner) Stop() { c.engine.Stop() }
