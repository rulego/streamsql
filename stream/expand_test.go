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
	"testing"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/require"
)

// TestExpandDataChannelRespectsMaxBufferSize 校验 expandDataChannel 读取
// PerformanceConfig 的 GrowthFactor/MinIncrement/TriggerThreshold 并以 MaxBufferSize
// 为硬上限，避免无界扩容 OOM（此前这些配置是声明了却不接通的死配置）。
func TestExpandDataChannelRespectsMaxBufferSize(t *testing.T) {
	s := &Stream{
		dataChan: make(chan map[string]any, 100),
		config: types.Config{PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{MaxBufferSize: 250},
			OverflowConfig: types.OverflowConfig{ExpansionConfig: types.ExpansionConfig{
				GrowthFactor: 1.5, MinIncrement: 1000, TriggerThreshold: 0.8,
			}},
		}},
		log: logger.GetDefault(),
	}

	// Fill past the 0.8 trigger threshold (90/100) to force expansion.
	for i := 0; i < 90; i++ {
		s.dataChan <- map[string]any{"i": i}
	}
	s.expandDataChannel()

	s.dataChanMux.RLock()
	cap1 := cap(s.dataChan)
	s.dataChanMux.RUnlock()
	require.True(t, cap1 > 100, "channel should expand from 100, got %d", cap1)
	require.True(t, cap1 <= 250, "channel must not exceed MaxBufferSize 250, got %d", cap1)

	// Top up the new channel past the trigger threshold; at the ceiling a further
	// expand must be a no-op (cannot grow past MaxBufferSize).
	for len(s.dataChan) < 220 {
		s.dataChan <- map[string]any{"x": 1}
	}
	s.expandDataChannel()

	s.dataChanMux.RLock()
	cap2 := cap(s.dataChan)
	s.dataChanMux.RUnlock()
	require.Equal(t, cap1, cap2, "at MaxBufferSize ceiling, channel must not expand further, got %d", cap2)
}
