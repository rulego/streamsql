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

package rsql

import (
	"strings"
	"testing"
)

// TestGlobalWindowParsing covers GLOBAL WINDOW + TRIGGER WHEN lexing/parsing.
func TestGlobalWindowParsing(t *testing.T) {
	t.Run("global window with group by and count trigger", func(t *testing.T) {
		sql := `SELECT deviceId, COUNT(*) AS cnt, AVG(temp) AS avg_temp
			FROM stream
			GROUP BY deviceId, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 1000`
		stmt, err := NewParser(sql).Parse()
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if strings.ToUpper(stmt.Window.Type) != "GLOBALWINDOW" {
			t.Errorf("Window.Type = %q, want GLOBALWINDOW", stmt.Window.Type)
		}
		if stmt.Window.TriggerCondition == "" {
			t.Error("TriggerCondition is empty")
		}
		wantKey := "deviceId"
		found := false
		for _, g := range stmt.GroupBy {
			if g == wantKey {
				found = true
			}
		}
		if !found {
			t.Errorf("GroupBy missing %q, got %v", wantKey, stmt.GroupBy)
		}
		// TRIGGER WHEN predicate must contain the comparison.
		if !strings.Contains(stmt.Window.TriggerCondition, "1000") {
			t.Errorf("TriggerCondition=%q missing threshold", stmt.Window.TriggerCondition)
		}
	})

	t.Run("global window without group by (field-driven trigger)", func(t *testing.T) {
		sql := `SELECT deviceId, MAX(temp) AS max_temp
			FROM stream
			GLOBAL WINDOW TRIGGER WHEN MAX(temp) > 50`
		stmt, err := NewParser(sql).Parse()
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if strings.ToUpper(stmt.Window.Type) != "GLOBALWINDOW" {
			t.Errorf("Window.Type = %q, want GLOBALWINDOW", stmt.Window.Type)
		}
		if !strings.Contains(stmt.Window.TriggerCondition, "50") {
			t.Errorf("TriggerCondition=%q missing threshold 50", stmt.Window.TriggerCondition)
		}
	})

	t.Run("global window without trigger is rejected", func(t *testing.T) {
		// NeverTrigger would silently swallow all data; parse must reject it.
		sql := `SELECT deviceId, COUNT(*) AS cnt
			FROM stream
			GROUP BY deviceId, GLOBAL WINDOW`
		_, _, err := Parse(sql)
		if err == nil {
			t.Fatal("expected error for GLOBAL WINDOW without TRIGGER WHEN, got nil")
		}
		if !strings.Contains(err.Error(), "TRIGGER WHEN") {
			t.Errorf("error should mention TRIGGER WHEN, got: %v", err)
		}
	})

	t.Run("global window with WITH multi-param and STATETTL", func(t *testing.T) {
		sql := `SELECT deviceId, COUNT(*) AS cnt
			FROM stream
			GROUP BY deviceId, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 100
			WITH(STATETTL='1h', IDLETIMEOUT='60s', MAXOUTOFORDERNESS='2s')`
		stmt, err := NewParser(sql).Parse()
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if stmt.Window.CountStateTTL.String() != "1h0m0s" {
			t.Errorf("CountStateTTL = %v, want 1h", stmt.Window.CountStateTTL)
		}
		if stmt.Window.IdleTimeout.String() != "1m0s" {
			t.Errorf("IdleTimeout = %v, want 1m", stmt.Window.IdleTimeout)
		}
		if stmt.Window.MaxOutOfOrderness.String() != "2s" {
			t.Errorf("MaxOutOfOrderness = %v, want 2s", stmt.Window.MaxOutOfOrderness)
		}
	})

	t.Run("global window trigger with AND compound", func(t *testing.T) {
		sql := `SELECT deviceId, COUNT(*) AS cnt, MAX(temp) AS mx
			FROM stream
			GROUP BY deviceId, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 10 AND MAX(temp) > 50`
		stmt, err := NewParser(sql).Parse()
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if !strings.Contains(stmt.Window.TriggerCondition, "&&") {
			t.Errorf("expected AND lowered to &&, got %q", stmt.Window.TriggerCondition)
		}
	})

	t.Run("global window config carries trigger condition", func(t *testing.T) {
		sql := `SELECT deviceId, COUNT(*) AS cnt FROM stream
			GROUP BY deviceId, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 5`
		cfg, _, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if cfg.WindowConfig.TriggerCondition == "" {
			t.Error("WindowConfig.TriggerCondition empty")
		}
		if len(cfg.WindowConfig.SelectFields) == 0 {
			t.Error("WindowConfig.SelectFields empty (global window needs it)")
		}
	})
}
