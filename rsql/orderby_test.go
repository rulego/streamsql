package rsql

import (
	"testing"

	"github.com/rulego/streamsql/types"
)

func parseOrderBySQL(t *testing.T, sql string) *SelectStatement {
	t.Helper()
	parser := NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse(%q) unexpected error: %v", sql, err)
	}
	return stmt
}

func TestParseOrderBy_SingleAscDefault(t *testing.T) {
	stmt := parseOrderBySQL(t, "SELECT * FROM t ORDER BY name")
	if len(stmt.OrderBy) != 1 || stmt.OrderBy[0].Expression != "name" || stmt.OrderBy[0].Direction != types.SortAsc {
		t.Fatalf("got %+v, want [{name ASC}]", stmt.OrderBy)
	}
}

func TestParseOrderBy_DescAndAsc(t *testing.T) {
	stmt := parseOrderBySQL(t, "SELECT * FROM t ORDER BY name DESC")
	if len(stmt.OrderBy) != 1 || stmt.OrderBy[0].Direction != types.SortDesc {
		t.Fatalf("DESC: got %+v", stmt.OrderBy)
	}
	stmt = parseOrderBySQL(t, "SELECT * FROM t ORDER BY name ASC")
	if len(stmt.OrderBy) != 1 || stmt.OrderBy[0].Direction != types.SortAsc {
		t.Fatalf("ASC: got %+v", stmt.OrderBy)
	}
}

func TestParseOrderBy_MultipleAndMixedDirections(t *testing.T) {
	stmt := parseOrderBySQL(t, "SELECT a, b FROM t ORDER BY a DESC, b ASC")
	if len(stmt.OrderBy) != 2 {
		t.Fatalf("want 2 keys, got %+v", stmt.OrderBy)
	}
	if stmt.OrderBy[0].Expression != "a" || stmt.OrderBy[0].Direction != types.SortDesc {
		t.Errorf("key0 = %+v", stmt.OrderBy[0])
	}
	if stmt.OrderBy[1].Expression != "b" || stmt.OrderBy[1].Direction != types.SortAsc {
		t.Errorf("key1 = %+v", stmt.OrderBy[1])
	}
}

func TestParseOrderBy_WithLimit(t *testing.T) {
	stmt := parseOrderBySQL(t, "SELECT * FROM t ORDER BY v DESC LIMIT 2")
	if len(stmt.OrderBy) != 1 || stmt.OrderBy[0].Direction != types.SortDesc {
		t.Fatalf("got %+v", stmt.OrderBy)
	}
	if stmt.Limit != 2 {
		t.Fatalf("Limit = %d, want 2", stmt.Limit)
	}
}

func TestParseOrderBy_AfterGroupByHaving(t *testing.T) {
	// Full clause chain; ORDER BY must not be swallowed by GROUP BY/HAVING.
	stmt := parseOrderBySQL(t, "SELECT g, count(*) AS c FROM t WHERE x > 5 GROUP BY g HAVING count(*) > 0 ORDER BY c DESC")
	if len(stmt.OrderBy) != 1 || stmt.OrderBy[0].Expression != "c" || stmt.OrderBy[0].Direction != types.SortDesc {
		t.Fatalf("got %+v", stmt.OrderBy)
	}
	if stmt.Having == "" {
		t.Error("HAVING lost")
	}
	if len(stmt.GroupBy) == 0 {
		t.Error("GROUP BY lost")
	}
}

func TestParseOrderBy_NoClause(t *testing.T) {
	stmt := parseOrderBySQL(t, "SELECT * FROM t WHERE x > 5")
	if len(stmt.OrderBy) != 0 {
		t.Fatalf("want no ORDER BY, got %+v", stmt.OrderBy)
	}
}

// TestParseOrderBy_NoSubstringFalseMatch: "ORDER" appearing inside an
// identifier, table name, or string literal must NOT be parsed as ORDER BY.
// This is the same robustness property parseLimit gained (H7 fix).
func TestParseOrderBy_NoSubstringFalseMatch(t *testing.T) {
	cases := []string{
		"SELECT * FROM orders",                 // table named "orders"
		"SELECT ordered FROM t",                // column "ordered"
		"SELECT * FROM t WHERE tag = 'ORDER'",  // string literal
		"SELECT * FROM t WHERE note = 'x ORDER y'",
	}
	for _, sql := range cases {
		parser := NewParser(sql)
		stmt, _ := parser.Parse()
		if len(stmt.OrderBy) != 0 {
			t.Errorf("Parse(%q): ORDER BY falsely detected as %+v", sql, stmt.OrderBy)
		}
	}
}
