package goqube

import (
	"testing"
)

func TestBulkUpdateQuery_BuildBulkUpdateQuery(t *testing.T) {
	q := &BulkUpdateQuery{
		Table:      "users",
		PrimaryKey: "id",
		FieldsValues: []map[string]interface{}{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		},
	}

	// Test valid dialects
	dialects := []Dialect{DialectMySQL, DialectPostgres, DialectSQLite, DialectSQLServer}
	for _, dialect := range dialects {
		t.Run(string(dialect), func(t *testing.T) {
			query, args, err := q.BuildBulkUpdateQuery(dialect)
			if err != nil {
				t.Fatalf("expected no error for dialect %s, got %v", dialect, err)
			}
			if query == "" {
				t.Errorf("expected non-empty query for dialect %s", dialect)
			}
			if len(args) == 0 {
				t.Errorf("expected non-empty args for dialect %s", dialect)
			}
		})
	}

	// Test unsupported dialect
	t.Run("Unsupported Dialect", func(t *testing.T) {
		_, _, err := q.BuildBulkUpdateQuery(Dialect("unsupported"))
		if err != ErrUnsupportedDialect {
			t.Errorf("expected ErrUnsupportedDialect, got %v", err)
		}
	})
}
