package goqube

import (
	"testing"
)

func TestBulkUpdateQuery_BuildBulkUpdateQuery(t *testing.T) {
	queryWithTypes := &BulkUpdateQuery{
		Table:      "users",
		PrimaryKey: "id",
		FieldsValues: []map[string]interface{}{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		},
		ColumnsType: map[string]string{
			"id":   "integer",
			"name": "text",
		},
	}

	// Test valid dialects with ColumnsType provided
	dialects := []Dialect{DialectMySQL, DialectPostgres, DialectSQLite, DialectSQLServer}
	for _, dialect := range dialects {
		t.Run(string(dialect)+"_with_ColumnsType", func(t *testing.T) {
			query, args, err := queryWithTypes.BuildBulkUpdateQuery(dialect)
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

	// Test that Postgres and SQL Server require ColumnsType
	queryWithoutTypes := &BulkUpdateQuery{
		Table:      "users",
		PrimaryKey: "id",
		FieldsValues: []map[string]interface{}{
			{"id": 1, "name": "Alice"},
		},
	}

	for _, dialect := range []Dialect{DialectPostgres, DialectSQLServer} {
		t.Run(string(dialect)+"_missing_ColumnsType", func(t *testing.T) {
			_, _, err := queryWithoutTypes.BuildBulkUpdateQuery(dialect)
			if err != ErrInvalidBulkUpdateQueryMissingColumnType {
				t.Errorf("expected ErrInvalidBulkUpdateQueryMissingColumnType for dialect %s, got %v", dialect, err)
			}
		})
	}

	// Test unsupported dialect
	t.Run("Unsupported Dialect", func(t *testing.T) {
		_, _, err := queryWithTypes.BuildBulkUpdateQuery(Dialect("unsupported"))
		if err != ErrUnsupportedDialect {
			t.Errorf("expected ErrUnsupportedDialect, got %v", err)
		}
	})
}
