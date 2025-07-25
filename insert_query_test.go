package goqube

import (
	"testing"
)

// TestInsertQuery_BuildInsertQuery tests the BuildInsertQuery method for different SQL dialects and scenarios.
// It checks for correct SQL generation, argument handling, and error responses for supported and unsupported dialects.
func TestInsertQuery_BuildInsertQuery(t *testing.T) {
	tests := []struct {
		name       string
		dialect    Dialect
		expectErr  error
		expectSQL  bool
		expectArgs bool
	}{
		{"MySQL", DialectMySQL, nil, true, true},
		{"Postgres", DialectPostgres, nil, true, true},
		{"SQLite", DialectSQLite, nil, true, true},
		{"SQLServer", DialectSQLServer, nil, true, true},
		{"Unsupported", "", ErrUnsupportedDialect, false, false},
	}
	// Create a simple insert query for testing.
	q := &InsertQuery{
		Table:  "users",
		Values: []map[string]interface{}{{"name": "Alice", "age": 30}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the insert query for the given dialect.
			sql, args, err := q.BuildInsertQuery(tt.dialect)
			if tt.expectErr != nil {
				// Check for expected error on unsupported dialects.
				if err != tt.expectErr {
					t.Errorf("expected error %v, got %v", tt.expectErr, err)
				}
			} else {
				// Ensure no unexpected error occurs.
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				// Validate that SQL is generated when expected.
				if tt.expectSQL && sql == "" {
					t.Errorf("expected non-empty SQL, got empty string")
				}
				// Validate that arguments are generated when expected.
				if tt.expectArgs && args == nil {
					t.Errorf("expected non-nil args, got nil")
				}
			}
		})
	}
}
