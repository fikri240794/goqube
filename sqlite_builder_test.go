package goqube

import (
	"reflect"
	"testing"
)

// Test_newSQLiteBuilder verifies that newSQLiteBuilder returns a valid builder with the correct placeholder format for SQLite.
func Test_newSQLiteBuilder(t *testing.T) {
	b := newSQLiteBuilder()
	if b == nil {
		// The builder should never be nil; fail the test if it is.
		t.Fatal("newSQLiteBuilder() returned nil")
	}
	if b.placeholderFormat != "?" {
		// The placeholder format for SQLite should be "?" for parameterized queries.
		t.Errorf("expected placeholderFormat '?', got '%s'", b.placeholderFormat)
	}
}

// Test_sqliteBuilder_BuildDeleteQuery tests the BuildDeleteQuery method for various DeleteQuery scenarios, including nil queries, empty tables, valid deletes, and deletes with filters.
func Test_sqliteBuilder_BuildDeleteQuery(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name     string
		q        *DeleteQuery
		wantSQL  string
		wantArgs []interface{}
		wantErr  error
	}{
		{
			name:     "nil query",
			q:        nil,
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidDeleteQuery,
		},
		{
			name:     "empty table",
			q:        &DeleteQuery{Table: ""},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidDeleteQuery,
		},
		{
			name:     "basic delete",
			q:        &DeleteQuery{Table: "users"},
			wantSQL:  "DELETE FROM users",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "delete with filter",
			q:        &DeleteQuery{Table: "users", Filter: &Filter{Field: Field{Column: "id"}, Operator: OperatorEqual, Value: FilterValue{Value: 1}}},
			wantSQL:  "DELETE FROM users WHERE id = ?",
			wantArgs: []interface{}{1},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call BuildDeleteQuery and capture the result and error for each test case.
			sql, args, err := b.BuildDeleteQuery(tt.q)
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Check the number of arguments and their values.
			if len(args) != len(tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			} else {
				for i := range args {
					if args[i] != tt.wantArgs[i] {
						t.Errorf("got args[%d]=%v, want %v", i, args[i], tt.wantArgs[i])
					}
				}
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqliteBuilder_BuildInsertQuery tests the BuildInsertQuery method for various InsertQuery scenarios, including nil queries, empty tables, no values, single row, and multi-row inserts.
func Test_sqliteBuilder_BuildInsertQuery(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name     string
		q        *InsertQuery
		wantSQL  string
		wantArgs []interface{}
		wantErr  error
	}{
		{
			name:     "nil query",
			q:        nil,
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidInsertQuery,
		},
		{
			name:     "empty table",
			q:        &InsertQuery{Table: "", Values: []map[string]interface{}{{"a": 1}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidInsertQuery,
		},
		{
			name:     "no values",
			q:        &InsertQuery{Table: "users", Values: nil},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidInsertQuery,
		},
		{
			name:     "single row",
			q:        &InsertQuery{Table: "users", Values: []map[string]interface{}{{"id": 1, "name": "foo"}}},
			wantSQL:  "INSERT INTO users (id, name) VALUES (?, ?)",
			wantArgs: []interface{}{1, "foo"},
			wantErr:  nil,
		},
		{
			name:     "multi row",
			q:        &InsertQuery{Table: "users", Values: []map[string]interface{}{{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}}},
			wantSQL:  "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)",
			wantArgs: []interface{}{1, "foo", 2, "bar"},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call BuildInsertQuery and capture the result and error for each test case.
			sql, args, err := b.BuildInsertQuery(tt.q)
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqliteBuilder_BuildSelectQuery tests the BuildSelectQuery method for various SelectQuery scenarios, including errors from subcomponents, joins, filters, and valid selects.
func Test_sqliteBuilder_BuildSelectQuery(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name     string
		q        *SelectQuery
		wantSQL  string
		wantArgs []interface{}
		wantErr  error
	}{
		{
			name:     "error from buildOrderBy",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Sorts: []Sort{{Field: Field{Column: ""}}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidOrderBy,
		},
		{
			name:     "error from buildGroupBy",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, GroupByFields: []Field{{Column: ""}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidGroupBy,
		},
		{
			name:     "select with join",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Joins: []Join{{Table: Table{Name: "roles"}, Type: JoinTypeInner, Filter: Filter{Field: Field{Column: "users.role_id"}, Operator: OperatorEqual, Value: FilterValue{Column: "roles.id"}}}}},
			wantSQL:  "SELECT id FROM users INNER JOIN roles ON users.role_id = roles.id",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "error from buildJoins",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Joins: []Join{{}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidTable,
		},
		{
			name:     "error from buildTable",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidTable,
		},
		{
			name:     "error from buildFields",
			q:        &SelectQuery{Fields: []Field{{Column: ""}}, Table: Table{Name: "users"}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidField,
		},
		{
			name:     "error from buildFilter",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Filter: &Filter{Field: Field{Column: ""}, Operator: OperatorEqual, Value: FilterValue{Value: 1}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidFilter,
		},
		{
			name:     "nil query",
			q:        nil,
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrUnsupportedDialect,
		},
		{
			name:     "raw query",
			q:        &SelectQuery{Raw: "SELECT 1"},
			wantSQL:  "SELECT 1",
			wantArgs: nil,
			wantErr:  nil,
		},
		{
			name:     "basic select",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}},
			wantSQL:  "SELECT id FROM users",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "select with where",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Filter: &Filter{Field: Field{Column: "id"}, Operator: OperatorEqual, Value: FilterValue{Value: 1}}},
			wantSQL:  "SELECT id FROM users WHERE id = ?",
			wantArgs: []interface{}{1},
			wantErr:  nil,
		},
		{
			name:     "select with group by",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, GroupByFields: []Field{{Column: "id"}}},
			wantSQL:  "SELECT id FROM users GROUP BY id",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "select with order by",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Sorts: []Sort{{Field: Field{Column: "id"}, Direction: SortDirectionDescending}}},
			wantSQL:  "SELECT id FROM users ORDER BY id DESC",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "select with limit and offset",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Take: 10, Skip: 5},
			wantSQL:  "SELECT id FROM users LIMIT ? OFFSET ?",
			wantArgs: []interface{}{int64(10), int64(5)},
			wantErr:  nil,
		},
		{
			name:     "select with alias",
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Alias: "u"},
			wantSQL:  "(SELECT id FROM users) AS u",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call BuildSelectQuery and capture the result and error for each test case.
			sql, args, err := b.BuildSelectQuery(tt.q)
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqliteBuilder_BuildUpdateQuery tests the BuildUpdateQuery method for various UpdateQuery scenarios, including nil queries, empty tables, no values, basic updates, and updates with filters.
func Test_sqliteBuilder_BuildUpdateQuery(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name     string
		q        *UpdateQuery
		wantSQL  string
		wantArgs []interface{}
		wantErr  error
	}{
		{
			name:     "nil query",
			q:        nil,
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidUpdateQuery,
		},
		{
			name:     "empty table",
			q:        &UpdateQuery{Table: "", FieldsValue: map[string]interface{}{"a": 1}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidUpdateQuery,
		},
		{
			name:     "no values",
			q:        &UpdateQuery{Table: "users", FieldsValue: nil},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidUpdateQuery,
		},
		{
			name:     "basic update",
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"name": "foo"}},
			wantSQL:  "UPDATE users SET name = ?",
			wantArgs: []interface{}{"foo"},
			wantErr:  nil,
		},
		{
			name:     "update with filter",
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"name": "foo"}, Filter: &Filter{Field: Field{Column: "id"}, Operator: OperatorEqual, Value: FilterValue{Value: 1}}},
			wantSQL:  "UPDATE users SET name = ? WHERE id = ?",
			wantArgs: []interface{}{"foo", 1},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call BuildUpdateQuery and capture the result and error for each test case.
			sql, args, err := b.BuildUpdateQuery(tt.q)
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqliteBuilder_buildFields tests the buildFields method for various field scenarios, including empty, valid, and invalid fields.
func Test_sqliteBuilder_buildFields(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name     string
		fields   []Field
		want     string
		wantArgs []interface{}
		wantErr  error
	}{
		{
			name:     "empty fields",
			fields:   nil,
			want:     "",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "single field",
			fields:   []Field{{Column: "id"}},
			want:     "id",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "invalid field",
			fields:   []Field{{Column: ""}},
			want:     "",
			wantArgs: []interface{}{},
			wantErr:  ErrInvalidField,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare a fresh args slice for each test case.
			args := make([]interface{}, 0)
			// Call buildFields and capture the result and error for each test case.
			got, err := b.buildFields(tt.fields, &args)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqliteBuilder_buildFilter tests the buildFilter method for various filter scenarios, including nil, invalid, and valid filters.
func Test_sqliteBuilder_buildFilter(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name     string
		filter   *Filter
		want     string
		wantArgs []interface{}
		wantErr  error
	}{
		{
			name:     "nil filter",
			filter:   nil,
			want:     "",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "invalid filter (empty field)",
			filter:   &Filter{Field: Field{Column: ""}, Operator: OperatorEqual, Value: FilterValue{Value: 1}},
			want:     "",
			wantArgs: []interface{}{},
			wantErr:  ErrInvalidFilter,
		},
		{
			name:     "simple filter",
			filter:   &Filter{Field: Field{Column: "id"}, Operator: OperatorEqual, Value: FilterValue{Value: 1}},
			want:     "id = ?",
			wantArgs: []interface{}{1},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare a fresh args slice for each test case.
			args := make([]interface{}, 0)
			// Call buildFilter and capture the result and error for each test case.
			got, err := b.buildFilter(tt.filter, &args, true)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqliteBuilder_buildGroupBy tests the buildGroupBy method for various group by scenarios, including empty, single, multiple, and invalid fields.
func Test_sqliteBuilder_buildGroupBy(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name    string
		fields  []Field
		want    string
		wantErr error
	}{
		{
			name:    "empty fields",
			fields:  nil,
			want:    "",
			wantErr: nil,
		},
		{
			name:    "single field",
			fields:  []Field{{Column: "id"}},
			want:    "id",
			wantErr: nil,
		},
		{
			name:    "multiple fields",
			fields:  []Field{{Column: "id"}, {Column: "name"}},
			want:    "id, name",
			wantErr: nil,
		},
		{
			name:    "invalid field",
			fields:  []Field{{Column: ""}},
			want:    "",
			wantErr: ErrInvalidGroupBy,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildGroupBy and capture the result and error for each test case.
			got, err := b.buildGroupBy(tt.fields)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqliteBuilder_buildJoins tests the buildJoins method for various join scenarios, including empty, invalid, and valid joins.
func Test_sqliteBuilder_buildJoins(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name     string
		joins    []Join
		want     string
		wantArgs []interface{}
		wantErr  error
	}{
		{
			name:     "empty joins",
			joins:    nil,
			want:     "",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "invalid join (empty table)",
			joins:    []Join{{}},
			want:     "",
			wantArgs: []interface{}{},
			wantErr:  ErrInvalidTable,
		},
		{
			name:     "simple join",
			joins:    []Join{{Table: Table{Name: "roles"}, Type: JoinTypeInner, Filter: Filter{Field: Field{Column: "users.role_id"}, Operator: OperatorEqual, Value: FilterValue{Column: "roles.id"}}}},
			want:     "INNER JOIN roles ON users.role_id = roles.id",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare a fresh args slice for each test case.
			args := make([]interface{}, 0)
			// Call buildJoins and capture the result and error for each test case.
			got, err := b.buildJoins(tt.joins, &args)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqliteBuilder_buildOrderBy tests the buildOrderBy method for various order by scenarios, including empty, single, multiple, and invalid sorts.
func Test_sqliteBuilder_buildOrderBy(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name    string
		sorts   []Sort
		want    string
		wantErr error
	}{
		{
			name:    "empty sorts",
			sorts:   nil,
			want:    "",
			wantErr: nil,
		},
		{
			name:    "single sort asc",
			sorts:   []Sort{{Field: Field{Column: "id"}, Direction: SortDirectionAscending}},
			want:    "id ASC",
			wantErr: nil,
		},
		{
			name:    "single sort desc",
			sorts:   []Sort{{Field: Field{Column: "id"}, Direction: SortDirectionDescending}},
			want:    "id DESC",
			wantErr: nil,
		},
		{
			name:    "multiple sorts",
			sorts:   []Sort{{Field: Field{Column: "id"}, Direction: SortDirectionAscending}, {Field: Field{Column: "name"}, Direction: SortDirectionDescending}},
			want:    "id ASC, name DESC",
			wantErr: nil,
		},
		{
			name:    "invalid sort (empty field)",
			sorts:   []Sort{{Field: Field{Column: ""}}},
			want:    "",
			wantErr: ErrInvalidOrderBy,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildOrderBy and capture the result and error for each test case.
			got, err := b.buildOrderBy(tt.sorts)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqliteBuilder_buildTable tests the buildTable method for various table scenarios, including empty, simple, and aliased tables.
func Test_sqliteBuilder_buildTable(t *testing.T) {
	b := newSQLiteBuilder()
	tests := []struct {
		name     string
		table    Table
		want     string
		wantArgs []interface{}
		wantErr  error
	}{
		{
			name:     "empty table",
			table:    Table{},
			want:     "",
			wantArgs: []interface{}{},
			wantErr:  ErrInvalidTable,
		},
		{
			name:     "simple table",
			table:    Table{Name: "users"},
			want:     "users",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name:     "table with alias",
			table:    Table{Name: "users", Alias: "u"},
			want:     "users AS u",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare a fresh args slice for each test case.
			args := make([]interface{}, 0)
			// Call buildTable and capture the result and error for each test case.
			got, err := b.buildTable(tt.table, &args)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Compare the error to the expected error.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}
