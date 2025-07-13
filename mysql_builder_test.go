package goqube

import (
	"reflect"
	"strings"
	"testing"
)

// Test_newMySQLBuilder verifies that newMySQLBuilder returns a valid builder with the correct placeholder format for MySQL.
func Test_newMySQLBuilder(t *testing.T) {
	b := newMySQLBuilder()
	if b == nil {
		// The builder should never be nil; fail the test if it is.
		t.Fatal("newMySQLBuilder() returned nil")
	}
	if b.placeholderFormat != "?" {
		// The placeholder format for MySQL should be "?" for parameterized queries.
		t.Errorf("placeholderFormat = %v, want ?", b.placeholderFormat)
	}
}

// TestMySQLBuilder_BuildDeleteQuery tests the BuildDeleteQuery method for various DeleteQuery scenarios, including nil queries, empty tables, valid deletes, and error cases.
func TestMySQLBuilder_BuildDeleteQuery(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name     string
		q        *DeleteQuery
		setup    func() *DeleteQuery
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "nil query",
			q:        nil,
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name:     "empty table",
			q:        &DeleteQuery{Table: ""},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name:     "valid table, no filter",
			q:        &DeleteQuery{Table: "users"},
			wantSQL:  "DELETE FROM users",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name: "valid table, with filter",
			// Setup a DeleteQuery with a filter on the "id" column.
			setup: func() *DeleteQuery {
				return &DeleteQuery{Table: "users", Filter: &Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}}
			},
			wantSQL:  "DELETE FROM users WHERE id = ?",
			wantArgs: []interface{}{1},
			wantErr:  false,
		},
		{
			name: "filter returns error",
			// Setup a DeleteQuery with an invalid filter (empty field) to trigger an error.
			setup: func() *DeleteQuery {
				return &DeleteQuery{Table: "users", Filter: &Filter{Field: Field{}, Operator: Operator("="), Value: FilterValue{Value: 1}}}
			},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.q
			if tt.setup != nil {
				// If a setup function is provided, use it to initialize the query.
				q = tt.setup()
			}
			// Call BuildDeleteQuery and capture the result and error for each test case.
			got, gotArgs, err := b.BuildDeleteQuery(q)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantSQL {
				t.Errorf("BuildDeleteQuery() = %v, want %v", got, tt.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("BuildDeleteQuery() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestMySQLBuilder_BuildInsertQuery tests the BuildInsertQuery method for various InsertQuery scenarios, including nil queries, empty tables, empty values, and valid inserts.
func TestMySQLBuilder_BuildInsertQuery(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name     string
		q        *InsertQuery
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "nil query",
			q:        nil,
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name:     "empty table",
			q:        &InsertQuery{Table: "", Values: []map[string]interface{}{{"id": 1}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name:     "empty values",
			q:        &InsertQuery{Table: "users", Values: []map[string]interface{}{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "valid insert, single row",
			// InsertQuery with a single row should generate correct SQL and arguments.
			q:        &InsertQuery{Table: "users", Values: []map[string]interface{}{{"id": 1, "name": "foo"}}},
			wantSQL:  "INSERT INTO users (id, name) VALUES (?, ?)",
			wantArgs: []interface{}{1, "foo"},
			wantErr:  false,
		},
		{
			name: "valid insert, multiple rows",
			// InsertQuery with multiple rows should generate correct SQL and arguments for all values.
			q:        &InsertQuery{Table: "users", Values: []map[string]interface{}{{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}}},
			wantSQL:  "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)",
			wantArgs: []interface{}{1, "foo", 2, "bar"},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call BuildInsertQuery and capture the result and error for each test case.
			got, gotArgs, err := b.BuildInsertQuery(tt.q)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildInsertQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantSQL {
				t.Errorf("BuildInsertQuery() = %v, want %v", got, tt.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("BuildInsertQuery() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestMySQLBuilder_BuildSelectQuery tests the BuildSelectQuery method for various SelectQuery scenarios, including raw queries, error cases, and valid selects.
func TestMySQLBuilder_BuildSelectQuery(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name     string
		q        *SelectQuery
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "nil query",
			q:        nil,
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "raw query",
			// SelectQuery with a raw SQL string should return the raw SQL without arguments.
			q:        &SelectQuery{Raw: "SELECT 1"},
			wantSQL:  "SELECT 1",
			wantArgs: nil,
			wantErr:  false,
		},
		{
			name: "error in fields",
			// SelectQuery with invalid fields should return an error.
			q:        &SelectQuery{Fields: []Field{{}}, Table: Table{Name: "users"}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "error in table",
			// SelectQuery with an invalid table should return an error.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "error in joins",
			// SelectQuery with an invalid join should return an error.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Joins: []Join{{Type: JoinType("inner"), Table: Table{}, Filter: Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "error in filter",
			// SelectQuery with an invalid filter should return an error.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Filter: &Filter{Field: Field{}, Operator: Operator("="), Value: FilterValue{Value: 1}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "error in group by",
			// SelectQuery with an invalid group by field should return an error.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, GroupByFields: []Field{{}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "error in order by",
			// SelectQuery with an invalid order by field should return an error.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Sorts: []Sort{{Field: Field{}}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "with take, skip, alias",
			// SelectQuery with LIMIT, OFFSET, and alias should generate a subquery with arguments.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Take: 10, Skip: 5, Alias: "sub"},
			wantSQL:  "(SELECT id FROM users LIMIT ? OFFSET ?) AS sub",
			wantArgs: []interface{}{int64(10), int64(5)},
			wantErr:  false,
		},
		{
			name: "full valid select",
			// Full SelectQuery with joins, filter, group by, order by, limit, and offset.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Joins: []Join{{Type: JoinType("inner"), Table: Table{Name: "roles"}, Filter: Filter{Field: Field{Column: "role_id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}}}, Filter: &Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 2}}, GroupByFields: []Field{{Column: "id"}}, Sorts: []Sort{{Field: Field{Column: "id"}, Direction: "ASC"}}, Take: 1, Skip: 2},
			wantSQL:  "SELECT id FROM users INNER roles ON role_id = ? WHERE id = ? GROUP BY id ORDER BY id ASC LIMIT ? OFFSET ?",
			wantArgs: []interface{}{1, 2, int64(1), int64(2)},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call BuildSelectQuery and capture the result and error for each test case.
			got, gotArgs, err := b.BuildSelectQuery(tt.q)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildSelectQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantSQL {
				t.Errorf("BuildSelectQuery() = %v, want %v", got, tt.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("BuildSelectQuery() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestMySQLBuilder_BuildUpdateQuery tests the BuildUpdateQuery method for various UpdateQuery scenarios, including nil queries, empty tables, empty fields, valid updates, and error cases.
func TestMySQLBuilder_BuildUpdateQuery(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name     string
		q        *UpdateQuery
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "nil query",
			q:        nil,
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name:     "empty table",
			q:        &UpdateQuery{Table: "", FieldsValue: map[string]interface{}{"id": 1}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name:     "empty fields",
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "valid update, no filter",
			// Valid UpdateQuery with multiple fields and no filter.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"name": "foo", "age": 30}},
			wantSQL:  "UPDATE users SET age = ?, name = ?",
			wantArgs: []interface{}{30, "foo"},
			wantErr:  false,
		},
		{
			name: "valid update, with filter",
			// Valid UpdateQuery with a filter on the "id" column.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"name": "bar"}, Filter: &Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}},
			wantSQL:  "UPDATE users SET name = ? WHERE id = ?",
			wantArgs: []interface{}{"bar", 1},
			wantErr:  false,
		},
		{
			name: "error in filter",
			// UpdateQuery with an invalid filter (empty field) should return an error.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"name": "baz"}, Filter: &Filter{Field: Field{}, Operator: Operator("="), Value: FilterValue{Value: 1}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call BuildUpdateQuery and capture the result and error for each test case.
			got, gotArgs, err := b.BuildUpdateQuery(tt.q)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildUpdateQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantSQL {
				t.Errorf("BuildUpdateQuery() = %v, want %v", got, tt.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("BuildUpdateQuery() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestMySQLBuilder_buildFields tests the buildFields method for various field scenarios, including single and multiple columns, aliases, table-qualified columns, and error cases.
func TestMySQLBuilder_buildFields(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name     string
		fields   []Field
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "single column",
			fields:   []Field{{Column: "id"}},
			wantSQL:  "id",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "multiple columns",
			fields:   []Field{{Column: "id"}, {Column: "name"}},
			wantSQL:  "id, name",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "column with alias",
			fields:   []Field{{Column: "id", Alias: "user_id"}},
			wantSQL:  "id AS user_id",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "table and column",
			fields:   []Field{{Table: "users", Column: "id"}},
			wantSQL:  "users.id",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "table, column, alias",
			fields:   []Field{{Table: "users", Column: "id", Alias: "uid"}},
			wantSQL:  "users.id AS uid",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "invalid field (empty)",
			fields:   []Field{{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments for each test case.
			args := []interface{}{}
			// Call buildFields and capture the result and error for each test case.
			got, err := b.buildFields(tt.fields, &args)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFields() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildFields() = %v, want %v", got, tt.wantSQL)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !tt.wantErr && !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("buildFields() args = %v, want %v", args, tt.wantArgs)
			}
		})
	}
}

// TestMySQLBuilder_buildFilter tests the buildFilter method for various filter scenarios, including error propagation, group logic, and value handling.
func TestMySQLBuilder_buildFilter(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name     string
		filter   *Filter
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "nil filter",
			filter:   nil,
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  false,
		},
		{
			name: "simple equality",
			// Filter with a simple equality condition should generate correct SQL and arguments.
			filter:   &Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
			wantSQL:  "id = ?",
			wantArgs: []interface{}{1},
			wantErr:  false,
		},
		{
			name: "AND group, isRoot true",
			// Group filter with AND logic at the root level.
			filter:   &Filter{Logic: LogicAnd, Filters: []Filter{{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}}, {Field: Field{Column: "b"}, Operator: Operator("="), Value: FilterValue{Value: 2}}}},
			wantSQL:  "a = ? AND b = ?",
			wantArgs: []interface{}{1, 2},
			wantErr:  false,
		},
		{
			name: "OR group, isRoot false",
			// Group filter with OR logic, not at the root level, should be wrapped in parentheses.
			filter:   &Filter{Logic: LogicOr, Filters: []Filter{{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}}, {Field: Field{Column: "b"}, Operator: Operator("="), Value: FilterValue{Value: 2}}}},
			wantSQL:  "(a = ? OR b = ?)",
			wantArgs: []interface{}{1, 2},
			wantErr:  false,
		},
		{
			name: "error in buildFieldForFilter",
			// Filter with an invalid field should return an error.
			filter:   &Filter{Field: Field{}, Operator: Operator("="), Value: FilterValue{Value: 1}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "error in buildFilterValue",
			// Filter with an invalid value for the IN operator should return an error.
			filter:   &Filter{Field: Field{Column: "id"}, Operator: Operator("IN"), Value: FilterValue{Value: 123}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "group with sub-filter error",
			// Group filter with a sub-filter that returns an error should propagate the error.
			filter:   &Filter{Logic: LogicAnd, Filters: []Filter{{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}}, {Field: Field{}, Operator: Operator("="), Value: FilterValue{Value: 2}}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments for each test case.
			args := []interface{}{}
			// The isRoot parameter is false only for the OR group test case.
			got, err := b.buildFilter(tt.filter, &args, tt.name != "OR group, isRoot false")
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFilter() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildFilter() = %v, want %v", got, tt.wantSQL)
			}
			// Compare the arguments slice to ensure correct parameterization.
			var gotArgs, wantArgs []interface{}
			if args == nil {
				gotArgs = []interface{}{}
			} else {
				gotArgs = args
			}
			if tt.wantArgs == nil {
				wantArgs = []interface{}{}
			} else {
				wantArgs = tt.wantArgs
			}
			if !tt.wantErr && !reflect.DeepEqual(gotArgs, wantArgs) {
				t.Errorf("buildFilter() args = %v, want %v", gotArgs, wantArgs)
			}
		})
	}
}

// TestMySQLBuilder_buildGroupBy tests the buildGroupBy method for various scenarios, including empty, single, and multiple fields, as well as error cases.
func TestMySQLBuilder_buildGroupBy(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name    string
		fields  []Field
		wantSQL string
		wantErr bool
	}{
		{
			name:    "empty fields",
			fields:  []Field{},
			wantSQL: "",
			wantErr: false,
		},
		{
			name:    "single field",
			fields:  []Field{{Column: "id"}},
			wantSQL: "id",
			wantErr: false,
		},
		{
			name:    "multiple fields",
			fields:  []Field{{Column: "id"}, {Column: "name"}},
			wantSQL: "id, name",
			wantErr: false,
		},
		{
			name:    "error in field",
			fields:  []Field{{}},
			wantSQL: "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildGroupBy and capture the result and error for each test case.
			got, err := b.buildGroupBy(tt.fields)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildGroupBy() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildGroupBy() = %v, want %v", got, tt.wantSQL)
			}
		})
	}
}

// TestMySQLBuilder_buildJoins tests the buildJoins method for different join scenarios, including no joins, single join, multiple joins, and error cases.
func TestMySQLBuilder_buildJoins(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name    string
		joins   []Join
		wantSQL string
		wantErr bool
	}{
		{
			name:    "no joins",
			joins:   nil,
			wantSQL: "",
			wantErr: false,
		},
		{
			name:    "single join",
			joins:   []Join{{Type: JoinType("INNER JOIN"), Table: Table{Name: "roles"}, Filter: Filter{Field: Field{Column: "id"}}}},
			wantSQL: "INNER JOIN roles ON id",
			wantErr: false,
		},
		{
			name:    "multiple joins",
			joins:   []Join{{Type: JoinType("LEFT JOIN"), Table: Table{Name: "a"}, Filter: Filter{Field: Field{Column: "x"}}}, {Type: JoinType("RIGHT JOIN"), Table: Table{Name: "b"}, Filter: Filter{Field: Field{Column: "y"}}}},
			wantSQL: "LEFT JOIN a ON x RIGHT JOIN b ON y",
			wantErr: false,
		},
		{
			name:    "error in join table",
			joins:   []Join{{Type: JoinType("INNER JOIN"), Table: Table{}, Filter: Filter{Field: Field{Column: "id"}}}},
			wantSQL: "",
			wantErr: true,
		},
		{
			name:    "error in join filter",
			joins:   []Join{{Type: JoinType("INNER JOIN"), Table: Table{Name: "roles"}, Filter: Filter{Field: Field{}}}},
			wantSQL: "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments for each test case.
			args := []interface{}{}
			// Call buildJoins and capture the result and error for each test case.
			got, err := b.buildJoins(tt.joins, &args)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildJoins() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Normalize the SQL string by removing extra spaces and stray placeholders for comparison.
			gotNorm := got
			for strings.Contains(gotNorm, " ?") {
				gotNorm = strings.ReplaceAll(gotNorm, " ?", "")
			}
			gotNorm = strings.TrimSpace(gotNorm)
			for strings.Contains(gotNorm, "  ") {
				gotNorm = strings.ReplaceAll(gotNorm, "  ", " ")
			}
			if gotNorm != tt.wantSQL {
				t.Errorf("buildJoins() = %v, want %v", gotNorm, tt.wantSQL)
			}
		})
	}
}

// TestMySQLBuilder_buildOrderBy tests the buildOrderBy method for various sorting scenarios, including no sorts, single and multiple sorts, and error cases.
func TestMySQLBuilder_buildOrderBy(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name    string
		sorts   []Sort
		wantSQL string
		wantErr bool
	}{
		{
			name:    "no sorts",
			sorts:   nil,
			wantSQL: "",
			wantErr: false,
		},
		{
			name:    "single sort asc",
			sorts:   []Sort{{Field: Field{Column: "id"}, Direction: "ASC"}},
			wantSQL: "id ASC",
			wantErr: false,
		},
		{
			name:    "single sort desc",
			sorts:   []Sort{{Field: Field{Column: "name"}, Direction: "DESC"}},
			wantSQL: "name DESC",
			wantErr: false,
		},
		{
			name:    "multiple sorts",
			sorts:   []Sort{{Field: Field{Column: "id"}, Direction: "ASC"}, {Field: Field{Column: "name"}, Direction: "DESC"}},
			wantSQL: "id ASC, name DESC",
			wantErr: false,
		},
		{
			name:    "error in sort field",
			sorts:   []Sort{{Field: Field{}, Direction: "ASC"}},
			wantSQL: "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildOrderBy and capture the result and error for each test case.
			got, err := b.buildOrderBy(tt.sorts)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildOrderBy() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildOrderBy() = %v, want %v", got, tt.wantSQL)
			}
		})
	}
}

// TestMySQLBuilder_buildTable tests the buildTable method for various table scenarios, including empty tables, simple tables, tables with aliases, subquery tables, and error cases.
func TestMySQLBuilder_buildTable(t *testing.T) {
	b := newMySQLBuilder()
	tests := []struct {
		name    string
		table   Table
		wantSQL string
		wantErr bool
	}{
		{
			name:    "empty table",
			table:   Table{},
			wantSQL: "",
			wantErr: true,
		},
		{
			name:    "simple table",
			table:   Table{Name: "users"},
			wantSQL: "users",
			wantErr: false,
		},
		{
			name:    "table with alias",
			table:   Table{Name: "users", Alias: "u"},
			wantSQL: "users AS u",
			wantErr: false,
		},
		{
			name:    "subquery table",
			table:   Table{SelectQuery: &SelectQuery{Raw: "SELECT 1"}, Alias: "sq"},
			wantSQL: "(SELECT 1) AS sq",
			wantErr: false,
		},
		{
			name:    "subquery table error",
			table:   Table{SelectQuery: &SelectQuery{}},
			wantSQL: "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments for each test case.
			args := []interface{}{}
			// Call buildTable and capture the result and error for each test case.
			got, err := b.buildTable(tt.table, &args)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildTable() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildTable() = %v, want %v", got, tt.wantSQL)
			}
		})
	}
}
