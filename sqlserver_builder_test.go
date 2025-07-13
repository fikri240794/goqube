package goqube

import (
	"reflect"
	"testing"
)

// Test_newSQLServerBuilder verifies that newSQLServerBuilder returns a valid builder with the correct placeholder format for SQL Server.
// It ensures the builder is not nil and uses the "@p%d" placeholder format as required by SQL Server queries.
func Test_newSQLServerBuilder(t *testing.T) {
	b := newSQLServerBuilder()
	if b == nil {
		// The builder should never be nil; fail the test if it is.
		t.Fatal("newSQLServerBuilder() returned nil")
	}
	if b.placeholderFormat != "@p%d" {
		// The placeholder format for SQL Server should be "@p%d" for parameterized queries.
		t.Errorf("expected placeholderFormat '@p%%d', got '%s'", b.placeholderFormat)
	}
}

// Test_sqlServerBuilder_BuildDeleteQuery verifies the SQL Server builder's ability to generate DELETE queries correctly for various scenarios.
// It checks for correct SQL, arguments, and error handling for nil queries, empty tables, basic deletes, and deletes with filters.
func Test_sqlServerBuilder_BuildDeleteQuery(t *testing.T) {
	b := newSQLServerBuilder()
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
			name: "delete with filter",
			// This test checks if the builder correctly adds a WHERE clause with a parameterized filter.
			q:        &DeleteQuery{Table: "users", Filter: &Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}},
			wantSQL:  "DELETE FROM users WHERE id = @p0",
			wantArgs: []interface{}{1},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the DELETE query using the builder and the test case input.
			sql, args, err := b.BuildDeleteQuery(tt.q)
			// Check if the generated SQL matches the expected SQL.
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Check if the number of arguments matches the expected count.
			if len(args) != len(tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			} else {
				// Compare each argument value for correctness.
				for i := range args {
					if args[i] != tt.wantArgs[i] {
						t.Errorf("got args[%d]=%v, want %v", i, args[i], tt.wantArgs[i])
					}
				}
			}
			// Check if the error matches the expected error for the test case.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_BuildInsertQuery verifies the SQL Server builder's ability to generate INSERT queries for various scenarios.
// It checks for correct SQL, arguments, and error handling for nil queries, empty tables, missing values, and single/multi-row inserts.
func Test_sqlServerBuilder_BuildInsertQuery(t *testing.T) {
	b := newSQLServerBuilder()
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
			name: "single row",
			// This test checks if the builder generates a correct INSERT statement for a single row.
			q:        &InsertQuery{Table: "users", Values: []map[string]interface{}{{"id": 1, "name": "foo"}}},
			wantSQL:  "INSERT INTO users (id, name) VALUES (@p0, @p1)",
			wantArgs: []interface{}{1, "foo"},
			wantErr:  nil,
		},
		{
			name: "multi row",
			// This test checks if the builder generates a correct INSERT statement for multiple rows.
			q:        &InsertQuery{Table: "users", Values: []map[string]interface{}{{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}}},
			wantSQL:  "INSERT INTO users (id, name) VALUES (@p0, @p1), (@p2, @p3)",
			wantArgs: []interface{}{1, "foo", 2, "bar"},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the INSERT query using the builder and the test case input.
			sql, args, err := b.BuildInsertQuery(tt.q)
			// Check if the generated SQL matches the expected SQL.
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Check if the arguments match the expected arguments.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Check if the error matches the expected error for the test case.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_BuildSelectQuery verifies the SQL Server builder's ability to generate SELECT queries for various scenarios.
// It checks for correct SQL, arguments, and error handling for nil queries, raw queries, filters, grouping, ordering, limits, joins, and error cases.
func Test_sqlServerBuilder_BuildSelectQuery(t *testing.T) {
	b := newSQLServerBuilder()
	tests := []struct {
		name     string
		q        *SelectQuery
		wantSQL  string
		wantArgs []interface{}
		wantErr  error
	}{
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
			name: "select with where",
			// This test checks if the builder adds a WHERE clause with a parameterized filter.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Filter: &Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}},
			wantSQL:  "SELECT id FROM users WHERE id = @p0",
			wantArgs: []interface{}{1},
			wantErr:  nil,
		},
		{
			name: "select with group by",
			// This test checks if the builder adds a GROUP BY clause.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, GroupByFields: []Field{{Column: "id"}}},
			wantSQL:  "SELECT id FROM users GROUP BY id",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name: "error from buildGroupBy",
			// This test checks if the builder returns an error for invalid group by fields.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, GroupByFields: []Field{{}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidGroupBy,
		},
		{
			name: "select with order by",
			// This test checks if the builder adds an ORDER BY clause.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Sorts: []Sort{{Field: Field{Column: "id"}, Direction: "DESC"}}},
			wantSQL:  "SELECT id FROM users ORDER BY id DESC",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name: "error from buildOrderBy",
			// This test checks if the builder returns an error for invalid order by fields.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Sorts: []Sort{{}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidOrderBy,
		},
		{
			name: "select with limit and offset",
			// This test checks if the builder adds OFFSET and FETCH clauses for pagination.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Take: 10, Skip: 5},
			wantSQL:  "SELECT id FROM users OFFSET @p0 ROWS FETCH NEXT @p1 ROWS ONLY",
			wantArgs: []interface{}{int64(5), int64(10)},
			wantErr:  nil,
		},
		{
			name: "select with take only",
			// This test checks if the builder adds OFFSET 0 and FETCH for a limited result set.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Take: 10},
			wantSQL:  "SELECT id FROM users OFFSET 0 ROWS FETCH NEXT @p0 ROWS ONLY",
			wantArgs: []interface{}{int64(10)},
			wantErr:  nil,
		},
		{
			name: "select with alias",
			// This test checks if the builder wraps the query as a subquery with an alias.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Alias: "u"},
			wantSQL:  "(SELECT id FROM users) AS u",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name: "error from buildFields",
			// This test checks if the builder returns an error for invalid fields.
			q:        &SelectQuery{Fields: []Field{{Column: ""}}, Table: Table{Name: "users"}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidField,
		},
		{
			name: "error from buildTable",
			// This test checks if the builder returns an error for an invalid table.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidTable,
		},
		{
			name: "error from buildFilter",
			// This test checks if the builder returns an error for an invalid filter.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Filter: &Filter{Field: Field{}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidFilter,
		},
		{
			name: "select with join",
			// This test checks if the builder adds a JOIN clause with the correct ON condition.
			q: &SelectQuery{
				Fields: []Field{{Column: "id"}},
				Table:  Table{Name: "users"},
				Joins: []Join{{
					Type:  JoinTypeInner,
					Table: Table{Name: "orders"},
					Filter: Filter{
						Field:    Field{Table: "users", Column: "id"},
						Operator: Operator("="),
						Value:    FilterValue{Table: "orders", Column: "user_id"},
					},
				}},
			},
			wantSQL:  "SELECT id FROM users INNER JOIN orders ON users.id = orders.user_id",
			wantArgs: []interface{}{},
			wantErr:  nil,
		},
		{
			name: "error from buildJoins",
			// This test checks if the builder returns an error for an invalid join.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Joins: []Join{{}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidTable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the SELECT query using the builder and the test case input.
			sql, args, err := b.BuildSelectQuery(tt.q)
			// Check if the generated SQL matches the expected SQL.
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Check if the arguments match the expected arguments.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Check if the error matches the expected error for the test case.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_BuildUpdateQuery verifies the SQL Server builder's ability to generate UPDATE queries for various scenarios.
// It checks for correct SQL, arguments, and error handling for nil queries, empty tables, empty fields, valid updates, and filter errors.
func Test_sqlServerBuilder_BuildUpdateQuery(t *testing.T) {
	b := newSQLServerBuilder()
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
			q:        &UpdateQuery{Table: "", FieldsValue: map[string]interface{}{"id": 1}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidUpdateQuery,
		},
		{
			name:     "empty fields",
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidUpdateQuery,
		},
		{
			name: "valid update, no filter",
			// This test checks if the builder generates a correct UPDATE statement without a WHERE clause.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1, "name": "foo"}},
			wantSQL:  "UPDATE users SET id = @p0, name = @p1",
			wantArgs: []interface{}{1, "foo"},
			wantErr:  nil,
		},
		{
			name: "valid update, with filter",
			// This test checks if the builder generates a correct UPDATE statement with a WHERE clause.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1, "name": "foo"}, Filter: &Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}},
			wantSQL:  "UPDATE users SET id = @p0, name = @p1 WHERE id = @p2",
			wantArgs: []interface{}{1, "foo", 1},
			wantErr:  nil,
		},
		{
			name: "filter returns error",
			// This test checks if the builder returns an error for an invalid filter.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1}, Filter: &Filter{Field: Field{}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  ErrInvalidFilter,
		},
		{
			name: "filter returns empty string",
			// This test checks if the builder generates an UPDATE statement when the filter is nil.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1}, Filter: nil},
			wantSQL:  "UPDATE users SET id = @p0",
			wantArgs: []interface{}{1},
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the UPDATE query using the builder and the test case input.
			sql, args, err := b.BuildUpdateQuery(tt.q)
			// Check if the generated SQL matches the expected SQL.
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Check if the arguments match the expected arguments.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("got args %v, want %v", args, tt.wantArgs)
			}
			// Check if the error matches the expected error for the test case.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_buildFields verifies the builder's ability to generate field lists for SELECT queries in various scenarios.
// It checks for correct SQL fragments and error handling for single/multiple columns, table prefixes, aliases, subqueries, and invalid fields.
func Test_sqlServerBuilder_buildFields(t *testing.T) {
	b := newSQLServerBuilder()
	dummySelect := &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}}
	tests := []struct {
		name    string
		fields  []Field
		want    string
		wantErr error
	}{
		{
			name:    "single column",
			fields:  []Field{{Column: "id"}},
			want:    "id",
			wantErr: nil,
		},
		{
			name:    "multiple columns",
			fields:  []Field{{Column: "id"}, {Column: "name"}},
			want:    "id, name",
			wantErr: nil,
		},
		{
			name:    "with table prefix",
			fields:  []Field{{Table: "users", Column: "id"}},
			want:    "users.id",
			wantErr: nil,
		},
		{
			name:    "with alias",
			fields:  []Field{{Column: "id", Alias: "user_id"}},
			want:    "id AS user_id",
			wantErr: nil,
		},
		{
			name: "with subquery",
			// This test checks if the builder generates a field with a subquery and alias.
			fields:  []Field{{SelectQuery: dummySelect, Alias: "sub"}},
			want:    "(SELECT id FROM users) AS sub",
			wantErr: nil,
		},
		{
			name: "error invalid field",
			// This test checks if the builder returns an error for an invalid field.
			fields:  []Field{{}},
			want:    "",
			wantErr: ErrInvalidField,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare the arguments slice for the buildFields function.
			args := []interface{}{}
			got, err := b.buildFields(tt.fields, &args)
			// Check if the generated field list matches the expected SQL fragment.
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Check if the error matches the expected error for the test case.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_buildFieldForFilter verifies the builder's ability to generate correct SQL fragments for fields used in filters.
// It checks for proper handling of subqueries, table prefixes, aliases, and error cases for invalid fields.
func Test_sqlServerBuilder_buildFieldForFilter(t *testing.T) {
	b := newSQLServerBuilder()
	// Create a dummy select query for subquery test cases.
	dummySelect := &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}}
	tests := []struct {
		name    string
		field   Field
		want    string
		wantErr error
	}{
		{
			name:    "subquery error",
			field:   Field{SelectQuery: &SelectQuery{}},
			want:    "",
			wantErr: ErrInvalidTable,
		},
		{
			name:    "single column",
			field:   Field{Column: "id"},
			want:    "id",
			wantErr: nil,
		},
		{
			name:    "with table prefix",
			field:   Field{Table: "users", Column: "id"},
			want:    "users.id",
			wantErr: nil,
		},
		{
			name:    "with alias",
			field:   Field{SelectQuery: dummySelect, Alias: "sub"},
			want:    "(SELECT id FROM users) AS sub",
			wantErr: nil,
		},
		{
			name:    "with subquery no alias",
			field:   Field{SelectQuery: dummySelect},
			want:    "(SELECT id FROM users)",
			wantErr: nil,
		},
		{
			name:    "error invalid field",
			field:   Field{},
			want:    "",
			wantErr: ErrInvalidFilter,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildFieldForFilter to generate the SQL fragment for the given field.
			got, err := b.buildFieldForFilter(tt.field)
			// Check if the generated SQL matches the expected value.
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Check if the error matches the expected error for the test case.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_buildFilter verifies the builder's ability to generate SQL WHERE clauses from various filter structures.
// It checks for correct SQL, argument handling, and error propagation for nil filters, logical groups, operators, and invalid cases.
func Test_sqlServerBuilder_buildFilter(t *testing.T) {
	b := newSQLServerBuilder()
	tests := []struct {
		name     string
		filter   *Filter
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "nil filter returns empty",
			filter:   nil,
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "error in buildFilterValue propagates to buildFilter",
			filter:   &Filter{Field: Field{Column: "a"}, Operator: OperatorIn, Value: FilterValue{Value: []int{}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "error in subfilter propagates to parent",
			filter: &Filter{Logic: LogicAnd, Filters: []Filter{
				{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
				{Field: Field{}},
			}},
			wantSQL:  "",
			wantArgs: []interface{}{1},
			wantErr:  true,
		},
		{
			name: "AND group with double space triggers normalization",
			filter: &Filter{Logic: LogicAnd, Filters: []Filter{
				{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
				{Field: Field{Column: "b"}, Operator: Operator("= "), Value: FilterValue{Value: 2}},
			}},
			wantSQL:  "a = @p0 AND b = @p1",
			wantArgs: []interface{}{1, 2},
			wantErr:  false,
		},
		{
			name: "OR group with nested group (isRoot false)",
			filter: &Filter{Logic: LogicOr, Filters: []Filter{
				{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
				{Logic: LogicAnd, Filters: []Filter{
					{Field: Field{Column: "b"}, Operator: Operator("="), Value: FilterValue{Value: 2}},
					{Field: Field{Column: "c"}, Operator: Operator("="), Value: FilterValue{Value: 3}},
				}},
			}},
			wantSQL:  "a = @p0 OR (b = @p1 AND c = @p2)",
			wantArgs: []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "single filter with LIKE operator",
			filter:   &Filter{Field: Field{Column: "name"}, Operator: Operator("LIKE"), Value: FilterValue{Value: "foo"}},
			wantSQL:  "name LIKE @p0",
			wantArgs: []interface{}{"%foo%"},
			wantErr:  false,
		},
		{
			name:     "single filter with NOT LIKE operator",
			filter:   &Filter{Field: Field{Column: "name"}, Operator: Operator("NOT LIKE"), Value: FilterValue{Value: "bar"}},
			wantSQL:  "name NOT LIKE @p0",
			wantArgs: []interface{}{"%bar%"},
			wantErr:  false,
		},
		{
			name:     "single filter with error in field",
			filter:   &Filter{Field: Field{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name:     "IS NULL operator",
			filter:   &Filter{Field: Field{Column: "name"}, Operator: OperatorIsNull},
			wantSQL:  "name IS NULL",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "IS NOT NULL operator",
			filter:   &Filter{Field: Field{Column: "name"}, Operator: OperatorIsNotNull},
			wantSQL:  "name IS NOT NULL",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "empty AND group returns empty",
			filter:   &Filter{Logic: LogicAnd, Filters: []Filter{}},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
		{
			name:     "empty OR group returns empty",
			filter:   &Filter{Logic: LogicOr, Filters: []Filter{}},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
		{
			name:     "AND group with all subfilters empty",
			filter:   &Filter{Logic: LogicAnd, Filters: []Filter{{}, {}}},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments and parameter index for buildFilter.
			args := []interface{}{}
			paramIndex := 0
			// Call buildFilter to generate the SQL WHERE clause and arguments.
			sql, err := b.buildFilter(tt.filter, &args, &paramIndex, true)
			// Check if the generated SQL matches the expected SQL.
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Check if the arguments match the expected arguments, handling nil cases.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				wantArgs := tt.wantArgs
				if wantArgs == nil {
					wantArgs = []interface{}{}
				}
				if !reflect.DeepEqual(args, wantArgs) {
					t.Errorf("got args %v, want %v", args, wantArgs)
				}
			}
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("got err %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_buildFilterValue verifies the builder's ability to generate SQL fragments and arguments for filter values with various operators.
// It checks for correct handling of LIKE, IN, subqueries, columns, and error cases for invalid or empty values.
func Test_sqlServerBuilder_buildFilterValue(t *testing.T) {
	b := newSQLServerBuilder()
	// Create a dummy select query for subquery value test cases.
	dummySelect := &SelectQuery{Raw: "SELECT 1"}
	tests := []struct {
		name     string
		op       Operator
		value    FilterValue
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "LIKE operator delegates to buildFilterValueLike",
			op:       OperatorLike,
			value:    FilterValue{Value: "foo"},
			wantSQL:  "@p0",
			wantArgs: []interface{}{"%foo%"},
			wantErr:  false,
		},
		{
			name:     "NOT LIKE operator delegates to buildFilterValueLike",
			op:       OperatorNotLike,
			value:    FilterValue{Value: "bar"},
			wantSQL:  "@p0",
			wantArgs: []interface{}{"%bar%"},
			wantErr:  false,
		},
		{
			name:     "subquery value",
			op:       Operator("="),
			value:    FilterValue{SelectQuery: dummySelect},
			wantSQL:  "(SELECT 1)",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "table-qualified column",
			op:       Operator("="),
			value:    FilterValue{Table: "users", Column: "id"},
			wantSQL:  "users.id",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "plain column",
			op:       Operator("="),
			value:    FilterValue{Column: "name"},
			wantSQL:  "name",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "IS NULL operator",
			op:       OperatorIsNull,
			value:    FilterValue{Value: nil},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "IN operator with array",
			op:       OperatorIn,
			value:    FilterValue{Value: []int{1, 2, 3}},
			wantSQL:  "(@p0, @p1, @p2)",
			wantArgs: []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "IN operator with empty array",
			op:       OperatorIn,
			value:    FilterValue{Value: []int{}},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
		{
			name:     "IN operator with non-array",
			op:       OperatorIn,
			value:    FilterValue{Value: 123},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
		{
			name:     "standard parameterized value",
			op:       Operator("="),
			value:    FilterValue{Value: 42},
			wantSQL:  "@p0",
			wantArgs: []interface{}{42},
			wantErr:  false,
		},
		{
			name:     "subquery value returns error",
			op:       Operator("="),
			value:    FilterValue{SelectQuery: &SelectQuery{}},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments and parameter index for buildFilterValue.
			args := []interface{}{}
			paramIndex := 0
			// Call buildFilterValue to generate the SQL fragment and arguments for the filter value.
			sql, err := b.buildFilterValue(tt.op, tt.value, &args, &paramIndex)
			// Check if the generated SQL matches the expected SQL.
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Check if the arguments match the expected arguments, handling nil cases.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				wantArgs := tt.wantArgs
				if wantArgs == nil {
					wantArgs = []interface{}{}
				}
				if !reflect.DeepEqual(args, wantArgs) {
					t.Errorf("got args %v, want %v", args, wantArgs)
				}
			}
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("got err %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_buildFilterValueLike verifies the builder's ability to generate SQL fragments and arguments for LIKE filter values.
// It checks for correct handling of subqueries, table-qualified columns, string values, and error cases for invalid or nil values.
func Test_sqlServerBuilder_buildFilterValueLike(t *testing.T) {
	b := newSQLServerBuilder()
	// Create a dummy select query for subquery value test cases.
	dummySelect := &SelectQuery{Raw: "SELECT 1"}
	tests := []struct {
		name     string
		value    FilterValue
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "LIKE with subquery",
			value:    FilterValue{SelectQuery: dummySelect},
			wantSQL:  "(@p0)",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "LIKE with subquery error",
			value:    FilterValue{SelectQuery: &SelectQuery{}},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
		{
			name:     "LIKE with table-qualified column",
			value:    FilterValue{Table: "users", Column: "name"},
			wantSQL:  "users.name",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "LIKE with string value",
			value:    FilterValue{Value: "foo"},
			wantSQL:  "@p0",
			wantArgs: []interface{}{"%foo%"},
			wantErr:  false,
		},
		{
			name:     "LIKE with non-string value",
			value:    FilterValue{Value: 123},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
		{
			name:     "LIKE with nil value",
			value:    FilterValue{Value: nil},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments and parameter index for buildFilterValueLike.
			args := []interface{}{}
			paramIndex := 0
			// Call buildFilterValueLike to generate the SQL fragment and arguments for the LIKE filter value.
			sql, err := b.buildFilterValueLike(tt.value, &args, &paramIndex)
			// Check if the generated SQL matches the expected SQL.
			if sql != tt.wantSQL {
				t.Errorf("got SQL %q, want %q", sql, tt.wantSQL)
			}
			// Check if the arguments match the expected arguments, handling nil cases.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				wantArgs := tt.wantArgs
				if wantArgs == nil {
					wantArgs = []interface{}{}
				}
				if !reflect.DeepEqual(args, wantArgs) {
					t.Errorf("got args %v, want %v", args, wantArgs)
				}
			}
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("got err %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_buildGroupBy verifies the builder's ability to generate SQL GROUP BY clauses for various field scenarios.
// It checks for correct SQL fragments and error handling for single/multiple columns, table prefixes, subqueries, and invalid fields.
func Test_sqlServerBuilder_buildGroupBy(t *testing.T) {
	b := newSQLServerBuilder()
	// Create a dummy select query for subquery test cases.
	dummySelect := &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}}
	tests := []struct {
		name    string
		fields  []Field
		want    string
		wantErr error
	}{
		{
			name:    "single column",
			fields:  []Field{{Column: "id"}},
			want:    "id",
			wantErr: nil,
		},
		{
			name:    "multiple columns",
			fields:  []Field{{Column: "id"}, {Column: "name"}},
			want:    "id, name",
			wantErr: nil,
		},
		{
			name:    "with table prefix",
			fields:  []Field{{Table: "users", Column: "id"}},
			want:    "users.id",
			wantErr: nil,
		},
		{
			name:    "with subquery",
			fields:  []Field{{SelectQuery: dummySelect, Alias: "sub"}},
			want:    "",
			wantErr: ErrInvalidField,
		},
		{
			name:    "error invalid field",
			fields:  []Field{{}},
			want:    "",
			wantErr: ErrInvalidField,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildGroupBy to generate the SQL GROUP BY clause for the given fields.
			got, err := b.buildGroupBy(tt.fields)
			// Check if the generated SQL matches the expected value.
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Check if the error state matches the expected result.
			if (err != nil) != (tt.wantErr != nil) {
				t.Errorf("got err %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_buildJoins verifies the builder's ability to generate SQL JOIN clauses for various join scenarios.
// It checks for correct SQL fragments and error handling for empty joins, single/multiple joins, subquery tables, and invalid join definitions.
func Test_sqlServerBuilder_buildJoins(t *testing.T) {
	b := newSQLServerBuilder()
	// Create a dummy select query for subquery join test cases.
	dummySelect := &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}}
	tests := []struct {
		name    string
		joins   []Join
		want    string
		wantErr error
	}{
		{
			name:    "empty joins",
			joins:   nil,
			want:    "",
			wantErr: nil,
		},
		{
			name: "single join",
			joins: []Join{{
				Type:   JoinTypeInner,
				Table:  Table{Name: "roles"},
				Filter: Filter{Field: Field{Column: "role_id"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
			}},
			want:    "INNER JOIN roles ON role_id = @p0",
			wantErr: nil,
		},
		{
			name: "multiple joins",
			joins: []Join{
				{
					Type:   JoinTypeInner,
					Table:  Table{Name: "roles"},
					Filter: Filter{Field: Field{Column: "role_id"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
				},
				{
					Type:   JoinTypeLeft,
					Table:  Table{Name: "permissions"},
					Filter: Filter{Field: Field{Column: "perm_id"}, Operator: Operator("="), Value: FilterValue{Value: 2}},
				},
			},
			want:    "INNER JOIN roles ON role_id = @p0 LEFT JOIN permissions ON perm_id = @p1",
			wantErr: nil,
		},
		{
			name: "join with subquery table",
			joins: []Join{{
				Type:   JoinTypeInner,
				Table:  Table{SelectQuery: dummySelect, Alias: "sub"},
				Filter: Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
			}},
			want:    "INNER JOIN (SELECT id FROM users) AS sub ON id = @p0",
			wantErr: nil,
		},
		{
			name: "join with error in table",
			joins: []Join{{
				Type:   JoinTypeInner,
				Table:  Table{},
				Filter: Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
			}},
			want:    "",
			wantErr: ErrInvalidTable,
		},
		{
			name: "join with error in filter",
			joins: []Join{{
				Type:   JoinTypeInner,
				Table:  Table{Name: "roles"},
				Filter: Filter{Field: Field{}}},
			},
			want:    "",
			wantErr: ErrInvalidFilter,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments and parameter index for buildJoins.
			args := []interface{}{}
			paramIndex := 0
			// Call buildJoins to generate the SQL JOIN clause for the given joins.
			got, err := b.buildJoins(tt.joins, &args, &paramIndex)
			// Check if the generated SQL matches the expected value.
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Check if the error matches the expected error for the test case.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_buildOrderBy verifies the builder's ability to generate SQL ORDER BY clauses for various sorting scenarios.
// It checks for correct SQL fragments and error handling for no sorts, single/multiple sorts, table prefixes, aliases, and invalid fields or directions.
func Test_sqlServerBuilder_buildOrderBy(t *testing.T) {
	b := newSQLServerBuilder()
	tests := []struct {
		name    string
		sorts   []Sort
		want    string
		wantErr error
	}{
		{
			name:    "no sorts",
			sorts:   nil,
			want:    "",
			wantErr: nil,
		},
		{
			name:    "single sort ASC",
			sorts:   []Sort{{Field: Field{Column: "id"}, Direction: "ASC"}},
			want:    "id ASC",
			wantErr: nil,
		},
		{
			name:    "single sort DESC",
			sorts:   []Sort{{Field: Field{Column: "name"}, Direction: "DESC"}},
			want:    "name DESC",
			wantErr: nil,
		},
		{
			name:    "multiple sorts",
			sorts:   []Sort{{Field: Field{Column: "id"}, Direction: "ASC"}, {Field: Field{Column: "name"}, Direction: "DESC"}},
			want:    "id ASC, name DESC",
			wantErr: nil,
		},
		{
			name:    "with table prefix",
			sorts:   []Sort{{Field: Field{Table: "users", Column: "id"}, Direction: "ASC"}},
			want:    "users.id ASC",
			wantErr: nil,
		},
		{
			name:    "with alias",
			sorts:   []Sort{{Field: Field{Column: "id", Alias: "user_id"}, Direction: "ASC"}},
			want:    "id ASC",
			wantErr: nil,
		},
		{
			name:    "error invalid field",
			sorts:   []Sort{{Field: Field{}, Direction: "ASC"}},
			want:    "",
			wantErr: ErrInvalidOrderBy,
		},
		{
			name:    "error invalid direction",
			sorts:   []Sort{{Field: Field{Column: "id"}, Direction: "INVALID"}},
			want:    "id INVALID",
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildOrderBy to generate the SQL ORDER BY clause for the given sorts.
			got, err := b.buildOrderBy(tt.sorts)
			// Check if the generated SQL matches the expected value.
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Check if the error matches the expected error for the test case.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_buildTable verifies the builder's ability to generate SQL fragments for table references in various scenarios.
// It checks for correct SQL fragments and error handling for simple tables, aliases, subqueries, and invalid table definitions.
func Test_sqlServerBuilder_buildTable(t *testing.T) {
	b := newSQLServerBuilder()
	// Create a dummy select query for subquery table test cases.
	dummySelect := &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}}
	tests := []struct {
		name    string
		table   Table
		want    string
		wantErr error
	}{
		{
			name:    "simple table",
			table:   Table{Name: "users"},
			want:    "users",
			wantErr: nil,
		},
		{
			name:    "table with alias",
			table:   Table{Name: "users", Alias: "u"},
			want:    "users AS u",
			wantErr: nil,
		},
		{
			name:    "subquery table with alias",
			table:   Table{SelectQuery: dummySelect, Alias: "sub"},
			want:    "(SELECT id FROM users) AS sub",
			wantErr: nil,
		},
		{
			name:    "subquery table without alias",
			table:   Table{SelectQuery: dummySelect},
			want:    "(SELECT id FROM users)",
			wantErr: nil,
		},
		{
			name:    "error invalid table (empty)",
			table:   Table{},
			want:    "",
			wantErr: ErrInvalidTable,
		},
		{
			name:    "error subquery table returns error",
			table:   Table{SelectQuery: &SelectQuery{}},
			want:    "",
			wantErr: ErrInvalidTable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare the arguments slice for the buildTable function.
			args := []interface{}{}
			// Call buildTable to generate the SQL fragment for the given table.
			got, err := b.buildTable(tt.table, &args)
			// Check if the generated SQL matches the expected value.
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			// Check if the error matches the expected error for the test case.
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Test_sqlServerBuilder_nextPlaceholder verifies the builder's ability to generate sequential parameter placeholders for SQL Server queries.
// It checks that the placeholders are correctly formatted and incremented for different starting indices.
func Test_sqlServerBuilder_nextPlaceholder(t *testing.T) {
	b := newSQLServerBuilder()
	tests := []struct {
		name     string
		startIdx int
		want     []string
	}{
		{
			name:     "sequential placeholders",
			startIdx: 0,
			want:     []string{"@p0", "@p1", "@p2", "@p3"},
		},
		{
			name:     "start from 5",
			startIdx: 5,
			want:     []string{"@p5", "@p6"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// idx is used to track the current placeholder index and is updated by nextPlaceholder.
			idx := tt.startIdx
			got := make([]string, len(tt.want))
			// Generate placeholders sequentially and store them in got.
			for i := range tt.want {
				got[i] = b.nextPlaceholder(&idx)
			}
			// Compare each generated placeholder with the expected value.
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("got[%d]=%q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
