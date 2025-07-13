package goqube

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Test_newPostgresBuilder verifies that newPostgresBuilder returns a valid builder with the correct placeholder format for PostgreSQL.
func Test_newPostgresBuilder(t *testing.T) {
	b := newPostgresBuilder()
	if b == nil {
		// The builder should never be nil; fail the test if it is.
		t.Fatal("newPostgresBuilder() returned nil")
	}
	if b.placeholderFormat != "$%d" {
		// The placeholder format for PostgreSQL should be "$%d" (e.g., $1, $2, ...).
		t.Errorf("placeholderFormat = %v, want $%%d", b.placeholderFormat)
	}
}

// TestPostgresBuilder_BuildDeleteQuery tests the BuildDeleteQuery method for various DeleteQuery scenarios, including nil queries, empty tables, and filters.
func TestPostgresBuilder_BuildDeleteQuery(t *testing.T) {
	b := newPostgresBuilder()
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
			wantSQL:  "DELETE FROM users WHERE id = $1",
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

// TestPostgresBuilder_BuildInsertQuery tests the BuildInsertQuery method for various InsertQuery scenarios, including nil queries, empty tables, and valid inserts.
func TestPostgresBuilder_BuildInsertQuery(t *testing.T) {
	b := newPostgresBuilder()
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
			name: "empty table",
			// InsertQuery with an empty table name should return an error.
			q:        &InsertQuery{Table: "", Values: []map[string]interface{}{{"id": 1}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "empty values",
			// InsertQuery with no values should return an error.
			q:        &InsertQuery{Table: "users", Values: []map[string]interface{}{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "valid insert, indexed placeholders",
			// Valid InsertQuery with multiple rows and indexed placeholders for arguments.
			q:        &InsertQuery{Table: "users", Values: []map[string]interface{}{{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}}},
			wantSQL:  "INSERT INTO users (id, name) VALUES ($1, $2), ($3, $4)",
			wantArgs: []interface{}{1, "foo", 2, "bar"},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

// TestPostgresBuilder_BuildSelectQuery tests the BuildSelectQuery method for various SelectQuery scenarios, including raw queries, error cases, and valid selects.
func TestPostgresBuilder_BuildSelectQuery(t *testing.T) {
	b := newPostgresBuilder()
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
			wantSQL:  "(SELECT id FROM users LIMIT $1 OFFSET $2) AS sub",
			wantArgs: []interface{}{int64(10), int64(5)},
			wantErr:  false,
		},
		{
			name: "full valid select",
			// Full SelectQuery with joins, filter, group by, order by, limit, and offset.
			q:        &SelectQuery{Fields: []Field{{Column: "id"}}, Table: Table{Name: "users"}, Joins: []Join{{Type: JoinType("inner"), Table: Table{Name: "roles"}, Filter: Filter{Field: Field{Column: "role_id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}}}, Filter: &Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 2}}, GroupByFields: []Field{{Column: "id"}}, Sorts: []Sort{{Field: Field{Column: "id"}, Direction: "ASC"}}, Take: 1, Skip: 2},
			wantSQL:  "SELECT id FROM users INNER roles ON role_id = $1 WHERE id = $2 GROUP BY id ORDER BY id ASC LIMIT $3 OFFSET $4",
			wantArgs: []interface{}{1, 2, int64(1), int64(2)},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

// TestPostgresBuilder_BuildUpdateQuery tests the BuildUpdateQuery method for various UpdateQuery scenarios, including nil queries, empty tables, empty fields, and valid updates.
func TestPostgresBuilder_BuildUpdateQuery(t *testing.T) {
	b := newPostgresBuilder()
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
			name: "empty table",
			// UpdateQuery with an empty table name should return an error.
			q:        &UpdateQuery{Table: "", FieldsValue: map[string]interface{}{"id": 1}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "empty fields",
			// UpdateQuery with no fields to update should return an error.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "valid update, no filter",
			// Valid UpdateQuery with multiple fields and no filter.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"name": "foo", "age": 30}},
			wantSQL:  "UPDATE users SET age = $1, name = $2",
			wantArgs: []interface{}{30, "foo"},
			wantErr:  false,
		},
		{
			name: "valid update, with filter",
			// Valid UpdateQuery with a filter on the "id" column.
			q:        &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"name": "bar"}, Filter: &Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}},
			wantSQL:  "UPDATE users SET name = $1 WHERE id = $2",
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

// TestPostgresBuilder_buildFilter tests the buildFilter method for various filter scenarios, including error propagation, group logic, and LIKE operators.
func TestPostgresBuilder_buildFilter(t *testing.T) {
	b := newPostgresBuilder()
	tests := []struct {
		name     string
		filter   *Filter
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name: "error in buildFilterValue propagates to buildFilter",
			// Filter with an IN operator and empty array should return an error.
			filter:   &Filter{Field: Field{Column: "a"}, Operator: OperatorIn, Value: FilterValue{Value: []int{}}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "error in subfilter propagates to parent",
			// Group filter with one invalid subfilter should propagate the error.
			filter: &Filter{Logic: LogicAnd, Filters: []Filter{
				{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
				{Field: Field{}},
			}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "AND group with double space triggers normalization",
			// Group filter with normalization of operator spacing.
			filter: &Filter{Logic: LogicAnd, Filters: []Filter{
				{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
				{Field: Field{Column: "b"}, Operator: Operator("= "), Value: FilterValue{Value: 2}},
			}},
			wantSQL:  "a = $1 AND b = $2",
			wantArgs: []interface{}{1, 2},
			wantErr:  false,
		},
		{
			name: "OR group with nested group (isRoot false)",
			// OR group with a nested AND group to test parenthesis and argument indexing.
			filter: &Filter{Logic: LogicOr, Filters: []Filter{
				{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
				{Logic: LogicAnd, Filters: []Filter{
					{Field: Field{Column: "b"}, Operator: Operator("="), Value: FilterValue{Value: 2}},
					{Field: Field{Column: "c"}, Operator: Operator("="), Value: FilterValue{Value: 3}},
				}},
			}},
			wantSQL:  "a = $1 OR (b = $2 AND c = $3)",
			wantArgs: []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name: "single filter with LIKE operator",
			// Filter with LIKE operator should use ILIKE and wrap value with %.
			filter:   &Filter{Field: Field{Column: "name"}, Operator: Operator("LIKE"), Value: FilterValue{Value: "foo"}},
			wantSQL:  "name ILIKE $1",
			wantArgs: []interface{}{"%foo%"},
			wantErr:  false,
		},
		{
			name: "single filter with NOT LIKE operator",
			// Filter with NOT LIKE operator should use NOT ILIKE and wrap value with %.
			filter:   &Filter{Field: Field{Column: "name"}, Operator: Operator("NOT LIKE"), Value: FilterValue{Value: "bar"}},
			wantSQL:  "name NOT ILIKE $1",
			wantArgs: []interface{}{"%bar%"},
			wantErr:  false,
		},
		{
			name: "single filter with error in field",
			// Filter with an invalid field should return an error.
			filter:   &Filter{Field: Field{}},
			wantSQL:  "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "nil filter returns empty",
			// Nil filter should return empty SQL and empty arguments without error.
			filter:   nil,
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []interface{}{}
			idx := 1
			got, err := b.buildFilter(tt.filter, &args, &idx, true)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantSQL {
				t.Errorf("buildFilter() = %v, want %v", got, tt.wantSQL)
			}
			if !tt.wantErr && !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("buildFilter() args = %v, want %v", args, tt.wantArgs)
			}
		})
	}
}

// TestPostgresBuilder_buildFieldForFilter tests the buildFieldForFilter method for various field scenarios, including subqueries, table-qualified columns, and error cases.
func TestPostgresBuilder_buildFieldForFilter(t *testing.T) {
	b := newPostgresBuilder()
	tests := []struct {
		name    string
		field   Field
		wantSQL string
		wantErr bool
	}{
		{
			name: "subquery with alias",
			// Field with a subquery and alias should generate SQL with alias.
			field:   Field{SelectQuery: &SelectQuery{Raw: "SELECT 1"}, Alias: "foo"},
			wantSQL: "(SELECT 1) AS foo",
			wantErr: false,
		},
		{
			name: "subquery without alias",
			// Field with a subquery and no alias should generate SQL without alias.
			field:   Field{SelectQuery: &SelectQuery{Raw: "SELECT 2"}},
			wantSQL: "(SELECT 2)",
			wantErr: false,
		},
		{
			name: "subquery error",
			// Field with an invalid subquery should return an error.
			field:   Field{SelectQuery: &SelectQuery{}},
			wantSQL: "",
			wantErr: true,
		},
		{
			name: "table and column",
			// Field with both table and column should generate table-qualified SQL.
			field:   Field{Table: "users", Column: "id"},
			wantSQL: "users.id",
			wantErr: false,
		},
		{
			name: "column only",
			// Field with only a column should generate SQL with just the column name.
			field:   Field{Column: "name"},
			wantSQL: "name",
			wantErr: false,
		},
		{
			name: "invalid field (empty)",
			// Field with no table, column, or subquery should return an error.
			field:   Field{},
			wantSQL: "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := b.buildFieldForFilter(tt.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFieldForFilter() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildFieldForFilter() = %v, want %v", got, tt.wantSQL)
			}
		})
	}
}

// TestPostgresBuilder_buildFilterValue tests the buildFilterValue method for various filter value scenarios, including LIKE, IN, subqueries, columns, and error cases.
func TestPostgresBuilder_buildFilterValue(t *testing.T) {
	b := newPostgresBuilder()
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
			wantSQL:  "$1",
			wantArgs: []interface{}{"%foo%"},
			wantErr:  false,
		},
		{
			name:     "NOT LIKE operator delegates to buildFilterValueLike",
			op:       OperatorNotLike,
			value:    FilterValue{Value: "bar"},
			wantSQL:  "$1",
			wantArgs: []interface{}{"%bar%"},
			wantErr:  false,
		},
		{
			name:     "subquery value",
			op:       Operator("="),
			value:    FilterValue{SelectQuery: &SelectQuery{Raw: "SELECT 1"}},
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
			wantSQL:  "($1, $2, $3)",
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
			wantSQL:  "$1",
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
			// Prepare arguments and placeholder index for each test case.
			args := []interface{}{}
			idx := 1
			// Call buildFilterValue and capture the result and error.
			got, err := b.buildFilterValue(tt.op, tt.value, &args, &idx)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFilterValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildFilterValue() = %v, want %v", got, tt.wantSQL)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("buildFilterValue() args = %v, want %v", args, tt.wantArgs)
			}
		})
	}
}

// TestPostgresBuilder_buildFilterValueLike tests the buildFilterValueLike method for various LIKE filter value scenarios, including subqueries, columns, string values, and error cases.
func TestPostgresBuilder_buildFilterValueLike(t *testing.T) {
	b := newPostgresBuilder()
	tests := []struct {
		name     string
		value    FilterValue
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "subquery returns error",
			value:    FilterValue{SelectQuery: &SelectQuery{}},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
		{
			name:     "subquery returns SQL",
			value:    FilterValue{SelectQuery: &SelectQuery{Raw: "SELECT 1"}},
			wantSQL:  "($1)",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "table-qualified column",
			value:    FilterValue{Table: "users", Column: "id"},
			wantSQL:  "users.id",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "string value",
			value:    FilterValue{Value: "foo"},
			wantSQL:  "$1",
			wantArgs: []interface{}{"%foo%"},
			wantErr:  false,
		},
		{
			name:     "non-string value",
			value:    FilterValue{Value: 123},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
		{
			name:     "nil value and no subquery/column",
			value:    FilterValue{},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments and placeholder index for each test case.
			args := []interface{}{}
			idx := 1
			// Call buildFilterValueLike and capture the result and error.
			got, err := b.buildFilterValueLike(tt.value, &args, &idx)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFilterValueLike() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildFilterValueLike() = %v, want %v", got, tt.wantSQL)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("buildFilterValueLike() args = %v, want %v", args, tt.wantArgs)
			}
		})
	}
}

// TestPostgresBuilder_buildGroupBy tests the buildGroupBy method for various group by scenarios, including empty fields, single field, multiple fields, and error cases.
func TestPostgresBuilder_buildGroupBy(t *testing.T) {
	b := newPostgresBuilder()
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

// TestPostgresBuilder_buildJoins tests the buildJoins method for various join scenarios, including no joins, single join, multiple joins, and error cases.
func TestPostgresBuilder_buildJoins(t *testing.T) {
	b := newPostgresBuilder()
	// fakeFilter simulates the filter logic for join conditions, returning an error if the column is "err".
	fakeFilter := func(f *Filter, args *[]interface{}) (string, error) {
		if f != nil && f.Field.Column == "err" {
			return "", fmt.Errorf("filter error")
		}
		if f != nil {
			return f.Field.Column, nil
		}
		return "", nil
	}
	// fakeSelect simulates the select query logic for subqueries in joins, returning an error if the raw SQL is "err".
	fakeSelect := func(q *SelectQuery) (string, []interface{}, error) {
		if q == nil || q.Raw == "err" {
			return "", nil, fmt.Errorf("select error")
		}
		return q.Raw, nil, nil
	}
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
			name:    "error in filter",
			joins:   []Join{{Type: JoinType("INNER JOIN"), Table: Table{Name: "roles"}, Filter: Filter{Field: Field{Column: "err"}}}},
			wantSQL: "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments for each test case.
			args := []interface{}{}
			// Call buildJoins and capture the result and error.
			got, err := b.buildJoins(tt.joins, &args, fakeSelect, fakeFilter)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildJoins() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildJoins() = %v, want %v", got, tt.wantSQL)
			}
		})
	}
}

// TestPostgresBuilder_buildOrderBy tests the buildOrderBy method for various order by scenarios, including empty sorts, single and multiple sorts, and error cases.
func TestPostgresBuilder_buildOrderBy(t *testing.T) {
	b := newPostgresBuilder()
	tests := []struct {
		name    string
		sorts   []Sort
		wantSQL string
		wantErr bool
	}{
		{
			name:    "empty sorts",
			sorts:   []Sort{},
			wantSQL: "",
			wantErr: false,
		},
		{
			name:    "single sort, no direction",
			sorts:   []Sort{{Field: Field{Column: "id"}}},
			wantSQL: "id",
			wantErr: false,
		},
		{
			name:    "single sort, with direction",
			sorts:   []Sort{{Field: Field{Column: "name"}, Direction: "DESC"}},
			wantSQL: "name DESC",
			wantErr: false,
		},
		{
			name:    "multiple sorts",
			sorts:   []Sort{{Field: Field{Column: "id"}}, {Field: Field{Column: "name"}, Direction: "ASC"}},
			wantSQL: "id, name ASC",
			wantErr: false,
		},
		{
			name:    "error in field",
			sorts:   []Sort{{Field: Field{}}},
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
			// Normalize the SQL string to avoid issues with spacing and commas.
			norm := func(s string) string {
				s = strings.ReplaceAll(s, ", ", ",")
				s = strings.ReplaceAll(s, ",", ",")
				return strings.Join(strings.Fields(s), " ")
			}
			if norm(got) != norm(tt.wantSQL) {
				t.Errorf("buildOrderBy() = %v, want %v", got, tt.wantSQL)
			}
		})
	}
}

// TestPostgresBuilder_buildTable tests the buildTable method for various table scenarios, including plain tables, aliases, subqueries, and error cases.
func TestPostgresBuilder_buildTable(t *testing.T) {
	b := newPostgresBuilder()
	tests := []struct {
		name     string
		table    Table
		wantSQL  string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name:     "plain table name",
			table:    Table{Name: "users"},
			wantSQL:  "users",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "table with alias",
			table:    Table{Name: "users", Alias: "u"},
			wantSQL:  "users AS u",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "subquery with alias",
			table:    Table{SelectQuery: &SelectQuery{Raw: "SELECT 1"}, Alias: "sub"},
			wantSQL:  "(SELECT 1) AS sub",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "subquery without alias",
			table:    Table{SelectQuery: &SelectQuery{Raw: "SELECT 2"}},
			wantSQL:  "(SELECT 2)",
			wantArgs: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "error in subquery",
			table:    Table{SelectQuery: &SelectQuery{}},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
		{
			name:     "invalid table (empty)",
			table:    Table{},
			wantSQL:  "",
			wantArgs: []interface{}{},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare arguments for each test case.
			args := []interface{}{}
			// Call buildTable and capture the result and error.
			got, err := b.buildTable(tt.table, &args)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildTable() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantSQL {
				t.Errorf("buildTable() = %v, want %v", got, tt.wantSQL)
			}
			// Compare the arguments slice to ensure correct parameterization.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("buildTable() args = %v, want %v", args, tt.wantArgs)
			}
		})
	}
}

// TestPostgresBuilder_nextPlaceholder tests the nextPlaceholder method for generating PostgreSQL-style placeholders with incrementing indices.
func TestPostgresBuilder_nextPlaceholder(t *testing.T) {
	b := newPostgresBuilder()
	tests := []struct {
		name      string
		startIdx  int
		nextCalls int
		want      []string
	}{
		{
			name:      "single call",
			startIdx:  1,
			nextCalls: 1,
			want:      []string{"$1"},
		},
		{
			name:      "multiple calls",
			startIdx:  2,
			nextCalls: 3,
			want:      []string{"$2", "$3", "$4"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize the placeholder index for each test case.
			idx := tt.startIdx
			var got []string
			// Call nextPlaceholder repeatedly and collect the results.
			for i := 0; i < tt.nextCalls; i++ {
				got = append(got, b.nextPlaceholder(&idx))
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("nextPlaceholder() = %v, want %v", got, tt.want)
			}
		})
	}
}
