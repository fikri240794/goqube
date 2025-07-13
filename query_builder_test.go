package goqube

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// TestDynamicQueryBuilder_buildFieldForFilter tests the buildFieldForFilter method for various field types and subquery scenarios.
// It verifies correct SQL generation and error handling for subqueries, qualified columns, and invalid fields.
func TestDynamicQueryBuilder_buildFieldForFilter(t *testing.T) {
	type args struct {
		f                Field
		buildSelectQuery func(*SelectQuery) (string, []interface{}, error)
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// Each test case checks a different field scenario, including subqueries, columns, and error propagation.
		{
			name: "subquery with alias",
			args: args{
				f: Field{
					SelectQuery: &SelectQuery{Raw: "SELECT 1"},
					Alias:       "sub",
				},
				buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) {
					return q.Raw, nil, nil
				},
			},
			want:    "(SELECT 1) AS sub",
			wantErr: false,
		},
		{
			name: "subquery without alias",
			args: args{
				f: Field{
					SelectQuery: &SelectQuery{Raw: "SELECT 2"},
				},
				buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) {
					return q.Raw, nil, nil
				},
			},
			want:    "(SELECT 2)",
			wantErr: false,
		},
		{
			name: "table and column",
			args: args{
				f:                Field{Table: "users", Column: "id"},
				buildSelectQuery: nil,
			},
			want:    "users.id",
			wantErr: false,
		},
		{
			name: "column only",
			args: args{
				f:                Field{Column: "name"},
				buildSelectQuery: nil,
			},
			want:    "name",
			wantErr: false,
		},
		{
			name: "invalid field (empty)",
			args: args{
				f:                Field{},
				buildSelectQuery: nil,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "subquery returns error",
			args: args{
				f: Field{SelectQuery: &SelectQuery{Raw: "SELECT 3"}},
				buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) {
					return "", nil, ErrInvalidField
				},
			},
			want:    "",
			wantErr: true,
		},
	}
	dqb := &dynamicQueryBuilder{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the buildFieldForFilter method with the provided arguments.
			got, err := dqb.buildFieldForFilter(tt.args.f, tt.args.buildSelectQuery)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFieldForFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.want {
				t.Errorf("buildFieldForFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildFields tests the buildFields method for various field types, subqueries, and aliases.
// It ensures correct SQL generation and argument collection for each field scenario, including error handling.
func TestDynamicQueryBuilder_buildFields(t *testing.T) {
	type args struct {
		fields           []Field
		args             *[]interface{}
		buildSelectQuery func(*SelectQuery) (string, []interface{}, error)
	}
	tests := []struct {
		name     string
		args     args
		want     string
		wantErr  bool
		wantArgs []interface{}
	}{
		// Each test case covers a different field scenario, such as subqueries, aliases, and error propagation.
		{
			name: "subquery with alias",
			args: args{
				fields: []Field{{SelectQuery: &SelectQuery{Raw: "SELECT 1"}, Alias: "sub"}},
				args:   &[]interface{}{},
				buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) {
					return q.Raw, []interface{}{42}, nil
				},
			},
			want:     "(SELECT 1) AS sub",
			wantErr:  false,
			wantArgs: []interface{}{42},
		},
		{
			name: "subquery without alias",
			args: args{
				fields: []Field{{SelectQuery: &SelectQuery{Raw: "SELECT 2"}}},
				args:   &[]interface{}{},
				buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) {
					return q.Raw, []interface{}{99}, nil
				},
			},
			want:     "(SELECT 2)",
			wantErr:  false,
			wantArgs: []interface{}{99},
		},
		{
			name: "table.column with alias",
			args: args{
				fields:           []Field{{Table: "users", Column: "id", Alias: "uid"}},
				args:             &[]interface{}{},
				buildSelectQuery: nil,
			},
			want:     "users.id AS uid",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
		{
			name: "table.column without alias",
			args: args{
				fields:           []Field{{Table: "users", Column: "name"}},
				args:             &[]interface{}{},
				buildSelectQuery: nil,
			},
			want:     "users.name",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
		{
			name: "column with alias",
			args: args{
				fields:           []Field{{Column: "email", Alias: "mail"}},
				args:             &[]interface{}{},
				buildSelectQuery: nil,
			},
			want:     "email AS mail",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
		{
			name: "column without alias",
			args: args{
				fields:           []Field{{Column: "age"}},
				args:             &[]interface{}{},
				buildSelectQuery: nil,
			},
			want:     "age",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
		{
			name: "invalid field (empty)",
			args: args{
				fields:           []Field{{}},
				args:             &[]interface{}{},
				buildSelectQuery: nil,
			},
			want:     "",
			wantErr:  true,
			wantArgs: []interface{}{},
		},
		{
			name: "subquery returns error",
			args: args{
				fields: []Field{{SelectQuery: &SelectQuery{Raw: "SELECT 3"}}},
				args:   &[]interface{}{},
				buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) {
					return "", nil, errors.New("fail")
				},
			},
			want:     "",
			wantErr:  true,
			wantArgs: []interface{}{},
		},
	}
	dqb := &dynamicQueryBuilder{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the buildFields method with the provided arguments and field types.
			got, err := dqb.buildFields(tt.args.fields, tt.args.args, tt.args.buildSelectQuery)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFields() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.want {
				t.Errorf("buildFields() = %v, want %v", got, tt.want)
			}
			// Ensure the collected arguments match the expected arguments.
			if !reflect.DeepEqual(*tt.args.args, tt.wantArgs) {
				t.Errorf("args = %v, want %v", *tt.args.args, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildFilter tests the buildFilter method for various filter scenarios, including nested groups and error propagation.
// It ensures correct SQL generation, argument collection, and normalization for different filter logic and edge cases.
func TestDynamicQueryBuilder_buildFilter(t *testing.T) {
	type args struct {
		f                *Filter
		args             *[]interface{}
		isRoot           bool
		buildSelectQuery func(*SelectQuery) (string, []interface{}, error)
	}
	tests := []struct {
		name     string
		args     args
		want     string
		wantErr  bool
		wantArgs []interface{}
	}{
		// Each test case covers a different filter scenario, such as nested groups, field errors, and normalization.
		{
			name: "nil filter",
			args: args{
				f:                nil,
				args:             &[]interface{}{},
				isRoot:           true,
				buildSelectQuery: nil,
			},
			want:     "",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
		{
			name: "simple field filter",
			args: args{
				f: &Filter{
					Field:    Field{Column: "age"},
					Operator: Operator("="),
					Value:    FilterValue{Value: 30},
				},
				args:             &[]interface{}{},
				isRoot:           true,
				buildSelectQuery: nil,
			},
			want:     "age = ?",
			wantErr:  false,
			wantArgs: []interface{}{30},
		},
		{
			name: "nested AND group",
			args: args{
				f: &Filter{
					Logic: "AND",
					Filters: []Filter{
						{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
						{Field: Field{Column: "b"}, Operator: Operator("<"), Value: FilterValue{Value: 2}},
					},
				},
				args:             &[]interface{}{},
				isRoot:           true,
				buildSelectQuery: nil,
			},
			want:     "a = ? AND b < ?",
			wantErr:  false,
			wantArgs: []interface{}{1, 2},
		},
		{
			name: "nested OR group (not root)",
			args: args{
				f: &Filter{
					Logic: "OR",
					Filters: []Filter{
						{Field: Field{Column: "x"}, Operator: Operator(">"), Value: FilterValue{Value: 10}},
						{Field: Field{Column: "y"}, Operator: Operator("<"), Value: FilterValue{Value: 5}},
					},
				},
				args:             &[]interface{}{},
				isRoot:           false,
				buildSelectQuery: nil,
			},
			want:     "(x > ? OR y < ?)",
			wantErr:  false,
			wantArgs: []interface{}{10, 5},
		},
		{
			name: "field error propagates",
			args: args{
				f: &Filter{
					Field:    Field{},
					Operator: Operator("="),
					Value:    FilterValue{Value: 1},
				},
				args:             &[]interface{}{},
				isRoot:           true,
				buildSelectQuery: nil,
			},
			want:     "",
			wantErr:  true,
			wantArgs: []interface{}{},
		},
		{
			name: "value error propagates",
			args: args{
				f: &Filter{
					Field:    Field{Column: "z"},
					Operator: Operator("IN"),
					Value:    FilterValue{Value: 123},
				},
				args:             &[]interface{}{},
				isRoot:           true,
				buildSelectQuery: nil,
			},
			want:     "",
			wantErr:  true,
			wantArgs: []interface{}{},
		},
		{
			name: "error in nested filter group propagates",
			args: args{
				f: &Filter{
					Logic: "AND",
					Filters: []Filter{
						{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
						{Field: Field{}, Operator: Operator("="), Value: FilterValue{Value: 2}},
					},
				},
				args:             &[]interface{}{},
				isRoot:           true,
				buildSelectQuery: nil,
			},
			want:     "",
			wantErr:  true,
			wantArgs: []interface{}{1},
		},
		{
			name: "normalization of double spaces in joined filter",
			args: args{
				f: &Filter{
					Logic: "AND",
					Filters: []Filter{
						{Field: Field{Column: "a"}, Operator: Operator("="), Value: FilterValue{Value: 1}},
						{Field: Field{Column: "b"}, Operator: Operator("="), Value: FilterValue{Value: 2}},
						{Field: Field{Column: "c"}, Operator: Operator("="), Value: FilterValue{Value: 3}},
						{Field: Field{Column: "d"}, Operator: Operator("="), Value: FilterValue{Value: 4}},
						{Field: Field{Column: "e"}, Operator: Operator("="), Value: FilterValue{Value: 5}},
					},
				},
				args:             &[]interface{}{},
				isRoot:           true,
				buildSelectQuery: nil,
			},
			want:     "a = ? AND b = ? AND c = ? AND d = ? AND e = ?",
			wantErr:  false,
			wantArgs: []interface{}{1, 2, 3, 4, 5},
		},
		{
			name: "trigger double space normalization with IS NULL operators",
			args: args{
				f: &Filter{
					Logic: "AND",
					Filters: []Filter{
						{Field: Field{Column: "name"}, Operator: Operator("IS NULL"), Value: FilterValue{}},
						{Field: Field{Column: "email"}, Operator: Operator("IS NOT NULL"), Value: FilterValue{}},
						{Field: Field{Column: "status"}, Operator: Operator("IS NULL"), Value: FilterValue{}},
					},
				},
				args:   &[]interface{}{},
				isRoot: true,
				buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) {
					return "SELECT * FROM users", nil, nil
				},
			},
			want:     "name IS NULL AND email IS NOT NULL AND status IS NULL",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
	}
	dqb := &dynamicQueryBuilder{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the buildFilter method with the provided filter and arguments.
			got, err := dqb.buildFilter(tt.args.f, tt.args.args, tt.args.isRoot, tt.args.buildSelectQuery)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.want {
				t.Errorf("buildFilter() = %v, want %v", got, tt.want)
			}
			// Ensure the collected arguments match the expected arguments.
			if tt.args.args != nil && !reflect.DeepEqual(*tt.args.args, tt.wantArgs) {
				t.Errorf("args = %v, want %v", *tt.args.args, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildFilterValue tests the buildFilterValue method for different operator and value scenarios.
// It verifies correct SQL placeholder generation and argument collection for columns, slices, arrays, and error cases.
func TestDynamicQueryBuilder_buildFilterValue(t *testing.T) {
	type args struct {
		op   Operator
		v    FilterValue
		args *[]interface{}
	}
	tests := []struct {
		name     string
		args     args
		want     string
		wantErr  bool
		wantArgs []interface{}
	}{
		// Each test case covers a different operator and value scenario, including slices, arrays, and error propagation.
		{
			name: "table and column",
			args: args{
				op:   Operator("="),
				v:    FilterValue{Table: "users", Column: "id"},
				args: &[]interface{}{},
			},
			want:     "users.id",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
		{
			name: "column only",
			args: args{
				op:   Operator("="),
				v:    FilterValue{Column: "name"},
				args: &[]interface{}{},
			},
			want:     "name",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
		{
			name: "is null operator",
			args: args{
				op:   OperatorIsNull,
				v:    FilterValue{},
				args: &[]interface{}{},
			},
			want:     "",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
		{
			name: "is not null operator",
			args: args{
				op:   OperatorIsNotNull,
				v:    FilterValue{},
				args: &[]interface{}{},
			},
			want:     "",
			wantErr:  false,
			wantArgs: []interface{}{},
		},
		{
			name: "in operator with slice",
			args: args{
				op:   OperatorIn,
				v:    FilterValue{Value: []int{1, 2, 3}},
				args: &[]interface{}{},
			},
			want:     "(?, ?, ?)",
			wantErr:  false,
			wantArgs: []interface{}{1, 2, 3},
		},
		{
			name: "not in operator with array",
			args: args{
				op:   OperatorNotIn,
				v:    FilterValue{Value: [2]int{4, 5}},
				args: &[]interface{}{},
			},
			want:     "(?, ?)",
			wantErr:  false,
			wantArgs: []interface{}{4, 5},
		},
		{
			name: "in operator with non-slice",
			args: args{
				op:   OperatorIn,
				v:    FilterValue{Value: 123},
				args: &[]interface{}{},
			},
			want:     "",
			wantErr:  true,
			wantArgs: []interface{}{},
		},
		{
			name: "in operator with empty slice",
			args: args{
				op:   OperatorIn,
				v:    FilterValue{Value: []int{}},
				args: &[]interface{}{},
			},
			want:     "",
			wantErr:  true,
			wantArgs: []interface{}{},
		},
		{
			name: "default case (single value)",
			args: args{
				op:   Operator("<"),
				v:    FilterValue{Value: 42},
				args: &[]interface{}{},
			},
			want:     "?",
			wantErr:  false,
			wantArgs: []interface{}{42},
		},
	}
	dqb := &dynamicQueryBuilder{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the buildFilterValue method with the provided operator and value.
			got, err := dqb.buildFilterValue(tt.args.op, tt.args.v, tt.args.args)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFilterValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL placeholder(s) with the expected value.
			if got != tt.want {
				t.Errorf("buildFilterValue() = %v, want %v", got, tt.want)
			}
			// Ensure the collected arguments match the expected arguments.
			if tt.args.args != nil && !reflect.DeepEqual(*tt.args.args, tt.wantArgs) {
				t.Errorf("args = %v, want %v", *tt.args.args, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildGroupBy tests the buildGroupBy method for various field combinations in GROUP BY clauses.
// It ensures correct SQL generation for single, multiple, and invalid fields, including error handling.
func TestDynamicQueryBuilder_buildGroupBy(t *testing.T) {
	tests := []struct {
		name    string
		input   []Field
		want    string
		wantErr bool
	}{
		// Each test case covers a different field scenario for GROUP BY, including error propagation.
		{
			name:    "table and column",
			input:   []Field{{Table: "users", Column: "id"}},
			want:    "users.id",
			wantErr: false,
		},
		{
			name:    "column only",
			input:   []Field{{Column: "name"}},
			want:    "name",
			wantErr: false,
		},
		{
			name:    "multiple fields",
			input:   []Field{{Table: "users", Column: "id"}, {Column: "name"}},
			want:    "users.id, name",
			wantErr: false,
		},
		{
			name:    "invalid field (empty)",
			input:   []Field{{}},
			want:    "",
			wantErr: true,
		},
	}
	dqb := &dynamicQueryBuilder{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the buildGroupBy method with the provided fields.
			got, err := dqb.buildGroupBy(tt.input)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildGroupBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.want {
				t.Errorf("buildGroupBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildJoins tests the buildJoins method for various join scenarios, including single, multiple, and error cases.
// It ensures correct SQL generation and error handling for different join types, table, and filter combinations.
func TestDynamicQueryBuilder_buildJoins(t *testing.T) {
	// Initialize a new dynamicQueryBuilder instance for testing.
	dqb := &dynamicQueryBuilder{}

	// Define test cases for different join scenarios.
	tests := []struct {
		name    string
		joins   []Join
		want    string
		wantErr bool
	}{
		{
			name:    "single join, valid table and filter",
			joins:   []Join{{Type: JoinType("inner"), Table: Table{Name: "users"}, Filter: Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}}},
			want:    "INNER users ON id = ?",
			wantErr: false,
		},
		{
			name: "multiple joins",
			joins: []Join{
				{Type: JoinType("left"), Table: Table{Name: "a"}, Filter: Filter{Field: Field{Column: "x"}, Operator: Operator("="), Value: FilterValue{Value: 2}}},
				{Type: JoinType("right"), Table: Table{Name: "b"}, Filter: Filter{Field: Field{Column: "y"}, Operator: Operator("<"), Value: FilterValue{Value: 3}}},
			},
			want:    "LEFT a ON x = ? RIGHT b ON y < ?",
			wantErr: false,
		},
		{
			name:    "table returns error",
			joins:   []Join{{Type: JoinType("inner"), Table: Table{}, Filter: Filter{Field: Field{Column: "id"}, Operator: Operator("="), Value: FilterValue{Value: 1}}}},
			want:    "",
			wantErr: true,
		},
		{
			name:    "filter returns error",
			joins:   []Join{{Type: JoinType("inner"), Table: Table{Name: "users"}, Filter: Filter{}}},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := &[]interface{}{}
			// Use stub functions for buildSelectQuery and buildFilter to simulate different error and success scenarios.
			got, err := dqb.buildJoins(tt.joins, args,
				func(q *SelectQuery) (string, []interface{}, error) { return "", nil, nil },
				func(f *Filter, args *[]interface{}) (string, error) {
					// Simulate error if filter fields are empty.
					if f.Field.Column == "" && f.Operator == "" && f.Value.Value == nil {
						return "", fmt.Errorf("filter error")
					}
					// Simulate error if table is invalid but value is not nil.
					if f.Field.Column == "" && f.Operator == "" && f.Value.Value != nil {
						return "", fmt.Errorf("table error")
					}
					// Return a formatted filter string for valid cases.
					return fmt.Sprintf("%s %s ?", f.Field.Column, f.Operator), nil
				},
			)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildJoins() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.want {
				t.Errorf("buildJoins() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildOrderBy tests the buildOrderBy method for various sorting scenarios, including single, multiple, and invalid fields.
// It ensures correct SQL generation and error handling for ORDER BY clauses with different field and direction combinations.
func TestDynamicQueryBuilder_buildOrderBy(t *testing.T) {
	// Create a new dynamicQueryBuilder instance for testing.
	dqb := &dynamicQueryBuilder{}

	// Define test cases for different ORDER BY scenarios.
	tests := []struct {
		name    string
		sorts   []Sort
		want    string
		wantErr bool
	}{
		{
			name:    "table and column",
			sorts:   []Sort{{Field: Field{Table: "users", Column: "id"}, Direction: "ASC"}},
			want:    "users.id ASC",
			wantErr: false,
		},
		{
			name:    "column only",
			sorts:   []Sort{{Field: Field{Column: "name"}, Direction: "DESC"}},
			want:    "name DESC",
			wantErr: false,
		},
		{
			name: "multiple sorts",
			sorts: []Sort{
				{Field: Field{Table: "users", Column: "id"}, Direction: "ASC"},
				{Field: Field{Column: "name"}, Direction: "DESC"},
			},
			want:    "users.id ASC, name DESC",
			wantErr: false,
		},
		{
			name:    "invalid field (empty)",
			sorts:   []Sort{{Field: Field{}, Direction: "ASC"}},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildOrderBy with the provided sorts and check the result.
			got, err := dqb.buildOrderBy(tt.sorts)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildOrderBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.want {
				t.Errorf("buildOrderBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildPlaceholdersAndArgs tests the buildPlaceholdersAndArgs method for various value and column scenarios.
// It ensures correct SQL placeholder generation and argument collection for multi-row inserts, single-row updates, and unsupported types.
func TestDynamicQueryBuilder_buildPlaceholdersAndArgs(t *testing.T) {
	// Create a new dynamicQueryBuilder instance for testing.
	dqb := &dynamicQueryBuilder{}

	// Define test cases for different value, column, and format scenarios.
	tests := []struct {
		name     string
		values   interface{}
		columns  []string
		format   string
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "multi-row insert, ? placeholder",
			values: []map[string]interface{}{
				{"id": 1, "name": "foo"},
				{"id": 2, "name": "bar"},
			},
			columns:  []string{"id", "name"},
			format:   "?",
			wantSQL:  "(?, ?), (?, ?)",
			wantArgs: []interface{}{1, "foo", 2, "bar"},
		},
		{
			name: "multi-row insert, indexed placeholder",
			values: []map[string]interface{}{
				{"id": 1, "name": "foo"},
				{"id": 2, "name": "bar"},
			},
			columns:  []string{"id", "name"},
			format:   "$%d",
			wantSQL:  "($1, $2), ($3, $4)",
			wantArgs: []interface{}{1, "foo", 2, "bar"},
		},
		{
			name:     "single-row update, ? placeholder",
			values:   map[string]interface{}{"id": 1, "name": "foo"},
			columns:  []string{"id", "name"},
			format:   "?",
			wantSQL:  "id = ?, name = ?",
			wantArgs: []interface{}{1, "foo"},
		},
		{
			name:     "single-row update, indexed placeholder",
			values:   map[string]interface{}{"id": 1, "name": "foo"},
			columns:  []string{"id", "name"},
			format:   "$%d",
			wantSQL:  "id = $1, name = $2",
			wantArgs: []interface{}{1, "foo"},
		},
		{
			name:     "default case (unsupported type)",
			values:   123,
			columns:  []string{"id"},
			format:   "?",
			wantSQL:  "",
			wantArgs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildPlaceholdersAndArgs with the provided values, columns, and format.
			gotSQL, gotArgs := dqb.buildPlaceholdersAndArgs(tt.values, tt.columns, tt.format)
			// Compare the generated SQL with the expected value.
			if gotSQL != tt.wantSQL {
				t.Errorf("buildPlaceholdersAndArgs() SQL = %v, want %v", gotSQL, tt.wantSQL)
			}
			// Ensure the collected arguments match the expected arguments.
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("buildPlaceholdersAndArgs() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildPlaceholdersAndArgsWithIndex tests the buildPlaceholdersAndArgsWithIndex method for indexed placeholders in SQL queries.
// It ensures correct SQL and argument generation for multi-row inserts, single-row updates, and unsupported types using a custom placeholder function.
func TestDynamicQueryBuilder_buildPlaceholdersAndArgsWithIndex(t *testing.T) {
	// Create a new dynamicQueryBuilder instance for testing.
	dqb := &dynamicQueryBuilder{}

	// Define test cases for different value, column, and indexed placeholder scenarios.
	tests := []struct {
		name            string
		values          interface{}
		columns         []string
		startIdx        int
		nextPlaceholder func(*int) string
		wantSQL         string
		wantArgs        []interface{}
	}{
		{
			name: "multi-row insert",
			values: []map[string]interface{}{
				{"id": 1, "name": "foo"},
				{"id": 2, "name": "bar"},
			},
			columns:         []string{"id", "name"},
			startIdx:        1,
			nextPlaceholder: func(idx *int) string { s := fmt.Sprintf("$%d", *idx); (*idx)++; return s },
			wantSQL:         "($1, $2), ($3, $4)",
			wantArgs:        []interface{}{1, "foo", 2, "bar"},
		},
		{
			name:            "single-row update",
			values:          map[string]interface{}{"id": 1, "name": "foo"},
			columns:         []string{"id", "name"},
			startIdx:        1,
			nextPlaceholder: func(idx *int) string { s := fmt.Sprintf("$%d", *idx); (*idx)++; return s },
			wantSQL:         "id = $1, name = $2",
			wantArgs:        []interface{}{1, "foo"},
		},
		{
			name:            "default case (unsupported type)",
			values:          123,
			columns:         []string{"id"},
			startIdx:        1,
			nextPlaceholder: func(idx *int) string { return "$1" },
			wantSQL:         "",
			wantArgs:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set the starting index for the placeholder function.
			idx := tt.startIdx
			// Call buildPlaceholdersAndArgsWithIndex with the provided values, columns, and placeholder function.
			gotSQL, gotArgs := dqb.buildPlaceholdersAndArgsWithIndex(tt.values, tt.columns, &idx, tt.nextPlaceholder)
			// Compare the generated SQL with the expected value.
			if gotSQL != tt.wantSQL {
				t.Errorf("buildPlaceholdersAndArgsWithIndex() SQL = %v, want %v", gotSQL, tt.wantSQL)
			}
			// Ensure the collected arguments match the expected arguments.
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("buildPlaceholdersAndArgsWithIndex() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildTable tests the buildTable method for various table scenarios, including subqueries, aliases, and error cases.
// It ensures correct SQL generation and argument collection for subqueries, plain tables, and error propagation.
func TestDynamicQueryBuilder_buildTable(t *testing.T) {
	// Create a new dynamicQueryBuilder instance for testing.
	dqb := &dynamicQueryBuilder{}

	// Define test cases for different table, alias, and subquery scenarios.
	tests := []struct {
		name             string
		table            Table
		args             *[]interface{}
		buildSelectQuery func(*SelectQuery) (string, []interface{}, error)
		want             string
		wantArgs         []interface{}
		wantErr          bool
	}{
		{
			name:             "subquery with alias",
			table:            Table{SelectQuery: &SelectQuery{Raw: "SELECT 1"}, Alias: "t1"},
			args:             &[]interface{}{},
			buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) { return q.Raw, []interface{}{42}, nil },
			want:             "(SELECT 1) AS t1",
			wantArgs:         []interface{}{42},
			wantErr:          false,
		},
		{
			name:             "subquery without alias",
			table:            Table{SelectQuery: &SelectQuery{Raw: "SELECT 2"}},
			args:             &[]interface{}{},
			buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) { return q.Raw, []interface{}{99}, nil },
			want:             "(SELECT 2)",
			wantArgs:         []interface{}{99},
			wantErr:          false,
		},
		{
			name:             "plain table with alias",
			table:            Table{Name: "users", Alias: "u"},
			args:             &[]interface{}{},
			buildSelectQuery: nil,
			want:             "users AS u",
			wantArgs:         []interface{}{},
			wantErr:          false,
		},
		{
			name:             "plain table without alias",
			table:            Table{Name: "users"},
			args:             &[]interface{}{},
			buildSelectQuery: nil,
			want:             "users",
			wantArgs:         []interface{}{},
			wantErr:          false,
		},
		{
			name:             "invalid table (empty)",
			table:            Table{},
			args:             &[]interface{}{},
			buildSelectQuery: nil,
			want:             "",
			wantArgs:         []interface{}{},
			wantErr:          true,
		},
		{
			name:             "subquery returns error",
			table:            Table{SelectQuery: &SelectQuery{Raw: "SELECT 3"}},
			args:             &[]interface{}{},
			buildSelectQuery: func(q *SelectQuery) (string, []interface{}, error) { return "", nil, fmt.Errorf("fail") },
			want:             "",
			wantArgs:         []interface{}{},
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Copy the args slice to avoid side effects between tests.
			args := *tt.args
			// Call buildTable with the provided table, args, and buildSelectQuery function.
			got, err := dqb.buildTable(tt.table, &args, tt.buildSelectQuery)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.want {
				t.Errorf("buildTable() = %v, want %v", got, tt.want)
			}
			// Ensure the collected arguments match the expected arguments.
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("buildTable() args = %v, want %v", args, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildDeleteQuery tests the buildDeleteQuery method for various delete scenarios, including table validation and filter application.
// It ensures correct SQL generation, argument handling, and error propagation for different filter and table cases.
func TestDynamicQueryBuilder_buildDeleteQuery(t *testing.T) {
	// Create a new dynamicQueryBuilder instance for testing.
	dqb := &dynamicQueryBuilder{}

	// Define test cases for different table and filter scenarios.
	tests := []struct {
		name        string
		table       string
		filter      *Filter
		args        *[]interface{}
		buildFilter func(*Filter, *[]interface{}) (string, error)
		want        string
		wantArgs    []interface{}
		wantErr     bool
	}{
		{
			name:        "valid table, no filter",
			table:       "users",
			filter:      nil,
			args:        &[]interface{}{},
			buildFilter: func(f *Filter, args *[]interface{}) (string, error) { return "", nil },
			want:        "DELETE FROM users",
			wantArgs:    []interface{}{},
			wantErr:     false,
		},
		{
			name:        "valid table, with filter",
			table:       "users",
			filter:      &Filter{},
			args:        &[]interface{}{1},
			buildFilter: func(f *Filter, args *[]interface{}) (string, error) { return "id = ?", nil },
			want:        "DELETE FROM users WHERE id = ?",
			wantArgs:    []interface{}{1},
			wantErr:     false,
		},
		{
			name:        "filter returns error",
			table:       "users",
			filter:      &Filter{},
			args:        &[]interface{}{},
			buildFilter: func(f *Filter, args *[]interface{}) (string, error) { return "", fmt.Errorf("fail") },
			want:        "",
			wantArgs:    nil,
			wantErr:     true,
		},
		{
			name:        "table is empty (error)",
			table:       "",
			filter:      nil,
			args:        &[]interface{}{},
			buildFilter: func(f *Filter, args *[]interface{}) (string, error) { return "", nil },
			want:        "",
			wantArgs:    nil,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Copy the args slice to avoid side effects between tests.
			args := *tt.args
			// Call buildDeleteQuery with the provided table, filter, args, and buildFilter function.
			got, gotArgs, err := dqb.buildDeleteQuery(tt.table, tt.filter, &args, tt.buildFilter)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.want {
				t.Errorf("buildDeleteQuery() = %v, want %v", got, tt.want)
			}
			// Ensure the collected arguments match the expected arguments.
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("buildDeleteQuery() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildInsertQuery tests the buildInsertQuery method for various insert scenarios, including placeholder formats and error cases.
// It ensures correct SQL and argument generation for nil queries, empty tables, empty values, and both simple and indexed placeholders.
func TestDynamicQueryBuilder_buildInsertQuery(t *testing.T) {
	// Create a new dynamicQueryBuilder instance for testing.
	dqb := &dynamicQueryBuilder{}

	// Define test cases for different insert query scenarios.
	tests := []struct {
		name            string
		q               *InsertQuery
		startIdx        int
		nextPlaceholder func(*int) string
		wantSQL         string
		wantArgs        []interface{}
		wantErr         bool
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
			name:            "simple placeholder (MySQL/SQLite)",
			q:               &InsertQuery{Table: "users", Values: []map[string]interface{}{{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}}},
			startIdx:        1,
			nextPlaceholder: nil,
			wantSQL:         "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)",
			wantArgs:        []interface{}{1, "foo", 2, "bar"},
			wantErr:         false,
		},
		{
			name:            "indexed placeholder (PostgreSQL/SQL Server)",
			q:               &InsertQuery{Table: "users", Values: []map[string]interface{}{{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}}},
			startIdx:        1,
			nextPlaceholder: func(idx *int) string { s := fmt.Sprintf("$%d", *idx); (*idx)++; return s },
			wantSQL:         "INSERT INTO users (id, name) VALUES ($1, $2), ($3, $4)",
			wantArgs:        []interface{}{1, "foo", 2, "bar"},
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildInsertQuery with the provided query, startIdx, and nextPlaceholder function.
			got, gotArgs, err := dqb.buildInsertQuery(tt.q, tt.startIdx, tt.nextPlaceholder)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildInsertQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.wantSQL {
				t.Errorf("buildInsertQuery() = %v, want %v", got, tt.wantSQL)
			}
			// Ensure the collected arguments match the expected arguments.
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("buildInsertQuery() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildUpdateQueryWithContinuousIndex tests the buildUpdateQueryWithContinuousIndex method for various update scenarios with indexed placeholders.
// It ensures correct SQL and argument generation for nil queries, empty tables, empty fields, valid updates, and error propagation in filter logic.
func TestDynamicQueryBuilder_buildUpdateQueryWithContinuousIndex(t *testing.T) {
	// Create a new dynamicQueryBuilder instance for testing.
	dqb := &dynamicQueryBuilder{}

	// Define test cases for different update query scenarios with continuous index placeholders.
	tests := []struct {
		name            string
		q               *UpdateQuery
		startIdx        int
		nextPlaceholder func(*int) string
		buildFilter     func(*Filter, *[]interface{}, *int, bool) (string, error)
		wantSQL         string
		wantArgs        []interface{}
		wantErr         bool
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
			name:            "valid update, no filter",
			q:               &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1, "name": "foo"}},
			startIdx:        1,
			nextPlaceholder: func(idx *int) string { s := fmt.Sprintf("$%d", *idx); (*idx)++; return s },
			buildFilter:     func(f *Filter, args *[]interface{}, idx *int, b bool) (string, error) { return "", nil },
			wantSQL:         "UPDATE users SET id = $1, name = $2",
			wantArgs:        []interface{}{1, "foo"},
			wantErr:         false,
		},
		{
			name:            "valid update, with filter",
			q:               &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1, "name": "foo"}, Filter: &Filter{}},
			startIdx:        1,
			nextPlaceholder: func(idx *int) string { s := fmt.Sprintf("$%d", *idx); (*idx)++; return s },
			buildFilter:     func(f *Filter, args *[]interface{}, idx *int, b bool) (string, error) { return "id = $3", nil },
			wantSQL:         "UPDATE users SET id = $1, name = $2 WHERE id = $3",
			wantArgs:        []interface{}{1, "foo"},
			wantErr:         false,
		},
		{
			name:            "filter returns error",
			q:               &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1}, Filter: &Filter{}},
			startIdx:        1,
			nextPlaceholder: func(idx *int) string { s := fmt.Sprintf("$%d", *idx); (*idx)++; return s },
			buildFilter:     func(f *Filter, args *[]interface{}, idx *int, b bool) (string, error) { return "", fmt.Errorf("fail") },
			wantSQL:         "",
			wantArgs:        nil,
			wantErr:         true,
		},
		{
			name:            "filter returns empty string",
			q:               &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1}, Filter: &Filter{}},
			startIdx:        1,
			nextPlaceholder: func(idx *int) string { s := fmt.Sprintf("$%d", *idx); (*idx)++; return s },
			buildFilter:     func(f *Filter, args *[]interface{}, idx *int, b bool) (string, error) { return "", nil },
			wantSQL:         "UPDATE users SET id = $1",
			wantArgs:        []interface{}{1},
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildUpdateQueryWithContinuousIndex with the provided query, startIdx, nextPlaceholder, and buildFilter function.
			got, gotArgs, err := dqb.buildUpdateQueryWithContinuousIndex(tt.q, tt.startIdx, tt.nextPlaceholder, tt.buildFilter)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildUpdateQueryWithContinuousIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.wantSQL {
				t.Errorf("buildUpdateQueryWithContinuousIndex() = %v, want %v", got, tt.wantSQL)
			}
			// Ensure the collected arguments match the expected arguments.
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("buildUpdateQueryWithContinuousIndex() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_buildUpdateQuery tests the buildUpdateQuery method for various update scenarios, including placeholder handling and filter logic.
// It ensures correct SQL and argument generation for nil queries, empty tables, empty fields, valid updates, and error propagation in filter and placeholder logic.
func TestDynamicQueryBuilder_buildUpdateQuery(t *testing.T) {
	// Create a new dynamicQueryBuilder instance for testing.
	dqb := &dynamicQueryBuilder{}

	// Define test cases for different update query scenarios.
	tests := []struct {
		name            string
		q               *UpdateQuery
		nextPlaceholder func(*int) string
		buildFilter     func(*Filter, *[]interface{}) (string, error)
		wantSQL         string
		wantArgs        []interface{}
		wantErr         bool
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
			name:            "valid update, no filter",
			q:               &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1, "name": "foo"}},
			nextPlaceholder: nil,
			buildFilter:     func(f *Filter, args *[]interface{}) (string, error) { return "", nil },
			wantSQL:         "UPDATE users SET id = ?, name = ?",
			wantArgs:        []interface{}{1, "foo"},
			wantErr:         false,
		},
		{
			name:            "valid update, with filter",
			q:               &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1, "name": "foo"}, Filter: &Filter{}},
			nextPlaceholder: nil,
			buildFilter:     func(f *Filter, args *[]interface{}) (string, error) { return "id = ?", nil },
			wantSQL:         "UPDATE users SET id = ?, name = ? WHERE id = ?",
			wantArgs:        []interface{}{1, "foo"},
			wantErr:         false,
		},
		{
			name:            "filter returns error",
			q:               &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1}, Filter: &Filter{}},
			nextPlaceholder: nil,
			buildFilter:     func(f *Filter, args *[]interface{}) (string, error) { return "", fmt.Errorf("fail") },
			wantSQL:         "",
			wantArgs:        nil,
			wantErr:         true,
		},
		{
			name:            "nextPlaceholder not nil (should error)",
			q:               &UpdateQuery{Table: "users", FieldsValue: map[string]interface{}{"id": 1}},
			nextPlaceholder: func(idx *int) string { return "$1" },
			buildFilter:     func(f *Filter, args *[]interface{}) (string, error) { return "", nil },
			wantSQL:         "",
			wantArgs:        nil,
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call buildUpdateQuery with the provided query, nextPlaceholder, and buildFilter function.
			got, gotArgs, err := dqb.buildUpdateQuery(tt.q, tt.nextPlaceholder, tt.buildFilter)
			// Check if the error state matches the expected result.
			if (err != nil) != tt.wantErr {
				t.Errorf("buildUpdateQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare the generated SQL with the expected value.
			if got != tt.wantSQL {
				t.Errorf("buildUpdateQuery() = %v, want %v", got, tt.wantSQL)
			}
			// Ensure the collected arguments match the expected arguments.
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("buildUpdateQuery() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

// TestDynamicQueryBuilder_nextPlaceholder tests the nextPlaceholder method for different placeholder formats and index handling.
// It ensures correct placeholder string generation and index incrementation for various SQL dialects.
func TestDynamicQueryBuilder_nextPlaceholder(t *testing.T) {
	// Define test cases for different placeholder formats and starting indices.
	tests := []struct {
		name     string
		format   string
		startIdx int
		want     string
		wantIdx  int
	}{
		{
			name:     "question mark format",
			format:   "?",
			startIdx: 1,
			want:     "?",
			wantIdx:  2,
		},
		{
			name:     "dollar format",
			format:   "$%d",
			startIdx: 3,
			want:     "$3",
			wantIdx:  4,
		},
		{
			name:     "at p format",
			format:   "@p%d",
			startIdx: 5,
			want:     "@p5",
			wantIdx:  6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a dynamicQueryBuilder with the specified placeholder format.
			dqb := &dynamicQueryBuilder{placeholderFormat: tt.format}
			idx := tt.startIdx
			// Call nextPlaceholder and check the returned placeholder and updated index.
			got := dqb.nextPlaceholder(&idx)
			if got != tt.want {
				t.Errorf("nextPlaceholder() = %v, want %v", got, tt.want)
			}
			if idx != tt.wantIdx {
				t.Errorf("nextPlaceholder() idx = %v, want %v", idx, tt.wantIdx)
			}
		})
	}
}
