package goqube

import "testing"

func testField_FieldEquality(t *testing.T, expectation, actual *Field) {
	if expectation == nil && actual == nil {
		t.Skip("expectation and actual is nil")
	}

	if expectation == nil && actual != nil {
		t.Errorf("expectation is nil, got %+v", actual)
	}

	if expectation != nil && actual == nil {
		t.Errorf("expectation is %+v, got nil", expectation)
	}

	if expectation.Column != actual.Column {
		t.Errorf("expectation column is %s, got %s", expectation.Column, actual.Column)
	}

	if expectation.SelectQuery == nil && actual.SelectQuery != nil {
		t.Errorf("expectation select query is nil, got %+v", actual.SelectQuery)
	}

	if expectation.SelectQuery != nil && actual.SelectQuery == nil {
		t.Errorf("expectation select query is %+v, got nil", expectation.SelectQuery)
	}

	if expectation.SelectQuery != nil && actual.SelectQuery != nil && !deepEqual(*expectation.SelectQuery, *actual.SelectQuery) {
		t.Errorf("expectation select query is %+v, got %+v", expectation.SelectQuery, actual.SelectQuery)
	}

	if expectation.Table != actual.Table {
		t.Errorf("expectation field is %s, got %s", expectation.Table, actual.Table)
	}

	if expectation.Alias != actual.Alias {
		t.Errorf("expectation operator is %s, got %s", expectation.Alias, actual.Alias)
	}
}

func TestField_NewField(t *testing.T) {
	testField_FieldEquality(t, &Field{Column: "field1"}, NewField("field1"))
}

func TestField_NewSelectQueryField(t *testing.T) {
	testField_FieldEquality(
		t,
		&Field{
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
			},
		},
		NewSelectQueryField(
			Select(NewField("field1")).
				From(NewTable("table1")),
		),
	)
}

func TestField_FromTable(t *testing.T) {
	testField_FieldEquality(t, &Field{Column: "field1", Table: "table1"}, NewField("field1").FromTable("table1"))
}

func TestField_As(t *testing.T) {
	testField_FieldEquality(t, &Field{Column: "field1", Alias: "alias1"}, NewField("field1").As("alias1"))
}

func TestField_validate(t *testing.T) {
	var testCases []struct {
		Name        string
		Field       *Field
		Dialect     Dialect
		Expectation error
	} = []struct {
		Name        string
		Field       *Field
		Dialect     Dialect
		Expectation error
	}{
		{
			Name:        "dialect is empty",
			Field:       &Field{},
			Dialect:     "",
			Expectation: ErrDialectIsRequired,
		},
		{
			Name:        "column is empty and select query is nil",
			Field:       &Field{},
			Dialect:     DialectPostgres,
			Expectation: ErrColumnIsRequired,
		},
		{
			Name: "column is not empty and select query is not nil",
			Field: &Field{
				Column:      "field1",
				SelectQuery: &SelectQuery{},
			},
			Dialect:     DialectPostgres,
			Expectation: ErrConflictFieldColumnAndFieldSelectQuery,
		},
		{
			Name: "alias is empty and select query is not nil",
			Field: &Field{
				SelectQuery: &SelectQuery{},
			},
			Dialect:     DialectPostgres,
			Expectation: ErrAliasIsRequired,
		},
		{
			Name: "field is valid",
			Field: &Field{
				Column: "field1",
			},
			Dialect:     DialectPostgres,
			Expectation: nil,
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual error = testCases[i].Field.validate(testCases[i].Dialect)

			if testCases[i].Expectation == nil && actual != nil {
				t.Errorf("expectation error is nil, got %s", actual.Error())
			}

			if testCases[i].Expectation != nil && actual == nil {
				t.Errorf("expectation error is %s, got nil", testCases[i].Expectation.Error())
			}

			if testCases[i].Expectation != nil && actual != nil && testCases[i].Expectation.Error() != actual.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Error(), actual.Error())
			}
		})
	}
}

func TestField_ToSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		Field       *Field
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Field       *Field
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:  "field is invalid",
			Field: &Field{},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrColumnIsRequired,
			},
		},
		{
			Name: "select query is not nil and to sql with args with alias is error",
			Field: &Field{
				Alias:       "alias1",
				SelectQuery: &SelectQuery{},
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrFieldsIsRequired,
			},
		},
		{
			Name: "select query is not nil",
			Field: &Field{
				Alias: "alias 1",
				SelectQuery: &SelectQuery{
					Fields: []*Field{
						{
							Column: "field1",
						},
					},
					Table: &Table{
						Name: "table1",
					},
				},
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "(select field1 from table1)",
				Args:  nil,
				Err:   nil,
			},
		},
		{
			Name: "table is not empty and select query is nil",
			Field: &Field{
				Column: "field1",
				Table:  "table1",
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "table1.field1",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var (
				actualQuery string
				actualArgs  []interface{}
				actualErr   error
			)

			actualQuery, actualArgs, actualErr = testCases[i].Field.ToSQLWithArgs(DialectPostgres, []interface{}{})

			if testCases[i].Expectation.Query != actualQuery {
				t.Errorf("expectation query is %s, got %s", testCases[i].Expectation.Query, actualQuery)
			}

			if len(testCases[i].Expectation.Args) != len(actualArgs) {
				t.Errorf("expectation args length is %d, got %d", len(testCases[i].Expectation.Args), len(actualArgs))
			} else {
				for j := range testCases[i].Expectation.Args {
					if !deepEqual(testCases[i].Expectation.Args[j], actualArgs[j]) {
						t.Errorf("expectation args element is %+v, got %+v", testCases[i].Expectation.Args[j], actualArgs[j])
					}
				}
			}

			if testCases[i].Expectation.Err == nil && actualErr != nil {
				t.Errorf("expectation error is nil, got %s", actualErr.Error())
			}
			if testCases[i].Expectation.Err != nil && actualErr == nil {
				t.Errorf("expectation error is %s, got nil", testCases[i].Expectation.Err.Error())
			}
			if testCases[i].Expectation.Err != nil && actualErr != nil && testCases[i].Expectation.Err.Error() != actualErr.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Err.Error(), actualErr.Error())
			}
		})
	}
}

func TestField_ToSQLWithArgsWithAlias(t *testing.T) {
	var testCases []struct {
		Name        string
		Field       *Field
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Field       *Field
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:  "to sql with args is error",
			Field: &Field{},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrColumnIsRequired,
			},
		},
		{
			Name: "alias is not empty and select query is nil",
			Field: &Field{
				Column: "field1",
				Alias:  "alias1",
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 as alias1",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: "alias is not empty and select query is not nil",
			Field: &Field{
				Alias: "alias1",
				SelectQuery: &SelectQuery{
					Fields: []*Field{
						{
							Column: "field1",
						},
					},
					Table: &Table{
						Name: "table1",
					},
				},
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "(select field1 from table1) as alias1",
			},
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var (
				actualQuery string
				actualArgs  []interface{}
				actualErr   error
			)

			actualQuery, actualArgs, actualErr = testCases[i].Field.ToSQLWithArgsWithAlias(DialectPostgres, []interface{}{})

			if testCases[i].Expectation.Query != actualQuery {
				t.Errorf("expetation query is %s, got %s", testCases[i].Expectation.Query, actualQuery)
			}

			if len(testCases[i].Expectation.Args) != len(actualArgs) {
				t.Errorf("expectation args length is %d, got %d", len(testCases[i].Expectation.Args), len(actualArgs))
			} else {
				for j := range testCases[i].Expectation.Args {
					if !deepEqual(testCases[i].Expectation.Args[j], actualArgs[j]) {
						t.Errorf("expectation args element is %+v, got %+v", testCases[i].Expectation.Args[j], actualArgs[j])
					}
				}
			}

			if testCases[i].Expectation.Err == nil && actualErr != nil {
				t.Errorf("expectation error is nil, got %s", actualErr.Error())
			}
			if testCases[i].Expectation.Err != nil && actualErr == nil {
				t.Errorf("expectation error is %s, got nil", testCases[i].Expectation.Err.Error())
			}
			if testCases[i].Expectation.Err != nil && actualErr != nil && testCases[i].Expectation.Err.Error() != actualErr.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Err.Error(), actualErr.Error())
			}
		})
	}
}
