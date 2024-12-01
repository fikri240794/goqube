package goqube

import "testing"

func testTable_TableEquality(t *testing.T, expectation, actual *Table) {
	if expectation == nil && actual == nil {
		t.Skip("expectation and actual is nil")
	}

	if expectation == nil && actual != nil {
		t.Errorf("expectation is nil, got %+v", actual)
	}

	if expectation != nil && actual == nil {
		t.Errorf("expectation is %+v, got nil", expectation)
	}

	if expectation.Name != actual.Name {
		t.Errorf("expectation table name is %s, got %s", expectation.Name, actual.Name)
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

	if expectation.Alias != actual.Alias {
		t.Errorf("expectation operator is %s, got %s", expectation.Alias, actual.Alias)
	}
}

func TestTable_NewTable(t *testing.T) {
	testTable_TableEquality(t, &Table{Name: "table1"}, NewTable("table1"))
}

func TestTable_NewSelectQueryTable(t *testing.T) {
	testTable_TableEquality(
		t,
		&Table{
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
		NewSelectQueryTable(
			Select(NewField("field1")).
				From(NewTable("table1")),
		),
	)
}

func TestTable_As(t *testing.T) {
	testTable_TableEquality(
		t,
		&Table{
			Name:  "table1",
			Alias: "alias1",
		},
		NewTable("table1").
			As("alias1"),
	)
}

func TestTable_validate(t *testing.T) {
	var testCases []struct {
		Name        string
		Table       *Table
		Dialect     Dialect
		Expectation error
	} = []struct {
		Name        string
		Table       *Table
		Dialect     Dialect
		Expectation error
	}{
		{
			Name:        "dialect is empty",
			Table:       &Table{},
			Dialect:     "",
			Expectation: ErrDialectIsRequired,
		},
		{
			Name: "name is not empty and select query is not nil",
			Table: &Table{
				Name:        "table1",
				SelectQuery: &SelectQuery{},
			},
			Dialect:     DialectPostgres,
			Expectation: ErrConflictTableNameAndTableSelectQuery,
		},
		{
			Name:        "name is empty and select query is nil",
			Table:       &Table{},
			Dialect:     DialectPostgres,
			Expectation: ErrNameIsRequired,
		},
		{
			Name: "alias is empty and select query is not nil",
			Table: &Table{
				SelectQuery: &SelectQuery{},
			},
			Dialect:     DialectPostgres,
			Expectation: ErrAliasIsRequired,
		},
		{
			Name: "table is valid",
			Table: &Table{
				Name: "table1",
			},
			Dialect:     DialectPostgres,
			Expectation: nil,
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual error = testCases[i].Table.validate(testCases[i].Dialect)

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

func TestTable_ToSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		Table       *Table
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Table       *Table
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:  "table is invalid",
			Table: &Table{},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrNameIsRequired,
			},
		},
		{
			Name: "name is not empty and select query is nil",
			Table: &Table{
				Name: "table1",
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "table1",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: "name is empty and select query is not nil and to sql with args with alias is error",
			Table: &Table{
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
			Name: "name is empty and select query is not nil",
			Table: &Table{
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
				Query: "(select field1 from table1)",
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

			actualQuery, actualArgs, actualErr = testCases[i].Table.ToSQLWithArgs(DialectPostgres, []interface{}{})

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

func TestTable_ToSQLWithArgsWithAlias(t *testing.T) {
	var testCases []struct {
		Name        string
		Table       *Table
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Table       *Table
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:  "to sql with args is error",
			Table: &Table{},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrNameIsRequired,
			},
		},
		{
			Name: "alias is not empty",
			Table: &Table{
				Name:  "table1",
				Alias: "alias1",
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "table1 as alias1",
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

			actualQuery, actualArgs, actualErr = testCases[i].Table.ToSQLWithArgsWithAlias(DialectPostgres, []interface{}{})

			if testCases[i].Expectation.Query != actualQuery {
				t.Errorf("expetation query is %s, got %s", testCases[i].Expectation.Query, actualQuery)
			}

			if len(testCases[i].Expectation.Args) != len(actualArgs) {
				t.Errorf("expectation args length is %d, got %d", len(testCases[i].Expectation.Args), len(actualArgs))
			}

			if len(testCases[i].Expectation.Args) == len(actualArgs) {
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
