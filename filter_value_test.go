package goqube

import "testing"

func testFilterValue_FilterValueEquality(t *testing.T, expectation, actual *FilterValue) {
	if expectation == nil && actual == nil {
		t.Skip("expectation and actual is nil")
	}

	if expectation == nil && actual != nil {
		t.Errorf("expectation is nil, got %+v", actual)
	}

	if expectation != nil && actual == nil {
		t.Errorf("expectation is %+v, got nil", expectation)
	}

	if !deepEqual(expectation.Value, actual.Value) {
		t.Errorf("expectation value is %+v, got %+v", expectation.Value, actual.Value)
	}

	if expectation.Table != actual.Table {
		t.Errorf("expectation table is %s, got %s", expectation.Table, actual.Table)
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
}

func TestFilterValue_NewFilterValue(t *testing.T) {
	var (
		expectation *FilterValue
		actual      *FilterValue
	)

	expectation = &FilterValue{
		Value: "value1",
	}

	actual = NewFilterValue("value1")

	testFilterValue_FilterValueEquality(t, expectation, actual)
}

func TestFilterValue_NewColumnFilterValue(t *testing.T) {
	var (
		expectation *FilterValue
		actual      *FilterValue
	)

	expectation = &FilterValue{
		Column: "field1",
	}

	actual = NewColumnFilterValue("field1")

	testFilterValue_FilterValueEquality(t, expectation, actual)
}

func TestFilterValue_NewSelectQueryFilterValue(t *testing.T) {
	var (
		expectation *FilterValue
		actual      *FilterValue
	)

	expectation = &FilterValue{
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
	}

	actual = NewSelectQueryFilterValue(
		Select(NewField("field1")).
			From(NewTable("table1")),
	)

	testFilterValue_FilterValueEquality(t, expectation, actual)
}

func TestFilterValue_FromTable(t *testing.T) {
	var (
		expectation *FilterValue
		actual      *FilterValue
	)

	expectation = &FilterValue{
		Table:  "table1",
		Column: "field1",
	}

	actual = NewColumnFilterValue("field1").
		FromTable("table1")

	testFilterValue_FilterValueEquality(t, expectation, actual)
}

func TestFilterValue_validate(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		FilterValue *FilterValue
		Expectation error
	} = []struct {
		Name        string
		Dialect     Dialect
		FilterValue *FilterValue
		Expectation error
	}{
		{
			Name:        "dialect is empty",
			Dialect:     "",
			FilterValue: &FilterValue{},
			Expectation: ErrDialectIsRequired,
		},
		{
			Name:    "filter value is valid",
			Dialect: DialectPostgres,
			FilterValue: &FilterValue{
				Value: "value1",
			},
			Expectation: nil,
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual error = testCases[i].FilterValue.validate(testCases[i].Dialect)

			if testCases[i].Expectation != nil && actual == nil {
				t.Error("expectation error is not nil, got nil")
			}

			if testCases[i].Expectation == nil && actual != nil {
				t.Error("expectation error is nil, got not nil")
			}

			if testCases[i].Expectation != nil && actual != nil && testCases[i].Expectation.Error() != actual.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Error(), actual.Error())
			}
		})
	}
}

func TestFilterValue_ToSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		FilterValue *FilterValue
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Dialect     Dialect
		FilterValue *FilterValue
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:        "dialect is empty",
			Dialect:     "",
			FilterValue: &FilterValue{},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrDialectIsRequired,
			},
		},
		{
			Name:    "select query is not nil and to sql with args with alias is error",
			Dialect: DialectPostgres,
			FilterValue: &FilterValue{
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
			Name:    "select query is not nil",
			Dialect: DialectPostgres,
			FilterValue: &FilterValue{
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
		{
			Name:    "select query is nil and column is not empty",
			Dialect: DialectPostgres,
			FilterValue: &FilterValue{
				Column: "field1",
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name:    "select query is nil and column is not empty and table is not empty",
			Dialect: DialectPostgres,
			FilterValue: &FilterValue{
				Table:  "table1",
				Column: "field1",
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
		{
			Name:    "select query is nil",
			Dialect: DialectPostgres,
			FilterValue: &FilterValue{
				Value: "value1",
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  []interface{}{"value1"},
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

			actualQuery, actualArgs, actualErr = testCases[i].FilterValue.ToSQLWithArgs(testCases[i].Dialect, []interface{}{})

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
