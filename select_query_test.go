package goqube

import (
	"fmt"
	"testing"
)

func testSelectQuery_SelectQueryEquality(t *testing.T, expectation, actual *SelectQuery) {
	if expectation == nil && actual == nil {
		t.Skip("expectation and actual is nil")
	}

	if expectation == nil && actual != nil {
		t.Errorf("expectation is nil, got %+v", actual)
	}

	if expectation != nil && actual == nil {
		t.Errorf("expectation is %+v, got nil", expectation)
	}

	if len(expectation.Fields) != len(actual.Fields) {
		t.Errorf("expectation length of fields is %d, got %d", len(expectation.Fields), len(actual.Fields))
	} else {
		for i := range expectation.Fields {
			if !deepEqual(expectation.Fields[i], actual.Fields[i]) {
				t.Errorf("expectation element of fields is %+v, got %+v", expectation.Fields[i], actual.Fields[i])
			}
		}
	}

	if expectation.Table != nil && actual.Table == nil {
		t.Errorf("expectation table is %+v, got nil", expectation.Table)
	}
	if expectation.Table == nil && actual.Table != nil {
		t.Errorf("expectation table is nil, got %+v", actual.Table)
	}
	if !deepEqual(expectation.Table, actual.Table) {
		t.Errorf("expectation table is %+v, got %+v", expectation.Table, actual.Table)
	}

	if len(expectation.Joins) != len(actual.Joins) {
		t.Errorf("expectation length of joins is %d, got %d", len(expectation.Joins), len(actual.Joins))
	} else {
		for i := range expectation.Joins {
			if !deepEqual(expectation.Joins[i], actual.Joins[i]) {
				t.Errorf("expectation element of joins is %+v, got %+v", expectation.Joins[i], actual.Joins[i])
			}
		}
	}

	if expectation.Filter != nil && actual.Filter == nil {
		t.Errorf("expectation filter is %+v, got nil", expectation.Filter)
	}
	if expectation.Filter == nil && actual.Filter != nil {
		t.Errorf("expectation filter is nil, got %+v", actual.Filter)
	}
	if !deepEqual(expectation.Filter, actual.Filter) {
		t.Errorf("expectation table is %+v, got %+v", expectation.Filter, actual.Filter)
	}

	if len(expectation.GroupByFields) != len(actual.GroupByFields) {
		t.Errorf("expectation length of group by fields is %d, got %d", len(expectation.GroupByFields), len(actual.GroupByFields))
	} else {
		for i := range expectation.GroupByFields {
			if !deepEqual(expectation.GroupByFields[i], actual.GroupByFields[i]) {
				t.Errorf("expectation element of group by fields is %+v, got %+v", expectation.GroupByFields[i], actual.GroupByFields[i])
			}
		}
	}

	if len(expectation.Sorts) != len(actual.Sorts) {
		t.Errorf("expectation length of sorts is %d, got %d", len(expectation.Sorts), len(actual.Sorts))
	} else {
		for i := range expectation.Sorts {
			if !deepEqual(expectation.Sorts[i], actual.Sorts[i]) {
				t.Errorf("expectation element of sorts is %+v, got %+v", expectation.Sorts[i], actual.Sorts[i])
			}
		}
	}

	if expectation.Take != actual.Take {
		t.Errorf("expectation take is %d, got %d", expectation.Take, actual.Take)
	}

	if expectation.Skip != actual.Skip {
		t.Errorf("expectation skip is %d, got %d", expectation.Skip, actual.Skip)
	}

	if expectation.Alias != actual.Alias {
		t.Errorf("expectation alias is %s, got %s", expectation.Alias, actual.Alias)
	}
}

func TestSelectQuery_Select(t *testing.T) {
	var (
		expectation *SelectQuery
		actual      *SelectQuery
	)

	expectation = &SelectQuery{
		Fields: []*Field{
			{
				Column: "field1",
			},
			{
				Column: "field2",
			},
			{
				Column: "field3",
			},
		},
	}

	actual = Select(NewField("field1"), NewField("field2"), NewField("field3"))

	testSelectQuery_SelectQueryEquality(t, expectation, actual)
}

func TestSelectQuery_From(t *testing.T) {
	var (
		expectation *SelectQuery
		actual      *SelectQuery
	)

	expectation = &SelectQuery{
		Fields: []*Field{
			{
				Column: "field1",
			},
			{
				Column: "field2",
			},
			{
				Column: "field3",
			},
		},
		Table: &Table{
			Name: "table1",
		},
	}

	actual = Select(NewField("field1"), NewField("field2"), NewField("field3")).
		From(NewTable("table1"))

	testSelectQuery_SelectQueryEquality(t, expectation, actual)
}

func TestSelectQuery_Join(t *testing.T) {
	var (
		expectation *SelectQuery
		actual      *SelectQuery
	)

	expectation = &SelectQuery{
		Fields: []*Field{
			{
				Column: "field1",
			},
			{
				Column: "field2",
			},
			{
				Column: "field3",
			},
		},
		Table: &Table{
			Name: "table1",
		},
		Joins: []*Join{
			{
				Type: InnerJoinType,
				Table: &Table{
					Name: "table2",
				},
				Filter: &Filter{
					Logic: LogicAnd,
					Filters: []*Filter{
						{
							Field: &Field{
								Table:  "table1",
								Column: "field1",
							},
							Operator: OperatorEqual,
							Value: &FilterValue{
								Table:  "table2",
								Column: "field1",
							},
						},
					},
				},
			},
		},
	}

	actual = Select(NewField("field1"), NewField("field2"), NewField("field3")).
		From(NewTable("table1")).
		Join(
			InnerJoin(NewTable("table2")).
				On(
					NewFilter().
						SetLogic(LogicAnd).
						AddFilter(
							NewField("field1").
								FromTable("table1"),
							OperatorEqual,
							NewColumnFilterValue("field1").
								FromTable("table2"),
						),
				),
		)

	testSelectQuery_SelectQueryEquality(t, expectation, actual)
}

func TestSelectQuery_Where(t *testing.T) {
	var (
		expectation *SelectQuery
		actual      *SelectQuery
	)

	expectation = &SelectQuery{
		Fields: []*Field{
			{
				Column: "field1",
			},
			{
				Column: "field2",
			},
			{
				Column: "field3",
			},
		},
		Table: &Table{
			Name: "table1",
		},
		Filter: &Filter{
			Logic: LogicAnd,
			Filters: []*Filter{
				{
					Field: &Field{
						Column: "field1",
					},
					Operator: OperatorEqual,
					Value: &FilterValue{
						Value: "value1",
					},
				},
			},
		},
	}

	actual = Select(NewField("field1"), NewField("field2"), NewField("field3")).
		From(NewTable("table1")).
		Where(
			NewFilter().
				SetLogic(LogicAnd).
				AddFilter(NewField("field1"), OperatorEqual, NewFilterValue("value1")),
		)

	testSelectQuery_SelectQueryEquality(t, expectation, actual)
}

func TestSelectQuery_GroupBy(t *testing.T) {
	var (
		expectation *SelectQuery
		actual      *SelectQuery
	)

	expectation = &SelectQuery{
		Fields: []*Field{
			{
				Column: "field1",
			},
			{
				Column: "field2",
			},
			{
				Column: "field3",
			},
		},
		Table: &Table{
			Name: "table1",
		},
		GroupByFields: []*Field{
			{
				Column: "field1",
			},
		},
	}

	actual = Select(NewField("field1"), NewField("field2"), NewField("field3")).
		From(NewTable("table1")).
		GroupBy(NewField("field1"))

	testSelectQuery_SelectQueryEquality(t, expectation, actual)
}

func TestSelectQuery_OrderBy(t *testing.T) {
	var (
		expectation *SelectQuery
		actual      *SelectQuery
	)

	expectation = &SelectQuery{
		Fields: []*Field{
			{
				Column: "field1",
			},
			{
				Column: "field2",
			},
			{
				Column: "field3",
			},
		},
		Table: &Table{
			Name: "table1",
		},
		Sorts: []*Sort{
			{
				Field: &Field{
					Column: "field1",
				},
				Direction: SortDirectionDescending,
			},
			{
				Field: &Field{
					Column: "field2",
				},
				Direction: SortDirectionAscending,
			},
		},
	}

	actual = Select(NewField("field1"), NewField("field2"), NewField("field3")).
		From(NewTable("table1")).
		OrderBy(
			NewSort(NewField("field1"), SortDirectionDescending),
			NewSort(NewField("field2"), SortDirectionAscending),
		)

	testSelectQuery_SelectQueryEquality(t, expectation, actual)
}

func TestSelectQuery_Limit(t *testing.T) {
	var (
		expectation *SelectQuery
		actual      *SelectQuery
	)

	expectation = &SelectQuery{
		Fields: []*Field{
			{
				Column: "field1",
			},
			{
				Column: "field2",
			},
			{
				Column: "field3",
			},
		},
		Table: &Table{
			Name: "table1",
		},
		Take: 10,
	}

	actual = Select(NewField("field1"), NewField("field2"), NewField("field3")).
		From(NewTable("table1")).
		Limit(10)

	testSelectQuery_SelectQueryEquality(t, expectation, actual)
}

func TestSelectQuery_Offset(t *testing.T) {
	var (
		expectation *SelectQuery
		actual      *SelectQuery
	)

	expectation = &SelectQuery{
		Fields: []*Field{
			{
				Column: "field1",
			},
			{
				Column: "field2",
			},
			{
				Column: "field3",
			},
		},
		Table: &Table{
			Name: "table1",
		},
		Skip: 10,
	}

	actual = Select(NewField("field1"), NewField("field2"), NewField("field3")).
		From(NewTable("table1")).
		Offset(10)

	testSelectQuery_SelectQueryEquality(t, expectation, actual)
}

func TestSelectQuery_As(t *testing.T) {
	var (
		expectation *SelectQuery
		actual      *SelectQuery
	)

	expectation = &SelectQuery{
		Fields: []*Field{
			{
				Column: "field1",
			},
			{
				Column: "field2",
			},
			{
				Column: "field3",
			},
		},
		Table: &Table{
			Name: "table1",
		},
		Alias: "alias1",
	}

	actual = Select(NewField("field1"), NewField("field2"), NewField("field3")).
		From(NewTable("table1")).
		As("alias1")

	testSelectQuery_SelectQueryEquality(t, expectation, actual)
}

func TestSelectQuery_validate(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		SelectQuery *SelectQuery
		Expectation error
	} = []struct {
		Name        string
		Dialect     Dialect
		SelectQuery *SelectQuery
		Expectation error
	}{
		{
			Name:        "dialect is empty",
			Dialect:     "",
			SelectQuery: &SelectQuery{},
			Expectation: ErrDialectIsRequired,
		},
		{
			Name:        "fields is empty",
			Dialect:     DialectPostgres,
			SelectQuery: &SelectQuery{},
			Expectation: ErrFieldsIsRequired,
		},
		{
			Name:    "fields element is nil",
			Dialect: DialectPostgres,
			SelectQuery: &SelectQuery{
				Fields: []*Field{nil},
			},
			Expectation: ErrFieldIsNil,
		},
		{
			Name:    "table is nil",
			Dialect: DialectPostgres,
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
			},
			Expectation: ErrTableIsRequired,
		},
		{
			Name:    "select query is valid",
			Dialect: DialectPostgres,
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
			Expectation: nil,
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual error = testCases[i].SelectQuery.validate(testCases[i].Dialect)

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

func TestSelectQuery_ToSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		SelectQuery *SelectQuery
		Dialect     Dialect
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		SelectQuery *SelectQuery
		Dialect     Dialect
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:        "fields is empty",
			SelectQuery: &SelectQuery{},
			Dialect:     DialectPostgres,
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
			Name: "fields is not empty and fields element is not nil and fields element to sql with args with alias is error",
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{},
				},
				Table: &Table{},
			},
			Dialect: DialectPostgres,
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
			Name: "fields is not empty and fields element is not nil",
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
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1",
				Args:  nil,
				Err:   nil,
			},
		},
		{
			Name: "table is not nil and table is to sql with args with alias is error",
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{},
			},
			Dialect: DialectPostgres,
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
			Name: "joins item is nil",
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Joins: []*Join{nil},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with invalid joins item", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Joins: []*Join{
					{
						Type: InnerJoinType,
						Table: &Table{
							Name: "table2",
						},
					},
				},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrFilterIsRequired,
			},
		},
		{
			Name: "joins is valid",
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Joins: []*Join{
					{
						Type: InnerJoinType,
						Table: &Table{
							Name: "table2",
						},
						Filter: &Filter{
							Logic: LogicAnd,
							Filters: []*Filter{
								{
									Field: &Field{
										Table:  "table1",
										Column: "field1",
									},
									Operator: OperatorEqual,
									Value: &FilterValue{
										Table:  "table2",
										Column: "field1",
									},
								},
							},
						},
					},
				},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1 inner join table2 on table1.field1 = table2.field1",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with invalid filter", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Filter: &Filter{
					Logic:   LogicAnd,
					Filters: []*Filter{},
				},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrFiltersIsRequired,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Filter: &Filter{
					Logic: LogicAnd,
					Filters: []*Filter{
						{
							Field: &Field{
								Column: "field1",
							},
							Operator: OperatorEqual,
							Value: &FilterValue{
								Value: "value1",
							},
						},
					},
				},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1 where field1 = $1",
				Args:  []interface{}{"value1"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with element group by fields is nil", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				GroupByFields: []*Field{nil},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with element group by fields is invalid", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				GroupByFields: []*Field{
					{},
				},
			},
			Dialect: DialectPostgres,
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
			Name: fmt.Sprintf("dialect %s with group by", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				GroupByFields: []*Field{
					{
						Column: "field1",
					},
					{
						Table:  "table2",
						Column: "field1",
					},
				},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1 group by field1, table2.field1",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with element sorts is nil", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Sorts: []*Sort{
					nil,
				},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with invalid sort", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Sorts: []*Sort{
					{
						Direction: SortDirectionDescending,
					},
				},
				Take: 100,
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrFieldIsRequired,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with sort", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Sorts: []*Sort{
					{
						Field: &Field{
							Column: "field1",
						},
						Direction: SortDirectionDescending,
					},
				},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1 order by field1 desc",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with take", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Take: 10,
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1 limit $1",
				Args:  []interface{}{10},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with skip", DialectPostgres),
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Skip: 10,
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "select field1 from table1 offset $1",
				Args:  []interface{}{10},
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

			actualQuery, actualArgs, actualErr = testCases[i].SelectQuery.ToSQLWithArgs(testCases[i].Dialect, []interface{}{})

			if testCases[i].Expectation.Err != nil && actualErr == nil {
				t.Error("expectation error is not nil, got nil")
			}

			if testCases[i].Expectation.Err == nil && actualErr != nil {
				t.Error("expectation error is nil, got not nil")
			}

			if testCases[i].Expectation.Err != nil && actualErr != nil && testCases[i].Expectation.Err.Error() != actualErr.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Err.Error(), actualErr.Error())
			}

			if testCases[i].Expectation.Query != actualQuery {
				t.Errorf("expectation query is %s, got %s", testCases[i].Expectation.Query, actualQuery)
			}

			if len(testCases[i].Expectation.Args) != len(actualArgs) {
				t.Errorf("expectation length of args is %d, got %d", len(testCases[i].Expectation.Args), len(actualArgs))
			}

			for j := range testCases[i].Expectation.Args {
				if !deepEqual(testCases[i].Expectation.Args[j], actualArgs[j]) {
					t.Errorf("expectation element of args is %+v, got %+v", testCases[i].Expectation.Args[j], actualArgs[j])
				}
			}
		})
	}
}

func TestSelectQuery_ToSQLWithArgsWithAlias(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		SelectQuery *SelectQuery
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Dialect     Dialect
		SelectQuery *SelectQuery
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:        "to sql with args is error",
			Dialect:     DialectPostgres,
			SelectQuery: &SelectQuery{},
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
			Name:    "alias is not empty",
			Dialect: DialectPostgres,
			SelectQuery: &SelectQuery{
				Fields: []*Field{
					{
						Column: "field1",
					},
				},
				Table: &Table{
					Name: "table1",
				},
				Alias: "alias1",
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "(select field1 from table1) as alias1",
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

			actualQuery, actualArgs, actualErr = testCases[i].SelectQuery.ToSQLWithArgsWithAlias(testCases[i].Dialect, []interface{}{})

			if testCases[i].Expectation.Err != nil && actualErr == nil {
				t.Error("expectation error is not nil, got nil")
			}

			if testCases[i].Expectation.Err == nil && actualErr != nil {
				t.Error("expectation error is nil, got not nil")
			}

			if testCases[i].Expectation.Err != nil && actualErr != nil && testCases[i].Expectation.Err.Error() != actualErr.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Err.Error(), actualErr.Error())
			}

			if testCases[i].Expectation.Query != actualQuery {
				t.Errorf("expectation query is %s, got %s", testCases[i].Expectation.Query, actualQuery)
			}

			if len(testCases[i].Expectation.Args) != len(actualArgs) {
				t.Errorf("expectation length of args is %d, got %d", len(testCases[i].Expectation.Args), len(actualArgs))
			}

			for j := range testCases[i].Expectation.Args {
				if !deepEqual(testCases[i].Expectation.Args[j], actualArgs[j]) {
					t.Errorf("expectation element of args is %+v, got %+v", testCases[i].Expectation.Args[j], actualArgs[j])
				}
			}
		})
	}
}
