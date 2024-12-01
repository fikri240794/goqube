package goqube

import "testing"

func testJoin_JoinEquality(t *testing.T, expectation, actual *Join) {
	if expectation == nil && actual == nil {
		t.Skip("expectation and actual is nil")
	}

	if expectation == nil && actual != nil {
		t.Errorf("expectation is nil, got %+v", actual)
	}

	if expectation != nil && actual == nil {
		t.Errorf("expectation is %+v, got nil", expectation)
	}

	if expectation.Type != actual.Type {
		t.Errorf("expectation join type is %s, got %s", expectation.Type, actual.Type)
	}

	if expectation.Table == nil && actual.Table != nil {
		t.Errorf("expectation table is nil, got %+v", actual.Table)
	}

	if expectation.Table != nil && actual.Table == nil {
		t.Errorf("expectation table is %+v, got nil", expectation.Table)
	}

	if expectation.Table != nil && actual.Table != nil && !deepEqual(*expectation.Table, *actual.Table) {
		t.Errorf("expectation table is %+v, got %+v", expectation.Table, actual.Table)
	}

	if expectation.Filter == nil && actual.Filter != nil {
		t.Errorf("expectation filter is nil, got %+v", actual.Filter)
	}

	if expectation.Filter != nil && actual.Filter == nil {
		t.Errorf("expectation filter is %+v, got nil", expectation.Filter)
	}

	if expectation.Filter != nil && actual.Filter != nil && !deepEqual(*expectation.Filter, *actual.Filter) {
		t.Errorf("expectation filter is %+v, got %+v", expectation.Filter, actual.Filter)
	}
}

func TestJoin_InnerJoin(t *testing.T) {
	var (
		expectation *Join
		actual      *Join
	)

	expectation = &Join{
		Type: InnerJoinType,
		Table: &Table{
			Name: "table2",
		},
	}

	actual = InnerJoin(NewTable("table2"))

	testJoin_JoinEquality(t, expectation, actual)
}

func TestJoin_LeftJoin(t *testing.T) {
	var (
		expectation *Join
		actual      *Join
	)

	expectation = &Join{
		Type: LeftJoinType,
		Table: &Table{
			Name: "table2",
		},
	}

	actual = LeftJoin(NewTable("table2"))

	testJoin_JoinEquality(t, expectation, actual)
}

func TestJoin_RightJoin(t *testing.T) {
	var (
		expectation *Join
		actual      *Join
	)

	expectation = &Join{
		Type: RightJoinType,
		Table: &Table{
			Name: "table2",
		},
	}

	actual = RightJoin(NewTable("table2"))

	testJoin_JoinEquality(t, expectation, actual)
}

func TestJoin_FullJoin(t *testing.T) {
	var (
		expectation *Join
		actual      *Join
	)

	expectation = &Join{
		Type: FullJoinType,
		Table: &Table{
			Name: "table2",
		},
	}

	actual = FullJoin(NewTable("table2"))

	testJoin_JoinEquality(t, expectation, actual)
}

func TestJoin_On(t *testing.T) {
	var (
		expectation *Join
		actual      *Join
	)

	expectation = &Join{
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
	}

	actual = InnerJoin(NewTable("table2")).
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
		)

	testJoin_JoinEquality(t, expectation, actual)
}

func TestJoin_vaidate(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		Join        *Join
		Expectation error
	} = []struct {
		Name        string
		Dialect     Dialect
		Join        *Join
		Expectation error
	}{
		{
			Name:        "dialect is empty",
			Dialect:     "",
			Join:        &Join{},
			Expectation: ErrDialectIsRequired,
		},
		{
			Name:        "join type is empty",
			Dialect:     DialectPostgres,
			Join:        &Join{},
			Expectation: ErrJoinTypeIsRequired,
		},
		{
			Name:    "table is nil",
			Dialect: DialectPostgres,
			Join: &Join{
				Type: InnerJoinType,
			},
			Expectation: ErrTableIsRequired,
		},
		{
			Name:    "filter is nil",
			Dialect: DialectPostgres,
			Join: &Join{
				Type: InnerJoinType,
				Table: &Table{
					Name: "table2",
				},
			},
			Expectation: ErrFilterIsRequired,
		},
		{
			Name:    "join is valid",
			Dialect: DialectPostgres,
			Join: &Join{
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
			Expectation: nil,
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual error = testCases[i].Join.validate(testCases[i].Dialect)

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

func TestJoin_ToSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		Join        *Join
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Dialect     Dialect
		Join        *Join
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:    "dialect is empty",
			Dialect: "",
			Join:    &Join{},
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
			Name:    "table to sql with args with alias error",
			Dialect: DialectPostgres,
			Join: &Join{
				Type:   InnerJoinType,
				Table:  &Table{},
				Filter: &Filter{},
			},
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
			Name:    "filter to sql with args error",
			Dialect: DialectPostgres,
			Join: &Join{
				Type: InnerJoinType,
				Table: &Table{
					Name: "table2",
				},
				Filter: &Filter{
					Logic:   LogicAnd,
					Filters: []*Filter{},
				},
			},
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
			Name:    "join is valid",
			Dialect: DialectPostgres,
			Join: &Join{
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
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "inner join table2 on table1.field1 = table2.field1",
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

			actualQuery, actualArgs, actualErr = testCases[i].Join.ToSQLWithArgs(testCases[i].Dialect, []interface{}{})

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
