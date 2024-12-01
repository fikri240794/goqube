package goqube

import (
	"fmt"
	"testing"
)

func testUpdateQuery_UpdateQueryEquality(t *testing.T, expectation, actual *UpdateQuery) {
	if expectation == nil && actual == nil {
		t.Skip("expectation and actual is nil")
	}

	if expectation == nil && actual != nil {
		t.Errorf("expectation is nil, got %+v", actual)
	}

	if expectation != nil && actual == nil {
		t.Errorf("expectation is %+v, got nil", expectation)
	}

	if expectation.Table != actual.Table {
		t.Errorf("expected table is %s, got %s", expectation.Table, actual.Table)
	}

	if len(expectation.FieldsValue) != len(actual.FieldsValue) {
		t.Errorf("expected length of fields value is %d, got %d", len(expectation.FieldsValue), len(actual.FieldsValue))
	}

	if len(expectation.FieldsValue) > 0 {
		for field, value := range expectation.FieldsValue {
			if !deepEqual(value, actual.FieldsValue[field]) {
				t.Errorf("expected value is %v, got %v", value, actual.FieldsValue[field])
			}
		}
	}

	if !deepEqual(expectation.Filter, actual.Filter) {
		t.Errorf("expectation filter is %v, got %v", expectation.Filter, actual.Filter)
	}
}

func TestUpdateQuery_Update(t *testing.T) {
	var (
		expectation *UpdateQuery
		actual      *UpdateQuery
	)

	expectation = &UpdateQuery{
		Table:       "table1",
		FieldsValue: map[string]interface{}{},
	}
	actual = Update("table1")

	testUpdateQuery_UpdateQueryEquality(t, expectation, actual)
}

func TestUpdateQuery_Set(t *testing.T) {
	var (
		expectation *UpdateQuery
		actual      *UpdateQuery
	)

	expectation = &UpdateQuery{
		Table: "table1",
		FieldsValue: map[string]interface{}{
			"field1": "value1",
			"field2": 2,
		},
	}

	actual = Update("table1").
		Set("field1", "value1").
		Set("field2", 2)

	testUpdateQuery_UpdateQueryEquality(t, expectation, actual)
}

func TestUpdateQuery_Where(t *testing.T) {
	var (
		expectation *UpdateQuery
		actual      *UpdateQuery
	)

	expectation = &UpdateQuery{
		Table: "table1",
		FieldsValue: map[string]interface{}{
			"field1": "value1",
			"field2": 2,
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

	actual = Update("table1").
		Set("field1", "value1").
		Set("field2", 2).
		Where(
			NewFilter().
				SetLogic(LogicAnd).
				AddFilter(NewField("field1"), OperatorEqual, NewFilterValue("value1")),
		)

	testUpdateQuery_UpdateQueryEquality(t, expectation, actual)
}

func TestUpdateQuery_validate(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		UpdateQuery *UpdateQuery
		Expectation error
	} = []struct {
		Name        string
		Dialect     Dialect
		UpdateQuery *UpdateQuery
		Expectation error
	}{
		{
			Name:        "dialect is empty",
			Dialect:     "",
			UpdateQuery: &UpdateQuery{},
			Expectation: ErrDialectIsRequired,
		},
		{
			Name:        "table is empty",
			Dialect:     DialectPostgres,
			UpdateQuery: &UpdateQuery{},
			Expectation: ErrTableIsRequired,
		},
		{
			Name:    "fields and value is empty",
			Dialect: DialectPostgres,
			UpdateQuery: &UpdateQuery{
				Table:       "table1",
				FieldsValue: map[string]interface{}{},
			},
			Expectation: ErrFieldsIsRequired,
		},
		{
			Name:    "field is empty",
			Dialect: DialectPostgres,
			UpdateQuery: &UpdateQuery{
				Table: "table1",
				FieldsValue: map[string]interface{}{
					"": "field1",
				},
			},
			Expectation: ErrFieldIsRequired,
		},
		{
			Name:    "filter is empty",
			Dialect: DialectPostgres,
			UpdateQuery: &UpdateQuery{
				Table: "table1",
				FieldsValue: map[string]interface{}{
					"field1": "value1",
				},
			},
			Expectation: ErrFilterIsRequired,
		},
		{
			Name:    "update query is valid",
			Dialect: DialectPostgres,
			UpdateQuery: &UpdateQuery{
				Table: "table1",
				FieldsValue: map[string]interface{}{
					"field1": "value1",
				},
				Filter: &Filter{
					Logic: LogicAnd,
					Filters: []*Filter{
						{
							Field:    NewField("field1"),
							Operator: OperatorEqual,
							Value:    NewFilterValue("value1"),
						},
					},
				},
			},
			Expectation: nil,
		},
	}

	for i := 0; i < len(testCases); i++ {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual error = testCases[i].UpdateQuery.validate(testCases[i].Dialect)

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

func TestUpdateQuery_ToSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		UpdateQuery *UpdateQuery
		Dialect     Dialect
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		UpdateQuery *UpdateQuery
		Dialect     Dialect
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:        "table is empty",
			UpdateQuery: &UpdateQuery{},
			Dialect:     DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   ErrTableIsRequired,
			},
		},

		{
			Name: fmt.Sprintf("update with dialect %s with filter is not nil and filter to sql with args is error", DialectPostgres),
			UpdateQuery: &UpdateQuery{
				Table: "table1",
				FieldsValue: map[string]interface{}{
					"field1": "value1",
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
			Name: fmt.Sprintf("update with dialect %s with filter", DialectPostgres),
			UpdateQuery: &UpdateQuery{
				Table: "table1",
				FieldsValue: map[string]interface{}{
					"field1": "value1",
				},
				Filter: &Filter{
					Logic: LogicAnd,
					Filters: []*Filter{
						{
							Field:    NewField("field2"),
							Operator: OperatorEqual,
							Value:    NewFilterValue("value2"),
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
				Query: "update table1 set field1 = $1 where field2 = $2",
				Args:  []interface{}{"value1", "value2"},
				Err:   nil,
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var (
				actualQuery string
				actualArgs  []interface{}
				actualErr   error
			)

			actualQuery, actualArgs, actualErr = testCases[i].UpdateQuery.ToSQLWithArgs(testCases[i].Dialect)

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

			for j := 0; j < len(testCases[i].Expectation.Args); j++ {
				if !deepEqual(testCases[i].Expectation.Args[j], actualArgs[j]) {
					t.Errorf("expectation element of args is %v, got %v", testCases[i].Expectation.Args[j], actualArgs[j])
				}
			}
		})
	}
}
