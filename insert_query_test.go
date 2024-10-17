package goqube

import (
	"fmt"
	"testing"
)

func testInsertQuery_InsertQueryEquality(t *testing.T, expectation, actual *InsertQuery) {
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
		t.Errorf("expectation table is %s, got %s", expectation.Table, actual.Table)
	}

	if len(expectation.FieldsValues) != len(actual.FieldsValues) {
		t.Errorf("expectation length of field values is %d, got %d", len(expectation.FieldsValues), len(actual.FieldsValues))
	}

	for field, values := range expectation.FieldsValues {
		if len(actual.FieldsValues[field]) != len(values) {
			t.Errorf("expectation length of field values is %d, got %d", len(expectation.FieldsValues), len(actual.FieldsValues))
		}
		for i := range values {
			if !deepEqual(values[i], actual.FieldsValues[field][i]) {
				t.Errorf("expectation element of values is %v, got %v", values[i], actual.FieldsValues[field][i])
			}
		}
	}
}

func TestInsertQuery_Insert(t *testing.T) {
	var (
		expectation *InsertQuery
		actual      *InsertQuery
	)

	expectation = &InsertQuery{
		FieldsValues: map[string][]interface{}{},
	}
	actual = Insert()

	testInsertQuery_InsertQueryEquality(t, expectation, actual)
}

func TestInsertQuery_Into(t *testing.T) {
	var (
		expectation *InsertQuery
		actual      *InsertQuery
	)

	expectation = &InsertQuery{
		FieldsValues: map[string][]interface{}{},
		Table:        "table1",
	}
	actual = Insert().
		Into("table1")

	testInsertQuery_InsertQueryEquality(t, expectation, actual)
}

func TestInsertQuery_Value(t *testing.T) {
	var (
		expectation *InsertQuery
		actual      *InsertQuery
	)

	expectation = &InsertQuery{
		FieldsValues: map[string][]interface{}{
			"field1": {"value1", "value2", "value3"},
			"field2": {1, 2, 3},
			"field3": {true, false, true},
		},
	}
	actual = Insert().
		Value("field1", "value1").
		Value("field2", 1).
		Value("field3", true).
		Value("field1", "value2").
		Value("field2", 2).
		Value("field3", false).
		Value("field1", "value3").
		Value("field2", 3).
		Value("field3", true)

	testInsertQuery_InsertQueryEquality(t, expectation, actual)
}

func TestInsertQuery_getColumnsAndRowsValues(t *testing.T) {
	var testCases []struct {
		Name                 string
		InsertQuery          *InsertQuery
		ExpectationColumns   []string
		ExpectationRowValues [][]interface{}
	} = []struct {
		Name                 string
		InsertQuery          *InsertQuery
		ExpectationColumns   []string
		ExpectationRowValues [][]interface{}
	}{
		{
			Name: "invalid row count",
			InsertQuery: &InsertQuery{
				Table: "table1",
				FieldsValues: map[string][]interface{}{
					"field1": {"value1", "value2", "value3", "value4"},
					"field2": {1, 2, 3},
					"field3": {true, false, true},
				},
			},
			ExpectationColumns: []string{"field1", "field2", "field3"},
			ExpectationRowValues: [][]interface{}{
				{"value1", 1, true},
				{"value2", 2, false},
				{"value3", 3, true},
				{"value4"},
			},
		},
		{
			Name: "insert query is valid",
			InsertQuery: &InsertQuery{
				Table: "table1",
				FieldsValues: map[string][]interface{}{
					"field1": {"value1", "value2", "value3"},
					"field2": {1, 2, 3},
					"field3": {true, false, true},
				},
			},
			ExpectationColumns: []string{"field1", "field2", "field3"},
			ExpectationRowValues: [][]interface{}{
				{"value1", 1, true},
				{"value2", 2, false},
				{"value3", 3, true},
			},
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var (
				actualColumns   []string
				actualRowValues [][]interface{}
			)

			actualColumns, actualRowValues = testCases[i].InsertQuery.getColumnsAndRowsValues()

			t.Logf("%d %v", i, actualRowValues)

			if len(testCases[i].ExpectationColumns) != len(actualColumns) {
				t.Errorf("expectation length of column is %d, got %d", len(testCases[i].ExpectationColumns), len(actualColumns))
			}

			for j := range testCases[i].ExpectationColumns {
				if testCases[i].ExpectationColumns[j] != actualColumns[j] {
					t.Errorf("expectation column is %s, got %s", testCases[i].ExpectationColumns[j], actualColumns[j])
				}
			}

			if len(testCases[i].ExpectationRowValues) != len(actualRowValues) {
				t.Errorf("expectation length of row is %d, got %d", len(testCases[i].ExpectationRowValues), len(actualRowValues))
			}

			for j := range testCases[i].ExpectationRowValues {
				if len(testCases[i].ExpectationRowValues[j]) != len(actualRowValues[j]) {
					t.Errorf("expectation length of values is %d, got %d", len(testCases[i].ExpectationRowValues[j]), len(actualRowValues[j]))
				}

				for k := range testCases[i].ExpectationRowValues[j] {
					if !deepEqual(testCases[i].ExpectationRowValues[j][k], actualRowValues[j][k]) {
						t.Errorf("expectation value is %v, got %v", testCases[i].ExpectationRowValues[j][k], actualRowValues[j][k])
					}
				}
			}
		})
	}
}

func TestInsertQuery_validate(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		InsertQuery *InsertQuery
		Expectation error
	} = []struct {
		Name        string
		Dialect     Dialect
		InsertQuery *InsertQuery
		Expectation error
	}{
		{
			Name:        "dialect is empty",
			Dialect:     "",
			InsertQuery: &InsertQuery{},
			Expectation: ErrDialectIsRequired,
		},
		{
			Name:        "table is empty",
			Dialect:     DialectPostgres,
			InsertQuery: &InsertQuery{},
			Expectation: ErrTableIsRequired,
		},
		{
			Name:    "fields is empty",
			Dialect: DialectPostgres,
			InsertQuery: &InsertQuery{
				Table:        "table1",
				FieldsValues: map[string][]interface{}{},
			},
			Expectation: ErrFieldsIsRequired,
		},
		{
			Name:    "field is empty",
			Dialect: DialectPostgres,
			InsertQuery: &InsertQuery{
				Table: "table1",
				FieldsValues: map[string][]interface{}{
					"": {"value1"},
				},
			},
			Expectation: ErrFieldIsRequired,
		},
		{
			Name:    "values is empty",
			Dialect: DialectPostgres,
			InsertQuery: &InsertQuery{
				Table: "table1",
				FieldsValues: map[string][]interface{}{
					"field1": {},
				},
			},
			Expectation: ErrValuesIsRequired,
		},
		{
			Name:    "value length is not equal to fields length",
			Dialect: DialectPostgres,
			InsertQuery: &InsertQuery{
				Table: "table1",
				FieldsValues: map[string][]interface{}{
					"field1": {"value1", "value2"},
					"field2": {1},
				},
			},
			Expectation: ErrValueLengthIsNotEqualToFieldsLength,
		},
		{
			Name:    "insert query is valid",
			Dialect: DialectPostgres,
			InsertQuery: &InsertQuery{
				Table: "table1",
				FieldsValues: map[string][]interface{}{
					"field1": {"value1", "value2"},
					"field2": {1, 2},
				},
			},
			Expectation: nil,
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actualErr error = testCases[i].InsertQuery.validate(testCases[i].Dialect)

			if testCases[i].Expectation != nil && actualErr == nil {
				t.Error("expectation error is not nil, got nil")
			}

			if testCases[i].Expectation == nil && actualErr != nil {
				t.Error("expectation error is nil, got not nil")
			}

			if testCases[i].Expectation != nil && actualErr != nil && testCases[i].Expectation.Error() != actualErr.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Error(), actualErr.Error())
			}
		})
	}
}

func TestInsertQuery_ToSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		InsertQuery *InsertQuery
		Dialect     Dialect
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		InsertQuery *InsertQuery
		Dialect     Dialect
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:        "table is empty",
			InsertQuery: &InsertQuery{},
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
			Name: fmt.Sprintf("insert query with dialect %s", DialectMySQL),
			InsertQuery: &InsertQuery{
				Table: "table1",
				FieldsValues: map[string][]interface{}{
					"field1": {"value1", "value2"},
					"field2": {1, 2},
				},
			},
			Dialect: DialectMySQL,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "insert into table1(field1, field2) values (?, ?), (?, ?)",
				Args:  []interface{}{"value1", 1, "value2", 2},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("insert query with dialect %s", DialectPostgres),
			InsertQuery: &InsertQuery{
				Table: "table1",
				FieldsValues: map[string][]interface{}{
					"field1": {"value1", "value2"},
					"field2": {1, 2},
				},
			},
			Dialect: DialectPostgres,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "insert into table1(field1, field2) values ($1, $2), ($3, $4)",
				Args:  []interface{}{"value1", 1, "value2", 2},
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

			actualQuery, actualArgs, actualErr = testCases[i].InsertQuery.ToSQLWithArgs(testCases[i].Dialect)

			if testCases[i].Expectation.Err != nil && actualErr == nil {
				t.Error("expectation error is not nil, got nil")
			}

			if testCases[i].Expectation.Err == nil && actualErr != nil {
				t.Error("expectation error is nil, got not nil")
			}

			if testCases[i].Expectation.Err != nil && actualErr != nil && testCases[i].Expectation.Err.Error() != actualErr.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Err.Error(), actualErr.Error())
			}

			if testCases[i].Expectation.Err == nil && actualErr == nil {
				if testCases[i].Expectation.Query != actualQuery {
					t.Errorf("expectation query is %s, got %s", testCases[i].Expectation.Query, actualQuery)
				}

				if len(testCases[i].Expectation.Args) != len(actualArgs) {
					t.Errorf("expectation length of args is %d, got %d", len(testCases[i].Expectation.Args), len(actualArgs))
				}

				for j := range testCases[i].Expectation.Args {
					if !deepEqual(testCases[i].Expectation.Args[j], actualArgs[j]) {
						t.Errorf("expectation element of args is %v, got %v", testCases[i].Expectation.Args[j], actualArgs[j])
					}
				}
			}
		})
	}
}
