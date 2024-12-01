package goqube

import (
	"testing"
)

func testSort_SortEquality(t *testing.T, expectation, actual *Sort) {
	if expectation == nil && actual == nil {
		t.Skip("expectation and actual is nil")
	}

	if expectation == nil && actual != nil {
		t.Errorf("expectation is nil, got %+v", actual)
	}

	if expectation != nil && actual == nil {
		t.Errorf("expectation is %+v, got nil", expectation)
	}

	if expectation.Field == nil && actual.Field != nil {
		t.Errorf("expectation field is nil, got %+v", actual.Field)
	}

	if expectation.Field != nil && actual.Field == nil {
		t.Errorf("expectation field is %+v, got nil", expectation.Field)
	}

	if expectation.Field != nil && actual.Field != nil && !deepEqual(expectation.Field, actual.Field) {
		t.Errorf("expectation field is %+v, got %+v", expectation.Field, actual.Field)
	}

	if expectation.Direction != actual.Direction {
		t.Errorf("expectation direction is %s, got %s", expectation.Direction, actual.Direction)
	}
}

func TestSort_NewSort(t *testing.T) {
	var (
		expectation *Sort
		actual      *Sort
	)

	expectation = &Sort{
		Field: &Field{
			Column: "field1",
		},
		Direction: SortDirectionAscending,
	}

	actual = NewSort(NewField("field1"), SortDirectionAscending)

	testSort_SortEquality(t, expectation, actual)
}

func TestSort_validate(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		Sort        *Sort
		Expectation error
	} = []struct {
		Name        string
		Dialect     Dialect
		Sort        *Sort
		Expectation error
	}{
		{
			Name:        "dialect is empty",
			Dialect:     "",
			Sort:        &Sort{},
			Expectation: ErrDialectIsRequired,
		},
		{
			Name:        "field is nil",
			Dialect:     DialectPostgres,
			Sort:        &Sort{},
			Expectation: ErrFieldIsRequired,
		},
		{
			Name:    "sort is valid",
			Dialect: DialectPostgres,
			Sort: &Sort{
				Field: &Field{
					Column: "field1",
				},
				Direction: SortDirectionDescending,
			},
			Expectation: nil,
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual error = testCases[i].Sort.validate(testCases[i].Dialect)

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

func TestSort_ToSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		Sort        *Sort
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Dialect     Dialect
		Sort        *Sort
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name:    "dialect is empty",
			Dialect: "",
			Sort:    &Sort{},
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
			Name:    "field is nil",
			Dialect: DialectPostgres,
			Sort:    &Sort{},
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
			Name:    "field is invalid",
			Dialect: DialectPostgres,
			Sort: &Sort{
				Field: &Field{},
			},
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
			Name:    "default direction",
			Dialect: DialectPostgres,
			Sort: &Sort{
				Field: &Field{
					Column: "field1",
				},
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 asc",
				Err:   nil,
			},
		},
		{
			Name:    "sort with direction",
			Dialect: DialectPostgres,
			Sort: &Sort{
				Field: &Field{
					Table:  "table1",
					Column: "field1",
				},
				Direction: SortDirectionDescending,
			},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "table1.field1 desc",
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

			actualQuery, actualArgs, actualErr = testCases[i].Sort.ToSQLWithArgs(testCases[i].Dialect, []interface{}{})

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

			if testCases[i].Expectation.Err != nil && actualErr == nil {
				t.Error("expectation error is not nil, got nil")
			}

			if testCases[i].Expectation.Err == nil && actualErr != nil {
				t.Error("expectation error is nil, got not nil")
			}

			if testCases[i].Expectation.Err != nil && actualErr != nil && testCases[i].Expectation.Err.Error() != actualErr.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Err.Error(), actualErr.Error())
			}
		})
	}
}
