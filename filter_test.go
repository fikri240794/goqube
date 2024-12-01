package goqube

import (
	"fmt"
	"reflect"
	"testing"
)

func testFilter_FilterEquality(t *testing.T, expectation, actual *Filter) {
	if expectation == nil && actual == nil {
		t.Skip("expectation and actual is nil")
	}

	if expectation == nil && actual != nil {
		t.Errorf("expectation is nil, got %+v", actual)
	}

	if expectation != nil && actual == nil {
		t.Errorf("expectation is %+v, got nil", expectation)
	}

	if expectation.Logic != actual.Logic {
		t.Errorf("expectation logic is %s, got %s", expectation.Logic, actual.Logic)
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

	if expectation.Operator != actual.Operator {
		t.Errorf("expectation operator is %s, got %s", expectation.Operator, actual.Operator)
	}

	if !deepEqual(expectation.Value, actual.Value) {
		t.Errorf("expectation value is %+v, got %+v", expectation.Value, actual.Value)
	}

	if expectation.Value == nil && actual.Value != nil {
		t.Errorf("expectation value is nil, got %+v", actual.Value)
	}

	if expectation.Value != nil && actual.Value == nil {
		t.Errorf("expectation value is %+v, got nil", expectation.Value)
	}

	if expectation.Value != nil && actual.Value != nil && !deepEqual(expectation.Value, actual.Value) {
		t.Errorf("expectation value is %+v, got %+v", expectation.Value, actual.Value)
	}

	if len(expectation.Filters) != len(actual.Filters) {
		t.Errorf("expectation length of filters is %d, got %d", len(expectation.Filters), len(actual.Filters))
	}

	for i := range expectation.Filters {
		testFilter_FilterEquality(t, expectation.Filters[i], actual.Filters[i])
	}
}

func TestFilter_NewFilter(t *testing.T) {
	testFilter_FilterEquality(t, &Filter{}, NewFilter())
}

func TestFilter_SetLogic(t *testing.T) {
	var testCases []struct {
		Name        string
		Logic       Logic
		Expectation *Filter
	} = []struct {
		Name        string
		Logic       Logic
		Expectation *Filter
	}{
		{
			Name:  "logic and",
			Logic: LogicAnd,
			Expectation: &Filter{
				Logic: LogicAnd,
			},
		},
		{
			Name:  "logic or",
			Logic: LogicOr,
			Expectation: &Filter{
				Logic: LogicOr,
			},
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual *Filter = NewFilter().
				SetLogic(testCases[i].Logic)
			testFilter_FilterEquality(t, testCases[i].Expectation, actual)
		})
	}
}

func TestFilter_SetCondition(t *testing.T) {
	var testCases []struct {
		Name        string
		Field       *Field
		Operator    Operator
		Value       *FilterValue
		Expectation *Filter
	} = []struct {
		Name        string
		Field       *Field
		Operator    Operator
		Value       *FilterValue
		Expectation *Filter
	}{
		{
			Name:     fmt.Sprintf("operator %s", OperatorEqual),
			Field:    NewField("field1"),
			Operator: OperatorEqual,
			Value:    NewFilterValue("value1"),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorEqual,
				Value: &FilterValue{
					Value: "value1",
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorNotEqual),
			Field:    NewField("field1"),
			Operator: OperatorNotEqual,
			Value:    NewFilterValue(true),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorNotEqual,
				Value: &FilterValue{
					Value: true,
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorGreaterThan),
			Field:    NewField("field1"),
			Operator: OperatorGreaterThan,
			Value:    NewFilterValue(int64(100)),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorGreaterThan,
				Value: &FilterValue{
					Value: int64(100),
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorGreaterThanOrEqual),
			Field:    NewField("field1"),
			Operator: OperatorGreaterThanOrEqual,
			Value:    NewFilterValue(float64(100)),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorGreaterThanOrEqual,
				Value: &FilterValue{
					Value: float64(100),
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorLessThan),
			Field:    NewField("field1"),
			Operator: OperatorLessThan,
			Value:    NewFilterValue(uint64(100)),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLessThan,
				Value: &FilterValue{
					Value: uint64(100),
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorLessThanOrEqual),
			Field:    NewField("field1"),
			Operator: OperatorLessThanOrEqual,
			Value:    NewFilterValue("2006-01-02T15:04:05+07:00"),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLessThanOrEqual,
				Value: &FilterValue{
					Value: "2006-01-02T15:04:05+07:00",
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorIsNull),
			Field:    NewField("field1"),
			Operator: OperatorIsNull,
			Value:    NewFilterValue(nil),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIsNull,
				Value: &FilterValue{
					Value: nil,
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorIsNotNull),
			Field:    NewField("field1"),
			Operator: OperatorIsNotNull,
			Value:    NewFilterValue(nil),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIsNotNull,
				Value: &FilterValue{
					Value: nil,
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorIn),
			Field:    NewField("field1"),
			Operator: OperatorIn,
			Value:    NewFilterValue([]string{"value1", "value 2", "value3"}),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					Value: []string{"value1", "value 2", "value3"},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorNotIn),
			Field:    NewField("field1"),
			Operator: OperatorNotIn,
			Value:    NewFilterValue([3]int64{1, 2, 3}),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorNotIn,
				Value: &FilterValue{
					Value: [3]int64{1, 2, 3},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorLike),
			Field:    NewField("field1"),
			Operator: OperatorLike,
			Value:    NewFilterValue("value1"),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLike,
				Value: &FilterValue{
					Value: "value1",
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorNotLike),
			Field:    NewField("field1"),
			Operator: OperatorNotLike,
			Value:    NewFilterValue("value1"),
			Expectation: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorNotLike,
				Value: &FilterValue{
					Value: "value1",
				},
			},
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual *Filter = NewFilter().
				SetCondition(
					testCases[i].Field,
					testCases[i].Operator,
					testCases[i].Value,
				)

			testFilter_FilterEquality(t, testCases[i].Expectation, actual)
		})
	}
}

func TestFilter_AddFilter(t *testing.T) {
	var testCases []struct {
		Name        string
		Field       *Field
		Operator    Operator
		Value       *FilterValue
		Expectation *Filter
	} = []struct {
		Name        string
		Field       *Field
		Operator    Operator
		Value       *FilterValue
		Expectation *Filter
	}{
		{
			Name:     fmt.Sprintf("operator %s", OperatorEqual),
			Field:    NewField("field1"),
			Operator: OperatorEqual,
			Value:    NewFilterValue("value1"),
			Expectation: &Filter{
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
		{
			Name:     fmt.Sprintf("operator %s", OperatorNotEqual),
			Field:    NewField("field1"),
			Operator: OperatorNotEqual,
			Value:    NewFilterValue(true),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorNotEqual,
						Value: &FilterValue{
							Value: true,
						},
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorGreaterThan),
			Field:    NewField("field1"),
			Operator: OperatorGreaterThan,
			Value:    NewFilterValue(int64(100)),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorGreaterThan,
						Value:    NewFilterValue(int64(100)),
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorGreaterThanOrEqual),
			Field:    NewField("field1"),
			Operator: OperatorGreaterThanOrEqual,
			Value:    NewFilterValue(float64(100)),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorGreaterThanOrEqual,
						Value: &FilterValue{
							Value: float64(100),
						},
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorLessThan),
			Field:    NewField("field1"),
			Operator: OperatorLessThan,
			Value:    NewFilterValue(uint64(100)),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorLessThan,
						Value: &FilterValue{
							Value: uint64(100),
						},
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorLessThanOrEqual),
			Field:    NewField("field1"),
			Operator: OperatorLessThanOrEqual,
			Value:    NewFilterValue("2006-01-02T15:04:05+07:00"),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorLessThanOrEqual,
						Value: &FilterValue{
							Value: "2006-01-02T15:04:05+07:00",
						},
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorIsNull),
			Field:    NewField("field1"),
			Operator: OperatorIsNull,
			Value:    NewFilterValue(nil),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorIsNull,
						Value: &FilterValue{
							Value: nil,
						},
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorIsNotNull),
			Field:    NewField("field1"),
			Operator: OperatorIsNotNull,
			Value:    NewFilterValue(nil),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorIsNotNull,
						Value: &FilterValue{
							Value: nil,
						},
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorIn),
			Field:    NewField("field1"),
			Operator: OperatorIn,
			Value:    NewFilterValue([]string{"value1", "value 2", "value3"}),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorIn,
						Value: &FilterValue{
							Value: []string{"value1", "value 2", "value3"},
						},
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorNotIn),
			Field:    NewField("field1"),
			Operator: OperatorNotIn,
			Value:    NewFilterValue([3]int64{1, 2, 3}),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorNotIn,
						Value: &FilterValue{
							Value: [3]int64{1, 2, 3},
						},
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorLike),
			Field:    NewField("field1"),
			Operator: OperatorLike,
			Value:    NewFilterValue("value1"),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorLike,
						Value: &FilterValue{
							Value: "value1",
						},
					},
				},
			},
		},
		{
			Name:     fmt.Sprintf("operator %s", OperatorNotLike),
			Field:    NewField("field1"),
			Operator: OperatorNotLike,
			Value:    NewFilterValue("value1"),
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorNotLike,
						Value: &FilterValue{
							Value: "value1",
						},
					},
				},
			},
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual *Filter = NewFilter().
				AddFilter(
					testCases[i].Field,
					testCases[i].Operator,
					testCases[i].Value,
				)

			testFilter_FilterEquality(t, testCases[i].Expectation, actual)
		})
	}
}

func TestFilter_AddFilters(t *testing.T) {
	var testCases []struct {
		Name        string
		Filter      *Filter
		Expectation *Filter
	} = []struct {
		Name        string
		Filter      *Filter
		Expectation *Filter
	}{
		{
			Name: fmt.Sprintf("operator %s", OperatorEqual),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorEqual,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Expectation: &Filter{
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
		{
			Name: fmt.Sprintf("operator %s", OperatorNotEqual),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorNotEqual,
				Value: &FilterValue{
					Value: true,
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorNotEqual,
						Value: &FilterValue{
							Value: true,
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorGreaterThan),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorGreaterThan,
				Value: &FilterValue{
					Value: int64(100),
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorGreaterThan,
						Value: &FilterValue{
							Value: int64(100),
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorGreaterThanOrEqual),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorGreaterThanOrEqual,
				Value: &FilterValue{
					Value: float64(100),
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorGreaterThanOrEqual,
						Value: &FilterValue{
							Value: float64(100),
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorLessThan),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLessThan,
				Value: &FilterValue{
					Value: uint64(100),
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorLessThan,
						Value: &FilterValue{
							Value: uint64(100),
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorLessThanOrEqual),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLessThanOrEqual,
				Value: &FilterValue{
					Value: "2006-01-02T15:04:05+07:00",
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorLessThanOrEqual,
						Value: &FilterValue{
							Value: "2006-01-02T15:04:05+07:00",
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorIsNull),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIsNull,
				Value: &FilterValue{
					Value: nil,
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorIsNull,
						Value: &FilterValue{
							Value: nil,
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorIsNotNull),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIsNotNull,
				Value: &FilterValue{
					Value: nil,
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorIsNotNull,
						Value: &FilterValue{
							Value: nil,
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorIn),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					Value: []string{"value1", "value 2", "value3"},
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorIn,
						Value: &FilterValue{
							Value: []string{"value1", "value 2", "value3"},
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorNotIn),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorNotIn,
				Value: &FilterValue{
					Value: [3]int64{1, 2, 3},
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorNotIn,
						Value: &FilterValue{
							Value: [3]int64{1, 2, 3},
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorLike),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLike,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorLike,
						Value: &FilterValue{
							Value: "value1",
						},
					},
				},
			},
		},
		{
			Name: fmt.Sprintf("operator %s", OperatorNotLike),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorNotLike,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Expectation: &Filter{
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorNotLike,
						Value: &FilterValue{
							Value: "value1",
						},
					},
				},
			},
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual *Filter = NewFilter().
				AddFilters(testCases[i].Filter)

			testFilter_FilterEquality(t, testCases[i].Expectation, actual)
		})
	}
}

func TestFilter_validate(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		Filter      *Filter
		Expectation error
	} = []struct {
		Name        string
		Dialect     Dialect
		Filter      *Filter
		Expectation error
	}{
		{
			Name:        "dialect is empty",
			Dialect:     "",
			Filter:      &Filter{},
			Expectation: ErrDialectIsRequired,
		},
		{
			Name:    "logic is not empty and field is not nil",
			Dialect: DialectPostgres,
			Filter: &Filter{
				Logic: LogicAnd,
				Field: &Field{},
			},
			Expectation: ErrFieldIsNotEmpty,
		},
		{
			Name:    "logic is not empty and operator is not empty",
			Dialect: DialectPostgres,
			Filter: &Filter{
				Logic:    LogicOr,
				Operator: OperatorEqual,
			},
			Expectation: ErrOperatorIsNotEmpty,
		},
		{
			Name:    fmt.Sprintf("logic is not empty and value is not nil or value kind is not %s", reflect.Invalid.String()),
			Dialect: DialectPostgres,
			Filter: &Filter{
				Logic: LogicAnd,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Expectation: ErrValueIsNotNil,
		},
		{
			Name:    "logic is not empty and filters length is zero",
			Dialect: DialectPostgres,
			Filter: &Filter{
				Logic:   LogicAnd,
				Filters: []*Filter{},
			},
			Expectation: ErrFiltersIsRequired,
		},
		{
			Name:    "logic is empty and filters length greater than zero",
			Dialect: DialectPostgres,
			Filter: &Filter{
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
			Expectation: ErrLogicIsRequired,
		},
		{
			Name:    "logic is empty and filters length is zero and field is nil",
			Dialect: DialectPostgres,
			Filter: &Filter{
				Operator: OperatorEqual,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Expectation: ErrFieldIsRequired,
		},
		{
			Name:    "logic is empty and filters length is zero and operator is empty",
			Dialect: DialectPostgres,
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Expectation: ErrOperatorIsRequired,
		},
		{
			Name:    fmt.Sprintf("logic is empty and filters length is zero and operator is not %s and operator is not %s and value is nil and value kind is %s", OperatorIsNull, OperatorIsNotNull, reflect.Invalid.String()),
			Dialect: DialectPostgres,
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorEqual,
				Value:    nil,
			},
			Expectation: ErrValueIsRequired,
		},
		{
			Name:    fmt.Sprintf("logic is empty and filters length is zero and operator is %s and value is not nil or value kind is not %s", OperatorIsNull, reflect.Invalid.String()),
			Dialect: DialectPostgres,
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIsNull,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Expectation: ErrValueIsNotNil,
		},
		{
			Name:    fmt.Sprintf("logic is empty and filters length is zero and operator is not %s and operator is not %s and value kind is %s", OperatorIn, OperatorNotIn, reflect.Slice.String()),
			Dialect: DialectPostgres,
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorEqual,
				Value: &FilterValue{
					Value: []int64{1, 2, 3},
				},
			},
			Expectation: fmt.Errorf(errUnsupportedValueTypeForOperatorf, reflect.Slice.String(), OperatorEqual),
		},
		{
			Name:    fmt.Sprintf("logic is empty and filters length is zero and operator is %s and value kind is not %s and %s", OperatorIn, reflect.Slice.String(), reflect.Array.String()),
			Dialect: DialectPostgres,
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					Value: int64(123),
				},
			},
			Expectation: fmt.Errorf(errUnsupportedValueTypeForOperatorf, reflect.Int64.String(), OperatorIn),
		},
		{
			Name:    fmt.Sprintf("logic is empty and filters length is zero and operator is %s and value length is zero", OperatorIn),
			Dialect: DialectPostgres,
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					Value: []int64{},
				},
			},
			Expectation: ErrValueIsRequired,
		},
		{
			Name:    "filter is valid",
			Dialect: DialectPostgres,
			Filter: &Filter{
				Logic: LogicAnd,
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorEqual,
						Value: &FilterValue{
							Value: int64(123),
						},
					},
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
			Expectation: nil,
		},
		{
			Name:    "filter is invalid",
			Dialect: DialectPostgres,
			Filter: &Filter{
				Logic: LogicAnd,
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorEqual,
						Value: &FilterValue{
							Value: int64(123),
						},
					},
					{
						Field: &Field{
							Column: "field2",
						},
						Operator: OperatorEqual,
						Value: &FilterValue{
							Value: []string{"a", "b", "c"},
						},
					},
				},
			},
			Expectation: fmt.Errorf(errUnsupportedValueTypeForOperatorf, reflect.Slice.String(), OperatorEqual),
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual error = testCases[i].Filter.validate(testCases[i].Dialect)

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

func TestFilter_toSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		Filter      *Filter
		Dialect     Dialect
		Args        []interface{}
		IsRoot      bool
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Filter      *Filter
		Dialect     Dialect
		Args        []interface{}
		IsRoot      bool
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name: "operator is not empty and field to sql with args with alias is error",
			Filter: &Filter{
				Field:    &Field{},
				Operator: OperatorEqual,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
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

		// MYSQL
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s", DialectMySQL, OperatorEqual),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorEqual,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 = ?",
				Args:  []interface{}{"value1"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s and filter value to sql with args is error", DialectMySQL, OperatorEqual),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorEqual,
				Value: &FilterValue{
					SelectQuery: &SelectQuery{},
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
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
			Name: fmt.Sprintf("dialect %s with filter operator %s", DialectMySQL, OperatorIsNull),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIsNull,
				Value:    nil,
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 is null",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s", DialectMySQL, OperatorIn),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					Value: []string{"value1", "value2", "value3"},
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 in (?, ?, ?)",
				Args:  []interface{}{"value1", "value2", "value3"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s and filter value select query is not nil and filter value to sql with args is error", DialectMySQL, OperatorIn),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					SelectQuery: &SelectQuery{},
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
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
			Name: fmt.Sprintf("dialect %s with filter operator %s and filter value select query is not nil", DialectMySQL, OperatorIn),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					SelectQuery: Select(NewField("field1")).
						From(NewTable("table1")).Where(
						NewFilter().
							SetLogic(LogicAnd).
							AddFilter(NewField("field1"), OperatorEqual, NewFilterValue("value1")),
					),
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 in (select field1 from table1 where field1 = ?)",
				Args:  []interface{}{"value1"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s", DialectMySQL, OperatorLike),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLike,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "cast(field1 as char) like concat('%', cast(? as char), '%')",
				Args:  []interface{}{"value1"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s and filter value to sql with args is error", DialectMySQL, OperatorLike),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLike,
				Value: &FilterValue{
					SelectQuery: &SelectQuery{},
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
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
			Name:    fmt.Sprintf("dialect %s with filters length is zero", DialectMySQL),
			Filter:  &Filter{},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s", DialectMySQL),
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
					{
						Logic: LogicOr,
						Filters: []*Filter{
							{
								Field: &Field{
									Column: "field2",
								},
								Operator: OperatorIsNull,
								Value:    nil,
							},
							{
								Field: &Field{
									Column: "field3",
								},
								Operator: OperatorIn,
								Value: &FilterValue{
									Value: []int64{1, 2, 3},
								},
							},
						},
					},
					{
						Field: &Field{
							Column: "field4",
						},
						Operator: OperatorLike,
						Value: &FilterValue{
							Value: "value4",
						},
					},
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "(field1 = ? and (field2 is null or field3 in (?, ?, ?)) and cast(field4 as char) like concat('%', cast(? as char), '%'))",
				Args:  []interface{}{"value1", 1, 2, 3, "value4"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with element filters is empty filter", DialectMySQL),
			Filter: &Filter{
				Logic: LogicAnd,
				Filters: []*Filter{
					{},
					{},
					{},
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with element filters is nil", DialectMySQL),
			Filter: &Filter{
				Logic: LogicAnd,
				Filters: []*Filter{
					nil,
					nil,
					nil,
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with element filters operator is %s and value is not %s and %s and typedSliceToInterfaceSlice is error", DialectMySQL, OperatorIn, reflect.Slice.String(), reflect.Array.String()),
			Filter: &Filter{
				Logic: LogicAnd,
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorIn,
						Value: &FilterValue{
							Value: "value1",
						},
					},
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   fmt.Errorf(errUnsupportedValueTypeForOperatorf, reflect.String.String(), OperatorIn),
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with isRoot is true", DialectMySQL),
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
					{
						Logic: LogicOr,
						Filters: []*Filter{
							{
								Field: &Field{
									Column: "field2",
								},
								Operator: OperatorIsNull,
								Value:    nil,
							},
							{
								Field: &Field{
									Column: "field3",
								},
								Operator: OperatorIn,
								Value: &FilterValue{
									Value: []int64{1, 2, 3},
								},
							},
						},
					},
					{
						Field: &Field{
							Column: "field4",
						},
						Operator: OperatorLike,
						Value: &FilterValue{
							Value: "value4",
						},
					},
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			IsRoot:  true,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 = ? and (field2 is null or field3 in (?, ?, ?)) and cast(field4 as char) like concat('%', cast(? as char), '%')",
				Args:  []interface{}{"value1", 1, 2, 3, "value4"},
				Err:   nil,
			},
		},

		// POSTGRES
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s", DialectPostgres, OperatorEqual),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorEqual,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 = $1",
				Args:  []interface{}{"value1"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s and filter value to sql with args is error", DialectPostgres, OperatorEqual),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorEqual,
				Value: &FilterValue{
					SelectQuery: &SelectQuery{},
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
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
			Name: fmt.Sprintf("dialect %s with filter operator %s", DialectPostgres, OperatorIsNull),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIsNull,
				Value:    nil,
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 is null",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s", DialectPostgres, OperatorIn),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					Value: []string{"value1", "value2", "value3"},
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 in ($1, $2, $3)",
				Args:  []interface{}{"value1", "value2", "value3"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s and filter value select query is not nil and filter value to sql with args is error", DialectPostgres, OperatorIn),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					SelectQuery: &SelectQuery{},
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
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
			Name: fmt.Sprintf("dialect %s with filter operator %s and filter value select query is not nil", DialectPostgres, OperatorIn),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorIn,
				Value: &FilterValue{
					SelectQuery: Select(NewField("field1")).
						From(NewTable("table1")).Where(
						NewFilter().
							SetLogic(LogicAnd).
							AddFilter(NewField("field1"), OperatorEqual, NewFilterValue("value1")),
					),
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 in (select field1 from table1 where field1 = $1)",
				Args:  []interface{}{"value1"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s", DialectPostgres, OperatorLike),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLike,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1::text ilike concat('%', $1::text, '%')",
				Args:  []interface{}{"value1"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with filter operator %s and filter value to sql with args is error", DialectPostgres, OperatorLike),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorLike,
				Value: &FilterValue{
					SelectQuery: &SelectQuery{},
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
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
			Name: fmt.Sprintf("dialect %s with filter operator %s", DialectPostgres, OperatorNotLike),
			Filter: &Filter{
				Field: &Field{
					Column: "field1",
				},
				Operator: OperatorNotLike,
				Value: &FilterValue{
					Value: "value1",
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1::text not ilike concat('%', $1::text, '%')",
				Args:  []interface{}{"value1"},
				Err:   nil,
			},
		},
		{
			Name:    fmt.Sprintf("dialect %s with filters length is zero", DialectPostgres),
			Filter:  &Filter{},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s", DialectPostgres),
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
					{
						Logic: LogicOr,
						Filters: []*Filter{
							{
								Field: &Field{
									Column: "field2",
								},
								Operator: OperatorIsNull,
								Value:    nil,
							},
							{
								Field: &Field{
									Column: "field3",
								},
								Operator: OperatorIn,
								Value: &FilterValue{
									Value: []int64{1, 2, 3},
								},
							},
						},
					},
					{
						Field: &Field{
							Column: "field4",
						},
						Operator: OperatorLike,
						Value: &FilterValue{
							Value: "value4",
						},
					},
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "(field1 = $1 and (field2 is null or field3 in ($2, $3, $4)) and field4::text ilike concat('%', $5::text, '%'))",
				Args:  []interface{}{"value1", 1, 2, 3, "value4"},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with element filters is empty filter", DialectPostgres),
			Filter: &Filter{
				Logic: LogicAnd,
				Filters: []*Filter{
					{},
					{},
					{},
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with element filters is nil", DialectPostgres),
			Filter: &Filter{
				Logic: LogicAnd,
				Filters: []*Filter{
					nil,
					nil,
					nil,
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  []interface{}{},
				Err:   nil,
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with element filters operator is %s and value is not %s and %s and typedSliceToInterfaceSlice is error", DialectPostgres, OperatorIn, reflect.Slice.String(), reflect.Array.String()),
			Filter: &Filter{
				Logic: LogicAnd,
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorIn,
						Value: &FilterValue{
							Value: "value1",
						},
					},
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  false,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  nil,
				Err:   fmt.Errorf(errUnsupportedValueTypeForOperatorf, reflect.String.String(), OperatorIn),
			},
		},
		{
			Name: fmt.Sprintf("dialect %s with isRoot is true", DialectPostgres),
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
					{
						Logic: LogicOr,
						Filters: []*Filter{
							{
								Field: &Field{
									Column: "field2",
								},
								Operator: OperatorIsNull,
								Value:    nil,
							},
							{
								Field: &Field{
									Column: "field3",
								},
								Operator: OperatorIn,
								Value: &FilterValue{
									Value: []int64{1, 2, 3},
								},
							},
						},
					},
					{
						Field: &Field{
							Column: "field4",
						},
						Operator: OperatorLike,
						Value: &FilterValue{
							Value: "value4",
						},
					},
				},
			},
			Dialect: DialectPostgres,
			Args:    []interface{}{},
			IsRoot:  true,
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 = $1 and (field2 is null or field3 in ($2, $3, $4)) and field4::text ilike concat('%', $5::text, '%')",
				Args:  []interface{}{"value1", 1, 2, 3, "value4"},
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

			actualQuery, actualArgs, actualErr = testCases[i].Filter.toSQLWithArgs(testCases[i].Dialect, testCases[i].Args, testCases[i].IsRoot)

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
					t.Errorf("expectation conditional query is %s, got %s", testCases[i].Expectation.Query, actualQuery)
				}

				if len(testCases[i].Expectation.Args) != len(actualArgs) {
					t.Errorf("expectation args length is %d, got %d", len(testCases[i].Expectation.Args), len(actualArgs))
				}

				for x := range testCases[i].Expectation.Args {
					if !deepEqual(testCases[i].Expectation.Args[x], actualArgs[x]) {
						t.Errorf("expectation element of args is %v, got %v", testCases[i].Expectation.Args[x], actualArgs[x])
					}
				}
			}
		})
	}
}

func TestFilter_ToSQLWithArgs(t *testing.T) {
	var testCases []struct {
		Name        string
		Filter      *Filter
		Dialect     Dialect
		Args        []interface{}
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	} = []struct {
		Name        string
		Filter      *Filter
		Dialect     Dialect
		Args        []interface{}
		Expectation struct {
			Query string
			Args  []interface{}
			Err   error
		}
	}{
		{
			Name: "invalid validation",
			Filter: &Filter{
				Logic: LogicAnd,
				Filters: []*Filter{
					{
						Field: &Field{
							Column: "field1",
						},
						Operator: OperatorEqual,
						Value: &FilterValue{
							Value: []string{"a", "b", "c"},
						},
					},
				},
			},
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "",
				Args:  []interface{}{},
				Err:   fmt.Errorf(errUnsupportedValueTypeForOperatorf, reflect.Slice.String(), OperatorEqual),
			},
		},
		{
			Name: "filter is valid",
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
			Dialect: DialectMySQL,
			Args:    []interface{}{},
			Expectation: struct {
				Query string
				Args  []interface{}
				Err   error
			}{
				Query: "field1 = ?",
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

			actualQuery, actualArgs, actualErr = testCases[i].Filter.ToSQLWithArgs(testCases[i].Dialect, testCases[i].Args)

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
					t.Errorf("expectation conditional query is %s, got %s", testCases[i].Expectation.Query, actualQuery)
				}

				if len(testCases[i].Expectation.Args) != len(actualArgs) {
					t.Errorf("expectation args length is %d, got %d", len(testCases[i].Expectation.Args), len(actualArgs))
				}

				for x := range testCases[i].Expectation.Args {
					if !deepEqual(testCases[i].Expectation.Args[x], actualArgs[x]) {
						t.Errorf("expectation element of args is %v, got %v", testCases[i].Expectation.Args[x], actualArgs[x])
					}
				}
			}
		})
	}
}
