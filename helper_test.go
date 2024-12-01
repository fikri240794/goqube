package goqube

import (
	"fmt"
	"reflect"
	"testing"
)

func Test_typedSliceToInterfaceSlice(t *testing.T) {
	var testCases []struct {
		Name        string
		Value       interface{}
		Expectation struct {
			Values []interface{}
			Error  error
		}
	} = []struct {
		Name        string
		Value       interface{}
		Expectation struct {
			Values []interface{}
			Error  error
		}
	}{
		{
			Name:  "value kind is not slice and value kind is not array",
			Value: "value1",
			Expectation: struct {
				Values []interface{}
				Error  error
			}{
				Values: nil,
				Error:  fmt.Errorf("unsupported %s value type", reflect.String.String()),
			},
		},
		{
			Name:  "slice of string to slice of interface",
			Value: []string{"value1", "value2", "value3"},
			Expectation: struct {
				Values []interface{}
				Error  error
			}{
				Values: []interface{}{"value1", "value2", "value3"},
				Error:  nil,
			},
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var (
				actualValues []interface{}
				actualErr    error
			)

			actualValues, actualErr = typedSliceToInterfaceSlice(testCases[i].Value)

			if testCases[i].Expectation.Error != nil && actualErr == nil {
				t.Error("expectation error is not nil, got nil")
			}

			if testCases[i].Expectation.Error == nil && actualErr != nil {
				t.Error("expectation error is nil, got not nil")
			}

			if testCases[i].Expectation.Error != nil && actualErr != nil && testCases[i].Expectation.Error.Error() != actualErr.Error() {
				t.Errorf("expectation error is %s, got %s", testCases[i].Expectation.Error.Error(), actualErr.Error())
			}

			if testCases[i].Expectation.Error == nil && actualErr == nil {
				if len(testCases[i].Expectation.Values) != len(actualValues) {
					t.Errorf("expectation values length is %d, got %d", len(testCases[i].Expectation.Values), len(actualValues))
				}

				for x := range testCases[i].Expectation.Values {
					if !deepEqual(testCases[i].Expectation.Values[x], actualValues[x]) {
						t.Errorf("expectation element slice of interface is %v, got %v", testCases[i].Expectation.Values[x], actualValues[x])
					}
				}
			}
		})
	}
}

func Test_getPlaceholder(t *testing.T) {
	var testCases []struct {
		Name        string
		Dialect     Dialect
		StartIdx    int
		EndIdx      int
		Expectation string
	} = []struct {
		Name        string
		Dialect     Dialect
		StartIdx    int
		EndIdx      int
		Expectation string
	}{
		{
			Name:        "unknown dialect",
			Dialect:     "unknown",
			StartIdx:    1,
			EndIdx:      1,
			Expectation: "",
		},
		{
			Name:        "zero start index",
			Dialect:     DialectMySQL,
			StartIdx:    0,
			EndIdx:      1,
			Expectation: "",
		},
		{
			Name:        "zero end index",
			Dialect:     DialectMySQL,
			StartIdx:    1,
			EndIdx:      0,
			Expectation: "",
		},
		{
			Name:        "end index less than start index",
			Dialect:     DialectMySQL,
			StartIdx:    1,
			EndIdx:      0,
			Expectation: "",
		},
		{
			Name:        "mysql with start index equal to end index",
			Dialect:     DialectMySQL,
			StartIdx:    1,
			EndIdx:      1,
			Expectation: "?",
		},
		{
			Name:        "mysql with start index less than end index",
			Dialect:     DialectMySQL,
			StartIdx:    1,
			EndIdx:      5,
			Expectation: "?, ?, ?, ?, ?",
		},
		{
			Name:        "postgres with start index equal to end index",
			Dialect:     DialectPostgres,
			StartIdx:    1,
			EndIdx:      1,
			Expectation: "$1",
		},
		{
			Name:        "postgres with start index less than end index",
			Dialect:     DialectPostgres,
			StartIdx:    1,
			EndIdx:      5,
			Expectation: "$1, $2, $3, $4, $5",
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			var actual string = getPlaceholder(testCases[i].Dialect, testCases[i].StartIdx, testCases[i].EndIdx)
			if testCases[i].Expectation != actual {
				t.Errorf("expected placeholder %s, got %s", testCases[i].Expectation, actual)
			}
		})
	}
}
