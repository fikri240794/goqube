package goqube

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

func typedSliceToInterfaceSlice(value interface{}) ([]interface{}, error) {
	var (
		reflectValue   reflect.Value
		interfaceSlice []interface{}
	)

	reflectValue = reflect.ValueOf(value)
	if reflectValue.Kind() != reflect.Slice && reflectValue.Kind() != reflect.Array {
		return nil, fmt.Errorf(errUnsupportedValueTypef, reflectValue.Kind().String())
	}

	interfaceSlice = []interface{}{}
	for i := 0; i < reflectValue.Len(); i++ {
		interfaceSlice = append(interfaceSlice, reflectValue.Index(i).Interface())
	}

	return interfaceSlice, nil
}

func getPlaceholder(dialect Dialect, startIdx, endIdx int) string {
	var placeholders []string = []string{}

	if startIdx <= 0 || endIdx <= 0 || endIdx < startIdx {
		return ""
	}

	switch dialect {
	case DialectMySQL:
		if startIdx == endIdx {
			return placeholderMap[dialect]
		}
		for i := startIdx; i <= endIdx; i++ {
			placeholders = append(placeholders, placeholderMap[dialect])
		}
		return strings.Join(placeholders, ", ")

	case DialectPostgres:
		if startIdx == endIdx {
			return fmt.Sprintf("%s%d", placeholderMap[dialect], endIdx)
		}
		for i := startIdx; i <= endIdx; i++ {
			placeholders = append(placeholders, fmt.Sprintf("%s%d", placeholderMap[dialect], i))
		}
		return strings.Join(placeholders, ", ")

	default:
		return ""
	}
}

func deepEqual(value1 interface{}, value2 interface{}) bool {
	var (
		val1  interface{}
		val1B []byte
		val2  interface{}
		val2B []byte
	)

	if reflect.DeepEqual(value1, value2) {
		return true
	}

	val1B, _ = json.Marshal(value1)
	json.Unmarshal(val1B, &val1)
	val2B, _ = json.Marshal(value2)
	json.Unmarshal(val2B, &val2)

	return reflect.DeepEqual(val1, val2)
}
