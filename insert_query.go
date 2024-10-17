package goqube

import (
	"fmt"
	"sort"
	"strings"
)

type InsertQuery struct {
	Table        string
	FieldsValues map[string][]interface{}
}

func Insert() *InsertQuery {
	return &InsertQuery{
		FieldsValues: map[string][]interface{}{},
	}
}

func (i *InsertQuery) Into(table string) *InsertQuery {
	i.Table = table
	return i
}

func (i *InsertQuery) Value(field string, value interface{}) *InsertQuery {
	i.FieldsValues[field] = append(i.FieldsValues[field], value)
	return i
}

func (i *InsertQuery) getColumnsAndRowsValues() ([]string, [][]interface{}) {
	var (
		columns    []string
		rowCount   int
		rowsValues [][]interface{}
	)

	columns = []string{}
	for field, value := range i.FieldsValues {
		columns = append(columns, field)
		if rowCount < len(value) {
			rowCount = len(value)
		}
	}

	sort.Slice(columns, func(i, j int) bool {
		return columns[i] < columns[j]
	})

	rowsValues = [][]interface{}{}
	for rowIndex := 0; rowIndex < rowCount; rowIndex++ {
		var rowValues []interface{} = []interface{}{}

		for columnIndex := 0; columnIndex < len(columns); columnIndex++ {
			if rowIndex >= len(i.FieldsValues[columns[columnIndex]]) {
				continue
			}

			rowValues = append(rowValues, i.FieldsValues[columns[columnIndex]][rowIndex])
		}

		rowsValues = append(rowsValues, rowValues)
	}

	return columns, rowsValues
}

func (i *InsertQuery) validate(dialect Dialect) error {
	var (
		columns    []string
		rowsValues [][]interface{}
	)

	if dialect == "" {
		return ErrDialectIsRequired
	}

	if i.Table == "" {
		return ErrTableIsRequired
	}

	columns, rowsValues = i.getColumnsAndRowsValues()

	if len(columns) == 0 {
		return ErrFieldsIsRequired
	}

	for columnIndex := 0; columnIndex < len(columns); columnIndex++ {
		if columns[columnIndex] == "" {
			return ErrFieldIsRequired
		}
	}

	if len(rowsValues) == 0 {
		return ErrValuesIsRequired
	}

	for rowIndex := 0; rowIndex < len(rowsValues); rowIndex++ {
		var rowValues []interface{} = rowsValues[rowIndex]

		if len(rowValues) != len(columns) {
			return ErrValueLengthIsNotEqualToFieldsLength
		}
	}

	return nil
}

func (i *InsertQuery) ToSQLWithArgs(dialect Dialect) (string, []interface{}, error) {
	var (
		columns      []string
		rowsValues   [][]interface{}
		query        string
		args         []interface{}
		placeholders []string
		err          error
	)

	err = i.validate(dialect)
	if err != nil {
		return "", nil, err
	}

	columns, rowsValues = i.getColumnsAndRowsValues()
	args = []interface{}{}

	for rowIndex := 0; rowIndex < len(rowsValues); rowIndex++ {
		var (
			placeholderStartIdx int
			placeholderEndIdx   int
			placeholder         string
		)

		args = append(args, rowsValues[rowIndex]...)
		placeholderStartIdx = len(args) - (len(rowsValues[rowIndex]) - 1)
		placeholderEndIdx = len(args)
		placeholder = fmt.Sprintf("(%s)", getPlaceholder(dialect, placeholderStartIdx, placeholderEndIdx))
		placeholders = append(placeholders, placeholder)
	}

	query = fmt.Sprintf("insert into %s(%s) values %s", i.Table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	return query, args, nil
}
