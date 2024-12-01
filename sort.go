package goqube

import (
	"fmt"
)

type Sort struct {
	Field     *Field
	Direction SortDirection
}

func NewSort(field *Field, direction SortDirection) *Sort {
	return &Sort{
		Field:     field,
		Direction: direction,
	}
}

func (s *Sort) validate(dialect Dialect) error {
	if dialect == "" {
		return ErrDialectIsRequired
	}

	if s.Field == nil {
		return ErrFieldIsRequired
	}

	return nil
}

func (s *Sort) ToSQLWithArgs(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var (
		field              string
		orderByQueryFormat string
		orderByQuery       string
		err                error
	)

	err = s.validate(dialect)
	if err != nil {
		return "", nil, err
	}

	field, args, err = s.Field.ToSQLWithArgsWithAlias(dialect, args)
	if err != nil {
		return "", nil, err
	}

	if s.Direction == "" {
		s.Direction = SortDirectionAscending
	}

	orderByQueryFormat = "%s %s"
	orderByQuery = fmt.Sprintf(orderByQueryFormat, field, s.Direction)

	return orderByQuery, args, nil
}
