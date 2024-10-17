package goqube

import "fmt"

type FilterValue struct {
	Value       interface{}
	Table       string
	Column      string
	SelectQuery *SelectQuery
}

func NewFilterValue(value interface{}) *FilterValue {
	return &FilterValue{
		Value: value,
	}
}

func NewColumnFilterValue(column string) *FilterValue {
	return &FilterValue{
		Column: column,
	}
}

func NewSelectQueryFilterValue(selectQuery *SelectQuery) *FilterValue {
	return &FilterValue{
		SelectQuery: selectQuery,
	}
}

func (v *FilterValue) FromTable(table string) *FilterValue {
	v.Table = table

	return v
}

func (v *FilterValue) validate(dialect Dialect) error {
	if dialect == "" {
		return ErrDialectIsRequired
	}

	return nil
}

func (v *FilterValue) ToSQLWithArgs(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var (
		query string
		err   error
	)

	err = v.validate(dialect)
	if err != nil {
		return "", nil, err
	}

	if v.SelectQuery != nil {
		query, args, err = v.SelectQuery.ToSQLWithArgsWithAlias(dialect, args)
		if err != nil {
			return "", nil, err
		}

		query = fmt.Sprintf("(%s)", query)

		return query, args, nil
	}

	if v.SelectQuery == nil && v.Column != "" {
		query = v.Column

		if v.Table != "" {
			query = fmt.Sprintf("%s.%s", v.Table, query)
		}

		return query, args, nil
	}

	args = append(args, v.Value)

	return "", args, nil
}
