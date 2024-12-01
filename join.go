package goqube

import "fmt"

type Join struct {
	Type   JoinType
	Table  *Table
	Filter *Filter
}

func InnerJoin(table *Table) *Join {
	return &Join{
		Type:  InnerJoinType,
		Table: table,
	}
}

func LeftJoin(table *Table) *Join {
	return &Join{
		Type:  LeftJoinType,
		Table: table,
	}
}

func RightJoin(table *Table) *Join {
	return &Join{
		Type:  RightJoinType,
		Table: table,
	}
}

func FullJoin(table *Table) *Join {
	return &Join{
		Type:  FullJoinType,
		Table: table,
	}
}

func (j *Join) On(filter *Filter) *Join {
	j.Filter = filter

	return j
}

func (j *Join) validate(dialect Dialect) error {
	if dialect == "" {
		return ErrDialectIsRequired
	}

	if j.Type == "" {
		return ErrJoinTypeIsRequired
	}

	if j.Table == nil {
		return ErrTableIsRequired
	}

	if j.Filter == nil {
		return ErrFilterIsRequired
	}

	return nil
}

func (j *Join) ToSQLWithArgs(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var (
		tableQuery  string
		filterQuery string
		query       string
		err         error
	)

	err = j.validate(dialect)
	if err != nil {
		return "", nil, err
	}

	tableQuery, args, err = j.Table.ToSQLWithArgsWithAlias(dialect, args)
	if err != nil {
		return "", nil, err
	}

	filterQuery, args, err = j.Filter.ToSQLWithArgs(dialect, args)
	if err != nil {
		return "", nil, err
	}

	query = fmt.Sprintf("%s %s on %s", j.Type, tableQuery, filterQuery)

	return query, args, nil
}
