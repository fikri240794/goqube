package goqube

import "fmt"

type Field struct {
	Table       string
	Column      string
	SelectQuery *SelectQuery
	Alias       string
}

func NewField(column string) *Field {
	return &Field{
		Column: column,
	}
}

func NewSelectQueryField(selectQuery *SelectQuery) *Field {
	return &Field{
		SelectQuery: selectQuery,
	}
}

func (f *Field) FromTable(table string) *Field {
	f.Table = table
	return f
}

func (f *Field) As(alias string) *Field {
	f.Alias = alias
	return f
}

func (f *Field) validate(dialect Dialect) error {
	if dialect == "" {
		return ErrDialectIsRequired
	}

	if f.Column == "" && f.SelectQuery == nil {
		return ErrColumnIsRequired
	}

	if f.Column != "" && f.SelectQuery != nil {
		return ErrConflictFieldColumnAndFieldSelectQuery
	}

	if f.Alias == "" && f.SelectQuery != nil {
		return ErrAliasIsRequired
	}

	return nil
}

func (f *Field) ToSQLWithArgs(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var (
		field string
		err   error
	)

	err = f.validate(dialect)
	if err != nil {
		return "", nil, err
	}

	field = f.Column
	if f.SelectQuery != nil {
		field, args, err = f.SelectQuery.ToSQLWithArgsWithAlias(dialect, args)
		if err != nil {
			return "", nil, err
		}

		field = fmt.Sprintf("(%s)", field)
	}

	if f.Table != "" && f.SelectQuery == nil {
		field = fmt.Sprintf("%s.%s", f.Table, field)
	}

	return field, args, nil
}

func (f *Field) ToSQLWithArgsWithAlias(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var (
		fieldWithAlias string
		err            error
	)

	fieldWithAlias, args, err = f.ToSQLWithArgs(dialect, args)
	if err != nil {
		return "", nil, err
	}

	if f.Alias != "" {
		fieldWithAlias = fmt.Sprintf("%s as %s", fieldWithAlias, f.Alias)
	}

	return fieldWithAlias, args, nil
}
