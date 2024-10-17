package goqube

import "fmt"

type Table struct {
	Name        string
	SelectQuery *SelectQuery
	Alias       string
}

func NewTable(name string) *Table {
	return &Table{
		Name: name,
	}
}

func NewSelectQueryTable(selectQuery *SelectQuery) *Table {
	return &Table{
		SelectQuery: selectQuery,
	}
}

func (t *Table) As(alias string) *Table {
	t.Alias = alias
	return t
}

func (t *Table) validate(dialect Dialect) error {
	if dialect == "" {
		return ErrDialectIsRequired
	}

	if t.Name != "" && t.SelectQuery != nil {
		return ErrConflictTableNameAndTableSelectQuery
	}

	if t.Name == "" && t.SelectQuery == nil {
		return ErrNameIsRequired
	}

	if t.Alias == "" && t.SelectQuery != nil {
		return ErrAliasIsRequired
	}

	return nil
}

func (t *Table) ToSQLWithArgs(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var (
		table string
		err   error
	)

	err = t.validate(dialect)
	if err != nil {
		return "", nil, err
	}

	table = t.Name
	if t.SelectQuery != nil {
		table, args, err = t.SelectQuery.ToSQLWithArgsWithAlias(dialect, args)
		if err != nil {
			return "", nil, err
		}

		table = fmt.Sprintf("(%s)", table)
	}

	return table, args, nil
}

func (t *Table) ToSQLWithArgsWithAlias(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var (
		table string
		err   error
	)

	table, args, err = t.ToSQLWithArgs(dialect, args)
	if err != nil {
		return "", nil, err
	}

	if t.Alias != "" {
		table = fmt.Sprintf("%s as %s", table, t.Alias)
	}

	return table, args, nil
}
