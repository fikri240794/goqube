package goqube

import (
	"fmt"
	"strings"
)

type SelectQuery struct {
	Fields        []*Field
	Table         *Table
	Joins         []*Join
	Filter        *Filter
	GroupByFields []*Field
	Sorts         []*Sort
	Take          uint64
	Skip          uint64
	Alias         string
}

func Select(fields ...*Field) *SelectQuery {
	return &SelectQuery{
		Fields: fields,
	}
}

func (s *SelectQuery) From(table *Table) *SelectQuery {
	s.Table = table
	return s
}

func (s *SelectQuery) Join(join *Join) *SelectQuery {
	s.Joins = append(s.Joins, join)
	return s
}

func (s *SelectQuery) Where(filter *Filter) *SelectQuery {
	s.Filter = filter
	return s
}

func (s *SelectQuery) GroupBy(fields ...*Field) *SelectQuery {
	s.GroupByFields = fields
	return s
}

func (s *SelectQuery) OrderBy(sorts ...*Sort) *SelectQuery {
	s.Sorts = sorts
	return s
}

func (s *SelectQuery) Limit(take uint64) *SelectQuery {
	s.Take = take
	return s
}

func (s *SelectQuery) Offset(skip uint64) *SelectQuery {
	s.Skip = skip
	return s
}

func (s *SelectQuery) As(alias string) *SelectQuery {
	s.Alias = alias
	return s
}

func (s *SelectQuery) validate(dialect Dialect) error {
	if dialect == "" {
		return ErrDialectIsRequired
	}

	if len(s.Fields) == 0 {
		return ErrFieldsIsRequired
	}

	for i := range s.Fields {
		if s.Fields[i] == nil {
			return ErrFieldIsNil
		}
	}

	if s.Table == nil {
		return ErrTableIsRequired
	}

	return nil
}

func (s *SelectQuery) ToSQLWithArgs(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var (
		fields         []string
		table          string
		query          string
		joinQueries    []string
		allJoinQueries string
		whereClause    string
		groupByFields  []string
		orderBy        string
		orderByClause  []string
		placeholder    string
		err            error
	)

	err = s.validate(dialect)
	if err != nil {
		return "", nil, err
	}

	for i := range s.Fields {
		if s.Fields != nil {
			var field string
			field, args, err = s.Fields[i].ToSQLWithArgsWithAlias(dialect, args)
			if err != nil {
				return "", nil, err
			}

			fields = append(fields, field)
		}
	}

	if s.Table != nil {
		table, args, err = s.Table.ToSQLWithArgsWithAlias(dialect, args)
		if err != nil {
			return "", nil, err
		}
	}

	query = fmt.Sprintf("select %s from %s", strings.Join(fields, ", "), table)

	if len(s.Joins) > 0 {
		joinQueries = []string{}

		for i := range s.Joins {
			if s.Joins[i] == nil {
				continue
			}

			var joinQuery string
			joinQuery, args, err = s.Joins[i].ToSQLWithArgs(dialect, args)
			if err != nil {
				return "", nil, err
			}

			joinQueries = append(joinQueries, joinQuery)
		}

		allJoinQueries = strings.Join(joinQueries, " ")
		if allJoinQueries != "" {
			query = fmt.Sprintf("%s %s", query, allJoinQueries)
		}
	}

	if s.Filter != nil {
		whereClause, args, err = s.Filter.ToSQLWithArgs(dialect, args)
		if err != nil {
			return "", nil, err
		}

		if whereClause != "" {
			query = fmt.Sprintf("%s where %s", query, whereClause)
		}
	}

	if len(s.GroupByFields) > 0 {
		for i := range s.GroupByFields {
			if s.GroupByFields[i] == nil {
				continue
			}

			var groupByField string
			groupByField, args, err = s.GroupByFields[i].ToSQLWithArgs(dialect, args)
			if err != nil {
				return "", nil, err
			}

			groupByFields = append(groupByFields, groupByField)
		}

		if len(groupByFields) > 0 {
			query = fmt.Sprintf("%s group by %s", query, strings.Join(groupByFields, ", "))
		}
	}

	if len(s.Sorts) > 0 {
		orderByClause = []string{}
		for i := range s.Sorts {
			if s.Sorts[i] == nil {
				continue
			}

			orderBy, args, err = s.Sorts[i].ToSQLWithArgs(dialect, args)
			if err != nil {
				return "", nil, err
			}

			orderByClause = append(orderByClause, orderBy)
		}

		if len(orderByClause) > 0 {
			query = fmt.Sprintf("%s order by %s", query, strings.Join(orderByClause, ", "))
		}
	}

	if s.Take > 0 {
		args = append(args, s.Take)
		placeholder = getPlaceholder(dialect, len(args), len(args))
		query = fmt.Sprintf("%s limit %s", query, placeholder)
	}

	if s.Skip > 0 {
		args = append(args, s.Skip)
		placeholder = getPlaceholder(dialect, len(args), len(args))
		query = fmt.Sprintf("%s offset %s", query, placeholder)
	}

	return query, args, nil
}

func (s *SelectQuery) ToSQLWithArgsWithAlias(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var (
		query string
		err   error
	)

	query, args, err = s.ToSQLWithArgs(dialect, args)
	if err != nil {
		return "", nil, err
	}

	if s.Alias != "" {
		query = fmt.Sprintf("(%s) as %s", query, s.Alias)
	}

	return query, args, nil
}
