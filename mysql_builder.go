package goqube

import (
	"sort"
	"strings"
)

// mysqlBuilder implements SQL query building logic specific to MySQL.
type mysqlBuilder struct {
	dynamicQueryBuilder
}

// newMySQLBuilder creates a new mysqlBuilder with MySQL-style placeholders ("?").
func newMySQLBuilder() *mysqlBuilder {
	return &mysqlBuilder{dynamicQueryBuilder{placeholderFormat: "?"}}
}

// BuildDeleteQuery builds a SQL DELETE statement and its arguments for MySQL.
func (b *mysqlBuilder) BuildDeleteQuery(q *DeleteQuery) (string, []interface{}, error) {
	if q == nil || q.Table == "" {
		return "", nil, ErrInvalidDeleteQuery
	}

	// Preallocate args slice with estimated capacity for typical DELETE queries
	args := make([]interface{}, 0, 8)

	// Use closure to maintain consistent filter building without recreating function on each call
	return b.buildDeleteQuery(q.Table, q.Filter, &args, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, true)
	})
}

// BuildInsertQuery builds a SQL INSERT statement and its arguments for MySQL.
func (b *mysqlBuilder) BuildInsertQuery(q *InsertQuery) (string, []interface{}, error) {
	return b.buildInsertQuery(q, 0, "?")
}

// BuildSelectQuery builds a SQL SELECT statement and its arguments for MySQL.
func (b *mysqlBuilder) BuildSelectQuery(q *SelectQuery) (string, []interface{}, error) {
	if q == nil {
		return "", nil, ErrUnsupportedDialect
	}

	// Early return for raw SQL queries to avoid unnecessary processing
	if q.Raw != "" {
		return q.Raw, q.RawArgs, nil
	}

	// Preallocate args slice with estimated capacity for typical SELECT queries
	args := make([]interface{}, 0, 16)
	buf := make([]byte, 0, 128)
	buf = append(buf, "SELECT "...)

	fields, err := b.buildFields(q.Fields, &args)
	if err != nil {
		return "", nil, err
	}
	buf = append(buf, fields...)

	table, err := b.buildTable(q.Table, &args)
	if err != nil {
		return "", nil, err
	}
	buf = append(buf, " FROM "...)
	buf = append(buf, table...)

	// Process JOINs only if they exist
	if len(q.Joins) > 0 {
		joins, err := b.buildJoins(q.Joins, &args)
		if err != nil {
			return "", nil, err
		}
		buf = append(buf, ' ')
		buf = append(buf, joins...)
	}

	// Process WHERE clause only if filter exists and produces content
	if q.Filter != nil {
		where, err := b.buildFilter(q.Filter, &args, true)
		if err != nil {
			return "", nil, err
		}
		if where != "" {
			buf = append(buf, " WHERE "...)
			buf = append(buf, where...)
		}
	}

	// Process GROUP BY only if grouping fields exist
	if len(q.GroupByFields) > 0 {
		groupBy, err := b.buildGroupBy(q.GroupByFields)
		if err != nil {
			return "", nil, err
		}
		buf = append(buf, " GROUP BY "...)
		buf = append(buf, groupBy...)
	}

	// Process ORDER BY only if sorting is specified
	if len(q.Sorts) > 0 {
		orderBy, err := b.buildOrderBy(q.Sorts)
		if err != nil {
			return "", nil, err
		}
		buf = append(buf, " ORDER BY "...)
		buf = append(buf, orderBy...)
	}

	// Add pagination clauses only when needed
	if q.Take > 0 {
		buf = append(buf, " LIMIT ?"...)
		args = append(args, int64(q.Take))
	}

	if q.Skip > 0 {
		buf = append(buf, " OFFSET ?"...)
		args = append(args, int64(q.Skip))
	}

	// Handle aliasing with optimized string building
	if q.Alias != "" {
		alias := strings.TrimSpace(string(buf))
		out := make([]byte, 0, len(alias)+len(q.Alias)+6)
		out = append(out, '(')
		out = append(out, alias...)
		out = append(out, ") AS "...)
		out = append(out, q.Alias...)
		return string(out), args, nil
	}

	return string(buf), args, nil
}

// BuildUpdateQuery builds a SQL UPDATE statement and its arguments for MySQL.
func (b *mysqlBuilder) BuildUpdateQuery(q *UpdateQuery) (string, []interface{}, error) {
	// Build the UPDATE query using the provided filter builder for complex WHERE logic.
	return b.buildUpdateQuery(q, "?", func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, true)
	})
}

// BuildBulkUpdateQuery builds a SQL bulk UPDATE statement and its arguments for MySQL.
func (b *mysqlBuilder) BuildBulkUpdateQuery(q *BulkUpdateQuery) (string, []interface{}, error) {
	if q == nil || q.Table == "" || len(q.FieldsValues) == 0 {
		return "", nil, ErrInvalidBulkUpdateQuery
	}
	if q.PrimaryKey == "" {
		return "", nil, ErrInvalidBulkUpdateQueryPrimaryKey
	}

	columns := make([]string, 0, len(q.FieldsValues[0]))
	for col := range q.FieldsValues[0] {
		if col != q.PrimaryKey {
			columns = append(columns, col)
		}
	}
	sort.Strings(columns)

	if len(columns) == 0 {
		return "", nil, ErrInvalidBulkUpdateQuery
	}

	var args []interface{}

	// Estimate the total buffer size up front so the whole query fits in a single allocation
	total := 64 + len(q.Table) + len(q.PrimaryKey)*2 + len(columns)*6 + len(q.FieldsValues)*(16+len(columns)*8)
	buf := make([]byte, 0, total)

	buf = append(buf, "UPDATE "...)
	buf = append(buf, q.Table...)
	buf = append(buf, " AS t JOIN ("...)
	for i, row := range q.FieldsValues {
		pkVal, ok := row[q.PrimaryKey]
		if !ok {
			return "", nil, ErrInvalidBulkUpdateQueryPrimaryKey
		}

		args = append(args, pkVal)

		if i > 0 {
			buf = append(buf, " UNION ALL "...)
		}

		// Build the "SELECT <placeholder list>" for this row inline.
		// The first row aliases placeholders with column names; later rows use plain "?".
		buf = append(buf, "SELECT "...)
		if i == 0 {
			buf = append(buf, "? AS "...)
			buf = append(buf, q.PrimaryKey...)
			for _, col := range columns {
				args = append(args, row[col])
				buf = append(buf, ", ? AS "...)
				buf = append(buf, col...)
			}
		} else {
			buf = append(buf, '?')
			for _, col := range columns {
				args = append(args, row[col])
				buf = append(buf, ", ?"...)
			}
		}
	}
	buf = append(buf, ") AS c ON t."...)
	buf = append(buf, q.PrimaryKey...)
	buf = append(buf, " = c."...)
	buf = append(buf, q.PrimaryKey...)
	buf = append(buf, " SET "...)
	for i, col := range columns {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, "t."...)
		buf = append(buf, col...)
		buf = append(buf, " = c."...)
		buf = append(buf, col...)
	}

	return string(buf), args, nil
}

// buildFields returns the SQL representation of fields for MySQL, supporting subqueries and aliases.
func (b *mysqlBuilder) buildFields(fields []Field, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildFields(fields, args, b.BuildSelectQuery)
}

// buildFilter returns the SQL representation of a filter condition for MySQL, supporting nested filters and logical operators.
func (b *mysqlBuilder) buildFilter(f *Filter, args *[]interface{}, isRoot bool) (string, error) {
	return b.dynamicQueryBuilder.buildFilter(f, args, isRoot, b.BuildSelectQuery)
}

// buildGroupBy returns the SQL representation of a GROUP BY clause for MySQL.
func (b *mysqlBuilder) buildGroupBy(fields []Field) (string, error) {
	return b.dynamicQueryBuilder.buildGroupBy(fields)
}

// buildJoins returns the SQL representation of JOIN clauses for MySQL, supporting subqueries, join types, and ON conditions.
func (b *mysqlBuilder) buildJoins(joins []Join, args *[]interface{}) (string, error) {
	// Use the shared logic from dynamicQueryBuilder for JOIN clause construction.
	return b.dynamicQueryBuilder.buildJoins(
		joins,
		args,
		b.BuildSelectQuery,
		func(f *Filter, args *[]interface{}) (string, error) {
			return b.buildFilter(f, args, true)
		},
	)
}

// buildOrderBy returns the SQL representation of an ORDER BY clause for MySQL.
func (b *mysqlBuilder) buildOrderBy(sorts []Sort) (string, error) {
	return b.dynamicQueryBuilder.buildOrderBy(sorts)
}

// buildTable returns the SQL representation of a table or subquery for MySQL, supporting table names and subqueries with aliasing.
func (b *mysqlBuilder) buildTable(t Table, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildTable(t, args, b.BuildSelectQuery)
}
