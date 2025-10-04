package goqube

import (
	"fmt"
	"strings"
)

// sqliteBuilder implements SQL query building logic specific to SQLite.
type sqliteBuilder struct {
	dynamicQueryBuilder
}

// newSQLiteBuilder creates a new sqliteBuilder with SQLite-style placeholders ("?").
func newSQLiteBuilder() *sqliteBuilder {
	return &sqliteBuilder{dynamicQueryBuilder{placeholderFormat: "?"}}
}

// BuildDeleteQuery builds a SQL DELETE statement and its arguments for SQLite.
func (b *sqliteBuilder) BuildDeleteQuery(q *DeleteQuery) (string, []interface{}, error) {
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

// BuildInsertQuery builds a SQL INSERT statement and its arguments for SQLite.
func (b *sqliteBuilder) BuildInsertQuery(q *InsertQuery) (string, []interface{}, error) {
	return b.buildInsertQuery(q, 0, nil)
}

// BuildSelectQuery builds a SQL SELECT statement and its arguments for SQLite.
func (b *sqliteBuilder) BuildSelectQuery(q *SelectQuery) (string, []interface{}, error) {
	if q == nil {
		return "", nil, ErrUnsupportedDialect
	}

	// Early return for raw SQL queries to avoid unnecessary processing
	if q.Raw != "" {
		return q.Raw, q.RawArgs, nil
	}

	// Preallocate args slice with estimated capacity for typical SELECT queries
	args := make([]interface{}, 0, 16)
	var sb strings.Builder
	sb.WriteString("SELECT ")

	fields, err := b.buildFields(q.Fields, &args)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(fields)

	table, err := b.buildTable(q.Table, &args)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(" FROM ")
	sb.WriteString(table)

	// Process JOINs only if they exist
	if len(q.Joins) > 0 {
		joins, err := b.buildJoins(q.Joins, &args)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(" ")
		sb.WriteString(joins)
	}

	// Process WHERE clause only if filter exists and produces content
	if q.Filter != nil {
		where, err := b.buildFilter(q.Filter, &args, true)
		if err != nil {
			return "", nil, err
		}
		if where != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(where)
		}
	}

	// Process GROUP BY only if grouping fields exist
	if len(q.GroupByFields) > 0 {
		groupBy, err := b.buildGroupBy(q.GroupByFields)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(" GROUP BY ")
		sb.WriteString(groupBy)
	}

	// Process ORDER BY only if sorting is specified
	if len(q.Sorts) > 0 {
		orderBy, err := b.buildOrderBy(q.Sorts)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(" ORDER BY ")
		sb.WriteString(orderBy)
	}

	// Add pagination clauses only when needed
	if q.Take > 0 {
		sb.WriteString(" LIMIT ?")
		args = append(args, int64(q.Take))
	}

	if q.Skip > 0 {
		sb.WriteString(" OFFSET ?")
		args = append(args, int64(q.Skip))
	}

	// Handle aliasing with optimized string building
	if q.Alias != "" {
		return fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sb.String()), q.Alias), args, nil
	}

	// SQLite-specific whitespace normalization for optimal compatibility
	query := sb.String()
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.ReplaceAll(query, "\t", " ")
	query = strings.Join(strings.Fields(query), " ")
	return query, args, nil
}

// BuildUpdateQuery builds a SQL UPDATE statement and its arguments for SQLite.
func (b *sqliteBuilder) BuildUpdateQuery(q *UpdateQuery) (string, []interface{}, error) {
	// Build the UPDATE query using the provided filter builder for complex WHERE logic.
	return b.buildUpdateQuery(q, nil, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, true)
	})
}

// buildFields returns the SQL representation of fields for SQLite, supporting subqueries and aliases.
func (b *sqliteBuilder) buildFields(fields []Field, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildFields(fields, args, b.BuildSelectQuery)
}

// buildFilter returns the SQL representation of a filter condition for SQLite, supporting nested filters and logical operators.
func (b *sqliteBuilder) buildFilter(f *Filter, args *[]interface{}, isRoot bool) (string, error) {
	return b.dynamicQueryBuilder.buildFilter(f, args, isRoot, b.BuildSelectQuery)
}

// buildGroupBy returns the SQL representation of a GROUP BY clause for SQLite.
func (b *sqliteBuilder) buildGroupBy(fields []Field) (string, error) {
	return b.dynamicQueryBuilder.buildGroupBy(fields)
}

// buildJoins returns the SQL representation of JOIN clauses for SQLite, supporting subqueries, join types, and ON conditions.
func (b *sqliteBuilder) buildJoins(joins []Join, args *[]interface{}) (string, error) {
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

// buildOrderBy returns the SQL representation of an ORDER BY clause for SQLite.
func (b *sqliteBuilder) buildOrderBy(sorts []Sort) (string, error) {
	return b.dynamicQueryBuilder.buildOrderBy(sorts)
}

// buildTable returns the SQL representation of a table or subquery for SQLite, supporting table names and subqueries with aliasing.
func (b *sqliteBuilder) buildTable(t Table, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildTable(t, args, b.BuildSelectQuery)
}
