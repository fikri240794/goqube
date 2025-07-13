package goqube

import (
	"fmt"
	"strings"
)

// sqliteBuilder implements SQL query building logic specific to SQLite.
// It embeds dynamicQueryBuilder and customizes placeholder formatting and SQL syntax for SQLite compatibility.
type sqliteBuilder struct {
	dynamicQueryBuilder
}

// newSQLiteBuilder creates a new sqliteBuilder with SQLite-style placeholders ("?").
// This ensures that all parameterized queries use the correct placeholder format for SQLite dialects.
func newSQLiteBuilder() *sqliteBuilder {
	return &sqliteBuilder{dynamicQueryBuilder{placeholderFormat: "?"}}
}

// BuildDeleteQuery builds a SQL DELETE statement and its arguments for SQLite.
// It validates the input, prepares arguments, and delegates filter building to support complex WHERE conditions.
func (b *sqliteBuilder) BuildDeleteQuery(q *DeleteQuery) (string, []interface{}, error) {
	// Ensure the query and table name are provided.
	if q == nil || q.Table == "" {
		return "", nil, ErrInvalidDeleteQuery
	}
	args := make([]interface{}, 0)
	// Build the DELETE query using the provided table, filter, and a custom filter builder.
	return b.buildDeleteQuery(q.Table, q.Filter, &args, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, true)
	})
}

// BuildInsertQuery builds a SQL INSERT statement and its arguments for SQLite.
// It uses SQLite-style placeholders and delegates the construction to the dynamicQueryBuilder implementation.
func (b *sqliteBuilder) BuildInsertQuery(q *InsertQuery) (string, []interface{}, error) {
	return b.buildInsertQuery(q, 0, nil)
}

// BuildSelectQuery builds a SQL SELECT statement and its arguments for SQLite.
// It supports raw queries, subqueries, joins, filters, grouping, sorting, pagination, and aliasing, returning the query, arguments, and error if invalid.
func (b *sqliteBuilder) BuildSelectQuery(q *SelectQuery) (string, []interface{}, error) {
	// Return error if the query object is nil.
	if q == nil {
		return "", nil, ErrUnsupportedDialect
	}
	// Return raw SQL if provided, bypassing builder logic.
	if q.Raw != "" {
		return q.Raw, nil, nil
	}
	args := make([]interface{}, 0)
	var sb strings.Builder
	sb.WriteString("SELECT ")
	// Build the SELECT fields, supporting subqueries and aliases.
	fields, err := b.buildFields(q.Fields, &args)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(fields)
	// Build the FROM clause with table or subquery.
	table, err := b.buildTable(q.Table, &args)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(" FROM ")
	sb.WriteString(table)
	// Build JOIN clauses if present.
	if len(q.Joins) > 0 {
		joins, err := b.buildJoins(q.Joins, &args)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(" ")
		sb.WriteString(joins)
	}
	// Build WHERE clause if a filter is provided.
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
	// Build GROUP BY clause if grouping fields are provided.
	if len(q.GroupByFields) > 0 {
		groupBy, err := b.buildGroupBy(q.GroupByFields)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(" GROUP BY ")
		sb.WriteString(groupBy)
	}
	// Build ORDER BY clause if sorting is specified.
	if len(q.Sorts) > 0 {
		orderBy, err := b.buildOrderBy(q.Sorts)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(" ORDER BY ")
		sb.WriteString(orderBy)
	}
	// Add LIMIT clause for pagination if specified.
	if q.Take > 0 {
		sb.WriteString(" LIMIT ?")
		args = append(args, int64(q.Take))
	}
	// Add OFFSET clause for pagination if specified.
	if q.Skip > 0 {
		sb.WriteString(" OFFSET ?")
		args = append(args, int64(q.Skip))
	}
	// Wrap the query in parentheses and alias if an alias is provided.
	if q.Alias != "" {
		return fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sb.String()), q.Alias), args, nil
	}
	// Clean up whitespace and formatting for SQLite compatibility.
	query := sb.String()
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.ReplaceAll(query, "\t", " ")
	query = strings.Join(strings.Fields(query), " ")
	return query, args, nil
}

// BuildUpdateQuery builds a SQL UPDATE statement and its arguments for SQLite.
// It delegates the construction to dynamicQueryBuilder and uses a custom filter builder for WHERE conditions.
func (b *sqliteBuilder) BuildUpdateQuery(q *UpdateQuery) (string, []interface{}, error) {
	// Build the UPDATE query using the provided filter builder for complex WHERE logic.
	return b.buildUpdateQuery(q, nil, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, true)
	})
}

// buildFields returns the SQL representation of fields for SQLite, supporting subqueries and aliases.
// It delegates to dynamicQueryBuilder and uses BuildSelectQuery for subquery handling.
func (b *sqliteBuilder) buildFields(fields []Field, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildFields(fields, args, b.BuildSelectQuery)
}

// buildFilter returns the SQL representation of a filter condition for SQLite, supporting nested filters and logical operators.
// It delegates to dynamicQueryBuilder and uses BuildSelectQuery for subquery handling in filters.
func (b *sqliteBuilder) buildFilter(f *Filter, args *[]interface{}, isRoot bool) (string, error) {
	return b.dynamicQueryBuilder.buildFilter(f, args, isRoot, b.BuildSelectQuery)
}

// buildGroupBy returns the SQL representation of a GROUP BY clause for SQLite.
// It delegates to dynamicQueryBuilder to support table-qualified and plain columns.
func (b *sqliteBuilder) buildGroupBy(fields []Field) (string, error) {
	return b.dynamicQueryBuilder.buildGroupBy(fields)
}

// buildJoins returns the SQL representation of JOIN clauses for SQLite, supporting subqueries, join types, and ON conditions.
// It delegates to dynamicQueryBuilder and uses BuildSelectQuery and a custom filter builder for ON clause logic.
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
// It delegates to dynamicQueryBuilder to support table-qualified columns, plain columns, and sort directions.
func (b *sqliteBuilder) buildOrderBy(sorts []Sort) (string, error) {
	return b.dynamicQueryBuilder.buildOrderBy(sorts)
}

// buildTable returns the SQL representation of a table or subquery for SQLite, supporting table names and subqueries with aliasing.
// It delegates to dynamicQueryBuilder and uses BuildSelectQuery for subquery handling.
func (b *sqliteBuilder) buildTable(t Table, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildTable(t, args, b.BuildSelectQuery)
}
