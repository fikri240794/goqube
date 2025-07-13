package goqube

import (
	"fmt"
	"reflect"
	"strings"
)

// postgresBuilder implements SQL query building logic specific to PostgreSQL.
// It embeds dynamicQueryBuilder and customizes placeholder formatting and SQL syntax.
type postgresBuilder struct {
	dynamicQueryBuilder
}

// newPostgresBuilder creates a new postgresBuilder with PostgreSQL-style placeholders (e.g., $1, $2).
func newPostgresBuilder() *postgresBuilder {
	return &postgresBuilder{dynamicQueryBuilder{placeholderFormat: "$%d"}}
}

// BuildDeleteQuery builds a SQL DELETE statement and its arguments for PostgreSQL.
// It returns the query string, the arguments slice, and an error if the query is invalid.
func (b *postgresBuilder) BuildDeleteQuery(q *DeleteQuery) (string, []interface{}, error) {
	// Validate that the query and table name are provided.
	if q == nil || q.Table == "" {
		return "", nil, ErrInvalidDeleteQuery
	}
	args := make([]interface{}, 0)
	paramIndex := 1
	// Build the DELETE query using the provided table, filter, and a custom filter builder for parameter indexing.
	return b.buildDeleteQuery(q.Table, q.Filter, &args, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, &paramIndex, true)
	})
}

// BuildInsertQuery builds a SQL INSERT statement and its arguments for PostgreSQL.
// It uses PostgreSQL-style indexed placeholders (e.g., $1, $2) and returns the query, arguments, and error if invalid.
func (b *postgresBuilder) BuildInsertQuery(q *InsertQuery) (string, []interface{}, error) {
	// Use buildInsertQuery with starting index 1 and the nextPlaceholder function for PostgreSQL.
	return b.buildInsertQuery(q, 1, b.nextPlaceholder)
}

// BuildSelectQuery builds a SQL SELECT statement and its arguments for PostgreSQL.
// It supports raw queries, subqueries, joins, filters, grouping, sorting, pagination, and aliasing, returning the query, arguments, and error if invalid.
func (b *postgresBuilder) BuildSelectQuery(q *SelectQuery) (string, []interface{}, error) {
	if q == nil {
		// Query object must not be nil.
		return "", nil, ErrInvalidFilter
	}
	if q.Raw != "" {
		// Return raw SQL if provided, bypassing builder logic.
		return q.Raw, nil, nil
	}
	args := make([]interface{}, 0)
	var sb strings.Builder
	paramIndex := 1
	sb.WriteString("SELECT ")
	// Build the SELECT fields, supporting subqueries and aliases.
	fields, err := b.buildFields(q.Fields, &args, b.BuildSelectQuery)
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
		joins, err := b.buildJoins(
			q.Joins,
			&args,
			b.BuildSelectQuery,
			func(f *Filter, args *[]interface{}) (string, error) {
				return b.buildFilter(f, args, &paramIndex, true)
			},
		)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(" ")
		sb.WriteString(joins)
	}
	// Build WHERE clause if a filter is provided.
	if q.Filter != nil {
		where, err := b.buildFilter(q.Filter, &args, &paramIndex, true)
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
		sb.WriteString(fmt.Sprintf(" LIMIT $%d", paramIndex))
		args = append(args, int64(q.Take))
		paramIndex++
	}
	// Add OFFSET clause for pagination if specified.
	if q.Skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET $%d", paramIndex))
		args = append(args, int64(q.Skip))
		paramIndex++
	}
	// Wrap the query in parentheses and alias if an alias is provided.
	if q.Alias != "" {
		return fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sb.String()), q.Alias), args, nil
	}
	return sb.String(), args, nil
}

// BuildUpdateQuery builds a SQL UPDATE statement and its arguments for PostgreSQL.
// It uses indexed placeholders (e.g., $1, $2) and returns the query, arguments, and error if invalid.
func (b *postgresBuilder) BuildUpdateQuery(q *UpdateQuery) (string, []interface{}, error) {
	// Use buildUpdateQueryWithContinuousIndex with starting index 1 and the nextPlaceholder function for PostgreSQL.
	return b.buildUpdateQueryWithContinuousIndex(q, 1, b.nextPlaceholder, b.buildFilter)
}

// buildFieldForFilter returns the SQL representation of a field for use in filter conditions in PostgreSQL.
// It supports subqueries, table-qualified columns, and plain columns, returning an error if the field is invalid.
func (b *postgresBuilder) buildFieldForFilter(f Field) (string, error) {
	if f.SelectQuery != nil {
		// If the field is a subquery, build and wrap it, optionally with an alias.
		sub, _, err := b.BuildSelectQuery(f.SelectQuery)
		if err != nil {
			return "", err
		}
		if f.Alias != "" {
			return fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sub), f.Alias), nil
		}
		return fmt.Sprintf("(%s)", strings.TrimSpace(sub)), nil
	} else if f.Table != "" && f.Column != "" {
		// If both table and column are set, return a qualified column name.
		return fmt.Sprintf("%s.%s", f.Table, f.Column), nil
	} else if f.Column != "" {
		// If only column is set, return the column name.
		return f.Column, nil
	}
	// Return an error if the field is not valid for filtering.
	return "", ErrInvalidFilter
}

// buildFilter returns the SQL representation of a filter condition for PostgreSQL, supporting nested filters and logical operators.
// It recursively builds subfilters, handles root/non-root grouping, and converts LIKE/NOT LIKE to ILIKE/NOT ILIKE for case-insensitive matching.
func (b *postgresBuilder) buildFilter(f *Filter, args *[]interface{}, paramIndex *int, isRoot bool) (string, error) {
	if f == nil {
		// Return empty string if filter is nil.
		return "", nil
	}
	if len(f.Filters) > 0 {
		// If there are nested filters, build each part recursively.
		var parts []string
		for _, sub := range f.Filters {
			part, err := b.buildFilter(&sub, args, paramIndex, false)
			if err != nil {
				return "", err
			}
			if part != "" {
				parts = append(parts, part)
			}
		}
		// Join all parts with the specified logical operator (AND/OR).
		joined := strings.Join(parts, fmt.Sprintf(" %s ", f.Logic))
		// Remove any double spaces for clean SQL.
		for strings.Contains(joined, "  ") {
			joined = strings.ReplaceAll(joined, "  ", " ")
		}
		joined = strings.TrimSpace(joined)
		if isRoot {
			// Do not wrap the root filter in parentheses.
			return joined, nil
		}
		// Wrap non-root filters in parentheses for correct SQL precedence.
		return fmt.Sprintf("(%s)", joined), nil
	}
	// Build the field and value for a simple filter condition.
	fieldStr, err := b.buildFieldForFilter(f.Field)
	if err != nil {
		return "", err
	}
	operator := string(f.Operator)
	// Convert LIKE/NOT LIKE to ILIKE/NOT ILIKE for case-insensitive search in PostgreSQL.
	switch operator {
	case "LIKE":
		operator = "ILIKE"
	case "NOT LIKE":
		operator = "NOT ILIKE"
	}
	valueStr, err := b.buildFilterValue(f.Operator, f.Value, args, paramIndex)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s %s", fieldStr, operator, valueStr), nil
}

// buildFilterValue returns the SQL representation of a filter value for use in WHERE or HAVING clauses in PostgreSQL.
// It handles subqueries, columns, NULL checks, IN/NOT IN with slices, LIKE/NOT LIKE, and indexed parameter placeholders.
func (b *postgresBuilder) buildFilterValue(op Operator, v FilterValue, args *[]interface{}, paramIndex *int) (string, error) {
	if op == OperatorLike || op == OperatorNotLike {
		// Delegate LIKE/NOT LIKE handling to a specialized function for PostgreSQL.
		return b.buildFilterValueLike(v, args, paramIndex)
	}
	if v.SelectQuery != nil {
		// If the value is a subquery, build it and append its arguments.
		sub, subArgs, err := b.BuildSelectQuery(v.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		return fmt.Sprintf("(%s)", strings.TrimSpace(sub)), nil
	} else if v.Table != "" && v.Column != "" {
		// If both table and column are set, return a qualified column name.
		return fmt.Sprintf("%s.%s", v.Table, v.Column), nil
	} else if v.Column != "" {
		// If only column is set, return the column name.
		return v.Column, nil
	} else if op == OperatorIsNull || op == OperatorIsNotNull {
		// For IS NULL/IS NOT NULL, no value is needed.
		return "", nil
	} else if op == OperatorIn || op == OperatorNotIn {
		// For IN/NOT IN, value must be a non-empty slice or array.
		val := reflect.ValueOf(v.Value)
		if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
			return "", ErrOperatorArray
		}
		if val.Len() == 0 {
			return "", ErrOperatorArrayEmpty
		}
		placeholders := make([]string, val.Len())
		for i := 0; i < val.Len(); i++ {
			*args = append(*args, val.Index(i).Interface())
			// Generate indexed placeholders for each value (e.g., $1, $2).
			placeholders[i] = fmt.Sprintf("$%d", *paramIndex)
			(*paramIndex)++
		}
		return fmt.Sprintf("(%s)", strings.TrimSpace(strings.Join(placeholders, ", "))), nil
	} else {
		// For other operators, use a single indexed parameter placeholder.
		*args = append(*args, v.Value)
		placeholder := fmt.Sprintf("$%d", *paramIndex)
		(*paramIndex)++
		return placeholder, nil
	}
}

// buildFilterValueLike returns the SQL representation of a LIKE/NOT LIKE filter value for PostgreSQL.
// It supports subqueries, table-qualified columns, and string values with pattern matching, using indexed placeholders.
func (b *postgresBuilder) buildFilterValueLike(v FilterValue, args *[]interface{}, paramIndex *int) (string, error) {
	if v.SelectQuery != nil {
		// If the value is a subquery, build it and append its arguments.
		_, subArgs, err := b.BuildSelectQuery(v.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		placeholder := fmt.Sprintf("($%d)", *paramIndex)
		(*paramIndex)++
		return placeholder, nil
	} else if v.Table != "" && v.Column != "" {
		// If both table and column are set, return a qualified column name.
		return fmt.Sprintf("%s.%s", v.Table, v.Column), nil
	} else if v.Value != nil {
		// For string values, wrap with % for pattern matching and use indexed placeholder.
		strVal, ok := v.Value.(string)
		if !ok {
			return "", ErrLikeValueType
		}
		*args = append(*args, fmt.Sprintf("%%%v%%", strVal))
		placeholder := fmt.Sprintf("$%d", *paramIndex)
		(*paramIndex)++
		return placeholder, nil
	} else {
		// Return error if value is not a string or subquery.
		return "", ErrLikeValueTypeOrSubquery
	}
}

// buildGroupBy returns the SQL representation of a GROUP BY clause for PostgreSQL.
// It delegates to the dynamicQueryBuilder implementation to support table-qualified and plain columns.
func (b *postgresBuilder) buildGroupBy(fields []Field) (string, error) {
	// Use the shared logic from dynamicQueryBuilder for GROUP BY clause construction.
	return b.dynamicQueryBuilder.buildGroupBy(fields)
}

// buildJoins returns the SQL representation of JOIN clauses for PostgreSQL.
// It delegates to the dynamicQueryBuilder implementation to handle subqueries, join types, and ON conditions.
func (b *postgresBuilder) buildJoins(
	joins []Join,
	args *[]interface{},
	buildSelectQuery func(*SelectQuery) (string, []interface{}, error),
	buildFilter func(f *Filter, args *[]interface{}) (string, error),
) (string, error) {
	// Use the shared logic from dynamicQueryBuilder for JOIN clause construction.
	return b.dynamicQueryBuilder.buildJoins(joins, args, buildSelectQuery, buildFilter)
}

// buildOrderBy returns the SQL representation of an ORDER BY clause for PostgreSQL.
// It delegates to the dynamicQueryBuilder implementation to support table-qualified columns, plain columns, and sort directions.
func (b *postgresBuilder) buildOrderBy(sorts []Sort) (string, error) {
	// Use the shared logic from dynamicQueryBuilder for ORDER BY clause construction.
	return b.dynamicQueryBuilder.buildOrderBy(sorts)
}

// buildTable returns the SQL representation of a table or subquery for PostgreSQL, supporting table names and subqueries with aliasing.
// It delegates to dynamicQueryBuilder and uses BuildSelectQuery for subquery handling.
func (b *postgresBuilder) buildTable(t Table, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildTable(t, args, b.BuildSelectQuery)
}

// nextPlaceholder generates the next indexed parameter placeholder (e.g., $1, $2) for PostgreSQL queries.
// It delegates to dynamicQueryBuilder to ensure consistent placeholder formatting.
func (b *postgresBuilder) nextPlaceholder(paramIndex *int) string {
	return b.dynamicQueryBuilder.nextPlaceholder(paramIndex)
}
