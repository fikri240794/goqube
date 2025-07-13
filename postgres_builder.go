package goqube

import (
	"fmt"
	"reflect"
	"strings"
)

// postgresBuilder implements SQL query building logic specific to PostgreSQL.
type postgresBuilder struct {
	dynamicQueryBuilder
}

// newPostgresBuilder creates a new postgresBuilder with PostgreSQL-style placeholders (e.g., $1, $2).
func newPostgresBuilder() *postgresBuilder {
	return &postgresBuilder{dynamicQueryBuilder{placeholderFormat: "$%d"}}
}

// BuildDeleteQuery builds a SQL DELETE statement and its arguments for PostgreSQL.
func (b *postgresBuilder) BuildDeleteQuery(q *DeleteQuery) (string, []interface{}, error) {
	if q == nil || q.Table == "" {
		return "", nil, ErrInvalidDeleteQuery
	}

	// Preallocate args slice for better memory efficiency
	args := make([]interface{}, 0, 8)
	paramIndex := 1

	// Use closure to capture paramIndex for consistent placeholder generation
	return b.buildDeleteQuery(q.Table, q.Filter, &args, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, &paramIndex, true)
	})
}

// BuildInsertQuery builds a SQL INSERT statement and its arguments for PostgreSQL.
func (b *postgresBuilder) BuildInsertQuery(q *InsertQuery) (string, []interface{}, error) {
	return b.buildInsertQuery(q, 1, b.nextPlaceholder)
}

// BuildSelectQuery builds a SQL SELECT statement and its arguments for PostgreSQL.
func (b *postgresBuilder) BuildSelectQuery(q *SelectQuery) (string, []interface{}, error) {
	if q == nil {
		return "", nil, ErrInvalidFilter
	}

	// Early return for raw SQL queries to avoid unnecessary processing
	if q.Raw != "" {
		return q.Raw, nil, nil
	}

	// Preallocate args slice with estimated capacity for typical SELECT queries
	args := make([]interface{}, 0, 16)
	var sb strings.Builder
	paramIndex := 1

	sb.WriteString("SELECT ")

	fields, err := b.buildFields(q.Fields, &args, b.BuildSelectQuery)
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

	// Process WHERE clause only if filter exists and produces content
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

	// Add pagination clauses using direct string building for better performance
	if q.Take > 0 {
		sb.WriteString(" LIMIT $")
		sb.WriteString(fmt.Sprintf("%d", paramIndex))
		args = append(args, int64(q.Take))
		paramIndex++
	}

	if q.Skip > 0 {
		sb.WriteString(" OFFSET $")
		sb.WriteString(fmt.Sprintf("%d", paramIndex))
		args = append(args, int64(q.Skip))
		paramIndex++
	}

	// Handle aliasing with optimized string building
	if q.Alias != "" {
		return fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sb.String()), q.Alias), args, nil
	}

	return sb.String(), args, nil
}

// BuildUpdateQuery builds a SQL UPDATE statement and its arguments for PostgreSQL.
func (b *postgresBuilder) BuildUpdateQuery(q *UpdateQuery) (string, []interface{}, error) {
	return b.buildUpdateQueryWithContinuousIndex(q, 1, b.nextPlaceholder, b.buildFilter)
}

// buildFieldForFilter returns the SQL representation of a field for use in filter conditions in PostgreSQL.
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

// buildFilter returns the SQL representation of a filter condition for PostgreSQL.
func (b *postgresBuilder) buildFilter(f *Filter, args *[]interface{}, paramIndex *int, isRoot bool) (string, error) {
	if f == nil {
		return "", nil
	}
	if len(f.Filters) > 0 {
		// Preallocate parts slice for better memory efficiency with nested filters
		parts := make([]string, 0, len(f.Filters))
		for _, sub := range f.Filters {
			part, err := b.buildFilter(&sub, args, paramIndex, false)
			if err != nil {
				return "", err
			}
			if part != "" {
				parts = append(parts, part)
			}
		}
		joined := strings.Join(parts, fmt.Sprintf(" %s ", f.Logic))
		// Optimize space normalization with strings.ReplaceAll for better performance
		for strings.Contains(joined, "  ") {
			joined = strings.ReplaceAll(joined, "  ", " ")
		}
		joined = strings.TrimSpace(joined)
		if isRoot {
			return joined, nil
		}
		return fmt.Sprintf("(%s)", joined), nil
	}
	fieldStr, err := b.buildFieldForFilter(f.Field)
	if err != nil {
		return "", err
	}
	operator := string(f.Operator)
	// PostgreSQL-specific optimization: convert LIKE operations to case-insensitive ILIKE
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
func (b *postgresBuilder) buildFilterValue(op Operator, v FilterValue, args *[]interface{}, paramIndex *int) (string, error) {
	if op == OperatorLike || op == OperatorNotLike {
		return b.buildFilterValueLike(v, args, paramIndex)
	}
	if v.SelectQuery != nil {
		sub, subArgs, err := b.BuildSelectQuery(v.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		return fmt.Sprintf("(%s)", strings.TrimSpace(sub)), nil
	} else if v.Table != "" && v.Column != "" {
		return v.Table + "." + v.Column, nil
	} else if v.Column != "" {
		return v.Column, nil
	} else if op == OperatorIsNull || op == OperatorIsNotNull {
		return "", nil
	} else if op == OperatorIn || op == OperatorNotIn {
		val := reflect.ValueOf(v.Value)
		if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
			return "", ErrOperatorArray
		}
		valLen := val.Len()
		if valLen == 0 {
			return "", ErrOperatorArrayEmpty
		}
		// Preallocate slice with exact size for better performance
		placeholders := make([]string, valLen)
		for i := 0; i < valLen; i++ {
			*args = append(*args, val.Index(i).Interface())
			placeholders[i] = fmt.Sprintf("$%d", *paramIndex)
			(*paramIndex)++
		}
		// Avoid TrimSpace call since strings.Join doesn't produce extra spaces
		return fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")), nil
	} else {
		*args = append(*args, v.Value)
		placeholder := fmt.Sprintf("$%d", *paramIndex)
		(*paramIndex)++
		return placeholder, nil
	}
}

// buildFilterValueLike returns the SQL representation of a LIKE/NOT LIKE filter value for PostgreSQL.
func (b *postgresBuilder) buildFilterValueLike(v FilterValue, args *[]interface{}, paramIndex *int) (string, error) {
	if v.SelectQuery != nil {
		_, subArgs, err := b.BuildSelectQuery(v.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		placeholder := "($" + fmt.Sprintf("%d", *paramIndex) + ")"
		(*paramIndex)++
		return placeholder, nil
	} else if v.Table != "" && v.Column != "" {
		return v.Table + "." + v.Column, nil
	} else if v.Value != nil {
		strVal, ok := v.Value.(string)
		if !ok {
			return "", ErrLikeValueType
		}
		*args = append(*args, "%"+strVal+"%")
		placeholder := "$" + fmt.Sprintf("%d", *paramIndex)
		(*paramIndex)++
		return placeholder, nil
	} else {
		return "", ErrLikeValueTypeOrSubquery
	}
}

// buildGroupBy returns the SQL representation of a GROUP BY clause for PostgreSQL.
func (b *postgresBuilder) buildGroupBy(fields []Field) (string, error) {
	return b.dynamicQueryBuilder.buildGroupBy(fields)
}

// buildJoins returns the SQL representation of JOIN clauses for PostgreSQL.
func (b *postgresBuilder) buildJoins(
	joins []Join,
	args *[]interface{},
	buildSelectQuery func(*SelectQuery) (string, []interface{}, error),
	buildFilter func(f *Filter, args *[]interface{}) (string, error),
) (string, error) {
	return b.dynamicQueryBuilder.buildJoins(joins, args, buildSelectQuery, buildFilter)
}

// buildOrderBy returns the SQL representation of an ORDER BY clause for PostgreSQL.
func (b *postgresBuilder) buildOrderBy(sorts []Sort) (string, error) {
	return b.dynamicQueryBuilder.buildOrderBy(sorts)
}

// buildTable returns the SQL representation of a table or subquery for PostgreSQL, supporting table names and subqueries with aliasing.
func (b *postgresBuilder) buildTable(t Table, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildTable(t, args, b.BuildSelectQuery)
}

// nextPlaceholder generates the next indexed parameter placeholder (e.g., $1, $2) for PostgreSQL queries.
func (b *postgresBuilder) nextPlaceholder(paramIndex *int) string {
	return b.dynamicQueryBuilder.nextPlaceholder(paramIndex)
}
