package goqube

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// sqlServerBuilder implements SQL query building logic specific to SQL Server.
type sqlServerBuilder struct {
	dynamicQueryBuilder
}

// newSQLServerBuilder creates a new sqlServerBuilder with SQL Server-style placeholders (e.g., @p1, @p2).
func newSQLServerBuilder() *sqlServerBuilder {
	return &sqlServerBuilder{dynamicQueryBuilder{placeholderFormat: "@p%d"}}
}

// adjustRawQueryPlaceholders adjusts placeholders in raw SQL to match the current parameter index for SQL Server
func (b *sqlServerBuilder) adjustRawQueryPlaceholders(rawSQL string, rawArgs []interface{}, paramIndex *int) (string, []interface{}) {
	if rawSQL == "" || len(rawArgs) == 0 {
		return rawSQL, rawArgs
	}

	// Track the starting parameter index for this raw query
	startIndex := *paramIndex

	// Use regex to find all @p0, @p1, etc. placeholders and adjust them
	re := regexp.MustCompile(`@p(\d+)`)
	adjustedSQL := re.ReplaceAllStringFunc(rawSQL, func(match string) string {
		// Extract the original placeholder number
		var originalIndex int
		fmt.Sscanf(match[2:], "%d", &originalIndex)
		// Map @p0 -> @p{startIndex}, @p1 -> @p{startIndex+1}, etc.
		return fmt.Sprintf("@p%d", startIndex+originalIndex)
	})

	// Advance the parameter index by the number of arguments
	*paramIndex += len(rawArgs)

	return adjustedSQL, rawArgs
}

// buildSelectQueryWithParamIndex builds a SelectQuery with parameter index awareness for subqueries
func (b *sqlServerBuilder) buildSelectQueryWithParamIndex(q *SelectQuery, paramIndex *int) (string, []interface{}, error) {
	if q == nil {
		return "", nil, ErrUnsupportedDialect
	}

	// Handle raw SQL queries with parameter index adjustment
	if q.Raw != "" {
		adjustedSQL, adjustedArgs := b.adjustRawQueryPlaceholders(q.Raw, q.RawArgs, paramIndex)
		return adjustedSQL, adjustedArgs, nil
	}

	// For non-raw queries, delegate to the standard BuildSelectQuery
	return b.BuildSelectQuery(q)
}

// BuildDeleteQuery builds a SQL DELETE statement and its arguments for SQL Server.
func (b *sqlServerBuilder) BuildDeleteQuery(q *DeleteQuery) (string, []interface{}, error) {
	if q == nil || q.Table == "" {
		return "", nil, ErrInvalidDeleteQuery
	}

	// Preallocate args slice with estimated capacity for typical DELETE queries
	args := make([]interface{}, 0, 8)
	paramIndex := 0

	// Use closure with indexed placeholders for SQL Server-specific parameter handling
	return b.buildDeleteQuery(q.Table, q.Filter, &args, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, &paramIndex, true)
	})
}

// BuildInsertQuery builds a SQL INSERT statement and its arguments for SQL Server.
func (b *sqlServerBuilder) BuildInsertQuery(q *InsertQuery) (string, []interface{}, error) {
	return b.buildInsertQuery(q, 0, b.nextPlaceholder)
}

// BuildSelectQuery builds a SQL SELECT statement and its arguments for SQL Server.
func (b *sqlServerBuilder) BuildSelectQuery(q *SelectQuery) (string, []interface{}, error) {
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

	// Initialize parameter index for SQL Server's @p0, @p1 placeholders
	paramIndex := 0

	fields, err := b.buildFieldsWithParamIndex(q.Fields, &args, &paramIndex)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(fields)

	table, err := b.buildTableWithParamIndex(q.Table, &args, &paramIndex)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(" FROM ")
	sb.WriteString(table)

	// Process JOINs only if they exist
	if len(q.Joins) > 0 {
		joins, err := b.buildJoins(q.Joins, &args, &paramIndex)
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

	// SQL Server pagination uses OFFSET/FETCH with specific syntax requirements
	if q.Skip > 0 {
		offsetPlaceholder := b.nextPlaceholder(&paramIndex)
		sb.WriteString(fmt.Sprintf(" OFFSET %s ROWS", offsetPlaceholder))
		args = append(args, int64(q.Skip))
		if q.Take > 0 {
			fetchPlaceholder := b.nextPlaceholder(&paramIndex)
			sb.WriteString(fmt.Sprintf(" FETCH NEXT %s ROWS ONLY", fetchPlaceholder))
			args = append(args, int64(q.Take))
		}
	} else if q.Take > 0 {
		// FETCH requires OFFSET, so use 0 when only TAKE is specified
		sb.WriteString(" OFFSET 0 ROWS")
		fetchPlaceholder := b.nextPlaceholder(&paramIndex)
		sb.WriteString(fmt.Sprintf(" FETCH NEXT %s ROWS ONLY", fetchPlaceholder))
		args = append(args, int64(q.Take))
	}

	// Handle aliasing with optimized string building
	if q.Alias != "" {
		return fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sb.String()), q.Alias), args, nil
	}

	// SQL Server-specific whitespace normalization and parentheses cleanup
	query := sb.String()
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.ReplaceAll(query, "\t", " ")
	query = strings.Join(strings.Fields(query), " ")
	query = strings.ReplaceAll(query, "( ", "(")
	query = strings.ReplaceAll(query, " )", ")")
	return query, args, nil
}

// BuildUpdateQuery builds a SQL UPDATE statement and its arguments for SQL Server.
func (b *sqlServerBuilder) BuildUpdateQuery(q *UpdateQuery) (string, []interface{}, error) {
	return b.buildUpdateQueryWithContinuousIndex(q, 0, b.nextPlaceholder, b.buildFilter)
}

// buildFields returns the SQL representation of fields for SQL Server, supporting subqueries and aliases.
func (b *sqlServerBuilder) buildFields(fields []Field, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildFields(fields, args, b.BuildSelectQuery)
}

// buildFieldsWithParamIndex returns the SQL representation of fields with parameter index awareness for SQL Server.
func (b *sqlServerBuilder) buildFieldsWithParamIndex(fields []Field, args *[]interface{}, paramIndex *int) (string, error) {
	return b.dynamicQueryBuilder.buildFields(fields, args, func(sq *SelectQuery) (string, []interface{}, error) {
		return b.buildSelectQueryWithParamIndex(sq, paramIndex)
	})
}

// buildTable returns the SQL representation of a table or subquery for SQL Server.
func (b *sqlServerBuilder) buildTable(t Table, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildTable(t, args, b.BuildSelectQuery)
}

// buildTableWithParamIndex returns the SQL representation of a table or subquery with parameter index awareness for SQL Server.
func (b *sqlServerBuilder) buildTableWithParamIndex(t Table, args *[]interface{}, paramIndex *int) (string, error) {
	return b.dynamicQueryBuilder.buildTable(t, args, func(sq *SelectQuery) (string, []interface{}, error) {
		return b.buildSelectQueryWithParamIndex(sq, paramIndex)
	})
}

// buildFieldForFilter returns the SQL representation of a field for use in filter conditions in SQL Server.
func (b *sqlServerBuilder) buildFieldForFilter(f Field) (string, error) {
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

// buildFilter returns the SQL representation of a filter condition for SQL Server.
func (b *sqlServerBuilder) buildFilter(f *Filter, args *[]interface{}, paramIndex *int, isRoot bool) (string, error) {
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

		// Direct string concatenation is more efficient than temporary variable reassignment
		joined := strings.Join(parts, " "+string(f.Logic)+" ")

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

	// Early return for NULL operators to avoid unnecessary processing
	if f.Operator == OperatorIsNull || f.Operator == OperatorIsNotNull {
		return fieldStr + " " + operator, nil
	}

	valueStr, err := b.buildFilterValue(f.Operator, f.Value, args, paramIndex)
	if err != nil {
		return "", err
	}

	return fieldStr + " " + operator + " " + valueStr, nil
}

// buildFilterValue returns the SQL representation of a filter value for SQL Server.
func (b *sqlServerBuilder) buildFilterValue(op Operator, v FilterValue, args *[]interface{}, paramIndex *int) (string, error) {
	if op == OperatorLike || op == OperatorNotLike {
		return b.buildFilterValueLike(v, args, paramIndex)
	}

	if v.SelectQuery != nil {
		// Use parameter index aware version for subqueries
		sub, subArgs, err := b.buildSelectQueryWithParamIndex(v.SelectQuery, paramIndex)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		return "(" + strings.TrimSpace(sub) + ")", nil
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
			placeholders[i] = b.nextPlaceholder(paramIndex)
		}

		// Avoid TrimSpace call since strings.Join doesn't produce extra spaces
		return "(" + strings.Join(placeholders, ", ") + ")", nil
	} else {
		*args = append(*args, v.Value)
		placeholder := b.nextPlaceholder(paramIndex)
		return placeholder, nil
	}
}

// buildFilterValueLike returns the SQL representation of a value for LIKE/NOT LIKE operators in SQL Server.
func (b *sqlServerBuilder) buildFilterValueLike(v FilterValue, args *[]interface{}, paramIndex *int) (string, error) {
	if v.SelectQuery != nil {
		_, subArgs, err := b.BuildSelectQuery(v.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		placeholder := b.nextPlaceholder(paramIndex)
		return "(" + placeholder + ")", nil
	} else if v.Table != "" && v.Column != "" {
		return v.Table + "." + v.Column, nil
	} else if v.Value != nil {
		strVal, ok := v.Value.(string)
		if !ok {
			return "", ErrLikeValueType
		}
		*args = append(*args, "%"+strVal+"%")
		placeholder := b.nextPlaceholder(paramIndex)
		return placeholder, nil
	} else {
		return "", ErrLikeValueTypeOrSubquery
	}
}

// buildGroupBy returns the SQL representation of the GROUP BY clause for SQL Server.
func (b *sqlServerBuilder) buildGroupBy(fields []Field) (string, error) {
	return b.dynamicQueryBuilder.buildGroupBy(fields)
}

// buildJoins returns the SQL representation of JOIN clauses for SQL Server, supporting subqueries and complex filter conditions.
func (b *sqlServerBuilder) buildJoins(joins []Join, args *[]interface{}, paramIndex *int) (string, error) {
	return b.dynamicQueryBuilder.buildJoins(
		joins,
		args,
		func(sq *SelectQuery) (string, []interface{}, error) {
			return b.buildSelectQueryWithParamIndex(sq, paramIndex)
		},
		func(f *Filter, args *[]interface{}) (string, error) {
			return b.buildFilter(f, args, paramIndex, true)
		},
	)
}

// buildOrderBy returns the SQL representation of the ORDER BY clause for SQL Server.
func (b *sqlServerBuilder) buildOrderBy(sorts []Sort) (string, error) {
	return b.dynamicQueryBuilder.buildOrderBy(sorts)
}

// nextPlaceholder returns the next parameter placeholder for SQL Server (e.g., @p1, @p2).
func (b *sqlServerBuilder) nextPlaceholder(paramIndex *int) string {
	return b.dynamicQueryBuilder.nextPlaceholder(paramIndex)
}
