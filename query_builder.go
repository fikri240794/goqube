package goqube

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// queryBuilder defines the interface for building SQL queries for different operations and dialects.
type queryBuilder interface {
	BuildDeleteQuery(q *DeleteQuery) (query string, args []interface{}, err error)
	BuildInsertQuery(q *InsertQuery) (query string, args []interface{}, err error)
	BuildSelectQuery(q *SelectQuery) (query string, args []interface{}, err error)
	BuildUpdateQuery(q *UpdateQuery) (query string, args []interface{}, err error)
}

// dynamicQueryBuilder provides shared logic for building SQL queries with customizable placeholder formats.
type dynamicQueryBuilder struct {
	placeholderFormat string // placeholderFormat defines the format for query parameter placeholders (e.g., ?, $1).
}

// buildFieldForFilter returns the SQL representation of a field for use in filter conditions.
func (b *dynamicQueryBuilder) buildFieldForFilter(f Field, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	if f.SelectQuery != nil {
		// Build subquery and wrap it, optionally with alias
		sub, _, err := buildSelectQuery(f.SelectQuery)
		if err != nil {
			return "", err
		}
		if f.Alias != "" {
			return fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sub), f.Alias), nil
		}
		return fmt.Sprintf("(%s)", strings.TrimSpace(sub)), nil
	}
	if f.Table != "" && f.Column != "" {
		// Return qualified column name (table.column)
		return fmt.Sprintf("%s.%s", f.Table, f.Column), nil
	}
	if f.Column != "" {
		return f.Column, nil
	}
	return "", ErrInvalidFilter
}

// buildFields returns the SQL representation of a slice of fields for use in SELECT, GROUP BY, or ORDER BY clauses.
func (b *dynamicQueryBuilder) buildFields(fields []Field, args *[]interface{}, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	// Preallocate slice for better performance
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f.SelectQuery != nil {
			// Build subquery and collect its arguments
			sub, subArgs, err := buildSelectQuery(f.SelectQuery)
			if err != nil {
				return "", err
			}
			*args = append(*args, subArgs...)
			if f.Alias != "" {
				out = append(out, fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sub), f.Alias))
			} else {
				out = append(out, fmt.Sprintf("(%s)", strings.TrimSpace(sub)))
			}
		} else if f.Table != "" && f.Column != "" {
			// Handle qualified column with optional alias
			if f.Alias != "" {
				out = append(out, fmt.Sprintf("%s.%s AS %s", f.Table, f.Column, f.Alias))
			} else {
				out = append(out, fmt.Sprintf("%s.%s", f.Table, f.Column))
			}
		} else if f.Column != "" {
			// Handle plain column with optional alias
			if f.Alias != "" {
				out = append(out, fmt.Sprintf("%s AS %s", f.Column, f.Alias))
			} else {
				out = append(out, f.Column)
			}
		} else {
			return "", ErrInvalidField
		}
	}
	return strings.Join(out, ", "), nil
}

// buildFilter returns the SQL representation of a filter condition, supporting nested filters and logical operators.
func (b *dynamicQueryBuilder) buildFilter(f *Filter, args *[]interface{}, isRoot bool, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	if f == nil {
		return "", nil
	}
	if len(f.Filters) > 0 {
		// Process nested filters recursively and collect valid parts
		parts := make([]string, 0, len(f.Filters))
		for _, sub := range f.Filters {
			part, err := b.buildFilter(&sub, args, false, buildSelectQuery)
			if err != nil {
				return "", err
			}
			if part != "" {
				parts = append(parts, part)
			}
		}
		// Join with logical operator and clean up spacing
		joined := strings.Join(parts, fmt.Sprintf(" %s ", f.Logic))
		// Normalize multiple spaces that might occur from complex nested filters
		for strings.Contains(joined, "  ") {
			joined = strings.ReplaceAll(joined, "  ", " ")
		}
		joined = strings.TrimSpace(joined)
		if isRoot {
			// Root filters don't need parentheses for proper SQL structure
			return joined, nil
		}
		// Non-root filters need parentheses to maintain correct precedence
		return fmt.Sprintf("(%s)", joined), nil
	}
	// Handle simple filter: field operator value
	fieldStr, err := b.buildFieldForFilter(f.Field, buildSelectQuery)
	if err != nil {
		return "", err
	}
	valueStr, err := b.buildFilterValue(f.Operator, f.Value, args)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s %s", fieldStr, f.Operator, valueStr), nil
}

// buildFilterValue returns the SQL representation of a filter value for use in WHERE or HAVING clauses.
func (b *dynamicQueryBuilder) buildFilterValue(op Operator, v FilterValue, args *[]interface{}) (string, error) {
	// Check for column references first (most common case)
	if v.Column != "" {
		if v.Table != "" {
			return fmt.Sprintf("%s.%s", v.Table, v.Column), nil
		}
		return v.Column, nil
	}

	// Handle NULL operators that don't require values
	if op == OperatorIsNull || op == OperatorIsNotNull {
		return "", nil
	}

	// Handle IN/NOT IN operators with slice/array validation
	if op == OperatorIn || op == OperatorNotIn {
		val := reflect.ValueOf(v.Value)
		if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
			return "", ErrOperatorArray
		}
		valLen := val.Len()
		if valLen == 0 {
			return "", ErrOperatorArrayEmpty
		}

		// Preallocate slice and avoid unnecessary TrimSpace call
		placeholders := make([]string, valLen)
		for i := 0; i < valLen; i++ {
			*args = append(*args, val.Index(i).Interface())
			placeholders[i] = "?"
		}
		return fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")), nil
	}

	// Default case: single parameter placeholder
	*args = append(*args, v.Value)
	return "?", nil
}

// buildGroupBy returns the SQL representation of a GROUP BY clause from a slice of fields.
func (b *dynamicQueryBuilder) buildGroupBy(fields []Field) (string, error) {
	// Preallocate slice for better performance when we have fields to process
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f.Table != "" && f.Column != "" {
			out = append(out, fmt.Sprintf("%s.%s", f.Table, f.Column))
		} else if f.Column != "" {
			out = append(out, f.Column)
		} else {
			return "", ErrInvalidGroupBy
		}
	}
	return strings.Join(out, ", "), nil
}

// buildJoins returns the SQL representation of JOIN clauses from a slice of Join structs.
func (b *dynamicQueryBuilder) buildJoins(
	joins []Join,
	args *[]interface{},
	buildSelectQuery func(*SelectQuery) (string, []interface{}, error),
	buildFilter func(f *Filter, args *[]interface{}) (string, error),
) (string, error) {
	// Preallocate slice for better performance when processing multiple joins
	out := make([]string, 0, len(joins))
	for _, j := range joins {
		table, err := b.buildTable(j.Table, args, buildSelectQuery)
		if err != nil {
			return "", err
		}
		filter, err := buildFilter(&j.Filter, args)
		if err != nil {
			return "", err
		}
		// Convert join type to uppercase for SQL standard compliance
		joinStr := fmt.Sprintf("%s %s ON %s", strings.ToUpper(string(j.Type)), table, filter)
		out = append(out, joinStr)
	}
	return strings.Join(out, " "), nil
}

// buildOrderBy returns the SQL representation of an ORDER BY clause from a slice of Sort structs.
func (b *dynamicQueryBuilder) buildOrderBy(sorts []Sort) (string, error) {
	// Preallocate slice for better performance when processing multiple sorts
	out := make([]string, 0, len(sorts))
	for _, s := range sorts {
		f := s.Field
		var orderExpr string
		if f.Table != "" && f.Column != "" {
			orderExpr = fmt.Sprintf("%s.%s %s", f.Table, f.Column, s.Direction)
		} else if f.Column != "" {
			orderExpr = fmt.Sprintf("%s %s", f.Column, s.Direction)
		} else {
			return "", ErrInvalidOrderBy
		}
		// Trim any extra spaces from the order expression
		out = append(out, strings.TrimSpace(orderExpr))
	}
	return strings.Join(out, ", "), nil
}

// buildPlaceholdersAndArgs generates SQL placeholders and argument slices for INSERT and UPDATE queries.
func (b *dynamicQueryBuilder) buildPlaceholdersAndArgs(values interface{}, columns []string, format string) (string, []interface{}) {
	var (
		rows         []string
		placeholders []string
		args         []interface{}
		idx          = 1
	)
	switch v := values.(type) {
	case []map[string]interface{}:
		// Preallocate slices for better performance when processing multiple rows
		rows = make([]string, 0, len(v))
		args = make([]interface{}, 0, len(v)*len(columns))

		for _, row := range v {
			// Preallocate inner slice for row placeholders
			ph := make([]string, 0, len(columns))
			for _, col := range columns {
				if format == "?" {
					ph = append(ph, "?")
				} else {
					ph = append(ph, fmt.Sprintf(format, idx))
					idx++
				}
				args = append(args, row[col])
			}
			rows = append(rows, fmt.Sprintf("(%s)", strings.TrimSpace(strings.Join(ph, ", "))))
		}
		return strings.Join(rows, ", "), args
	case map[string]interface{}:
		// Preallocate slices for better performance when processing single row
		placeholders = make([]string, 0, len(columns))
		args = make([]interface{}, 0, len(columns))

		for _, col := range columns {
			if format == "?" {
				placeholders = append(placeholders, fmt.Sprintf("%s = ?", col))
			} else {
				placeholders = append(placeholders, fmt.Sprintf("%s = "+format, col, idx))
				idx++
			}
			args = append(args, v[col])
		}
		return strings.Join(placeholders, ", "), args
	default:
		return "", nil
	}
}

// buildTable returns the SQL representation of a table or subquery for use in FROM or JOIN clauses.
func (b *dynamicQueryBuilder) buildTable(t Table, args *[]interface{}, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	if t.SelectQuery != nil {
		// Build subquery and collect its arguments
		sub, subArgs, err := buildSelectQuery(t.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)

		// Wrap subquery in parentheses, optionally with alias
		subQuery := fmt.Sprintf("(%s)", strings.TrimSpace(sub))
		if t.Alias != "" {
			return fmt.Sprintf("%s AS %s", subQuery, t.Alias), nil
		}
		return subQuery, nil
	}

	if t.Name != "" {
		// Handle regular table names with optional alias
		if t.Alias != "" {
			return fmt.Sprintf("%s AS %s", t.Name, t.Alias), nil
		}
		return t.Name, nil
	}

	return "", ErrInvalidTable
}

// buildDeleteQuery constructs a DELETE SQL statement for the specified table and filter.
func (b *dynamicQueryBuilder) buildDeleteQuery(
	table string,
	filter *Filter,
	args *[]interface{},
	buildFilter func(*Filter, *[]interface{}) (string, error),
) (string, []interface{}, error) {
	if table == "" {
		return "", nil, ErrInvalidDeleteQuery
	}

	// Use strings.Builder for efficient string concatenation
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(table)

	if filter != nil {
		where, err := buildFilter(filter, args)
		if err != nil {
			return "", nil, err
		}
		// Only add WHERE clause if filter produces non-empty result
		if where != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(where)
		}
	}

	return sb.String(), *args, nil
}

// buildInsertQuery constructs an INSERT SQL statement for the given table and values with support for multiple placeholder formats.
func (b *dynamicQueryBuilder) buildInsertQuery(
	q *InsertQuery,
	startIndex int,
	nextPlaceholder func(*int) string,
) (string, []interface{}, error) {
	if q == nil || q.Table == "" || len(q.Values) == 0 {
		return "", nil, ErrInvalidInsertQuery
	}

	// Extract and sort column names for consistent order
	columns := make([]string, 0, len(q.Values[0]))
	for col := range q.Values[0] {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Build query using appropriate placeholder strategy
	var placeholders string
	var args []interface{}
	if nextPlaceholder == nil {
		placeholders, args = b.buildPlaceholdersAndArgs(q.Values, columns, "?")
	} else {
		paramIndex := startIndex
		placeholders, args = b.buildPlaceholdersAndArgsWithIndex(q.Values, columns, &paramIndex, nextPlaceholder)
	}

	// Use strings.Builder for efficient string concatenation
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(q.Table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES ")
	sb.WriteString(placeholders)

	return sb.String(), args, nil
}

// buildUpdateQueryWithContinuousIndex constructs an UPDATE SQL statement with indexed placeholders.
func (b *dynamicQueryBuilder) buildUpdateQueryWithContinuousIndex(
	q *UpdateQuery,
	startIndex int,
	nextPlaceholder func(*int) string,
	buildFilter func(*Filter, *[]interface{}, *int, bool) (string, error),
) (string, []interface{}, error) {
	if q == nil || q.Table == "" || len(q.FieldsValue) == 0 {
		return "", nil, ErrInvalidUpdateQuery
	}

	// Preallocate field names slice for better memory efficiency
	fieldNames := make([]string, 0, len(q.FieldsValue))
	for col := range q.FieldsValue {
		fieldNames = append(fieldNames, col)
	}
	sort.Strings(fieldNames)

	paramIndex := startIndex
	setClause, args := b.buildPlaceholdersAndArgsWithIndex(q.FieldsValue, fieldNames, &paramIndex, nextPlaceholder)

	// Use strings.Builder for efficient string concatenation
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(q.Table)
	sb.WriteString(" SET ")
	sb.WriteString(setClause)

	if q.Filter != nil {
		where, err := buildFilter(q.Filter, &args, &paramIndex, true)
		if err != nil {
			return "", nil, err
		}
		// Only add WHERE clause if filter produces non-empty result
		if where != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(where)
		}
	}

	return sb.String(), args, nil
}

// buildUpdateQuery constructs an UPDATE SQL statement using default '?' placeholders.
func (b *dynamicQueryBuilder) buildUpdateQuery(
	q *UpdateQuery,
	nextPlaceholder func(*int) string,
	buildFilter func(*Filter, *[]interface{}) (string, error),
) (string, []interface{}, error) {
	if q == nil || q.Table == "" || len(q.FieldsValue) == 0 {
		return "", nil, ErrInvalidUpdateQuery
	}

	// Early validation: indexed placeholders require different method
	if nextPlaceholder != nil {
		return "", nil, fmt.Errorf("use buildUpdateQueryWithContinuousIndex for indexed placeholders")
	}

	// Preallocate field names slice for better memory efficiency
	fieldNames := make([]string, 0, len(q.FieldsValue))
	for col := range q.FieldsValue {
		fieldNames = append(fieldNames, col)
	}
	sort.Strings(fieldNames)

	setClause, args := b.buildPlaceholdersAndArgs(q.FieldsValue, fieldNames, "?")

	// Use strings.Builder for efficient string concatenation
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(q.Table)
	sb.WriteString(" SET ")
	sb.WriteString(setClause)

	if q.Filter != nil {
		where, err := buildFilter(q.Filter, &args)
		if err != nil {
			return "", nil, err
		}
		// Only add WHERE clause if filter produces non-empty result
		if where != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(where)
		}
	}

	return sb.String(), args, nil
}

// buildPlaceholdersAndArgsWithIndex generates SQL placeholders and argument slices with indexed placeholders.
func (b *dynamicQueryBuilder) buildPlaceholdersAndArgsWithIndex(
	values interface{},
	columns []string,
	paramIndex *int,
	nextPlaceholder func(*int) string,
) (string, []interface{}) {
	switch v := values.(type) {
	case []map[string]interface{}:
		// Preallocate slices for better memory efficiency with bulk operations
		placeholders := make([]string, 0, len(v))
		args := make([]interface{}, 0, len(v)*len(columns))

		for _, row := range v {
			rowPlaceholders := make([]string, len(columns))
			for i, col := range columns {
				args = append(args, row[col])
				rowPlaceholders[i] = nextPlaceholder(paramIndex)
			}
			// Avoid TrimSpace call since strings.Join doesn't add extra spaces
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
		}
		return strings.Join(placeholders, ", "), args

	case map[string]interface{}:
		// Preallocate slices for single row operations
		setParts := make([]string, len(columns))
		args := make([]interface{}, len(columns))

		for i, col := range columns {
			args[i] = v[col]
			setParts[i] = fmt.Sprintf("%s = %s", col, nextPlaceholder(paramIndex))
		}
		return strings.Join(setParts, ", "), args

	default:
		return "", nil
	}
}

// nextPlaceholder returns the next parameter placeholder string and increments the index.
func (b *dynamicQueryBuilder) nextPlaceholder(paramIndex *int) string {
	// Increment index first for both paths to avoid duplicate logic
	*paramIndex++

	if b.placeholderFormat == "?" {
		return "?"
	}

	// Use previous index value for custom format placeholders
	return fmt.Sprintf(b.placeholderFormat, *paramIndex-1)
}
