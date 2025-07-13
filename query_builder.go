package goqube

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// queryBuilder defines the interface for building SQL queries for different operations and dialects.
// Each method returns the query string, its arguments, and an error if the query is invalid.
type queryBuilder interface {
	BuildDeleteQuery(q *DeleteQuery) (query string, args []interface{}, err error)
	BuildInsertQuery(q *InsertQuery) (query string, args []interface{}, err error)
	BuildSelectQuery(q *SelectQuery) (query string, args []interface{}, err error)
	BuildUpdateQuery(q *UpdateQuery) (query string, args []interface{}, err error)
}

// dynamicQueryBuilder provides shared logic for building SQL queries with customizable placeholder formats.
// It is embedded by concrete builders for specific SQL dialects.
type dynamicQueryBuilder struct {
	placeholderFormat string // placeholderFormat defines the format for query parameter placeholders (e.g., ?, $1).
}

// buildFieldForFilter returns the SQL representation of a field for use in filter conditions.
// It handles subqueries, table-qualified columns, and plain columns, returning an error if the field is invalid.
func (b *dynamicQueryBuilder) buildFieldForFilter(f Field, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	if f.SelectQuery != nil {
		// If the field is a subquery, build and wrap it, optionally with an alias.
		sub, _, err := buildSelectQuery(f.SelectQuery)
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

// buildFields returns the SQL representation of a slice of fields for use in SELECT, GROUP BY, or ORDER BY clauses.
// It handles subqueries, table-qualified columns, aliases, and collects arguments for subqueries as needed.
func (b *dynamicQueryBuilder) buildFields(fields []Field, args *[]interface{}, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	var out []string
	for _, f := range fields {
		if f.SelectQuery != nil {
			// If the field is a subquery, build it and append its arguments.
			sub, subArgs, err := buildSelectQuery(f.SelectQuery)
			if err != nil {
				return "", err
			}
			*args = append(*args, subArgs...)
			if f.Alias != "" {
				// Add alias for subquery field.
				out = append(out, fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sub), f.Alias))
			} else {
				out = append(out, fmt.Sprintf("(%s)", strings.TrimSpace(sub)))
			}
		} else if f.Table != "" && f.Column != "" {
			// If both table and column are set, add qualified column, optionally with alias.
			if f.Alias != "" {
				out = append(out, fmt.Sprintf("%s.%s AS %s", f.Table, f.Column, f.Alias))
			} else {
				out = append(out, fmt.Sprintf("%s.%s", f.Table, f.Column))
			}
		} else if f.Column != "" {
			// If only column is set, add column, optionally with alias.
			if f.Alias != "" {
				out = append(out, fmt.Sprintf("%s AS %s", f.Column, f.Alias))
			} else {
				out = append(out, f.Column)
			}
		} else {
			// Return error if the field is not valid for selection.
			return "", ErrInvalidField
		}
	}
	return strings.Join(out, ", "), nil
}

// buildFilter returns the SQL representation of a filter condition, supporting nested filters and logical operators.
// It recursively builds subfilters, handles root/non-root grouping, and returns an error if the filter is invalid.
func (b *dynamicQueryBuilder) buildFilter(f *Filter, args *[]interface{}, isRoot bool, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	if f == nil {
		// Return empty string if filter is nil.
		return "", nil
	}
	if len(f.Filters) > 0 {
		// If there are nested filters, build each part recursively.
		var parts []string
		for _, sub := range f.Filters {
			part, err := b.buildFilter(&sub, args, false, buildSelectQuery)
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
// It handles columns, table-qualified columns, NULL checks, IN/NOT IN with slices, and parameter placeholders.
func (b *dynamicQueryBuilder) buildFilterValue(op Operator, v FilterValue, args *[]interface{}) (string, error) {
	if v.Table != "" && v.Column != "" {
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
			placeholders[i] = "?"
		}
		// Join all placeholders for the IN/NOT IN clause.
		return fmt.Sprintf("(%s)", strings.TrimSpace(strings.Join(placeholders, ", "))), nil
	} else {
		// For other operators, use a single parameter placeholder.
		*args = append(*args, v.Value)
		return "?", nil
	}
}

// buildGroupBy returns the SQL representation of a GROUP BY clause from a slice of fields.
// It supports table-qualified columns and plain columns, and returns an error if a field is invalid.
func (b *dynamicQueryBuilder) buildGroupBy(fields []Field) (string, error) {
	var out []string
	for _, f := range fields {
		if f.Table != "" && f.Column != "" {
			// Add table-qualified column to GROUP BY.
			out = append(out, fmt.Sprintf("%s.%s", f.Table, f.Column))
		} else if f.Column != "" {
			// Add plain column to GROUP BY.
			out = append(out, f.Column)
		} else {
			// Return error if the field is not valid for GROUP BY.
			return "", ErrInvalidGroupBy
		}
	}
	return strings.Join(out, ", "), nil
}

// buildJoins returns the SQL representation of JOIN clauses from a slice of Join structs.
// It builds each join by resolving the table, join type, and ON filter condition, returning an error if any part is invalid.
func (b *dynamicQueryBuilder) buildJoins(
	joins []Join,
	args *[]interface{},
	buildSelectQuery func(*SelectQuery) (string, []interface{}, error),
	buildFilter func(f *Filter, args *[]interface{}) (string, error),
) (string, error) {
	var out []string
	for _, j := range joins {
		// Build the table part of the join (can be a subquery or table name).
		table, err := b.buildTable(j.Table, args, buildSelectQuery)
		if err != nil {
			return "", err
		}
		// Build the ON filter condition for the join.
		filter, err := buildFilter(&j.Filter, args)
		if err != nil {
			return "", err
		}
		// Format the JOIN clause with type, table, and ON condition.
		joinStr := fmt.Sprintf("%s %s ON %s", strings.ToUpper(string(j.Type)), table, filter)
		out = append(out, joinStr)
	}
	return strings.Join(out, " "), nil
}

// buildOrderBy returns the SQL representation of an ORDER BY clause from a slice of Sort structs.
// It supports table-qualified columns, plain columns, and sort directions, and returns an error if a field is invalid.
func (b *dynamicQueryBuilder) buildOrderBy(sorts []Sort) (string, error) {
	var out []string
	for _, s := range sorts {
		f := s.Field
		if f.Table != "" && f.Column != "" {
			// Add table-qualified column and direction to ORDER BY.
			out = append(out, fmt.Sprintf("%s.%s %s", f.Table, f.Column, s.Direction))
		} else if f.Column != "" {
			// Add plain column and direction to ORDER BY.
			out = append(out, fmt.Sprintf("%s %s", f.Column, s.Direction))
		} else {
			// Return error if the field is not valid for ORDER BY.
			return "", ErrInvalidOrderBy
		}
	}
	// Trim spaces for each ORDER BY part.
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	return strings.Join(out, ", "), nil
}

// buildPlaceholdersAndArgs generates SQL placeholders and argument slices for INSERT and UPDATE queries.
// It supports both single-row (map) and multi-row (slice of maps) input, and handles custom or default placeholder formats.
func (b *dynamicQueryBuilder) buildPlaceholdersAndArgs(values interface{}, columns []string, format string) (string, []interface{}) {
	var (
		rows         []string
		placeholders []string
		args         []interface{}
		idx          = 1
	)
	switch v := values.(type) {
	case []map[string]interface{}:
		// Handle multiple rows for bulk INSERT.
		for _, row := range v {
			var ph []string
			for _, col := range columns {
				if format == "?" {
					ph = append(ph, "?")
				} else {
					// Use custom placeholder format (e.g., $1, $2).
					ph = append(ph, fmt.Sprintf(format, idx))
					idx++
				}
				args = append(args, row[col])
			}
			rows = append(rows, fmt.Sprintf("(%s)", strings.TrimSpace(strings.Join(ph, ", "))))
		}
		return strings.Join(rows, ", "), args
	case map[string]interface{}:
		// Handle single row for UPDATE or single-row INSERT.
		for _, col := range columns {
			if format == "?" {
				placeholders = append(placeholders, fmt.Sprintf("%s = ?", col))
			} else {
				// Use custom placeholder format for each column.
				placeholders = append(placeholders, fmt.Sprintf("%s = "+format, col, idx))
				idx++
			}
			args = append(args, v[col])
		}
		return strings.Join(placeholders, ", "), args
	default:
		// Return empty string and nil if input is not supported.
		return "", nil
	}
}

// buildTable returns the SQL representation of a table or subquery for use in FROM or JOIN clauses.
// It supports subqueries with optional aliases, plain table names, and returns an error if the table is invalid.
func (b *dynamicQueryBuilder) buildTable(t Table, args *[]interface{}, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	if t.SelectQuery != nil {
		// If the table is a subquery, build it and append its arguments.
		sub, subArgs, err := buildSelectQuery(t.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		if t.Alias != "" {
			// Add alias for subquery table.
			return fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sub), t.Alias), nil
		}
		return fmt.Sprintf("(%s)", strings.TrimSpace(sub)), nil
	} else if t.Name != "" {
		// If table name is set, add it, optionally with alias.
		if t.Alias != "" {
			return fmt.Sprintf("%s AS %s", t.Name, t.Alias), nil
		}
		return t.Name, nil
	}
	// Return error if the table is not valid.
	return "", ErrInvalidTable
}

// buildDeleteQuery constructs a DELETE SQL statement for the specified table and filter.
// It returns the query string, its arguments, and an error if the table name is empty or filter building fails.
func (b *dynamicQueryBuilder) buildDeleteQuery(
	table string,
	filter *Filter,
	args *[]interface{},
	buildFilter func(*Filter, *[]interface{}) (string, error),
) (string, []interface{}, error) {
	if table == "" {
		// Table name must not be empty for a valid DELETE query.
		return "", nil, ErrInvalidDeleteQuery
	}
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(table)
	if filter != nil {
		// Build the WHERE clause if a filter is provided.
		where, err := buildFilter(filter, args)
		if err != nil {
			return "", nil, err
		}
		if where != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(where)
		}
	}
	return sb.String(), *args, nil
}

// buildInsertQuery constructs an INSERT SQL statement for the given table and values.
// It supports both default and custom placeholder formats, returning the query, its arguments, and an error if input is invalid.
func (b *dynamicQueryBuilder) buildInsertQuery(
	q *InsertQuery,
	startIndex int,
	nextPlaceholder func(*int) string,
) (string, []interface{}, error) {
	if q == nil || q.Table == "" || len(q.Values) == 0 {
		// Table name and values must be provided for a valid INSERT query.
		return "", nil, ErrInvalidInsertQuery
	}
	// Extract column names from the first value map.
	columns := make([]string, 0, len(q.Values[0]))
	for col := range q.Values[0] {
		columns = append(columns, col)
	}
	sort.Strings(columns) // Ensure columns are in a consistent order.
	if nextPlaceholder == nil {
		// Use default '?' placeholders for parameterized queries.
		placeholders, args := b.buildPlaceholdersAndArgs(q.Values, columns, "?")
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", q.Table, strings.Join(columns, ", "), placeholders)
		return query, args, nil
	} else {
		// Use custom placeholder format (e.g., $1, $2) for dialects like PostgreSQL.
		paramIndex := startIndex
		placeholders, args := b.buildPlaceholdersAndArgsWithIndex(q.Values, columns, &paramIndex, nextPlaceholder)
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", q.Table, strings.Join(columns, ", "), placeholders)
		return query, args, nil
	}
}

// buildUpdateQueryWithContinuousIndex constructs an UPDATE SQL statement with indexed placeholders (e.g., $1, $2).
// It returns the query string, its arguments, and an error if the input is invalid or filter building fails.
func (b *dynamicQueryBuilder) buildUpdateQueryWithContinuousIndex(
	q *UpdateQuery,
	startIndex int,
	nextPlaceholder func(*int) string,
	buildFilter func(*Filter, *[]interface{}, *int, bool) (string, error),
) (string, []interface{}, error) {
	if q == nil || q.Table == "" || len(q.FieldsValue) == 0 {
		// Table name and fields must be provided for a valid UPDATE query.
		return "", nil, ErrInvalidUpdateQuery
	}
	var sb strings.Builder
	// Collect and sort field names for consistent SET clause order.
	fieldNames := make([]string, 0, len(q.FieldsValue))
	for col := range q.FieldsValue {
		fieldNames = append(fieldNames, col)
	}
	sort.Strings(fieldNames)
	paramIndex := startIndex
	// Build the SET clause with indexed placeholders and collect arguments.
	setClause, args := b.buildPlaceholdersAndArgsWithIndex(q.FieldsValue, fieldNames, &paramIndex, nextPlaceholder)
	sb.WriteString(fmt.Sprintf("UPDATE %s SET %s", q.Table, setClause))
	if q.Filter != nil {
		// Build the WHERE clause if a filter is provided, using the current parameter index.
		where, err := buildFilter(q.Filter, &args, &paramIndex, true)
		if err != nil {
			return "", nil, err
		}
		if where != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(where)
		}
	}
	return sb.String(), args, nil
}

// buildUpdateQuery constructs an UPDATE SQL statement using default '?' placeholders.
// It returns the query string, its arguments, and an error if the input is invalid or if indexed placeholders are requested.
func (b *dynamicQueryBuilder) buildUpdateQuery(
	q *UpdateQuery,
	nextPlaceholder func(*int) string,
	buildFilter func(*Filter, *[]interface{}) (string, error),
) (string, []interface{}, error) {
	if q == nil || q.Table == "" || len(q.FieldsValue) == 0 {
		// Table name and fields must be provided for a valid UPDATE query.
		return "", nil, ErrInvalidUpdateQuery
	}
	var sb strings.Builder
	// Collect and sort field names for consistent SET clause order.
	fieldNames := make([]string, 0, len(q.FieldsValue))
	for col := range q.FieldsValue {
		fieldNames = append(fieldNames, col)
	}
	sort.Strings(fieldNames)
	var args []interface{}
	var setClause string
	if nextPlaceholder == nil {
		// Use default '?' placeholders for parameterized queries.
		setClause, args = b.buildPlaceholdersAndArgs(q.FieldsValue, fieldNames, "?")
	} else {
		// Indexed placeholders are not supported in this method.
		return "", nil, fmt.Errorf("use buildUpdateQueryWithContinuousIndex for indexed placeholders")
	}
	sb.WriteString(fmt.Sprintf("UPDATE %s SET %s", q.Table, setClause))
	if q.Filter != nil {
		// Build the WHERE clause if a filter is provided.
		where, err := buildFilter(q.Filter, &args)
		if err != nil {
			return "", nil, err
		}
		if where != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(where)
		}
	}
	return sb.String(), args, nil
}

// buildPlaceholdersAndArgsWithIndex generates SQL placeholders and argument slices with indexed placeholders (e.g., $1, $2).
// It supports both multi-row (slice of maps) and single-row (map) input, returning the placeholders string and argument list.
func (b *dynamicQueryBuilder) buildPlaceholdersAndArgsWithIndex(
	values interface{},
	columns []string,
	paramIndex *int,
	nextPlaceholder func(*int) string,
) (string, []interface{}) {
	var (
		placeholders []string
		args         []interface{}
	)
	switch v := values.(type) {
	case []map[string]interface{}:
		// Handle multiple rows for bulk INSERT with indexed placeholders.
		for _, row := range v {
			rowPlaceholders := make([]string, len(columns))
			for i, col := range columns {
				args = append(args, row[col])
				// Generate the next indexed placeholder (e.g., $1, $2).
				rowPlaceholders[i] = nextPlaceholder(paramIndex)
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.TrimSpace(strings.Join(rowPlaceholders, ", "))))
		}
		return strings.Join(placeholders, ", "), args
	case map[string]interface{}:
		// Handle single row for UPDATE or single-row INSERT with indexed placeholders.
		setParts := make([]string, len(columns))
		for i, col := range columns {
			args = append(args, v[col])
			// Generate the next indexed placeholder for each column assignment.
			setParts[i] = fmt.Sprintf("%s = %s", col, nextPlaceholder(paramIndex))
		}
		return strings.Join(setParts, ", "), args
	default:
		// Return empty string and nil if input is not supported.
		return "", nil
	}
}

// nextPlaceholder returns the next parameter placeholder string based on the configured format (e.g., ?, $1, $2).
// It increments the parameter index for each call to ensure unique placeholders in the query.
func (b *dynamicQueryBuilder) nextPlaceholder(paramIndex *int) string {
	if b.placeholderFormat == "?" {
		// For default format, always return '?' and increment the index.
		(*paramIndex)++
		return "?"
	}
	// For custom formats (e.g., $1, $2), use the current index in the format string.
	placeholder := fmt.Sprintf(b.placeholderFormat, *paramIndex)
	(*paramIndex)++
	return placeholder
}
