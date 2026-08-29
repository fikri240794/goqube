package goqube

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// queryBuilder defines the interface for building SQL queries for different operations and dialects.
type queryBuilder interface {
	BuildDeleteQuery(q *DeleteQuery) (query string, args []interface{}, err error)
	BuildInsertQuery(q *InsertQuery) (query string, args []interface{}, err error)
	BuildSelectQuery(q *SelectQuery) (query string, args []interface{}, err error)
	BuildUpdateQuery(q *UpdateQuery) (query string, args []interface{}, err error)
	BuildBulkUpdateQuery(q *BulkUpdateQuery) (query string, args []interface{}, err error)
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
		trimmed := strings.TrimSpace(sub)
		buf := make([]byte, 0, len(trimmed)+len(f.Alias)+6)
		buf = append(buf, '(')
		buf = append(buf, trimmed...)
		buf = append(buf, ')')
		if f.Alias != "" {
			buf = append(buf, ' ', 'A', 'S', ' ')
			buf = append(buf, f.Alias...)
		}
		return string(buf), nil
	}
	if f.Table != "" && f.Column != "" {
		// Return qualified column name (table.column)
		buf := make([]byte, 0, len(f.Table)+len(f.Column)+1)
		buf = append(buf, f.Table...)
		buf = append(buf, '.')
		buf = append(buf, f.Column...)
		return string(buf), nil
	}
	if f.Column != "" {
		return f.Column, nil
	}
	return "", ErrInvalidFilter
}

// buildFields returns the SQL representation of a slice of fields for use in SELECT, GROUP BY, or ORDER BY clauses.
func (b *dynamicQueryBuilder) buildFields(fields []Field, args *[]interface{}, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	buf := make([]byte, 0, 64)
	for i, f := range fields {
		if i > 0 {
			buf = append(buf, ',', ' ')
		}
		if f.SelectQuery != nil {
			// Build subquery and collect its arguments
			sub, subArgs, err := buildSelectQuery(f.SelectQuery)
			if err != nil {
				return "", err
			}
			*args = append(*args, subArgs...)
			buf = append(buf, '(')
			buf = append(buf, strings.TrimSpace(sub)...)
			buf = append(buf, ')')
			if f.Alias != "" {
				buf = append(buf, ' ', 'A', 'S', ' ')
				buf = append(buf, f.Alias...)
			}
		} else if f.Table != "" && f.Column != "" {
			// Handle qualified column with optional alias
			buf = append(buf, f.Table...)
			buf = append(buf, '.')
			buf = append(buf, f.Column...)
			if f.Alias != "" {
				buf = append(buf, ' ', 'A', 'S', ' ')
				buf = append(buf, f.Alias...)
			}
		} else if f.Column != "" {
			// Handle plain column with optional alias
			buf = append(buf, f.Column...)
			if f.Alias != "" {
				buf = append(buf, ' ', 'A', 'S', ' ')
				buf = append(buf, f.Alias...)
			}
		} else {
			return "", ErrInvalidField
		}
	}
	return string(buf), nil
}

// buildFilter returns the SQL representation of a filter condition, supporting nested filters and logical operators.
func (b *dynamicQueryBuilder) buildFilter(f *Filter, args *[]interface{}, isRoot bool, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	if f == nil {
		return "", nil
	}
	if len(f.Filters) > 0 {
		// Process nested filters recursively and collect valid parts
		parts := make([]string, 0, len(f.Filters))
		for i := range f.Filters {
			part, err := b.buildFilter(&f.Filters[i], args, false, buildSelectQuery)
			if err != nil {
				return "", err
			}
			if part != "" {
				parts = append(parts, part)
			}
		}
		// Join with logical operator and clean up spacing
		joined := strings.Join(parts, " "+string(f.Logic)+" ")
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
		buf := make([]byte, 0, len(joined)+2)
		buf = append(buf, '(')
		buf = append(buf, joined...)
		buf = append(buf, ')')
		return string(buf), nil
	}
	// Handle simple filter: field operator value
	fieldStr, err := b.buildFieldForFilter(f.Field, buildSelectQuery)
	if err != nil {
		return "", err
	}
	valueStr, err := b.buildFilterValueWithSelectQuery(f.Operator, f.Value, args, buildSelectQuery)
	if err != nil {
		return "", err
	}
	// Build simple filter: field operator value
	buf := make([]byte, 0, len(fieldStr)+len(f.Operator)+len(valueStr)+2)
	buf = append(buf, fieldStr...)
	buf = append(buf, ' ')
	buf = append(buf, f.Operator...)
	buf = append(buf, ' ')
	buf = append(buf, valueStr...)
	return string(buf), nil
}

// buildFilterValue returns the SQL representation of a filter value for use in WHERE or HAVING clauses.
func (b *dynamicQueryBuilder) buildFilterValue(op Operator, v FilterValue, args *[]interface{}) (string, error) {
	// Check for column references first (most common case)
	if v.Column != "" {
		if v.Table != "" {
			buf := make([]byte, 0, len(v.Table)+len(v.Column)+1)
			buf = append(buf, v.Table...)
			buf = append(buf, '.')
			buf = append(buf, v.Column...)
			return string(buf), nil
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

		// Build placeholder list and collect argument values
		buf := make([]byte, 0, 2+valLen*3)
		buf = append(buf, '(')
		for i := 0; i < valLen; i++ {
			*args = append(*args, val.Index(i).Interface())
			if i > 0 {
				buf = append(buf, ',', ' ')
			}
			buf = append(buf, '?')
		}
		buf = append(buf, ')')
		return string(buf), nil
	}

	// Default case: single parameter placeholder
	*args = append(*args, v.Value)
	return "?", nil
}

// buildFilterValueWithSelectQuery returns the SQL representation of a filter value with SelectQuery support
func (b *dynamicQueryBuilder) buildFilterValueWithSelectQuery(op Operator, v FilterValue, args *[]interface{}, buildSelectQuery func(*SelectQuery) (string, []interface{}, error)) (string, error) {
	// Handle subquery first
	if v.SelectQuery != nil {
		sub, subArgs, err := buildSelectQuery(v.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		buf := make([]byte, 0, len(sub)+2)
		buf = append(buf, '(')
		buf = append(buf, strings.TrimSpace(sub)...)
		buf = append(buf, ')')
		return string(buf), nil
	}

	// For non-subquery cases, delegate to regular buildFilterValue
	return b.buildFilterValue(op, v, args)
}

// buildGroupBy returns the SQL representation of a GROUP BY clause from a slice of fields.
func (b *dynamicQueryBuilder) buildGroupBy(fields []Field) (string, error) {
	// First pass: validate fields and estimate buffer size for a single allocation
	total := 0
	for _, f := range fields {
		if f.Table != "" && f.Column != "" {
			total += len(f.Table) + len(f.Column) + 1
		} else if f.Column != "" {
			total += len(f.Column)
		} else {
			return "", ErrInvalidGroupBy
		}
	}
	if len(fields) == 0 {
		return "", nil
	}

	// Build the whole clause in a single buffer to avoid per-field allocations
	buf := make([]byte, 0, total+2*(len(fields)-1))
	for i, f := range fields {
		if i > 0 {
			buf = append(buf, ',', ' ')
		}
		if f.Table != "" && f.Column != "" {
			buf = append(buf, f.Table...)
			buf = append(buf, '.')
			buf = append(buf, f.Column...)
		} else {
			buf = append(buf, f.Column...)
		}
	}
	return string(buf), nil
}

// buildJoins returns the SQL representation of JOIN clauses from a slice of Join structs.
func (b *dynamicQueryBuilder) buildJoins(
	joins []Join,
	args *[]interface{},
	buildSelectQuery func(*SelectQuery) (string, []interface{}, error),
	buildFilter func(f *Filter, args *[]interface{}) (string, error),
) (string, error) {
	buf := make([]byte, 0, 64)
	for i := range joins {
		table, err := b.buildTable(joins[i].Table, args, buildSelectQuery)
		if err != nil {
			return "", err
		}
		filter, err := buildFilter(&joins[i].Filter, args)
		if err != nil {
			return "", err
		}
		// Convert join type to uppercase for SQL standard compliance
		if i > 0 {
			buf = append(buf, ' ')
		}
		buf = append(buf, strings.ToUpper(string(joins[i].Type))...)
		buf = append(buf, ' ')
		buf = append(buf, table...)
		buf = append(buf, ' ', 'O', 'N', ' ')
		buf = append(buf, filter...)
	}
	return string(buf), nil
}

// buildOrderBy returns the SQL representation of an ORDER BY clause from a slice of Sort structs.
func (b *dynamicQueryBuilder) buildOrderBy(sorts []Sort) (string, error) {
	buf := make([]byte, 0, 32)
	for i, s := range sorts {
		f := s.Field
		var orderExpr string
		if f.Table != "" && f.Column != "" {
			part := make([]byte, 0, len(f.Table)+len(f.Column)+len(s.Direction)+2)
			part = append(part, f.Table...)
			part = append(part, '.')
			part = append(part, f.Column...)
			part = append(part, ' ')
			part = append(part, s.Direction...)
			orderExpr = strings.TrimSpace(string(part))
		} else if f.Column != "" {
			part := make([]byte, 0, len(f.Column)+len(s.Direction)+1)
			part = append(part, f.Column...)
			part = append(part, ' ')
			part = append(part, s.Direction...)
			orderExpr = strings.TrimSpace(string(part))
		} else {
			return "", ErrInvalidOrderBy
		}
		if i > 0 {
			buf = append(buf, ',', ' ')
		}
		buf = append(buf, orderExpr...)
	}
	return string(buf), nil
}

// buildPlaceholdersAndArgs generates SQL placeholders and argument slices for INSERT and UPDATE queries.
func (b *dynamicQueryBuilder) buildPlaceholdersAndArgs(values interface{}, columns []string, format string) (string, []interface{}) {
	var (
		args []interface{}
		idx  = 1
	)
	switch v := values.(type) {
	case []map[string]interface{}:
		// Preallocate slices for better performance when processing multiple rows
		args = make([]interface{}, 0, len(v)*len(columns))
		buf := make([]byte, 0, len(v)*(len(columns)*3+2))

		for ri, row := range v {
			// Build row placeholder list with separators
			rowBuf := make([]byte, 0, len(columns)*3)
			for ci, col := range columns {
				if ci > 0 {
					rowBuf = append(rowBuf, ',', ' ')
				}
				if format == "?" {
					rowBuf = append(rowBuf, '?')
				} else {
					rowBuf = append(rowBuf, format[:len(format)-2]...)
					rowBuf = strconv.AppendInt(rowBuf, int64(idx), 10)
					idx++
				}
				args = append(args, row[col])
			}
			trimmed := strings.TrimSpace(string(rowBuf))
			if ri > 0 {
				buf = append(buf, ',', ' ')
			}
			buf = append(buf, '(')
			buf = append(buf, trimmed...)
			buf = append(buf, ')')
		}
		return string(buf), args
	case map[string]interface{}:
		// Preallocate slices for better performance when processing single row
		args = make([]interface{}, 0, len(columns))
		buf := make([]byte, 0, len(columns)*16)

		for i, col := range columns {
			if i > 0 {
				buf = append(buf, ',', ' ')
			}
			buf = append(buf, col...)
			buf = append(buf, ' ', '=')
			if format == "?" {
				buf = append(buf, ' ', '?')
			} else {
				buf = append(buf, ' ')
				buf = append(buf, format[:len(format)-2]...)
				buf = strconv.AppendInt(buf, int64(idx), 10)
				idx++
			}
			args = append(args, v[col])
		}
		return string(buf), args
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
		buf := make([]byte, 0, len(sub)+len(t.Alias)+6)
		buf = append(buf, '(')
		buf = append(buf, strings.TrimSpace(sub)...)
		buf = append(buf, ')')
		if t.Alias != "" {
			buf = append(buf, ' ', 'A', 'S', ' ')
			buf = append(buf, t.Alias...)
		}
		return string(buf), nil
	}

	if t.Name != "" {
		// Handle regular table names with optional alias
		if t.Alias != "" {
			buf := make([]byte, 0, len(t.Name)+len(t.Alias)+4)
			buf = append(buf, t.Name...)
			buf = append(buf, ' ', 'A', 'S', ' ')
			buf = append(buf, t.Alias...)
			return string(buf), nil
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

	// Build the query with a byte buffer for efficient concatenation
	buf := make([]byte, 0, len(table)+32)
	buf = append(buf, "DELETE FROM "...)
	buf = append(buf, table...)

	if filter != nil {
		where, err := buildFilter(filter, args)
		if err != nil {
			return "", nil, err
		}
		// Only add WHERE clause if filter produces non-empty result
		if where != "" {
			buf = append(buf, " WHERE "...)
			buf = append(buf, where...)
		}
	}

	return string(buf), *args, nil
}

// buildInsertQuery constructs an INSERT SQL statement for the given table and values with support for multiple placeholder formats.
func (b *dynamicQueryBuilder) buildInsertQuery(
	q *InsertQuery,
	startIndex int,
	placeholderFormat string,
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
	if placeholderFormat == "?" {
		placeholders, args = b.buildPlaceholdersAndArgs(q.Values, columns, "?")
	} else {
		paramIndex := startIndex
		placeholders, args = b.buildPlaceholdersAndArgsWithIndex(q.Values, columns, &paramIndex, placeholderFormat)
	}

	// Build the query with a byte buffer for efficient concatenation
	buf := make([]byte, 0, len(q.Table)+len(columns)*8+32)
	buf = append(buf, "INSERT INTO "...)
	buf = append(buf, q.Table...)
	buf = append(buf, " ("...)
	for i, col := range columns {
		if i > 0 {
			buf = append(buf, ',', ' ')
		}
		buf = append(buf, col...)
	}
	buf = append(buf, ") VALUES "...)
	buf = append(buf, placeholders...)

	return string(buf), args, nil
}

// buildUpdateQueryWithContinuousIndex constructs an UPDATE SQL statement with indexed placeholders.
func (b *dynamicQueryBuilder) buildUpdateQueryWithContinuousIndex(
	q *UpdateQuery,
	startIndex int,
	placeholderFormat string,
	buildFilter func(*Filter, *[]interface{}, int, bool) (string, int, error),
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
	setClause, args := b.buildPlaceholdersAndArgsWithIndex(q.FieldsValue, fieldNames, &paramIndex, placeholderFormat)

	// Build the query with a byte buffer for efficient concatenation
	buf := make([]byte, 0, len(q.Table)+len(setClause)+32)
	buf = append(buf, "UPDATE "...)
	buf = append(buf, q.Table...)
	buf = append(buf, " SET "...)
	buf = append(buf, setClause...)

	if q.Filter != nil {
		where, _, err := buildFilter(q.Filter, &args, paramIndex, true)
		if err != nil {
			return "", nil, err
		}
		// Only add WHERE clause if filter produces non-empty result
		if where != "" {
			buf = append(buf, " WHERE "...)
			buf = append(buf, where...)
		}
	}

	return string(buf), args, nil
}

// buildUpdateQuery constructs an UPDATE SQL statement using default '?' placeholders.
func (b *dynamicQueryBuilder) buildUpdateQuery(
	q *UpdateQuery,
	placeholderFormat string,
	buildFilter func(*Filter, *[]interface{}) (string, error),
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

	setClause, args := b.buildPlaceholdersAndArgs(q.FieldsValue, fieldNames, placeholderFormat)

	// Build the query with a byte buffer for efficient concatenation
	buf := make([]byte, 0, len(q.Table)+len(setClause)+32)
	buf = append(buf, "UPDATE "...)
	buf = append(buf, q.Table...)
	buf = append(buf, " SET "...)
	buf = append(buf, setClause...)

	if q.Filter != nil {
		where, err := buildFilter(q.Filter, &args)
		if err != nil {
			return "", nil, err
		}
		// Only add WHERE clause if filter produces non-empty result
		if where != "" {
			buf = append(buf, " WHERE "...)
			buf = append(buf, where...)
		}
	}

	return string(buf), args, nil
}

// buildPlaceholdersAndArgsWithIndex generates SQL placeholders and argument slices with indexed placeholders.
func (b *dynamicQueryBuilder) buildPlaceholdersAndArgsWithIndex(
	values interface{},
	columns []string,
	paramIndex *int,
	placeholderFormat string,
) (string, []interface{}) {
	switch v := values.(type) {
	case []map[string]interface{}:
		// Preallocate slices for better memory efficiency with bulk operations
		args := make([]interface{}, 0, len(v)*len(columns))
		buf := make([]byte, 0, len(v)*(len(columns)*3+2))

		for ri, row := range v {
			if ri > 0 {
				buf = append(buf, ',', ' ')
			}
			buf = append(buf, '(')
			for ci, col := range columns {
				if ci > 0 {
					buf = append(buf, ',', ' ')
				}
				args = append(args, row[col])
				buf = append(buf, placeholderFormat[:len(placeholderFormat)-2]...)
				buf = strconv.AppendInt(buf, int64(*paramIndex), 10)
				(*paramIndex)++
			}
			buf = append(buf, ')')
		}
		return string(buf), args

	case map[string]interface{}:
		// Preallocate slice for single row operations
		args := make([]interface{}, len(columns))
		buf := make([]byte, 0, len(columns)*16)

		for i, col := range columns {
			if i > 0 {
				buf = append(buf, ',', ' ')
			}
			args[i] = v[col]
			buf = append(buf, col...)
			buf = append(buf, ' ', '=')
			buf = append(buf, ' ')
			buf = append(buf, placeholderFormat[:len(placeholderFormat)-2]...)
			buf = strconv.AppendInt(buf, int64(*paramIndex), 10)
			(*paramIndex)++
		}
		return string(buf), args

	default:
		return "", nil
	}
}

// buildReturningClause returns the RETURNING clause for PostgreSQL and SQLite dialects.
// If returning is nil or empty, it returns an empty string (no clause added).
func (b *dynamicQueryBuilder) buildReturningClause(returning []string) string {
	if len(returning) == 0 {
		return ""
	}
	buf := make([]byte, 0, len(returning)*8+16)
	buf = append(buf, " RETURNING "...)
	for i, col := range returning {
		if i > 0 {
			buf = append(buf, ',', ' ')
		}
		buf = append(buf, col...)
	}
	return string(buf)
}

// buildOutputClause returns the OUTPUT clause for SQL Server dialect.
// The prefix parameter determines whether to use "inserted" or "deleted" depending on the operation.
// If returning is nil or empty, it returns an empty string (no clause added).
func (b *dynamicQueryBuilder) buildOutputClause(returning []string, prefix string) string {
	if len(returning) == 0 {
		return ""
	}
	buf := make([]byte, 0, len(returning)*(len(prefix)+16)+16)
	buf = append(buf, " OUTPUT "...)
	for i, col := range returning {
		if i > 0 {
			buf = append(buf, ',', ' ')
		}
		buf = append(buf, prefix...)
		buf = append(buf, '.')
		buf = append(buf, col...)
	}
	return string(buf)
}
