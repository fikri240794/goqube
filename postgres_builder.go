package goqube

import (
	"reflect"
	"regexp"
	"sort"
	"strconv"
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

// adjustRawQueryPlaceholders adjusts placeholders in raw SQL to match the current parameter index
func (b *postgresBuilder) adjustRawQueryPlaceholders(rawSQL string, rawArgs []interface{}, paramIndex *int) (string, []interface{}) {
	if rawSQL == "" || len(rawArgs) == 0 {
		return rawSQL, rawArgs
	}

	// Track the starting parameter index for this raw query
	startIndex := *paramIndex

	// Use regex to find all $1, $2, etc. placeholders and adjust them
	re := regexp.MustCompile(`\$(\d+)`)
	adjustedSQL := re.ReplaceAllStringFunc(rawSQL, func(match string) string {
		// Extract the original placeholder number
		originalIndex, _ := strconv.Atoi(match[1:])
		// Map $1 -> $startIndex, $2 -> $startIndex+1, etc.
		buf := make([]byte, 0, 8)
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(startIndex+originalIndex-1), 10)
		return string(buf)
	})

	// Advance the parameter index by the number of arguments
	*paramIndex += len(rawArgs)

	return adjustedSQL, rawArgs
}

// buildSelectQueryWithParamIndex builds a SelectQuery with parameter index awareness for subqueries
func (b *postgresBuilder) buildSelectQueryWithParamIndex(q *SelectQuery, paramIndex *int) (string, []interface{}, error) {
	if q == nil {
		return "", nil, ErrInvalidFilter
	}

	// Handle raw SQL queries with parameter index adjustment
	if q.Raw != "" {
		adjustedSQL, adjustedArgs := b.adjustRawQueryPlaceholders(q.Raw, q.RawArgs, paramIndex)
		return adjustedSQL, adjustedArgs, nil
	}

	// For non-raw queries, delegate to the standard BuildSelectQuery
	return b.BuildSelectQuery(q)
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
	query, argsOut, err := b.buildDeleteQuery(q.Table, q.Filter, &args, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, &paramIndex, true)
	})
	if err != nil {
		return "", nil, err
	}
	if clause := b.buildReturningClause(q.Returning); clause != "" {
		query += clause
	}
	return query, argsOut, nil
}

// BuildInsertQuery builds a SQL INSERT statement and its arguments for PostgreSQL.
func (b *postgresBuilder) BuildInsertQuery(q *InsertQuery) (string, []interface{}, error) {
	query, args, err := b.buildInsertQuery(q, 1, "$%d")
	if err != nil {
		return "", nil, err
	}
	if clause := b.buildReturningClause(q.Returning); clause != "" {
		query += clause
	}
	return query, args, nil
}

// BuildSelectQuery builds a SQL SELECT statement and its arguments for PostgreSQL.
func (b *postgresBuilder) BuildSelectQuery(q *SelectQuery) (string, []interface{}, error) {
	if q == nil {
		return "", nil, ErrInvalidFilter
	}

	// Early return for raw SQL queries to avoid unnecessary processing
	if q.Raw != "" {
		return q.Raw, q.RawArgs, nil
	}

	// Preallocate args slice with estimated capacity for typical SELECT queries
	args := make([]interface{}, 0, 16)
	buf := make([]byte, 0, 128)
	paramIndex := 1

	buf = append(buf, "SELECT "...)

	fields, err := b.buildFields(q.Fields, &args, func(sq *SelectQuery) (string, []interface{}, error) {
		return b.buildSelectQueryWithParamIndex(sq, &paramIndex)
	})
	if err != nil {
		return "", nil, err
	}
	buf = append(buf, fields...)

	table, err := b.buildTableWithParamIndex(q.Table, &args, &paramIndex)
	if err != nil {
		return "", nil, err
	}
	buf = append(buf, " FROM "...)
	buf = append(buf, table...)

	// Process JOINs only if they exist
	if len(q.Joins) > 0 {
		joins, err := b.buildJoins(
			q.Joins,
			&args,
			func(sq *SelectQuery) (string, []interface{}, error) {
				return b.buildSelectQueryWithParamIndex(sq, &paramIndex)
			},
			func(f *Filter, args *[]interface{}) (string, error) {
				return b.buildFilter(f, args, &paramIndex, true)
			},
		)
		if err != nil {
			return "", nil, err
		}
		buf = append(buf, ' ')
		buf = append(buf, joins...)
	}

	// Process WHERE clause only if filter exists and produces content
	if q.Filter != nil {
		where, err := b.buildFilter(q.Filter, &args, &paramIndex, true)
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

	// Add pagination clauses using direct string building for better performance
	if q.Take > 0 {
		buf = append(buf, " LIMIT $"...)
		buf = strconv.AppendInt(buf, int64(paramIndex), 10)
		args = append(args, int64(q.Take))
		paramIndex++
	}

	if q.Skip > 0 {
		buf = append(buf, " OFFSET $"...)
		buf = strconv.AppendInt(buf, int64(paramIndex), 10)
		args = append(args, int64(q.Skip))
		paramIndex++
	}

	// Handle aliasing with optimized string building
	if q.Alias != "" {
		sqlStr := strings.TrimSpace(string(buf))
		out := make([]byte, 0, len(sqlStr)+len(q.Alias)+6)
		out = append(out, '(')
		out = append(out, sqlStr...)
		out = append(out, ") AS "...)
		out = append(out, q.Alias...)
		return string(out), args, nil
	}

	return string(buf), args, nil
}

// BuildUpdateQuery builds a SQL UPDATE statement and its arguments for PostgreSQL.
func (b *postgresBuilder) BuildUpdateQuery(q *UpdateQuery) (string, []interface{}, error) {
	query, args, err := b.buildUpdateQueryWithContinuousIndex(q, 1, "$%d", func(f *Filter, args *[]interface{}, idx int, isRoot bool) (string, int, error) {
		where, err := b.buildFilter(f, args, &idx, isRoot)
		return where, idx, err
	})
	if err != nil {
		return "", nil, err
	}
	if clause := b.buildReturningClause(q.Returning); clause != "" {
		query += clause
	}
	return query, args, nil
}

// BuildBulkUpdateQuery builds a SQL bulk UPDATE statement and its arguments for PostgreSQL.
func (b *postgresBuilder) BuildBulkUpdateQuery(q *BulkUpdateQuery) (string, []interface{}, error) {
	if q == nil || q.Table == "" || len(q.FieldsValues) == 0 {
		return "", nil, ErrInvalidBulkUpdateQuery
	}
	if q.PrimaryKey == "" {
		return "", nil, ErrInvalidBulkUpdateQueryPrimaryKey
	}

	// Extract and sort column names for consistent order (excluding primary key)
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

	// Validate that ColumnsType is provided and contains all required columns
	// PostgreSQL requires explicit type casts (::type) for VALUES clause placeholders
	if len(q.ColumnsType) == 0 {
		return "", nil, ErrInvalidBulkUpdateQueryMissingColumnType
	}
	if _, ok := q.ColumnsType[q.PrimaryKey]; !ok {
		return "", nil, ErrInvalidBulkUpdateQueryMissingColumnType
	}
	for _, col := range columns {
		if _, ok := q.ColumnsType[col]; !ok {
			return "", nil, ErrInvalidBulkUpdateQueryMissingColumnType
		}
	}

	// Preallocate args and the query buffer
	var args []interface{}
	paramIndex := 1

	pkType := q.ColumnsType[q.PrimaryKey]
	colTypes := make(map[string]string, len(columns))
	for _, col := range columns {
		colTypes[col] = q.ColumnsType[col]
	}

	buf := make([]byte, 0, 128)
	buf = append(buf, "UPDATE "...)
	buf = append(buf, q.Table...)
	buf = append(buf, " AS t SET "...)

	for i, col := range columns {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, col...)
		buf = append(buf, " = c."...)
		buf = append(buf, col...)
	}

	buf = append(buf, " FROM (VALUES "...)

	for ri, row := range q.FieldsValues {
		pkVal, ok := row[q.PrimaryKey]
		if !ok {
			return "", nil, ErrInvalidBulkUpdateQueryPrimaryKey
		}

		if ri > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, '(')

		// Add primary key first with type cast (PostgreSQL requires ::type for VALUES placeholders)
		args = append(args, pkVal)
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(paramIndex), 10)
		buf = append(buf, "::"...)
		buf = append(buf, pkType...)
		paramIndex++

		// Add other columns with type cast
		for _, col := range columns {
			args = append(args, row[col])
			buf = append(buf, ", $"...)
			buf = strconv.AppendInt(buf, int64(paramIndex), 10)
			buf = append(buf, "::"...)
			buf = append(buf, colTypes[col]...)
			paramIndex++
		}

		buf = append(buf, ')')
	}

	buf = append(buf, ") AS c("...)
	buf = append(buf, q.PrimaryKey...)
	for _, col := range columns {
		buf = append(buf, ", "...)
		buf = append(buf, col...)
	}
	buf = append(buf, ") WHERE t."...)
	buf = append(buf, q.PrimaryKey...)
	buf = append(buf, " = c."...)
	buf = append(buf, q.PrimaryKey...)
	buf = append(buf, "::"...)
	buf = append(buf, pkType...)

	query := string(buf)
	if clause := b.buildReturningClause(q.Returning); clause != "" {
		query += clause
	}
	return query, args, nil
}

// buildFieldForFilter returns the SQL representation of a field for use in filter conditions in PostgreSQL.
func (b *postgresBuilder) buildFieldForFilter(f Field) (string, error) {
	if f.SelectQuery != nil {
		// If the field is a subquery, build and wrap it, optionally with an alias.
		sub, _, err := b.BuildSelectQuery(f.SelectQuery)
		if err != nil {
			return "", err
		}
		sub = strings.TrimSpace(sub)
		if f.Alias != "" {
			buf := make([]byte, 0, len(sub)+len(f.Alias)+6)
			buf = append(buf, '(')
			buf = append(buf, sub...)
			buf = append(buf, ") AS "...)
			buf = append(buf, f.Alias...)
			return string(buf), nil
		}
		buf := make([]byte, 0, len(sub)+2)
		buf = append(buf, '(')
		buf = append(buf, sub...)
		buf = append(buf, ')')
		return string(buf), nil
	} else if f.Table != "" && f.Column != "" {
		// If both table and column are set, return a qualified column name.
		buf := make([]byte, 0, len(f.Table)+len(f.Column)+1)
		buf = append(buf, f.Table...)
		buf = append(buf, '.')
		buf = append(buf, f.Column...)
		return string(buf), nil
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
		for i := range f.Filters {
			part, err := b.buildFilter(&f.Filters[i], args, paramIndex, false)
			if err != nil {
				return "", err
			}
			if part != "" {
				parts = append(parts, part)
			}
		}
		// Build the " LOGIC " separator without fmt to avoid escape-to-heap
		sepBuf := make([]byte, 0, len(f.Logic)+2)
		sepBuf = append(sepBuf, ' ')
		sepBuf = append(sepBuf, f.Logic...)
		sepBuf = append(sepBuf, ' ')
		joined := strings.Join(parts, string(sepBuf))
		// Optimize space normalization with strings.ReplaceAll for better performance
		for strings.Contains(joined, "  ") {
			joined = strings.ReplaceAll(joined, "  ", " ")
		}
		joined = strings.TrimSpace(joined)
		if isRoot {
			return joined, nil
		}
		buf := make([]byte, 0, len(joined)+2)
		buf = append(buf, '(')
		buf = append(buf, joined...)
		buf = append(buf, ')')
		return string(buf), nil
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
	// Build "field op value" in a single buffer without fmt
	buf := make([]byte, 0, len(fieldStr)+len(operator)+len(valueStr)+2)
	buf = append(buf, fieldStr...)
	buf = append(buf, ' ')
	buf = append(buf, operator...)
	buf = append(buf, ' ')
	buf = append(buf, valueStr...)
	return string(buf), nil
}

// buildFilterValue returns the SQL representation of a filter value for use in WHERE or HAVING clauses in PostgreSQL.
func (b *postgresBuilder) buildFilterValue(op Operator, v FilterValue, args *[]interface{}, paramIndex *int) (string, error) {
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
		sub = strings.TrimSpace(sub)
		buf := make([]byte, 0, len(sub)+2)
		buf = append(buf, '(')
		buf = append(buf, sub...)
		buf = append(buf, ')')
		return string(buf), nil
	} else if v.Table != "" && v.Column != "" {
		buf := make([]byte, 0, len(v.Table)+len(v.Column)+1)
		buf = append(buf, v.Table...)
		buf = append(buf, '.')
		buf = append(buf, v.Column...)
		return string(buf), nil
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
		// Build the parenthesized placeholder list in one buffer
		buf := make([]byte, 0, 2+valLen*6)
		buf = append(buf, '(')
		for i := 0; i < valLen; i++ {
			*args = append(*args, val.Index(i).Interface())
			if i > 0 {
				buf = append(buf, ", "...)
			}
			buf = append(buf, '$')
			buf = strconv.AppendInt(buf, int64(*paramIndex), 10)
			(*paramIndex)++
		}
		buf = append(buf, ')')
		return string(buf), nil
	} else {
		*args = append(*args, v.Value)
		buf := make([]byte, 0, 8)
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(*paramIndex), 10)
		(*paramIndex)++
		return string(buf), nil
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
		buf := make([]byte, 0, 8)
		buf = append(buf, '(')
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(*paramIndex), 10)
		buf = append(buf, ')')
		(*paramIndex)++
		return string(buf), nil
	} else if v.Table != "" && v.Column != "" {
		buf := make([]byte, 0, len(v.Table)+len(v.Column)+1)
		buf = append(buf, v.Table...)
		buf = append(buf, '.')
		buf = append(buf, v.Column...)
		return string(buf), nil
	} else if v.Value != nil {
		strVal, ok := v.Value.(string)
		if !ok {
			return "", ErrLikeValueType
		}
		*args = append(*args, "%"+strVal+"%")
		buf := make([]byte, 0, 8)
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(*paramIndex), 10)
		(*paramIndex)++
		return string(buf), nil
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

// buildTableWithParamIndex returns the SQL representation of a table or subquery with parameter index awareness for PostgreSQL.
func (b *postgresBuilder) buildTableWithParamIndex(t Table, args *[]interface{}, paramIndex *int) (string, error) {
	return b.dynamicQueryBuilder.buildTable(t, args, func(sq *SelectQuery) (string, []interface{}, error) {
		return b.buildSelectQueryWithParamIndex(sq, paramIndex)
	})
}
