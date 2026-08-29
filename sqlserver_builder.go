package goqube

import (
	"reflect"
	"regexp"
	"sort"
	"strconv"
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
		originalIndex, _ := strconv.Atoi(match[2:])
		// Map @p0 -> @p{startIndex}, @p1 -> @p{startIndex+1}, etc.
		buf := make([]byte, 0, len(match)+8)
		buf = append(buf, "@p"...)
		buf = strconv.AppendInt(buf, int64(startIndex+originalIndex), 10)
		return string(buf)
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
	query, argsOut, err := b.buildDeleteQuery(q.Table, q.Filter, &args, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, &paramIndex, true)
	})
	if err != nil {
		return "", nil, err
	}
	if clause := b.buildOutputClause(q.Returning, "deleted"); clause != "" {
		// OUTPUT goes before WHERE for DELETE in SQL Server
		if strings.Contains(query, " WHERE ") {
			query = strings.Replace(query, " WHERE ", clause+" WHERE ", 1)
		} else {
			// No WHERE clause, append OUTPUT after table name
			query += clause
		}
	}
	return query, argsOut, nil
}

// BuildInsertQuery builds a SQL INSERT statement and its arguments for SQL Server.
func (b *sqlServerBuilder) BuildInsertQuery(q *InsertQuery) (string, []interface{}, error) {
	query, args, err := b.buildInsertQuery(q, 0, "@p%d")
	if err != nil {
		return "", nil, err
	}
	if clause := b.buildOutputClause(q.Returning, "inserted"); clause != "" {
		// OUTPUT goes before VALUES in SQL Server
		query = strings.Replace(query, " VALUES", clause+" VALUES", 1)
	}
	return query, args, nil
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
	buf := make([]byte, 0, 128)
	buf = append(buf, "SELECT "...)

	// Initialize parameter index for SQL Server's @p0, @p1 placeholders
	paramIndex := 0

	fields, err := b.buildFieldsWithParamIndex(q.Fields, &args, &paramIndex)
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
		joins, err := b.buildJoins(q.Joins, &args, &paramIndex)
		if err != nil {
			return "", nil, err
		}
		buf = append(buf, " "...)
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

	// SQL Server pagination uses OFFSET/FETCH with specific syntax requirements
	if q.Skip > 0 {
		offsetPlaceholder := b.nextPlaceholder(&paramIndex)
		buf = append(buf, " OFFSET "...)
		buf = append(buf, offsetPlaceholder...)
		buf = append(buf, " ROWS"...)
		args = append(args, int64(q.Skip))
		if q.Take > 0 {
			fetchPlaceholder := b.nextPlaceholder(&paramIndex)
			buf = append(buf, " FETCH NEXT "...)
			buf = append(buf, fetchPlaceholder...)
			buf = append(buf, " ROWS ONLY"...)
			args = append(args, int64(q.Take))
		}
	} else if q.Take > 0 {
		// FETCH requires OFFSET, so use 0 when only TAKE is specified
		buf = append(buf, " OFFSET 0 ROWS"...)
		fetchPlaceholder := b.nextPlaceholder(&paramIndex)
		buf = append(buf, " FETCH NEXT "...)
		buf = append(buf, fetchPlaceholder...)
		buf = append(buf, " ROWS ONLY"...)
		args = append(args, int64(q.Take))
	}

	// Handle aliasing with optimized string building
	if q.Alias != "" {
		trimmed := strings.TrimSpace(string(buf))
		aliasBuf := make([]byte, 0, len(trimmed)+len(q.Alias)+8)
		aliasBuf = append(aliasBuf, "("...)
		aliasBuf = append(aliasBuf, trimmed...)
		aliasBuf = append(aliasBuf, ") AS "...)
		aliasBuf = append(aliasBuf, q.Alias...)
		return string(aliasBuf), args, nil
	}

	// SQL Server-specific whitespace normalization and parentheses cleanup
	query := string(buf)
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.ReplaceAll(query, "\t", " ")
	query = strings.Join(strings.Fields(query), " ")
	query = strings.ReplaceAll(query, "( ", "(")
	query = strings.ReplaceAll(query, " )", ")")
	return query, args, nil
}

// BuildUpdateQuery builds a SQL UPDATE statement and its arguments for SQL Server.
func (b *sqlServerBuilder) BuildUpdateQuery(q *UpdateQuery) (string, []interface{}, error) {
	query, args, err := b.buildUpdateQueryWithContinuousIndex(q, 0, "@p%d", func(f *Filter, args *[]interface{}, idx int, isRoot bool) (string, int, error) {
		where, err := b.buildFilter(f, args, &idx, isRoot)
		return where, idx, err
	})
	if err != nil {
		return "", nil, err
	}
	if clause := b.buildOutputClause(q.Returning, "inserted"); clause != "" {
		// OUTPUT goes before WHERE for UPDATE in SQL Server
		if strings.Contains(query, " WHERE ") {
			query = strings.Replace(query, " WHERE ", clause+" WHERE ", 1)
		} else {
			// No WHERE clause, append OUTPUT after SET clause
			query += clause
		}
	}
	return query, args, nil
}

// BuildBulkUpdateQuery builds a SQL bulk UPDATE statement and its arguments for SQL Server.
func (b *sqlServerBuilder) BuildBulkUpdateQuery(q *BulkUpdateQuery) (string, []interface{}, error) {
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

	// Validate that ColumnsType is provided and contains all required columns
	// SQL Server requires explicit CONVERT type casts for VALUES clause placeholders
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

	var args []interface{}
	paramIndex := 0

	pkType := q.ColumnsType[q.PrimaryKey]
	colTypes := make(map[string]string, len(columns))
	for _, col := range columns {
		colTypes[col] = q.ColumnsType[col]
	}

	buf := make([]byte, 0, 256)
	buf = append(buf, "UPDATE t SET "...)

	for i, col := range columns {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, col...)
		buf = append(buf, " = c."...)
		buf = append(buf, col...)
	}

	buf = append(buf, " FROM "...)
	buf = append(buf, q.Table...)
	buf = append(buf, " AS t INNER JOIN (VALUES "...)

	for ri, row := range q.FieldsValues {
		pkVal, ok := row[q.PrimaryKey]
		if !ok {
			return "", nil, ErrInvalidBulkUpdateQueryPrimaryKey
		}

		// Add primary key first with CONVERT type cast
		args = append(args, pkVal)
		if ri > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, "("...)
		buf = append(buf, "CONVERT("...)
		buf = append(buf, pkType...)
		buf = append(buf, ", "...)
		buf = append(buf, b.nextPlaceholder(&paramIndex)...)
		buf = append(buf, ")"...)

		// Add other columns with CONVERT type cast
		for _, col := range columns {
			args = append(args, row[col])
			buf = append(buf, ", CONVERT("...)
			buf = append(buf, colTypes[col]...)
			buf = append(buf, ", "...)
			buf = append(buf, b.nextPlaceholder(&paramIndex)...)
			buf = append(buf, ")"...)
		}
		buf = append(buf, ")"...)
	}

	buf = append(buf, ") AS c("...)
	buf = append(buf, q.PrimaryKey...)
	for _, col := range columns {
		buf = append(buf, ", "...)
		buf = append(buf, col...)
	}

	buf = append(buf, ") ON t."...)
	buf = append(buf, q.PrimaryKey...)
	buf = append(buf, " = CONVERT("...)
	buf = append(buf, pkType...)
	buf = append(buf, ", c."...)
	buf = append(buf, q.PrimaryKey...)
	buf = append(buf, ")"...)

	query := string(buf)
	if clause := b.buildOutputClause(q.Returning, "inserted"); clause != "" {
		query += clause
	}
	return query, args, nil
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
		trimmed := strings.TrimSpace(sub)
		if f.Alias != "" {
			buf := make([]byte, 0, len(trimmed)+len(f.Alias)+8)
			buf = append(buf, "("...)
			buf = append(buf, trimmed...)
			buf = append(buf, ") AS "...)
			buf = append(buf, f.Alias...)
			return string(buf), nil
		}
		buf := make([]byte, 0, len(trimmed)+2)
		buf = append(buf, "("...)
		buf = append(buf, trimmed...)
		buf = append(buf, ")"...)
		return string(buf), nil
	} else if f.Table != "" && f.Column != "" {
		// If both table and column are set, return a qualified column name.
		buf := make([]byte, 0, len(f.Table)+len(f.Column)+1)
		buf = append(buf, f.Table...)
		buf = append(buf, "."...)
		buf = append(buf, f.Column...)
		return string(buf), nil
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
		for i := range f.Filters {
			part, err := b.buildFilter(&f.Filters[i], args, paramIndex, false)
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
		buf := make([]byte, 0, len(joined)+2)
		buf = append(buf, "("...)
		buf = append(buf, joined...)
		buf = append(buf, ")"...)
		return string(buf), nil
	}

	fieldStr, err := b.buildFieldForFilter(f.Field)
	if err != nil {
		return "", err
	}

	operator := string(f.Operator)

	// Early return for NULL operators to avoid unnecessary processing
	if f.Operator == OperatorIsNull || f.Operator == OperatorIsNotNull {
		buf := make([]byte, 0, len(fieldStr)+len(operator)+1)
		buf = append(buf, fieldStr...)
		buf = append(buf, ' ')
		buf = append(buf, operator...)
		return string(buf), nil
	}

	valueStr, err := b.buildFilterValue(f.Operator, f.Value, args, paramIndex)
	if err != nil {
		return "", err
	}

	buf := make([]byte, 0, len(fieldStr)+len(operator)+len(valueStr)+2)
	buf = append(buf, fieldStr...)
	buf = append(buf, ' ')
	buf = append(buf, operator...)
	buf = append(buf, ' ')
	buf = append(buf, valueStr...)
	return string(buf), nil
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

		// Preallocate slice with exact size for better performance
		placeholders := make([]string, valLen)
		total := 2
		for i := 0; i < valLen; i++ {
			*args = append(*args, val.Index(i).Interface())
			placeholders[i] = b.nextPlaceholder(paramIndex)
			total += len(placeholders[i]) + 2
		}

		// Avoid TrimSpace call since strings.Join doesn't produce extra spaces
		buf := make([]byte, 0, total)
		buf = append(buf, '(')
		for i, p := range placeholders {
			if i > 0 {
				buf = append(buf, ',', ' ')
			}
			buf = append(buf, p...)
		}
		buf = append(buf, ')')
		return string(buf), nil
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
		buf := make([]byte, 0, len(placeholder)+2)
		buf = append(buf, '(')
		buf = append(buf, placeholder...)
		buf = append(buf, ')')
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
	// Increment index first to keep placeholder numbering in sync.
	*paramIndex++

	// Use previous index value for the "@p%d" placeholder format.
	// The SQL Server placeholder format always ends in %d (set in
	// newSQLServerBuilder); build the placeholder with a byte buffer to
	// avoid fmt.Sprintf's interface boxing (one heap alloc per placeholder).
	idx := *paramIndex - 1
	buf := make([]byte, 0, len(b.placeholderFormat)+8)
	buf = append(buf, strings.TrimSuffix(b.placeholderFormat, "%d")...)
	buf = strconv.AppendInt(buf, int64(idx), 10)
	return string(buf)
}
