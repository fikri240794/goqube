package goqube

import (
	"fmt"
	"reflect"
	"strings"
)

// sqlServerBuilder implements SQL query building logic specific to SQL Server.
// It embeds dynamicQueryBuilder and customizes placeholder formatting and SQL syntax for SQL Server compatibility.
type sqlServerBuilder struct {
	dynamicQueryBuilder
}

// newSQLServerBuilder creates a new sqlServerBuilder with SQL Server-style placeholders (e.g., @p1, @p2).
// This ensures that all parameterized queries use the correct placeholder format for SQL Server dialects.
func newSQLServerBuilder() *sqlServerBuilder {
	return &sqlServerBuilder{dynamicQueryBuilder{placeholderFormat: "@p%d"}}
}

// BuildDeleteQuery builds a SQL DELETE statement and its arguments for SQL Server.
// It validates the input, prepares arguments, and delegates filter building to support complex WHERE conditions with indexed placeholders.
func (b *sqlServerBuilder) BuildDeleteQuery(q *DeleteQuery) (string, []interface{}, error) {
	// Ensure the query and table name are provided.
	if q == nil || q.Table == "" {
		return "", nil, ErrInvalidDeleteQuery
	}
	args := make([]interface{}, 0)
	paramIndex := 0
	// Build the DELETE query using the provided table, filter, and a custom filter builder with parameter indexing.
	return b.buildDeleteQuery(q.Table, q.Filter, &args, func(f *Filter, args *[]interface{}) (string, error) {
		return b.buildFilter(f, args, &paramIndex, true)
	})
}

// BuildInsertQuery builds a SQL INSERT statement and its arguments for SQL Server.
// It uses SQL Server-style indexed placeholders and delegates the construction to the dynamicQueryBuilder implementation.
func (b *sqlServerBuilder) BuildInsertQuery(q *InsertQuery) (string, []interface{}, error) {
	return b.buildInsertQuery(q, 0, b.nextPlaceholder)
}

// BuildSelectQuery builds a SQL SELECT statement and its arguments for SQL Server.
// It supports raw queries, subqueries, joins, filters, grouping, sorting, pagination, and aliasing, returning the query, arguments, and error if invalid.
func (b *sqlServerBuilder) BuildSelectQuery(q *SelectQuery) (string, []interface{}, error) {
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
	paramIndex := 0
	// Build JOIN clauses if present.
	if len(q.Joins) > 0 {
		joins, err := b.buildJoins(q.Joins, &args, &paramIndex)
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
	// Add OFFSET and FETCH clauses for pagination if specified.
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
		sb.WriteString(" OFFSET 0 ROWS")
		fetchPlaceholder := b.nextPlaceholder(&paramIndex)
		sb.WriteString(fmt.Sprintf(" FETCH NEXT %s ROWS ONLY", fetchPlaceholder))
		args = append(args, int64(q.Take))
	}
	// Wrap the query in parentheses and alias if an alias is provided.
	if q.Alias != "" {
		return fmt.Sprintf("(%s) AS %s", strings.TrimSpace(sb.String()), q.Alias), args, nil
	}
	// Clean up whitespace and formatting for SQL Server compatibility.
	query := sb.String()
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.ReplaceAll(query, "\t", " ")
	query = strings.Join(strings.Fields(query), " ")
	query = strings.ReplaceAll(query, "( ", "(")
	query = strings.ReplaceAll(query, " )", ")")
	return query, args, nil
}

// BuildUpdateQuery builds a SQL UPDATE statement and its arguments for SQL Server.
// It uses indexed placeholders and delegates the construction to dynamicQueryBuilder with a custom filter builder for WHERE conditions.
func (b *sqlServerBuilder) BuildUpdateQuery(q *UpdateQuery) (string, []interface{}, error) {
	// Build the UPDATE query using the provided filter builder for complex WHERE logic.
	return b.buildUpdateQueryWithContinuousIndex(q, 0, b.nextPlaceholder, b.buildFilter)
}

// buildFields returns the SQL representation of fields for SQL Server, supporting subqueries and aliases.
// It delegates to dynamicQueryBuilder and uses BuildSelectQuery for subquery handling.
func (b *sqlServerBuilder) buildFields(fields []Field, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildFields(fields, args, b.BuildSelectQuery)
}

// buildFieldForFilter returns the SQL representation of a field for use in filter conditions in SQL Server.
// It supports subqueries, table-qualified columns, and plain columns, returning an error if the field is invalid.
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

// buildFilter returns the SQL representation of a filter condition for SQL Server, supporting nested filters and logical operators.
// It recursively builds subfilters, handles root/non-root grouping, and supports parameter indexing for placeholders.
func (b *sqlServerBuilder) buildFilter(f *Filter, args *[]interface{}, paramIndex *int, isRoot bool) (string, error) {
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
		joined := strings.Join(parts, "\n"+string(f.Logic)+" ")
		joined = strings.ReplaceAll(joined, "\n", " ")
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
	// For IS NULL/IS NOT NULL, no value is needed.
	if f.Operator == OperatorIsNull || f.Operator == OperatorIsNotNull {
		return fieldStr + " " + operator, nil
	}
	valueStr, err := b.buildFilterValue(f.Operator, f.Value, args, paramIndex)
	if err != nil {
		return "", err
	}
	return fieldStr + " " + operator + " " + valueStr, nil
}

// buildFilterValue returns the SQL representation of a filter value for SQL Server, handling subqueries, columns, arrays, and parameter placeholders.
// It supports various operators, including LIKE, IN, and NULL checks, and appends arguments for parameterized queries.
func (b *sqlServerBuilder) buildFilterValue(op Operator, v FilterValue, args *[]interface{}, paramIndex *int) (string, error) {
	// Handle LIKE and NOT LIKE operators with a dedicated function.
	if op == OperatorLike || op == OperatorNotLike {
		return b.buildFilterValueLike(v, args, paramIndex)
	}
	// If the value is a subquery, build the subquery and append its arguments.
	if v.SelectQuery != nil {
		sub, subArgs, err := b.BuildSelectQuery(v.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		return "(" + strings.TrimSpace(sub) + ")", nil
	} else if v.Table != "" && v.Column != "" {
		// If both table and column are set, return qualified column name.
		return v.Table + "." + v.Column, nil
	} else if v.Column != "" {
		// If only column is set, return the column name.
		return v.Column, nil
	} else if op == OperatorIsNull || op == OperatorIsNotNull {
		// For IS NULL/IS NOT NULL, no value is needed.
		return "", nil
	} else if op == OperatorIn || op == OperatorNotIn {
		// For IN/NOT IN, ensure the value is a non-empty slice or array.
		val := reflect.ValueOf(v.Value)
		if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
			return "", ErrOperatorArray
		}
		if val.Len() == 0 {
			return "", ErrOperatorArrayEmpty
		}
		// Build a placeholder for each element and append values to args.
		placeholders := make([]string, val.Len())
		for i := 0; i < val.Len(); i++ {
			*args = append(*args, val.Index(i).Interface())
			placeholders[i] = b.nextPlaceholder(paramIndex)
		}
		return "(" + strings.TrimSpace(strings.Join(placeholders, ", ")) + ")", nil
	} else {
		// For other operators, use a single placeholder and append the value.
		*args = append(*args, v.Value)
		placeholder := b.nextPlaceholder(paramIndex)
		return placeholder, nil
	}
}

// buildFilterValueLike returns the SQL representation of a value for LIKE/NOT LIKE operators in SQL Server.
// It supports subqueries, qualified columns, and string pattern matching, appending arguments as needed.
func (b *sqlServerBuilder) buildFilterValueLike(v FilterValue, args *[]interface{}, paramIndex *int) (string, error) {
	// If the value is a subquery, build it and use a placeholder for the result.
	if v.SelectQuery != nil {
		_, subArgs, err := b.BuildSelectQuery(v.SelectQuery)
		if err != nil {
			return "", err
		}
		*args = append(*args, subArgs...)
		placeholder := b.nextPlaceholder(paramIndex)
		return "(" + placeholder + ")", nil
	} else if v.Table != "" && v.Column != "" {
		// If both table and column are set, return qualified column name.
		return v.Table + "." + v.Column, nil
	} else if v.Value != nil {
		// For string values, wrap with % for pattern matching and append as argument.
		strVal, ok := v.Value.(string)
		if !ok {
			return "", ErrLikeValueType
		}
		*args = append(*args, "%"+strVal+"%")
		placeholder := b.nextPlaceholder(paramIndex)
		return placeholder, nil
	} else {
		// Return error if value type is not supported for LIKE.
		return "", ErrLikeValueTypeOrSubquery
	}
}

// buildGroupBy returns the SQL representation of the GROUP BY clause for SQL Server.
// It delegates the construction to dynamicQueryBuilder, supporting multiple fields and subqueries.
func (b *sqlServerBuilder) buildGroupBy(fields []Field) (string, error) {
	return b.dynamicQueryBuilder.buildGroupBy(fields)
}

// buildJoins returns the SQL representation of JOIN clauses for SQL Server, supporting subqueries and complex filter conditions.
// It delegates to dynamicQueryBuilder, passing a custom filter builder to handle parameter indexing for each join condition.
func (b *sqlServerBuilder) buildJoins(joins []Join, args *[]interface{}, paramIndex *int) (string, error) {
	// The filter callback ensures each join's ON condition uses the correct parameter index.
	return b.dynamicQueryBuilder.buildJoins(
		joins,
		args,
		b.BuildSelectQuery,
		func(f *Filter, args *[]interface{}) (string, error) {
			return b.buildFilter(f, args, paramIndex, true)
		},
	)
}

// buildOrderBy returns the SQL representation of the ORDER BY clause for SQL Server.
// It delegates the construction to dynamicQueryBuilder, supporting multiple sort fields and directions.
func (b *sqlServerBuilder) buildOrderBy(sorts []Sort) (string, error) {
	return b.dynamicQueryBuilder.buildOrderBy(sorts)
}

// buildTable returns the SQL representation of a table or subquery for SQL Server.
// It delegates to dynamicQueryBuilder and supports subqueries via BuildSelectQuery.
func (b *sqlServerBuilder) buildTable(t Table, args *[]interface{}) (string, error) {
	return b.dynamicQueryBuilder.buildTable(t, args, b.BuildSelectQuery)
}

// nextPlaceholder returns the next parameter placeholder for SQL Server (e.g., @p1, @p2).
// It increments the parameter index and formats the placeholder for use in parameterized queries.
func (b *sqlServerBuilder) nextPlaceholder(paramIndex *int) string {
	return b.dynamicQueryBuilder.nextPlaceholder(paramIndex)
}
