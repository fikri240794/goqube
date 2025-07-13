package goqube

// SelectQuery represents a SQL SELECT statement with support for fields, filters, joins, sorting, and pagination.
// It is used to build complex SELECT queries programmatically.
type SelectQuery struct {
	Alias         string  // Alias is an optional name for referencing this query as a subquery.
	Fields        []Field // Fields are the columns or expressions to select.
	Filter        *Filter // Filter is the WHERE or HAVING condition for the query.
	GroupByFields []Field // GroupByFields are the columns used in the GROUP BY clause.
	Joins         []Join  // Joins defines the tables to join and their conditions.
	Raw           string  // Raw allows for custom raw SQL to be included in the query.
	Skip          uint64  // Skip specifies the number of rows to skip (OFFSET).
	Sorts         []Sort  // Sorts defines the ORDER BY clauses for sorting results.
	Table         Table   // Table is the main table from which to select data.
	Take          uint64  // Take specifies the maximum number of rows to return (LIMIT).
}

// BuildSelectQuery builds the SQL SELECT statement and its arguments for the specified dialect.
// It returns the query string, the arguments slice, and an error if the dialect is unsupported.
func (q *SelectQuery) BuildSelectQuery(dialect Dialect) (string, []interface{}, error) {
	var qb queryBuilder

	// Select the appropriate query builder based on the SQL dialect.
	switch dialect {
	case DialectMySQL:
		qb = newMySQLBuilder()
	case DialectPostgres:
		qb = newPostgresBuilder()
	case DialectSQLite:
		qb = newSQLiteBuilder()
	case DialectSQLServer:
		qb = newSQLServerBuilder()
	default:
		return "", nil, ErrUnsupportedDialect // Return error if dialect is not supported.
	}

	return qb.BuildSelectQuery(q)
}
