package goqube

// DeleteQuery represents a SQL DELETE statement with a target table and an optional filter condition.
// It is used to build parameterized DELETE queries for different SQL dialects.
type DeleteQuery struct {
	Table  string  // Table is the name of the table from which to delete data.
	Filter *Filter // Filter specifies the WHERE condition for the delete operation.
}

// BuildDeleteQuery builds the SQL DELETE statement and its arguments for the specified dialect.
// It returns the query string, the arguments slice, and an error if the dialect is unsupported.
func (q *DeleteQuery) BuildDeleteQuery(dialect Dialect) (string, []interface{}, error) {
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

	return qb.BuildDeleteQuery(q)
}
