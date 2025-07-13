package goqube

// InsertQuery represents a SQL INSERT statement with a target table and values to insert.
// It is used to build parameterized INSERT queries for different SQL dialects.
type InsertQuery struct {
	Table  string                   // Table is the name of the table to insert data into.
	Values []map[string]interface{} // Values is a slice of maps, each representing a row to insert.
}

// BuildInsertQuery builds the SQL INSERT statement and its arguments for the specified dialect.
// It returns the query string, the arguments slice, and an error if the dialect is unsupported.
func (q *InsertQuery) BuildInsertQuery(dialect Dialect) (string, []interface{}, error) {
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

	return qb.BuildInsertQuery(q)
}
