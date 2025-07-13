package goqube

// UpdateQuery represents a SQL UPDATE statement with fields to update, a filter, and a target table.
// It is used to build parameterized UPDATE queries for different SQL dialects.
type UpdateQuery struct {
	FieldsValue map[string]interface{} // FieldsValue maps column names to their new values.
	Filter      *Filter                // Filter specifies the WHERE condition for the update.
	Table       string                 // Table is the name of the table to update.
}

// BuildUpdateQuery builds the SQL UPDATE statement and its arguments for the specified dialect.
// It returns the query string, the arguments slice, and an error if the dialect is unsupported.
func (q *UpdateQuery) BuildUpdateQuery(dialect Dialect) (string, []interface{}, error) {
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

	return qb.BuildUpdateQuery(q)
}
