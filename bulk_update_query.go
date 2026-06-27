package goqube

// BulkUpdateQuery represents a SQL bulk UPDATE statement with fields to update, and a target table.
// It is used to build highly performant parameterized bulk UPDATE queries for different SQL dialects.
//
// ColumnsType is REQUIRED for PostgreSQL and SQL Server dialects. It maps column names to their
// SQL type names (e.g., "id" -> "integer", "name" -> "text"). Without it, the database cannot
// infer parameter types in the VALUES clause and will return an error.
// For MySQL and SQLite, ColumnsType is optional as those dialects handle type inference natively.
type BulkUpdateQuery struct {
	Table        string                   // Table is the name of the table to update.
	PrimaryKey   string                   // PrimaryKey is the column name used to identify existing records.
	FieldsValues []map[string]interface{} // FieldsValues is a slice of maps, each representing a row to update including its primary key.
	ColumnsType  map[string]string        // ColumnsType maps column names to SQL type names (e.g., "id" -> "integer", "name" -> "text"). Required for PostgreSQL and SQL Server.
}

// BuildBulkUpdateQuery builds the SQL UPDATE statement and its arguments for the specified dialect.
// It returns the query string, the arguments slice, and an error if the dialect is unsupported or query is invalid.
func (q *BulkUpdateQuery) BuildBulkUpdateQuery(dialect Dialect) (string, []interface{}, error) {
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

	return qb.BuildBulkUpdateQuery(q)
}
