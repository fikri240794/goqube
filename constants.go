package goqube

type (
	// Dialect represents a supported database dialect as a string type.
	Dialect string
	// Logic represents logical operators (e.g., AND, OR) for query conditions.
	Logic string
	// Operator represents SQL comparison and logical operators.
	Operator string
	// JoinType represents SQL join types for table relations.
	JoinType string
	// SortDirection represents the direction for ORDER BY clauses.
	SortDirection string
)

const (
	// DialectMySQL is the constant for the MySQL database dialect.
	DialectMySQL Dialect = "mysql"
	// DialectPostgres is the constant for the PostgreSQL database dialect.
	DialectPostgres Dialect = "postgres"
	// DialectSQLite is the constant for the SQLite database dialect.
	DialectSQLite Dialect = "sqlite"
	// DialectSQLServer is the constant for the SQL Server database dialect.
	DialectSQLServer Dialect = "sqlserver"

	// LogicAnd represents the logical AND operator in SQL queries.
	LogicAnd Logic = "AND"
	// LogicOr represents the logical OR operator in SQL queries.
	LogicOr Logic = "OR"

	// OperatorEqual represents the '=' operator for equality comparison.
	OperatorEqual Operator = "="
	// OperatorGreaterThan represents the '>' operator for greater-than comparison.
	OperatorGreaterThan Operator = ">"
	// OperatorGreaterThanOrEqual represents the '>=' operator for greater-than-or-equal comparison.
	OperatorGreaterThanOrEqual Operator = ">="
	// OperatorIn represents the 'IN' operator for checking membership in a list.
	OperatorIn Operator = "IN"
	// OperatorIsNotNull represents the 'IS NOT NULL' operator for non-null checks.
	OperatorIsNotNull Operator = "IS NOT NULL"
	// OperatorIsNull represents the 'IS NULL' operator for null checks.
	OperatorIsNull Operator = "IS NULL"
	// OperatorLessThan represents the '<' operator for less-than comparison.
	OperatorLessThan Operator = "<"
	// OperatorLessThanOrEqual represents the '<=' operator for less-than-or-equal comparison.
	OperatorLessThanOrEqual Operator = "<="
	// OperatorLike represents the 'LIKE' operator for pattern matching.
	OperatorLike Operator = "LIKE"
	// OperatorNotEqual represents the '!=' operator for inequality comparison.
	OperatorNotEqual Operator = "!="
	// OperatorNotIn represents the 'NOT IN' operator for checking absence in a list.
	OperatorNotIn Operator = "NOT IN"
	// OperatorNotLike represents the 'NOT LIKE' operator for negative pattern matching.
	OperatorNotLike Operator = "NOT LIKE"

	// JoinTypeInner represents the INNER JOIN type in SQL.
	JoinTypeInner JoinType = "INNER JOIN"
	// JoinTypeLeft represents the LEFT JOIN type in SQL.
	JoinTypeLeft JoinType = "LEFT JOIN"
	// JoinTypeRight represents the RIGHT JOIN type in SQL.
	JoinTypeRight JoinType = "RIGHT JOIN"

	// SortDirectionAscending represents ascending order (ASC) in SQL sorting.
	SortDirectionAscending SortDirection = "ASC"
	// SortDirectionDescending represents descending order (DESC) in SQL sorting.
	SortDirectionDescending SortDirection = "DESC"
)
