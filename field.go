package goqube

// Field represents a column, subquery, or table field used in SQL queries.
// It can be used for SELECT, WHERE, GROUP BY, or ORDER BY clauses.
type Field struct {
	Alias       string       // Alias is the optional name used to reference this field in the query result.
	Column      string       // Column is the name of the table column represented by this field.
	SelectQuery *SelectQuery // SelectQuery is an optional subquery used as a field.
	Table       string       // Table is the name of the table this field belongs to.
}
