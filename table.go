package goqube

// Table represents a database table or a subquery used as a table in SQL queries.
// It is used to define the source of data for SELECT, JOIN, or other SQL operations.
type Table struct {
	Alias       string       // Alias is the optional name used to reference this table in the query.
	Name        string       // Name is the actual name of the database table.
	SelectQuery *SelectQuery // SelectQuery is an optional subquery used as a table source.
}
