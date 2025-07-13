package goqube

// Join represents a SQL JOIN clause, defining how two tables are related in a query.
// It includes the join type, the table to join, and the filter condition for the join.
type Join struct {
	Filter Filter   // Filter specifies the ON condition for the join.
	Table  Table    // Table is the table being joined.
	Type   JoinType // Type is the kind of join (e.g., INNER, LEFT, RIGHT).
}
