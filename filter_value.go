package goqube

// FilterValue represents a value or expression used in SQL filter conditions (e.g., WHERE or HAVING).
// It can reference a column, a subquery, a table, or a direct value for comparison.
type FilterValue struct {
	Column      string       // Column is the name of the table column used in the filter.
	SelectQuery *SelectQuery // SelectQuery is an optional subquery used as a filter value.
	Table       string       // Table is the name of the table this filter value belongs to.
	Value       interface{}  // Value is the actual value or expression to compare against.
}
