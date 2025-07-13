package goqube

// Filter represents a single condition or a group of conditions used in SQL WHERE or HAVING clauses.
// It supports nested filters, logical operators, and various comparison operators for flexible query building.
type Filter struct {
	Field    Field       // Field is the column or expression being filtered.
	Filters  []Filter    // Filters is a list of nested filter conditions for grouping.
	Logic    Logic       // Logic specifies how this filter combines with others (e.g., AND, OR).
	Operator Operator    // Operator is the comparison operator used in the filter (e.g., =, >, IN).
	Value    FilterValue // Value is the value or expression to compare the field against.
}
