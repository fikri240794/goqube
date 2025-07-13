package goqube

// Sort represents an ORDER BY clause in SQL, specifying how query results should be sorted.
// It defines the field to sort by and the direction (ascending or descending).
type Sort struct {
	Direction SortDirection // Direction specifies ascending or descending order.
	Field     Field         // Field is the column or expression to sort by.
}
