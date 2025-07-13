package goqube

import "errors"

// Error variables for validating query structure and builder usage.
var (
	// ErrInvalidDeleteQuery is returned when a delete query is missing a table.
	ErrInvalidDeleteQuery = errors.New("invalid delete query: table required")
	// ErrInvalidField is returned when a field does not specify a column or subquery.
	ErrInvalidField = errors.New("invalid field: must have column or subquery")
	// ErrInvalidFilter is returned when a filter field is not valid.
	ErrInvalidFilter = errors.New("invalid filter field")
	// ErrInvalidGroupBy is returned when the GROUP BY clause is missing or invalid.
	ErrInvalidGroupBy = errors.New("invalid group by field")
	// ErrInvalidInsertQuery is returned when an insert query lacks a table or values.
	ErrInvalidInsertQuery = errors.New("invalid insert query: table and values required")
	// ErrInvalidOrderBy is returned when the ORDER BY clause is missing or invalid.
	ErrInvalidOrderBy = errors.New("invalid order by field")
	// ErrInvalidPlaceholderType is returned when placeholders are not a map or slice of maps.
	ErrInvalidPlaceholderType = errors.New("invalid placeholder type: must be map[string]interface{} or []map[string]interface{}")
	// ErrInvalidTable is returned when a table does not have a name or subquery.
	ErrInvalidTable = errors.New("invalid table: must have name or subquery")
	// ErrInvalidUpdateQuery is returned when an update query lacks a table or fields.
	ErrInvalidUpdateQuery = errors.New("invalid update query: table and fields required")
	// ErrLikeValueType is returned when LIKE/NOT LIKE receives a non-string value.
	ErrLikeValueType = errors.New("LIKE/NOT LIKE value must be string, got %T")
	// ErrLikeValueTypeOrSubquery is returned when LIKE/NOT LIKE receives an invalid type (not string, subquery, or column).
	ErrLikeValueTypeOrSubquery = errors.New("LIKE/NOT LIKE value must be string, subquery, or table column")
	// ErrLikeValueTypeOrTable is returned when LIKE/NOT LIKE receives an invalid type (not string or column).
	ErrLikeValueTypeOrTable = errors.New("LIKE/NOT LIKE value must be string or table column")
	// ErrOperatorArray is returned when an operator expects an array or slice value.
	ErrOperatorArray = errors.New("operator requires array/slice value")
	// ErrOperatorArrayEmpty is returned when an operator expects a non-empty array or slice.
	ErrOperatorArrayEmpty = errors.New("operator requires non-empty array/slice")
	// ErrUnsupportedDialect is returned when the SQL dialect is not supported.
	ErrUnsupportedDialect = errors.New("unsupported SQL dialect")
)
