package goqube

import "errors"

type Dialect string

const (
	DialectMySQL    Dialect = "mysql"
	DialectPostgres Dialect = "postgres"
)

var placeholderMap map[Dialect]string = map[Dialect]string{
	DialectMySQL:    "?",
	DialectPostgres: "$",
}

type Logic string
type Operator string

const (
	LogicAnd Logic = "and"
	LogicOr  Logic = "or"

	OperatorEqual              Operator = "equal"
	OperatorNotEqual           Operator = "not_equal"
	OperatorGreaterThan        Operator = "greater_than"
	OperatorGreaterThanOrEqual Operator = "greater_than_or_equal"
	OperatorLessThan           Operator = "less_than"
	OperatorLessThanOrEqual    Operator = "less_than_or_equal"
	OperatorIsNull             Operator = "is_null"
	OperatorIsNotNull          Operator = "is_not_null"
	OperatorIn                 Operator = "in"
	OperatorNotIn              Operator = "not_in"
	OperatorLike               Operator = "like"
	OperatorNotLike            Operator = "not_like"
)

var filterOperatorMap map[Operator]string = map[Operator]string{
	OperatorEqual:              "=",
	OperatorNotEqual:           "!=",
	OperatorGreaterThan:        ">",
	OperatorGreaterThanOrEqual: ">=",
	OperatorLessThan:           "<",
	OperatorLessThanOrEqual:    "<=",
	OperatorIsNull:             "is null",
	OperatorIsNotNull:          "is not null",
	OperatorIn:                 "in",
	OperatorNotIn:              "not in",
	OperatorLike:               "like",
	OperatorNotLike:            "not like",
}

type SortDirection string

const (
	SortDirectionAscending  SortDirection = "asc"
	SortDirectionDescending SortDirection = "desc"
)

const (
	errForOperatorf                     string = "%s for operator %s"
	errUnsupportedValueTypeForOperatorf string = "unsupported %s value type for operator %s"
	errUnsupportedValueTypef            string = "unsupported %s value type"
)

var (
	ErrAliasIsRequired                        error = errors.New("alias is required")
	ErrColumnIsRequired                       error = errors.New("column is required")
	ErrConflictFieldColumnAndFieldSelectQuery error = errors.New("conflict between field column and field select query")
	ErrConflictTableNameAndTableSelectQuery   error = errors.New("conflict between table name and table select query")
	ErrDialectIsRequired                      error = errors.New("dialect is required")
	ErrFieldIsNil                             error = errors.New("field is nil")
	ErrFieldIsNotEmpty                        error = errors.New("field is not empty")
	ErrFieldIsRequired                        error = errors.New("field is required")
	ErrFieldsIsRequired                       error = errors.New("fields is required")
	ErrFilterIsRequired                       error = errors.New("filter is required")
	ErrFilterValueIsNil                       error = errors.New("filter value is nil")
	ErrFiltersIsRequired                      error = errors.New("filters is required")
	ErrJoinTypeIsRequired                     error = errors.New("join type is required")
	ErrLogicIsRequired                        error = errors.New("logic is required")
	ErrNameIsRequired                         error = errors.New("name is required")
	ErrOperatorIsNotEmpty                     error = errors.New("operator is not empty")
	ErrOperatorIsRequired                     error = errors.New("operator is required")
	ErrTableIsRequired                        error = errors.New("table is required")
	ErrValueIsNotNil                          error = errors.New("value is not nil")
	ErrValueIsRequired                        error = errors.New("value is required")
	ErrValueLengthIsNotEqualToFieldsLength    error = errors.New("value length is not equal to fields length")
	ErrValuesIsRequired                       error = errors.New("values is required")
)

type JoinType string

const (
	InnerJoinType JoinType = "inner join"
	LeftJoinType  JoinType = "left join"
	RightJoinType JoinType = "right join"
	FullJoinType  JoinType = "full join"
)
