package goqube

import (
	"fmt"
	"reflect"
	"strings"
)

type Filter struct {
	Logic    Logic
	Field    *Field
	Operator Operator
	Value    *FilterValue
	Filters  []*Filter
}

func NewFilter() *Filter {
	return &Filter{}
}

func (f *Filter) SetLogic(logic Logic) *Filter {
	f.Logic = logic
	return f
}

func (f *Filter) SetCondition(field *Field, operator Operator, value *FilterValue) *Filter {
	f.Field = field
	f.Operator = operator
	f.Value = value
	return f
}

func (f *Filter) AddFilter(field *Field, operator Operator, value *FilterValue) *Filter {
	f.Filters = append(f.Filters, &Filter{Field: field, Operator: operator, Value: value})
	return f
}

func (f *Filter) AddFilters(filters ...*Filter) *Filter {
	f.Filters = append(f.Filters, filters...)
	return f
}

func (f *Filter) validate(dialect Dialect) error {
	var reflectValue reflect.Value

	if dialect == "" {
		return ErrDialectIsRequired
	}

	if f.Value != nil && f.Value.Column == "" && f.Value.SelectQuery == nil {
		reflectValue = reflect.ValueOf(f.Value.Value)
	}

	if f.Logic != "" && f.Field != nil {
		return ErrFieldIsNotEmpty
	}

	if f.Logic != "" && f.Operator != "" {
		return ErrOperatorIsNotEmpty
	}

	if f.Logic != "" && f.Value != nil &&
		(f.Value.Column != "" || f.Value.SelectQuery != nil ||
			(f.Value.SelectQuery == nil && (f.Value.Value != nil || reflectValue.Kind() != reflect.Invalid))) {
		return ErrValueIsNotNil
	}

	if f.Logic != "" && len(f.Filters) == 0 {
		return ErrFiltersIsRequired
	}

	if f.Logic == "" && len(f.Filters) > 0 {
		return ErrLogicIsRequired
	}

	if f.Logic == "" && len(f.Filters) == 0 {
		if f.Field == nil {
			return ErrFieldIsRequired
		}

		if f.Operator == "" {
			return ErrOperatorIsRequired
		}

		if f.Operator != OperatorIsNull && f.Operator != OperatorIsNotNull &&
			(f.Value == nil ||
				(f.Value != nil && f.Value.Column == "" && f.Value.SelectQuery == nil && f.Value.Value == nil && reflectValue.Kind() == reflect.Invalid)) {
			return ErrValueIsRequired
		}

		if (f.Operator == OperatorIsNull || f.Operator == OperatorIsNotNull) &&
			f.Value != nil &&
			(f.Value.Column != "" && f.Value.SelectQuery != nil ||
				(f.Value.SelectQuery == nil && (f.Value.Value != nil || reflectValue.Kind() != reflect.Invalid))) {
			return ErrValueIsNotNil
		}

		if f.Operator != OperatorIn && f.Operator != OperatorNotIn &&
			f.Value != nil &&
			(f.Value.Column == "" && f.Value.SelectQuery == nil && (reflectValue.Kind() == reflect.Slice || reflectValue.Kind() == reflect.Array)) {
			return fmt.Errorf(errUnsupportedValueTypeForOperatorf, reflectValue.Kind().String(), f.Operator)
		}

		if (f.Operator == OperatorIn || f.Operator == OperatorNotIn) && f.Value != nil && f.Value.Column == "" && f.Value.SelectQuery == nil {
			if reflectValue.Kind() != reflect.Slice && reflectValue.Kind() != reflect.Array {
				return fmt.Errorf(errUnsupportedValueTypeForOperatorf, reflectValue.Kind().String(), f.Operator)
			}

			if reflectValue.Len() == 0 {
				return ErrValueIsRequired
			}
		}
	}

	for i := range f.Filters {
		var err error = f.Filters[i].validate(dialect)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *Filter) toSQLWithArgs(dialect Dialect, args []interface{}, isRoot bool) (string, []interface{}, error) {
	var (
		field                string
		queryValue           string
		conditionQueryFormat string
		filterOperator       string
		placeholderStartIdx  int
		placeholderEndIdx    int
		placeholder          string
		conditionQuery       string
		conditionQueries     []string
		whereClause          string
		err                  error
	)

	if f.Operator != "" {
		field, args, err = f.Field.ToSQLWithArgsWithAlias(dialect, args)
		if err != nil {
			return "", nil, err
		}
	}

	switch f.Operator {
	case OperatorEqual, OperatorNotEqual, OperatorGreaterThan, OperatorGreaterThanOrEqual, OperatorLessThan, OperatorLessThanOrEqual:
		queryValue, args, err = f.Value.ToSQLWithArgs(dialect, args)
		if err != nil {
			return "", nil, err
		}

		conditionQueryFormat = "%s %s %s"
		filterOperator = filterOperatorMap[f.Operator]
		conditionQuery = fmt.Sprintf(conditionQueryFormat, field, filterOperator, queryValue)

		if queryValue == "" {
			placeholderStartIdx = len(args)
			placeholderEndIdx = len(args)
			placeholder = getPlaceholder(dialect, placeholderStartIdx, placeholderEndIdx)
			conditionQuery = fmt.Sprintf(conditionQueryFormat, field, filterOperator, placeholder)
		}

		return conditionQuery, args, nil

	case OperatorIsNull, OperatorIsNotNull:
		conditionQueryFormat = "%s %s"
		filterOperator = filterOperatorMap[f.Operator]
		conditionQuery = fmt.Sprintf(conditionQueryFormat, field, filterOperator)

		return conditionQuery, args, nil

	case OperatorIn, OperatorNotIn:
		filterOperator = filterOperatorMap[f.Operator]

		if f.Value.SelectQuery == nil {
			var interfaceSlice []interface{}

			conditionQueryFormat = "%s %s (%s)"

			interfaceSlice, err = typedSliceToInterfaceSlice(f.Value.Value)
			if err != nil {
				err = fmt.Errorf(errForOperatorf, err.Error(), f.Operator)
				return "", nil, err
			}

			args = append(args, interfaceSlice...)
			placeholderStartIdx = len(args) - (len(interfaceSlice) - 1)
			placeholderEndIdx = len(args)
			placeholder = getPlaceholder(dialect, placeholderStartIdx, placeholderEndIdx)
			conditionQuery = fmt.Sprintf(conditionQueryFormat, field, filterOperator, placeholder)
		} else {
			queryValue, args, err = f.Value.ToSQLWithArgs(dialect, args)
			if err != nil {
				return "", nil, err
			}

			conditionQueryFormat = "%s %s %s"
			conditionQuery = fmt.Sprintf(conditionQueryFormat, field, filterOperator, queryValue)
		}

		return conditionQuery, args, nil

	case OperatorLike, OperatorNotLike:
		queryValue, args, err = f.Value.ToSQLWithArgs(dialect, args)
		if err != nil {
			return "", nil, err
		}

		switch dialect {
		case DialectMySQL:
			conditionQueryFormat = "cast(%s as char) %s concat('%%', cast(%s as char), '%%')"
			filterOperator = filterOperatorMap[f.Operator]
		case DialectPostgres:
			conditionQueryFormat = "%s::text %s concat('%%', %s::text, '%%')"
			filterOperator = fmt.Sprintf("i%s", filterOperatorMap[OperatorLike])
			if f.Operator == OperatorNotLike {
				filterOperator = fmt.Sprintf("not i%s", filterOperatorMap[OperatorLike])
			}
		}

		conditionQuery = fmt.Sprintf(conditionQueryFormat, field, filterOperator, queryValue)

		if queryValue == "" {
			placeholderStartIdx = len(args)
			placeholderEndIdx = len(args)
			placeholder = getPlaceholder(dialect, placeholderStartIdx, placeholderEndIdx)
			conditionQuery = fmt.Sprintf(conditionQueryFormat, field, filterOperator, placeholder)
		}

		return conditionQuery, args, nil
	}

	if len(f.Filters) == 0 {
		return "", args, nil
	}

	for i := range f.Filters {
		var (
			subConditionQuery string
			subArgs           []interface{}
		)

		if f.Filters[i] == nil {
			return "", args, nil
		}

		subConditionQuery, subArgs, err = f.Filters[i].toSQLWithArgs(dialect, args, false)
		if err != nil {
			return "", nil, err
		}

		if subConditionQuery != "" {
			conditionQueries = append(conditionQueries, subConditionQuery)
		}

		args = subArgs
	}

	if len(conditionQueries) == 0 {
		return "", args, nil
	}

	whereClause = fmt.Sprintf("(%s)", strings.Join(conditionQueries, fmt.Sprintf(" %s ", f.Logic)))
	if isRoot {
		whereClause = strings.Join(conditionQueries, fmt.Sprintf(" %s ", f.Logic))
	}

	return whereClause, args, nil
}

func (f *Filter) ToSQLWithArgs(dialect Dialect, args []interface{}) (string, []interface{}, error) {
	var err error = f.validate(dialect)
	if err != nil {
		return "", nil, err
	}

	return f.toSQLWithArgs(dialect, args, true)
}
