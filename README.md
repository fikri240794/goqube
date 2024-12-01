# Go Query Builder (goqube)
Go dynamic SQL query builder. Intended for building simple query with basic logic. Currently, supported dialect is MySQL and Postgres only.

## Installation
```bash
go get github.com/fikri240794/goqube
```

## Usage
### Example for SELECT:
```go
package main

import (
	"log"

	qb "github.com/fikri240794/goqube"
)

func main() {
	var (
		selectQuery *qb.SelectQuery
		query       string
		args        []interface{}
		err         error
	)

	selectQuery = qb.Select(
		qb.NewField("field1"),
		qb.NewField("field2"),
		qb.NewField("field3"),
		qb.NewField("field4"),
		qb.NewField("field5"),
	).
		From(qb.NewTable("table1")).
		Join(
			qb.InnerJoin(qb.NewTable("table2")).
				On(
					qb.NewFilter().
						SetLogic(qb.LogicAnd).
						AddFilter(
							qb.NewField("field1").
								FromTable("table1"),
							qb.OperatorEqual,
							qb.NewColumnFilterValue("field1").
								FromTable("table2"),
						),
				),
		).
		Join(
			qb.LeftJoin(
				qb.NewTable("table3").
					As("t3"),
			).On(
				qb.NewFilter().
					SetLogic(qb.LogicAnd).
					AddFilter(
						qb.NewField("field1").
							FromTable("table1"),
						qb.OperatorEqual,
						qb.NewColumnFilterValue("field1").
							FromTable("t3"),
					),
			),
		).
		Join(
			qb.RightJoin(
				qb.NewTable("table4").
					As("t4"),
			).On(
				qb.NewFilter().
					SetLogic(qb.LogicAnd).
					AddFilter(
						qb.NewField("field1").
							FromTable("table1"),
						qb.OperatorEqual,
						qb.NewColumnFilterValue("field1").
							FromTable("t4"),
					).AddFilter(
					qb.NewField("field2").
						FromTable("t4"),
					qb.OperatorGreaterThanOrEqual,
					qb.NewFilterValue(10),
				),
			),
		).
		Where(
			qb.NewFilter().
				SetLogic(qb.LogicAnd).
				AddFilters(
					qb.NewFilter().
						SetCondition(
							qb.NewField("field1").
								FromTable("table1"),
							qb.OperatorEqual,
							qb.NewFilterValue("value1"),
						),
					qb.NewFilter().
						SetCondition(
							qb.NewField("field2").
								FromTable("table2"),
							qb.OperatorNotEqual,
							qb.NewFilterValue(true),
						),
					qb.NewFilter().
						SetLogic(qb.LogicOr).
						AddFilter(
							qb.NewField("field3").
								FromTable("t3"),
							qb.OperatorGreaterThan,
							qb.NewFilterValue(50),
						).
						AddFilter(
							qb.NewField("field4").
								FromTable("t4"),
							qb.OperatorGreaterThanOrEqual,
							qb.NewFilterValue(75.4),
						),
					qb.NewFilter().
						SetLogic(qb.LogicOr).
						AddFilter(
							qb.NewField("field5").
								FromTable("table1"),
							qb.OperatorLessThan,
							qb.NewFilterValue("value5"),
						).
						AddFilter(
							qb.NewField("field6").
								FromTable("table2"),
							qb.OperatorLessThanOrEqual,
							qb.NewFilterValue("value6"),
						),
					qb.NewFilter().
						SetLogic(qb.LogicAnd).
						AddFilter(
							qb.NewField("field7").
								FromTable("t3"),
							qb.OperatorIsNull,
							nil,
						).
						AddFilter(
							qb.NewField("field8").
								FromTable("t4"),
							qb.OperatorIsNotNull,
							nil,
						),
					qb.NewFilter().
						SetLogic(qb.LogicOr).
						AddFilters(
							qb.NewFilter().
								SetLogic(qb.LogicAnd).
								AddFilter(
									qb.NewField("field9").
										FromTable("table1"),
									qb.OperatorIn,
									qb.NewFilterValue(
										[]string{
											"value9.1",
											"value9.2",
											"value9.3",
										},
									),
								).
								AddFilter(
									qb.NewField("field10").
										FromTable("table2"),
									qb.OperatorNotIn,
									qb.NewFilterValue(
										[3]float64{
											10.1,
											10.2,
											10.3,
										},
									),
								),
							qb.NewFilter().
								SetCondition(
									qb.NewField("field11").
										FromTable("t3"),
									qb.OperatorLike,
									qb.NewFilterValue("value11"),
								),
							qb.NewFilter().
								SetCondition(
									qb.NewField("field12").
										FromTable("t4"),
									qb.OperatorNotLike,
									qb.NewFilterValue("value12"),
								),
						),
				),
		).
		GroupBy(
			qb.NewField("field1").
				FromTable("table1"),
			qb.NewField("field1").
				FromTable("table2"),
		).
		OrderBy(
			qb.NewSort(
				qb.NewField("field1").
					FromTable("table1"),
				qb.SortDirectionAscending,
			),
			qb.NewSort(
				qb.NewField("field2").
					FromTable("t3"),
				qb.SortDirectionDescending,
			),
		).
		Limit(50).
		Offset(50)

	query, args, err = selectQuery.ToSQLWithArgsWithAlias(qb.DialectPostgres, []interface{}{})

	log.Printf("query: %s", query)
	/*
		-- QUERY --
		select
			field1,
			field2,
			field3,
			field4,
			field5
		from
			table1
			inner join table2 on table1.field1 = table2.field1
			left join table3 as t3 on table1.field1 = t3.field1
			right join table4 as t4 on table1.field1 = t4.field1
			and t4.field2 >= $1
		where
			table1.field1 = $2
			and table2.field2 != $3
			and (
				t3.field3 > $4
				or t4.field4 >= $5
			)
			and (
				table1.field5 < $6
				or table2.field6 <= $7
			)
			and (
				t3.field7 is null
				and t4.field8 is not null
			)
			and (
				(
					table1.field9 in ($8, $9, $10)
					and table2.field10 not in ($11, $12, $13)
				)
				or t3.field11::text ilike concat('%', $14::text, '%')
				or t4.field12::text not ilike concat('%', $15::text, '%')
			)
		group by
			table1.field1,
			table2.field1
		order by
			table1.field1 asc,
			t3.field2 desc
		limit
			$16
		offset
			$17
	*/

	log.Printf("args: %v", args)
	/*
		-- ARGS --
		[
			10
			value1
			true
			50
			75.4
			value5
			value6
			value9.1
			value9.2
			value9.3
			10.1
			10.2
			10.3
			value11
			value12
			50,
			50
		]
	*/

	log.Printf("err: %v", err) // nil
}
```

### Example for INSERT:
```go
package main

import (
	"log"
	sq "github.com/fikri240794/goqube"
)

func main() {
	var (
		insertQuery *qb.InsertQuery
		query       string
		args        []interface{}
		err         error
	)

	insertQuery = qb.Insert().
		Into("table1").
		Value("field1", 1).
		Value("field2", "value2.1").
		Value("field3", 3.14).
		Value("field4", 4).
		Value("field5", false).
		Value("field1", 2).
		Value("field2", "value2.2").
		Value("field3", 3.14).
		Value("field4", 4).
		Value("field5", false).
		Value("field1", 3).
		Value("field2", "value2.1").
		Value("field3", 3.14).
		Value("field4", 4).
		Value("field5", false)

	query, args, err = insertQuery.ToSQLWithArgs(qb.DialectPostgres)

	log.Printf("query: %s", query)
	/*
		-- QUERY --
		insert into
			table1(field1, field2, field3, field4, field5)
		values
			($1, $2, $3, $4, $5),
			($6, $7, $8, $9, $10),
			($11, $12, $13, $14, $15)
	*/

	log.Printf("args: %v", args)
	/*
		-- ARGS --
		[
			1,
			"value2.1",
			3.14,
			4,
			false,
			2,
			"value2.2",
			3.14,
			4,
			false,
			3,
			"value2.1",
			3.14,
			4,
			false
		]
	*/

	log.Printf("err: %v", err) // nil
}
```

### Example for UPDATE:
```go
package main

import (
	"log"
	sq "github.com/fikri240794/goqube"
)

func main() {
	var (
		updateQuery *qb.UpdateQuery
		query       string
		args        []interface{}
		err         error
	)

	updateQuery = qb.Update("table1").
		Set("field2", 1).
		Set("field3", "value3").
		Set("field4", 4.265).
		Set("field5", true).
		Where(
			qb.NewFilter().
				SetLogic(qb.LogicAnd).
				AddFilter(
					qb.NewField("field1"),
					qb.OperatorEqual,
					qb.NewFilterValue("value1"),
				),
		)

	query, args, err = updateQuery.ToSQLWithArgs(qb.DialectPostgres)

	log.Printf("query: %s", query)
	/*
		-- QUERY --
		update
			table1
		set
			field2 = $1,
			field3 = $2,
			field4 = $3,
			field5 = $4
		where
			field1 = $5
	*/
	log.Printf("args: %v", args)
	/*
		-- ARGS --
		[
			1,
			"value3",
			4.265,
			true,
			"value1"
		]
	*/
	log.Printf("err: %v", err) // nil
}
```

### Example for DELETE:
```go
package main

import (
	"log"
	sq "github.com/fikri240794/goqube"
)

func main() {
	var (
		deleteQuery *qb.DeleteQuery
		query       string
		args        []interface{}
		err         error
	)

	deleteQuery = qb.Delete().
		From("table1").
		Where(
			qb.NewFilter().
				SetLogic(qb.LogicAnd).
				AddFilter(
					qb.NewField("field1"),
					qb.OperatorEqual,
					qb.NewFilterValue("value1"),
				),
		)

	query, args, err = deleteQuery.ToSQLWithArgs(qb.DialectPostgres)

	log.Printf("query: %s", query)
	/*
		-- QUERY --
		delete from
			table1
		where
			field1 = $1
	*/

	log.Printf("args: %v", args)
	/*
		-- ARGS --
		["value1"]
	*/

	log.Printf("err: %v", err) // nil
}
```