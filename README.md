# goqube
A simple and flexible SQL query builder for Go, supporting multiple SQL dialects (SQL Server, MySQL, PostgreSQL, SQLite).

## Features
- Build SELECT, INSERT, UPDATE, DELETE queries with Go structs
- Supports filters, sorting, grouping, joins, and subqueries
- Parameterized queries for safe SQL execution
- Extensible for different SQL dialects

## Installation
```
go get github.com/fikri240794/goqube
```

## Basic Usage
All query examples below are simple examples for the Postgres dialect (DialectPostgres). The API is simple and consistent for all query types and other dialects.

---
### SELECT Example
```go
q := &SelectQuery{
    Fields: []Field{{Column: "id"}, {Column: "name"}},
    Table:  Table{Name: "users"},
    Filter: &Filter{
        Field:    Field{Column: "status"},
        Operator: OperatorEqual,
        Value:    FilterValue{Value: "active"},
    },
    Sorts: []Sort{{Field: Field{Column: "id"}, Direction: "DESC"}},
    Take:  10,
    Skip:  0,
}
sql, args, err := q.BuildSelectQuery(DialectPostgres)
fmt.Println("SQL:", sql)
fmt.Println("Args:", args)
// Output
// SQL: SELECT id, name FROM users WHERE status = $1 ORDER BY id DESC OFFSET $2 LIMIT $3
// Args: [active 0 10]
```

---
### INSERT Example
```go
q := &InsertQuery{
    Table: "users",
    Values: []map[string]interface{}{
        {"id": 1, "name": "foo"},
    },
}
sql, args, err := q.BuildInsertQuery(DialectPostgres)
fmt.Println("SQL:", sql)
fmt.Println("Args:", args)
// Output:
// SQL: INSERT INTO users (id, name) VALUES ($1, $2)
// Args: [1 foo]
```

---
### UPDATE Example
```go
q := &UpdateQuery{
    Table: "users",
    FieldsValue: map[string]interface{}{
        "name": "bar",
    },
    Filter: &Filter{
        Field:    Field{Column: "id"},
        Operator: OperatorEqual,
        Value:    FilterValue{Value: 1},
    },
}
sql, args, err := q.BuildUpdateQuery(DialectPostgres)
fmt.Println("SQL:", sql)
fmt.Println("Args:", args)
// Output:
// SQL: UPDATE users SET name = $1 WHERE id = $2
// Args: [bar 1]
```

---
### DELETE Example
```go
q := &DeleteQuery{
    Table: "users",
    Filter: &Filter{
        Field:    Field{Column: "id"},
        Operator: OperatorEqual,
        Value:    FilterValue{Value: 1},
    },
}
sql, args, err := q.BuildDeleteQuery(DialectPostgres)
fmt.Println("SQL:", sql)
fmt.Println("Args:", args)
// Output:
// SQL: DELETE FROM users WHERE id = $1
// Args: [1]
```