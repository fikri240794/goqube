# 🧊 GoQube

> SQL query builder for Go with multi-dialect support

## ✨ Features

- 🎯 **Multi-dialect support** - PostgreSQL, MySQL, SQLite, SQL Server
- 🛡️ **Type-safe query building** - Compile-time safety with Go structs
- 🔒 **SQL injection protection** - Parameterized queries by default
- 🧩 **Rich operations** - Complex filters, joins, sorting, and grouping

## 📦 Installation

```bash
go get github.com/fikri240794/goqube
```

## 🚀 Quick Start

```go
import "github.com/fikri240794/goqube"

// Build a simple SELECT query
query := &goqube.SelectQuery{
    Fields: []goqube.Field{{Column: "id"}, {Column: "name"}},
    Table:  goqube.Table{Name: "users"},
    Filter: &goqube.Filter{
        Field:    goqube.Field{Column: "active"},
        Operator: goqube.OperatorEqual,
        Value:    goqube.FilterValue{Value: true},
    },
}

sql, args, err := query.BuildSelectQuery(goqube.DialectPostgres)
// SQL: SELECT id, name FROM users WHERE active = $1
// Args: [true]
```

## 📚 Examples

> 💡 **Want more examples?** Check out the [examples](examples/) directory for comprehensive demonstrations of all query types and dialects!

### 🔍 SELECT Example
```go
q := &SelectQuery{
    Fields: []Field{{Column: "id"}, {Column: "name"}},
    Table:  Table{Name: "users"},
    Filter: &Filter{
        Field:    Field{Column: "status"},
        Operator: OperatorEqual,
        Value:    FilterValue{Value: "active"},
    },
    Sorts: []Sort{{Field: Field{Column: "id"}, Direction: SortDirectionDescending}},
    Take:  10,
    Skip:  0,
}
sql, args, err := q.BuildSelectQuery(DialectPostgres)
// SQL: SELECT id, name FROM users WHERE status = $1 ORDER BY id DESC LIMIT $2 OFFSET $3
// Args: [active 10 0]
```

### ➕ INSERT Example
```go
q := &InsertQuery{
    Table: "users",
    Values: []map[string]interface{}{
        {"id": 1, "name": "foo"},
    },
}
sql, args, err := q.BuildInsertQuery(DialectPostgres)
// SQL: INSERT INTO users (id, name) VALUES ($1, $2)
// Args: [1 foo]
```

### ✏️ UPDATE Example
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
// SQL: UPDATE users SET name = $1 WHERE id = $2
// Args: [bar 1]
```

### 🔄 Bulk UPDATE Example
```go
q := &BulkUpdateQuery{
    Table:      "users",
    PrimaryKey: "id",
    FieldsValues: []map[string]interface{}{
        {"id": 1, "name": "foo", "age": 30},
        {"id": 2, "name": "bar", "age": 40},
    },
    ColumnsType: map[string]string{
        "id":   "integer",
        "age":  "integer",
        "name": "text",
    },
}
sql, args, err := q.BuildBulkUpdateQuery(DialectPostgres)
// SQL: UPDATE users AS t SET age = c.age, name = c.name FROM (VALUES ($1::integer, $2::integer, $3::text), ($4::integer, $5::integer, $6::text)) AS c(id, age, name) WHERE t.id = c.id::integer
// Args: [1 30 foo 2 40 bar]

// ⚠️ ColumnsType is REQUIRED for PostgreSQL and SQL Server dialects.
// It enables proper type casting in VALUES placeholders ($1::integer)
// and in the JOIN condition (c.id::integer / CONVERT(type, c.id)).
// For MySQL and SQLite, ColumnsType is optional.
```

### 🗑️ DELETE Example
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
// SQL: DELETE FROM users WHERE id = $1
// Args: [1]
```

## 🗄️ Supported SQL Dialects

<div align="center">

| Database | Placeholder Style | Example Query |
|----------|-------------------|---------------|
| **PostgreSQL** | `$1, $2, $3` | `SELECT * FROM users WHERE id = $1` |
| **MySQL** | `?` | `SELECT * FROM users WHERE id = ?` |
| **SQLite** | `?` | `SELECT * FROM users WHERE id = ?` |
| **SQL Server** | `@p0, @p1, @p2` | `SELECT * FROM users WHERE id = @p0` |

</div>

---