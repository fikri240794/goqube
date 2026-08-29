package main

import (
	"fmt"
	"log"

	"github.com/fikri240794/goqube"
)

func main() {
	fmt.Println("=== GoQube Examples ===")
	fmt.Println()

	// Test all dialects
	dialects := []struct {
		name    string
		dialect goqube.Dialect
	}{
		{"PostgreSQL", goqube.DialectPostgres},
		{"MySQL", goqube.DialectMySQL},
		{"SQLite", goqube.DialectSQLite},
		{"SQL Server", goqube.DialectSQLServer},
	}

	for _, d := range dialects {
		fmt.Printf("=== %s Examples ===\n", d.name)
		fmt.Println()

		// Simple SELECT
		fmt.Println("1. Simple SELECT:")
		simpleSelect(d.dialect, false)
		fmt.Println()

		// Complex SELECT with JOINs
		fmt.Println("2. Complex SELECT with JOINs:")
		complexSelect(d.dialect, false)
		fmt.Println()

		// Simple INSERT
		fmt.Println("3. Simple INSERT:")
		simpleInsert(d.dialect, false)
		fmt.Println()

		// Batch INSERT
		fmt.Println("4. Batch INSERT:")
		batchInsert(d.dialect, false)
		fmt.Println()

		// Simple UPDATE
		fmt.Println("5. Simple UPDATE:")
		simpleUpdate(d.dialect, false)
		fmt.Println()

		// Complex UPDATE with subquery
		fmt.Println("6. Complex UPDATE:")
		complexUpdate(d.dialect, false)
		fmt.Println()

		// Bulk UPDATE
		fmt.Println("7. Bulk UPDATE:")
		bulkUpdate(d.dialect, false)
		fmt.Println()

		// Simple DELETE
		fmt.Println("8. Simple DELETE:")
		simpleDelete(d.dialect, false)
		fmt.Println()

		// Complex DELETE with multiple conditions
		fmt.Println("9. Complex DELETE:")
		complexDelete(d.dialect, false)
		fmt.Println()

		// INSERT with RETURNING (not supported in MySQL)
		if d.dialect != goqube.DialectMySQL {
			fmt.Println("10. INSERT with RETURNING:")
			insertWithReturning(d.dialect, false)
			fmt.Println()

			// UPDATE with RETURNING
			fmt.Println("11. UPDATE with RETURNING:")
			updateWithReturning(d.dialect, false)
			fmt.Println()

			// DELETE with RETURNING
			fmt.Println("12. DELETE with RETURNING:")
			deleteWithReturning(d.dialect, false)
			fmt.Println()
		}

		fmt.Println("=" + fmt.Sprintf("%*s", len(d.name)+20, "="))
		fmt.Println()
	}
}

// Simple SELECT example.
// When triggerError is true, an invalid query is built so the error branch runs.
func simpleSelect(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.SelectQuery{
		Fields: []goqube.Field{
			{Column: "id"},
			{Column: "name"},
			{Column: "email"},
		},
		Table: goqube.Table{Name: "users"},
		Filter: &goqube.Filter{
			Field:    goqube.Field{Column: "active"},
			Operator: goqube.OperatorEqual,
			Value:    goqube.FilterValue{Value: true},
		},
		Sorts: []goqube.Sort{
			{Field: goqube.Field{Column: "created_at"}, Direction: goqube.SortDirectionDescending},
		},
		Take: 10,
		Skip: 5,
	}

	if triggerError {
		// Empty query: no table, so the builder returns ErrInvalidTable on every dialect.
		query = &goqube.SelectQuery{}
	}

	sql, args, err := query.BuildSelectQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// Complex SELECT with JOINs example.
// When triggerError is true, an invalid query is built so the error branch runs.
func complexSelect(dialect goqube.Dialect, triggerError bool) {
	// Define dialect-specific raw queries and placeholders
	var (
		commentCountQuery    string
		userFilterQuery      string
		postFilterQuery      string
		scoreComparisonQuery string
		args1                []interface{}
		args2                []interface{}
		args3                []interface{}
		args4                []interface{}
	)

	switch dialect {
	case goqube.DialectMySQL, goqube.DialectSQLite:
		commentCountQuery = "SELECT COUNT(*) FROM comments WHERE post_id = p.id AND status = ?"
		userFilterQuery = "SELECT * FROM users WHERE registration_date > ? AND country = ?"
		postFilterQuery = "SELECT * FROM posts WHERE created_at > ? AND status = ?"
		scoreComparisonQuery = "SELECT AVG(score) FROM user_scores WHERE exam_date > ?"
		args1 = []interface{}{"approved"}
		args2 = []interface{}{"2023-01-01", "ID"}
		args3 = []interface{}{"2023-06-01", "published"}
		args4 = []interface{}{"2023-01-01"}
	case goqube.DialectPostgres:
		commentCountQuery = "SELECT COUNT(*) FROM comments WHERE post_id = p.id AND status = $1"
		userFilterQuery = "SELECT * FROM users WHERE registration_date > $1 AND country = $2"
		postFilterQuery = "SELECT * FROM posts WHERE created_at > $1 AND status = $2"
		scoreComparisonQuery = "SELECT AVG(score) FROM user_scores WHERE exam_date > $1"
		args1 = []interface{}{"approved"}
		args2 = []interface{}{"2023-01-01", "ID"}
		args3 = []interface{}{"2023-06-01", "published"}
		args4 = []interface{}{"2023-01-01"}
	case goqube.DialectSQLServer:
		commentCountQuery = "SELECT COUNT(*) FROM comments WHERE post_id = p.id AND status = @p0"
		userFilterQuery = "SELECT * FROM users WHERE registration_date > @p0 AND country = @p1"
		postFilterQuery = "SELECT * FROM posts WHERE created_at > @p0 AND status = @p1"
		scoreComparisonQuery = "SELECT AVG(score) FROM user_scores WHERE exam_date > @p0"
		args1 = []interface{}{"approved"}
		args2 = []interface{}{"2023-01-01", "ID"}
		args3 = []interface{}{"2023-06-01", "published"}
		args4 = []interface{}{"2023-01-01"}
	}

	query := &goqube.SelectQuery{
		Fields: []goqube.Field{
			{Table: "u", Column: "name"},
			{Table: "p", Column: "title"},
			{Table: "c", Column: "name", Alias: "category_name"},
			{
				SelectQuery: &goqube.SelectQuery{
					Raw:     commentCountQuery,
					RawArgs: args1,
				},
				Alias: "comment_count",
			},
		},
		Table: goqube.Table{
			SelectQuery: &goqube.SelectQuery{
				Raw:     userFilterQuery,
				RawArgs: args2,
			},
			Alias: "u",
		},
		Joins: []goqube.Join{
			{
				Type: goqube.JoinTypeLeft,
				Table: goqube.Table{
					SelectQuery: &goqube.SelectQuery{
						Raw:     postFilterQuery,
						RawArgs: args3,
					},
					Alias: "p",
				},
				Filter: goqube.Filter{
					Field:    goqube.Field{Table: "u", Column: "id"},
					Operator: goqube.OperatorEqual,
					Value:    goqube.FilterValue{Table: "p", Column: "user_id"},
				},
			},
			{
				Type:  goqube.JoinTypeInner,
				Table: goqube.Table{Name: "categories", Alias: "c"},
				Filter: goqube.Filter{
					Field:    goqube.Field{Table: "p", Column: "category_id"},
					Operator: goqube.OperatorEqual,
					Value:    goqube.FilterValue{Table: "c", Column: "id"},
				},
			},
		},
		Filter: &goqube.Filter{
			Logic: goqube.LogicAnd,
			Filters: []goqube.Filter{
				{
					Field:    goqube.Field{Table: "u", Column: "active"},
					Operator: goqube.OperatorEqual,
					Value:    goqube.FilterValue{Value: true},
				},
				{
					Field:    goqube.Field{Table: "p", Column: "published_at"},
					Operator: goqube.OperatorIsNotNull,
				},
				{
					Field:    goqube.Field{Table: "u", Column: "score"},
					Operator: goqube.OperatorGreaterThan,
					Value: goqube.FilterValue{
						SelectQuery: &goqube.SelectQuery{
							Raw:     scoreComparisonQuery,
							RawArgs: args4,
						},
					},
				},
			},
		},
		GroupByFields: []goqube.Field{
			{Table: "u", Column: "id"},
			{Table: "c", Column: "id"},
		},
		Take: 20,
	}

	if triggerError {
		// Empty query: no table, so the builder returns ErrInvalidTable on every dialect.
		query = &goqube.SelectQuery{}
	}

	sql, args, err := query.BuildSelectQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// Simple INSERT example.
// When triggerError is true, an invalid query is built so the error branch runs.
func simpleInsert(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.InsertQuery{
		Table: "users",
		Values: []map[string]interface{}{
			{
				"name":       "John Doe",
				"email":      "john@example.com",
				"active":     true,
				"created_at": "2024-01-01 10:00:00",
			},
		},
	}

	if triggerError {
		// Empty query: missing table and values, so the builder returns ErrInvalidInsertQuery.
		query = &goqube.InsertQuery{}
	}

	sql, args, err := query.BuildInsertQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// Batch INSERT example.
// When triggerError is true, an invalid query is built so the error branch runs.
func batchInsert(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.InsertQuery{
		Table: "products",
		Values: []map[string]interface{}{
			{"name": "Laptop", "price": 999.99, "category_id": 1, "stock": 50},
			{"name": "Mouse", "price": 29.99, "category_id": 2, "stock": 100},
			{"name": "Keyboard", "price": 79.99, "category_id": 2, "stock": 75},
			{"name": "Monitor", "price": 299.99, "category_id": 1, "stock": 25},
		},
	}

	if triggerError {
		// Empty query: missing table and values, so the builder returns ErrInvalidInsertQuery.
		query = &goqube.InsertQuery{}
	}

	sql, args, err := query.BuildInsertQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// Simple UPDATE example.
// When triggerError is true, an invalid query is built so the error branch runs.
func simpleUpdate(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.UpdateQuery{
		Table: "users",
		FieldsValue: map[string]interface{}{
			"name":       "Jane Doe",
			"email":      "jane@example.com",
			"updated_at": "2024-01-01 12:00:00",
		},
		Filter: &goqube.Filter{
			Field:    goqube.Field{Column: "id"},
			Operator: goqube.OperatorEqual,
			Value:    goqube.FilterValue{Value: 1},
		},
	}

	if triggerError {
		// Empty query: missing table and fields, so the builder returns ErrInvalidUpdateQuery.
		query = &goqube.UpdateQuery{}
	}

	sql, args, err := query.BuildUpdateQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// Complex UPDATE with subquery example.
// When triggerError is true, an invalid query is built so the error branch runs.
func complexUpdate(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.UpdateQuery{
		Table: "users",
		FieldsValue: map[string]interface{}{
			"last_login":  "2024-01-01 15:30:00",
			"login_count": 5,
			"status":      "premium",
		},
		Filter: &goqube.Filter{
			Logic: goqube.LogicAnd,
			Filters: []goqube.Filter{
				{
					Field:    goqube.Field{Column: "active"},
					Operator: goqube.OperatorEqual,
					Value:    goqube.FilterValue{Value: true},
				},
				{
					Field:    goqube.Field{Column: "created_at"},
					Operator: goqube.OperatorGreaterThan,
					Value:    goqube.FilterValue{Value: "2023-01-01"},
				},
				{
					Field:    goqube.Field{Column: "role"},
					Operator: goqube.OperatorIn,
					Value:    goqube.FilterValue{Value: []string{"admin", "user"}},
				},
			},
		},
	}

	if triggerError {
		// Empty query: missing table and fields, so the builder returns ErrInvalidUpdateQuery.
		query = &goqube.UpdateQuery{}
	}

	sql, args, err := query.BuildUpdateQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// Simple DELETE example.
// When triggerError is true, an invalid query is built so the error branch runs.
func simpleDelete(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.DeleteQuery{
		Table: "users",
		Filter: &goqube.Filter{
			Field:    goqube.Field{Column: "id"},
			Operator: goqube.OperatorEqual,
			Value:    goqube.FilterValue{Value: 123},
		},
	}

	if triggerError {
		// Empty query: missing table, so the builder returns ErrInvalidDeleteQuery.
		query = &goqube.DeleteQuery{}
	}

	sql, args, err := query.BuildDeleteQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// Complex DELETE with multiple conditions example.
// When triggerError is true, an invalid query is built so the error branch runs.
func complexDelete(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.DeleteQuery{
		Table: "logs",
		Filter: &goqube.Filter{
			Logic: goqube.LogicAnd,
			Filters: []goqube.Filter{
				{
					Field:    goqube.Field{Column: "created_at"},
					Operator: goqube.OperatorLessThan,
					Value:    goqube.FilterValue{Value: "2023-01-01"},
				},
				{
					Field:    goqube.Field{Column: "level"},
					Operator: goqube.OperatorIn,
					Value:    goqube.FilterValue{Value: []string{"DEBUG", "INFO"}},
				},
				{
					Logic: goqube.LogicOr,
					Filters: []goqube.Filter{
						{
							Field:    goqube.Field{Column: "size"},
							Operator: goqube.OperatorGreaterThan,
							Value:    goqube.FilterValue{Value: 1000000},
						},
						{
							Field:    goqube.Field{Column: "archived"},
							Operator: goqube.OperatorEqual,
							Value:    goqube.FilterValue{Value: true},
						},
					},
				},
			},
		},
	}

	if triggerError {
		// Empty query: missing table, so the builder returns ErrInvalidDeleteQuery.
		query = &goqube.DeleteQuery{}
	}

	sql, args, err := query.BuildDeleteQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// INSERT with RETURNING example.
// When triggerError is true, an invalid query is built so the error branch runs.
func insertWithReturning(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.InsertQuery{
		Table: "users",
		Values: []map[string]interface{}{
			{"name": "John Doe", "email": "john@example.com"},
		},
		Returning: []string{"id", "created_at"},
	}

	if triggerError {
		// Empty query: missing table and values, so the builder returns ErrInvalidInsertQuery.
		query = &goqube.InsertQuery{}
	}

	sql, args, err := query.BuildInsertQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// UPDATE with RETURNING example.
// When triggerError is true, an invalid query is built so the error branch runs.
func updateWithReturning(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.UpdateQuery{
		Table: "users",
		FieldsValue: map[string]interface{}{
			"name": "Jane Doe",
		},
		Filter: &goqube.Filter{
			Field:    goqube.Field{Column: "id"},
			Operator: goqube.OperatorEqual,
			Value:    goqube.FilterValue{Value: 1},
		},
		Returning: []string{"id", "updated_at"},
	}

	if triggerError {
		// Empty query: missing table and fields, so the builder returns ErrInvalidUpdateQuery.
		query = &goqube.UpdateQuery{}
	}

	sql, args, err := query.BuildUpdateQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// DELETE with RETURNING example.
// When triggerError is true, an invalid query is built so the error branch runs.
func deleteWithReturning(dialect goqube.Dialect, triggerError bool) {
	query := &goqube.DeleteQuery{
		Table: "users",
		Filter: &goqube.Filter{
			Field:    goqube.Field{Column: "id"},
			Operator: goqube.OperatorEqual,
			Value:    goqube.FilterValue{Value: 123},
		},
		Returning: []string{"id", "deleted_at"},
	}

	if triggerError {
		// Empty query: missing table, so the builder returns ErrInvalidDeleteQuery.
		query = &goqube.DeleteQuery{}
	}

	sql, args, err := query.BuildDeleteQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}

// Bulk UPDATE example.
// When triggerError is true, an invalid query is built so the error branch runs.
func bulkUpdate(dialect goqube.Dialect, triggerError bool) {
	// Data to bulk update
	usersData := []map[string]interface{}{
		{"id": 1, "name": "Alice Smith", "age": 28, "status": "active"},
		{"id": 2, "name": "Bob Jones", "age": 35, "status": "inactive"},
		{"id": 3, "name": "Charlie Brown", "age": 22, "status": "active"},
	}

	// ColumnsType is REQUIRED for PostgreSQL and SQL Server dialects.
	// For MySQL and SQLite, ColumnsType is optional but recommended for consistency.
	query := &goqube.BulkUpdateQuery{
		Table:        "users",
		PrimaryKey:   "id",
		FieldsValues: usersData,
		ColumnsType: map[string]string{
			"id":     "integer",
			"age":    "integer",
			"name":   "text",
			"status": "text",
		},
	}

	if triggerError {
		// Empty query: missing table, primary key, and values, so the builder returns ErrInvalidBulkUpdateQuery.
		query = &goqube.BulkUpdateQuery{}
	}

	sql, args, err := query.BuildBulkUpdateQuery(dialect)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Args: %v\n", args)
}
