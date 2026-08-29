package main

import (
	"io"
	"log"
	"os"
	"testing"

	"github.com/fikri240794/goqube"
)

// allDialects returns every supported SQL dialect.
func allDialects() []goqube.Dialect {
	return []goqube.Dialect{
		goqube.DialectPostgres,
		goqube.DialectMySQL,
		goqube.DialectSQLite,
		goqube.DialectSQLServer,
	}
}

// TestExamplesMain runs the real entry point once. It drives all 12 example
// functions with triggerError=false across all four dialects (including the
// MySQL branch that skips the RETURNING examples), covering every happy-path
// statement in main() and every dialect case in the complexSelect switch.
func TestExamplesMain(t *testing.T) {
	main()
}

// TestErrorBranches forces the "if err != nil { log.Printf(...); return }"
// branch of every example function for every dialect. Each function builds an
// intentionally invalid query when triggerError=true, and every builder
// returns a deterministic validation error for it, so the previously
// unreachable error branches are all executed.
func TestErrorBranches(t *testing.T) {
	// Keep the expected error logs out of test output.
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	for _, d := range allDialects() {
		simpleSelect(d, true)
		complexSelect(d, true)
		simpleInsert(d, true)
		batchInsert(d, true)
		simpleUpdate(d, true)
		complexUpdate(d, true)
		bulkUpdate(d, true)
		simpleDelete(d, true)
		complexDelete(d, true)
		insertWithReturning(d, true)
		updateWithReturning(d, true)
		deleteWithReturning(d, true)
	}
}
