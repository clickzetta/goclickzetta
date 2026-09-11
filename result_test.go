package goclickzetta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestClickzettaResultAccessors(t *testing.T) {
	want := errors.New("boom")
	res := &clickzettaResult{affectedRows: 3, insertID: 9, queryID: "job-1", status: QueryFailed, err: want}

	if got, err := res.RowsAffected(); got != 3 || err != nil {
		t.Errorf("RowsAffected() = (%d, %v), want (3, nil)", got, err)
	}
	if got, err := res.LastInsertId(); got != 9 || err != nil {
		t.Errorf("LastInsertId() = (%d, %v), want (9, nil)", got, err)
	}
	if got := res.GetQueryID(); got != "job-1" {
		t.Errorf("GetQueryID() = %q, want %q", got, "job-1")
	}
	if got := res.GetStatus(); got != QueryFailed {
		t.Errorf("GetStatus() = %q, want %q", got, QueryFailed)
	}
	if got := res.GetError(); !errors.Is(got, want) {
		t.Errorf("GetError() = %v, want %v", got, want)
	}
}

// TestExecResultCarriesJobIDButNoRowCount covers what a caller actually gets back
// from a write, and pins the part that is missing: affectedRows is never
// populated anywhere in the driver, so RowsAffected always answers 0 no matter
// how many rows a statement touched. Callers who branch on it are branching on a
// constant. The job id is real and is the handle for looking the statement up
// server side.
func TestExecResultCarriesJobIDButNoRowCount(t *testing.T) {
	conn := initConn(t)
	table := createStmtIntegrationTable(t, conn)

	res, err := conn.ExecContext(conn.ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob')", table), nil)
	if err != nil {
		t.Fatalf("ExecContext() error = %v", err)
	}
	czRes, ok := res.(ClickzettaResult)
	if !ok {
		t.Fatalf("result is %T, want ClickzettaResult", res)
	}
	if czRes.GetQueryID() == "" {
		t.Error("GetQueryID() is empty, want the server job id")
	}
	if czRes.GetStatus() != QueryStatusComplete {
		t.Errorf("GetStatus() = %q, want %q", czRes.GetStatus(), QueryStatusComplete)
	}
	if err := czRes.GetError(); err != nil {
		t.Errorf("GetError() = %v, want nil", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected() error = %v", err)
	}
	if affected != 0 {
		t.Fatalf("RowsAffected() = %d; the driver now reports affected rows, so update the callers that were told to ignore it", affected)
	}
	// The server has no last insert id concept, and the field is never set.
	if id, err := res.LastInsertId(); id != 0 || err != nil {
		t.Errorf("LastInsertId() = (%d, %v), want (0, nil)", id, err)
	}
}

func TestExecResultReportsFailure(t *testing.T) {
	conn := initConn(t)
	res, err := conn.ExecContext(conn.ctx, "SELECT * FROM a_table_that_does_not_exist_9f2c", nil)
	if err == nil {
		t.Fatal("ExecContext() error = nil, want a table not found error")
	}
	czRes, ok := res.(ClickzettaResult)
	if !ok {
		t.Fatalf("result is %T, want ClickzettaResult even on failure", res)
	}
	if czRes.GetStatus() != QueryFailed {
		t.Errorf("GetStatus() = %q, want %q", czRes.GetStatus(), QueryFailed)
	}
	if czRes.GetError() == nil {
		t.Error("GetError() = nil, want the failure")
	}
}

// TestRowsExposeQueryIDAndStatus covers the two accessors a caller reaches for
// when a query needs to be traced server side.
func TestRowsExposeQueryIDAndStatus(t *testing.T) {
	conn := initConn(t)
	rows, err := conn.QueryContext(conn.ctx, "SELECT 1", nil)
	if err != nil {
		t.Fatalf("QueryContext() error = %v", err)
	}
	defer rows.Close()
	czRows, ok := rows.(*clickzettaRows)
	if !ok {
		t.Fatalf("rows is %T, want *clickzettaRows", rows)
	}
	if czRows.GetQueryID() == "" {
		t.Error("GetQueryID() is empty, want the server job id")
	}
	if czRows.GetStatus() != QueryStatusComplete {
		t.Errorf("GetStatus() = %q, want %q", czRows.GetStatus(), QueryStatusComplete)
	}
}

// TestSQLResultRowsAffectedIsZero is the same gap seen from database/sql, where
// it is most likely to bite: this is the value a caller reads after an UPDATE.
func TestSQLResultRowsAffectedIsZero(t *testing.T) {
	db, err := sql.Open("clickzetta", integrationDSN(t))
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	table := fmt.Sprintf("goclickzetta_res_it_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY)", table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			t.Errorf("drop table: %v", err)
		}
	})
	res, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1), (2), (3)", table))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected() error = %v", err)
	}
	if affected != 0 {
		t.Fatalf("RowsAffected() = %d after inserting 3 rows; the driver reports it now, update this test", affected)
	}
}
