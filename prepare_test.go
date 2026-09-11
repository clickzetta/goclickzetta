package goclickzetta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPrepareContextRejectsClosedConnection(t *testing.T) {
	conn := &ClickzettaConn{ctx: context.Background()}
	stmt, err := conn.PrepareContext(context.Background(), "SELECT 1")
	if !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("PrepareContext() error = %v, want driver.ErrBadConn", err)
	}
	if stmt != nil {
		t.Errorf("PrepareContext() stmt = %v, want nil", stmt)
	}
}

func TestPrepareKeepsQueryVerbatim(t *testing.T) {
	conn := newTestConn(&mockInternalClient{})
	const query = "SELECT ? AS a, ? AS b"
	stmt, err := conn.Prepare(query)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	czStmt, ok := stmt.(*ClickzettaStmt)
	if !ok {
		t.Fatalf("Prepare() stmt is %T, want *ClickzettaStmt", stmt)
	}
	if czStmt.query != query {
		t.Errorf("stmt.query = %q, want %q", czStmt.query, query)
	}
	// -1 tells database/sql the placeholder count is unknown, which is what
	// keeps it from rejecting the argument list before the driver sees it.
	if got := czStmt.NumInput(); got != -1 {
		t.Errorf("NumInput() = %d, want -1", got)
	}
}

// TestStmtCloseTearsDownTheConnection pins a bug. ClickzettaStmt.Close calls
// Close on the connection it was prepared on, which runs cleanup: the HTTP
// client is closed and cfg and ctx are set to nil. Under database/sql a
// statement and the connection that carries it have separate lifetimes - closing
// a statement is routine and is not supposed to destroy the connection, which
// stays in the pool and gets handed to the next caller in this state.
//
// The fix is for Close to be the no-op its own comment already claims it is.
// This test fails then, and should be deleted at that point.
func TestStmtCloseTearsDownTheConnection(t *testing.T) {
	conn := newTestConn(&mockInternalClient{})
	stmt := &ClickzettaStmt{conn: conn, query: "SELECT 1"}
	if err := stmt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if conn.cfg != nil || conn.ctx != nil {
		t.Fatal("Stmt.Close no longer tears the connection down, the bug is fixed; delete this test")
	}
}

// TestPreparedStatementRoundTrip exercises the Prepare path end to end. Bindings
// were only ever tested through db.Exec and db.Query, which interpolate on a
// fresh statement each time; here one statement is prepared once and run
// repeatedly, which is how a caller who cares about the cost of parsing uses it.
func TestPreparedStatementRoundTrip(t *testing.T) {
	db, err := sql.Open("clickzetta", integrationDSN(t))
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	// Registered first so it runs last: the DROP below has to reach the server
	// before the pool is torn down.
	t.Cleanup(func() { _ = db.Close() })

	table := fmt.Sprintf("goclickzetta_prep_it_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY, name STRING)", table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			t.Errorf("drop table: %v", err)
		}
	})

	insert, err := db.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", table))
	if err != nil {
		t.Fatalf("PrepareContext(insert) error = %v", err)
	}
	rows := []struct {
		id   int64
		name string
	}{
		{1, "alice"},
		{2, "o'brien"}, // the quote goes through the same escaping as db.Exec
		{3, "back\\slash"},
	}
	for _, r := range rows {
		if _, err := insert.ExecContext(ctx, r.id, r.name); err != nil {
			t.Fatalf("insert %d: %v", r.id, err)
		}
	}

	sel, err := db.PrepareContext(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = ?", table))
	if err != nil {
		t.Fatalf("PrepareContext(select) error = %v", err)
	}
	// The same statement handle serves every lookup; a driver that mutated the
	// query while interpolating would break on the second one.
	for _, r := range rows {
		var got string
		if err := sel.QueryRowContext(ctx, r.id).Scan(&got); err != nil {
			t.Fatalf("select %d: %v", r.id, err)
		}
		if got != r.name {
			t.Errorf("id %d name = %q, want %q", r.id, got, r.name)
		}
	}
}

// TestConnectorOpensDatabase covers sql.OpenDB, the path a caller takes to hand
// the driver a Config it built in code instead of a DSN string. None of it was
// reached before: NewConnector, Connect and Driver were all at zero.
func TestConnectorOpensDatabase(t *testing.T) {
	cfg := integrationConfig(t)
	connector := NewConnector(ClickzettaDriver{}, cfg)
	if _, ok := connector.Driver().(ClickzettaDriver); !ok {
		t.Errorf("Driver() = %T, want ClickzettaDriver", connector.Driver())
	}

	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext() error = %v", err)
	}
	var got int64
	if err := db.QueryRowContext(ctx, "SELECT 42").Scan(&got); err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}
	if got != 42 {
		t.Errorf("SELECT 42 = %d, want 42", got)
	}
}

func TestConnectorConnectPropagatesConfig(t *testing.T) {
	cfg := integrationConfig(t)
	conn, err := NewConnector(ClickzettaDriver{}, cfg).Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	czConn, ok := conn.(*ClickzettaConn)
	if !ok {
		t.Fatalf("Connect() conn is %T, want *ClickzettaConn", conn)
	}
	t.Cleanup(func() { _ = czConn.Close() })
	if czConn.cfg.Workspace != cfg.Workspace || czConn.cfg.Instance != cfg.Instance {
		t.Errorf("connection config = %+v, want workspace %q instance %q", czConn.cfg, cfg.Workspace, cfg.Instance)
	}
}
