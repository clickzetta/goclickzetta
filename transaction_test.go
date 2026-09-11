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

func TestTxCommandString(t *testing.T) {
	tests := []struct {
		name    string
		cmd     txCommand
		want    string
		wantErr bool
	}{
		{name: "commit", cmd: commit, want: "COMMIT"},
		{name: "rollback", cmd: rollback, want: "ROLLBACK"},
		{name: "unknown", cmd: txCommand(7), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.cmd.string()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("string() error = nil, want an unsupported command error")
				}
				return
			}
			if err != nil {
				t.Fatalf("string() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("string() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBeginTxRejectsClosedConnection(t *testing.T) {
	// A connection that has been through Close has no internal client left.
	conn := &ClickzettaConn{ctx: context.Background()}
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("BeginTx() error = %v, want driver.ErrBadConn", err)
	}
	if tx != nil {
		t.Errorf("BeginTx() tx = %v, want nil", tx)
	}
}

func TestBeginReusesConnectionContext(t *testing.T) {
	conn := newTestConn(&mockInternalClient{})
	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	czTx, ok := tx.(*clickzettaTx)
	if !ok {
		t.Fatalf("Begin() tx is %T, want *clickzettaTx", tx)
	}
	if czTx.cc != conn {
		t.Error("tx does not carry the connection it was opened on")
	}
}

// TestTxIsSingleUse covers the one guarantee the current implementation does
// make: a transaction handle is spent after the first Commit or Rollback, so a
// second call reports a bad connection rather than pretending to work.
func TestTxIsSingleUse(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(driver.Tx) error
	}{
		{name: "commit", call: func(tx driver.Tx) error { return tx.Commit() }},
		{name: "rollback", call: func(tx driver.Tx) error { return tx.Rollback() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := newTestConn(&mockInternalClient{})
			tx, err := conn.Begin()
			if err != nil {
				t.Fatalf("Begin() error = %v", err)
			}
			if err := tc.call(tx); err != nil {
				t.Fatalf("first call error = %v", err)
			}
			if err := tc.call(tx); !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("second call error = %v, want driver.ErrBadConn", err)
			}
		})
	}
}

// TestTransactionsDoNotIsolateWrites pins a trap rather than a feature.
// execTxCommand builds the COMMIT or ROLLBACK text and then throws it away: it
// never reaches the server. So every statement inside a transaction is committed
// as it runs, and Rollback discards nothing. Anyone relying on database/sql
// transaction semantics here is silently unprotected.
//
// This test fails the day transactions are implemented for real. That is the
// point: update it then, and the driver stops claiming support it does not have.
func TestTransactionsDoNotIsolateWrites(t *testing.T) {
	db, err := sql.Open("clickzetta", integrationDSN(t))
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	table := fmt.Sprintf("goclickzetta_tx_it_%d", time.Now().UnixNano())
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

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", table)); err != nil {
		t.Fatalf("insert inside transaction: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	var rows int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&rows); err != nil {
		t.Fatalf("count after rollback: %v", err)
	}
	if rows != 1 {
		t.Fatalf("count after rollback = %d, want 1; the rollback now reaches the server, so transactions are implemented and this test needs rewriting", rows)
	}
}
