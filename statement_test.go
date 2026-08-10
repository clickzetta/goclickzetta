package goclickzetta

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func initConn(t *testing.T) *ClickzettaConn {
	t.Helper()
	dsn := getTestDSN(t)
	driver := ClickzettaDriver{}
	conn, err := driver.Open(dsn)
	if err != nil {
		t.Fatalf("open statement integration connection: %v", err)
	}
	if conn == nil {
		t.Fatal("statement integration connection is nil")
	}
	clickzettaConn, ok := conn.(*ClickzettaConn)
	if !ok {
		t.Fatalf("unexpected statement integration connection type %T", conn)
	}
	return clickzettaConn
}

func closeConn(t *testing.T, conn *ClickzettaConn) {
	t.Helper()
	if err := conn.Close(); err != nil {
		t.Errorf("close statement integration connection: %v", err)
	}
}

func createStmtIntegrationTable(t *testing.T, connection *ClickzettaConn) string {
	t.Helper()

	tableName := fmt.Sprintf("goclickzetta_stmt_it_%d", time.Now().UnixNano())
	testCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := connection.ExecContext(testCtx, fmt.Sprintf("CREATE TABLE %s (id BIGINT, name STRING)", tableName), nil); err != nil {
		t.Fatalf("create statement integration test table: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if _, err := connection.ExecContext(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName), nil); err != nil {
			t.Errorf("drop statement integration test table %s: %v", tableName, err)
		}
	})
	return tableName
}

func seedStmtIntegrationTable(t *testing.T, connection *ClickzettaConn, tableName string) {
	t.Helper()
	if _, err := connection.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob')", tableName), nil); err != nil {
		t.Fatalf("seed statement integration test table: %v", err)
	}
}

func TestStmt(t *testing.T) {
	t.Run("TestStmtClose", TestStmtClose)
	t.Run("TestStmtExecContext", TestStmtExecContext)
	t.Run("TestStmtQueryContext", TestStmtQueryContext)
	t.Run("TestStmtExec", TestStmtExec)
	t.Run("TestStmtQuery", TestStmtQuery)
}
func TestStmtClose(t *testing.T) {
	connection := initConn(t)
	stmt := ClickzettaStmt{
		conn:  connection,
		query: "SELECT 1",
	}
	err := stmt.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestStmtExecContext(t *testing.T) {
	connection := initConn(t)
	t.Cleanup(func() { closeConn(t, connection) })
	tableName := createStmtIntegrationTable(t, connection)
	stmt := ClickzettaStmt{
		conn:  connection,
		query: fmt.Sprintf("INSERT INTO %s VALUES (1, 'alice')", tableName),
	}
	result, err := stmt.ExecContext(connection.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, ok := result.(ClickzettaResult)
	if !ok {
		t.Fatal("result is not ClickzettaResult")
	}
	assert.Equal(t, res.GetStatus(), queryStatus(QueryStatusComplete))

}

func TestStmtQueryContext(t *testing.T) {
	connection := initConn(t)
	t.Cleanup(func() { closeConn(t, connection) })
	tableName := createStmtIntegrationTable(t, connection)
	seedStmtIntegrationTable(t, connection, tableName)
	stmt := ClickzettaStmt{
		conn:  connection,
		query: fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", tableName),
	}
	data, err := stmt.QueryContext(connection.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	result := make([]driver.Value, 2)
	rowCount := 0
	for {
		err := data.Next(result)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("query statement rows: %v", err)
		}
		rowCount++
	}
	assert.Equal(t, rowCount, 2)
}

func TestStmtExec(t *testing.T) {
	connection := initConn(t)
	t.Cleanup(func() { closeConn(t, connection) })
	tableName := createStmtIntegrationTable(t, connection)
	stmt := ClickzettaStmt{
		conn:  connection,
		query: fmt.Sprintf("INSERT INTO %s VALUES (1, 'alice')", tableName),
	}
	result, err := stmt.Exec(nil)
	if err != nil {
		t.Fatal(err)
	}
	res, ok := result.(ClickzettaResult)
	if !ok {
		t.Fatal("result is not ClickzettaResult")
	}
	assert.Equal(t, res.GetStatus(), queryStatus(QueryStatusComplete))

}

func TestStmtQuery(t *testing.T) {
	connection := initConn(t)
	t.Cleanup(func() { closeConn(t, connection) })
	tableName := createStmtIntegrationTable(t, connection)
	seedStmtIntegrationTable(t, connection, tableName)
	stmt := ClickzettaStmt{
		conn:  connection,
		query: fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", tableName),
	}
	data, err := stmt.Query(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	result := make([]driver.Value, 2)
	rowCount := 0
	for {
		err := data.Next(result)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("query statement rows: %v", err)
		}
		rowCount++
	}
	assert.Equal(t, rowCount, 2)
}
