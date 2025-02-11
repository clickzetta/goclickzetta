package goclickzetta

import (
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/zeebo/assert"
)

func initConn() *ClickzettaConn {
	dsn := "username:passwprd@https(mock.clickzetta.com)/schema?virtualCluster=default&workspace=mock&instance=mock"
	driver := ClickzettaDriver{}
	conn, err := driver.Open(dsn)
	if err != nil {
		return nil
	}
	if conn == nil {
		return nil
	}
	return conn.(*ClickzettaConn)
}

func closeConn(conn *ClickzettaConn) {
	err := conn.Close()
	if err != nil {
		return
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
	connection := initConn()
	defer closeConn(connection)
	if connection == nil {
		t.Error("connection is nil")
	}
	stmt := ClickzettaStmt{
		conn:  connection,
		query: "select * from clickzetta_sample_data.ecommerce_events_history.ecommerce_events_multicategorystore_live limit 10;",
	}
	err := stmt.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestStmtExecContext(t *testing.T) {
	connection := initConn()
	defer closeConn(connection)
	if connection == nil {
		t.Error("connection is nil")
	}
	stmt := ClickzettaStmt{
		conn:  connection,
		query: "select * from clickzetta_sample_data.ecommerce_events_history.ecommerce_events_multicategorystore_live limit 10;",
	}
	result, err := stmt.ExecContext(connection.ctx, nil)
	if err != nil {
		t.Error(err)
	}
	res, ok := result.(ClickzettaResult)
	if !ok {
		t.Error("result is not ClickzettaResult")
	}
	assert.Equal(t, res.GetStatus(), queryStatus(QueryStatusComplete))

}

func TestStmtQueryContext(t *testing.T) {
	connection := initConn()
	defer closeConn(connection)
	if connection == nil {
		t.Error("connection is nil")
	}
	stmt := ClickzettaStmt{
		conn:  connection,
		query: "select * from clickzetta_sample_data.ecommerce_events_history.ecommerce_events_multicategorystore_live limit 10;",
	}
	data, err := stmt.QueryContext(connection.ctx, nil)
	if err != nil {
		t.Error(err)
	}
	result := make([]driver.Value, 10)
	for data.Next(result) != io.EOF {
		fmt.Println("fetch rows")
	}
	assert.Equal(t, len(result), 10)
}

func TestStmtExec(t *testing.T) {
	connection := initConn()
	defer closeConn(connection)
	if connection == nil {
		t.Error("connection is nil")
	}
	stmt := ClickzettaStmt{
		conn:  connection,
		query: "select * from clickzetta_sample_data.ecommerce_events_history.ecommerce_events_multicategorystore_live limit 10;",
	}
	result, err := stmt.Exec(nil)
	if err != nil {
		t.Error(err)
	}
	res, ok := result.(ClickzettaResult)
	if !ok {
		t.Error("result is not ClickzettaResult")
	}
	assert.Equal(t, res.GetStatus(), queryStatus(QueryStatusComplete))

}

func TestStmtQuery(t *testing.T) {
	connection := initConn()
	defer closeConn(connection)
	if connection == nil {
		t.Error("connection is nil")
	}
	stmt := ClickzettaStmt{
		conn:  connection,
		query: "select * from clickzetta_sample_data.ecommerce_events_history.ecommerce_events_multicategorystore_live limit 10;",
	}
	data, err := stmt.Query(nil)
	if err != nil {
		t.Error(err)
	}
	result := make([]driver.Value, 10)
	for data.Next(result) != io.EOF {
		fmt.Println("fetch rows")
	}
	assert.Equal(t, len(result), 10)
}
