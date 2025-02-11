package goclickzetta

import (
	"context"
	"database/sql/driver"
)

type ClickzettaStmt struct {
	conn  *ClickzettaConn
	query string
}

func (stmt *ClickzettaStmt) Close() error {
	logger.WithContext(stmt.conn.ctx).Infoln("Stmt.Close")
	// noop
	err := stmt.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (stmt *ClickzettaStmt) NumInput() int {
	logger.WithContext(stmt.conn.ctx).Infoln("Stmt.NumInput")
	return -1
}

func (stmt *ClickzettaStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	logger.WithContext(stmt.conn.ctx).Infoln("Stmt.ExecContext")
	return stmt.conn.ExecContext(ctx, stmt.query, args)
}

func (stmt *ClickzettaStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	logger.WithContext(stmt.conn.ctx).Infoln("Stmt.QueryContext")
	return stmt.conn.QueryContext(ctx, stmt.query, args)
}

func (stmt *ClickzettaStmt) Exec(args []driver.Value) (driver.Result, error) {
	logger.WithContext(stmt.conn.ctx).Infoln("Stmt.Exec")
	return stmt.conn.Exec(stmt.query, args)
}

func (stmt *ClickzettaStmt) Query(args []driver.Value) (driver.Rows, error) {
	logger.WithContext(stmt.conn.ctx).Infoln("Stmt.Query")
	return stmt.conn.Query(stmt.query, args)
}
