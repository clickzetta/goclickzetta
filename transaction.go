package goclickzetta

import (
	"context"
	"database/sql/driver"
	"errors"
)

type clickzettaTx struct {
	cc  *ClickzettaConn
	ctx context.Context
}

type txCommand int

const (
	commit txCommand = iota
	rollback
)

func (cmd txCommand) string() (string, error) {
	switch cmd {
	case commit:
		return "COMMIT", nil
	case rollback:
		return "ROLLBACK", nil
	}
	return "", errors.New("unsupported transaction command")
}

func (tx *clickzettaTx) Commit() error {
	return tx.execTxCommand(commit)
}

func (tx *clickzettaTx) Rollback() error {
	return tx.execTxCommand(rollback)
}

func (tx *clickzettaTx) execTxCommand(command txCommand) (err error) {
	txStr, err := command.string()
	logger.Info("execTxCommand: %v", txStr)
	if err != nil {
		return
	}
	if tx.cc == nil || tx.cc.internal == nil {
		return driver.ErrBadConn
	}
	tx.cc = nil
	return
}
