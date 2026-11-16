package goclickzetta

import (
	"database/sql/driver"
	"errors"
	"io"
)

type ClickzettaRows interface {
	GetQueryID() string
	GetStatus() queryStatus
	GetResultRows() ([]interface{}, error)
}

type clickzettaRows struct {
	queryID           string
	status            queryStatus
	err               error
	cn                *ClickzettaConn
	response          *execResponse
	currentBatchIndex int
	currentBatchSize  int
}

func (rows *clickzettaRows) Close() (err error) {
	logger.WithContext(rows.cn.ctx).Debugln("Rows.Close")
	return nil
}

func (rows *clickzettaRows) Columns() []string {
	logger.WithContext(rows.cn.ctx).Debugln("Rows.Columns")
	if rows.response.Success {
		columns := make([]string, len(rows.response.Data.Schema))
		for i, column := range rows.response.Data.Schema {
			columns[i] = column.Name
		}
		return columns
	}
	return make([]string, 0)
}

func (rows *clickzettaRows) Next(dest []driver.Value) error {
	logger.Debugln("Rows.Next")
	if err := rows.fill(); err != nil {
		return err
	}
	result := rows.response.Data.Data[rows.currentBatchIndex]
	resList, ok := result.([]interface{})
	if !ok {
		return io.EOF
	}

	rows.currentBatchIndex++
	for i, v := range resList {
		dest[i] = v
	}
	return nil
}

func (rows *clickzettaRows) GetQueryID() string {
	return rows.queryID
}

func (rows *clickzettaRows) GetStatus() queryStatus {
	return rows.status
}

func (rows *clickzettaRows) GetResultRows() error {
	err := rows.response.Data.read()
	if err != nil {
		return err
	}
	return nil
}

func (rows *clickzettaRows) NextResultSet() error {
	logger.WithContext(rows.cn.ctx).Debugln("Rows.NextResultSet")
	return rows.fill()
}

// HasNextResultSet reports whether Next has something to return: a row, or
// a read error that Next will report.
func (rows *clickzettaRows) HasNextResultSet() bool {
	logger.WithContext(rows.cn.ctx).Debugln("Rows.HasNextResultSet")
	return !errors.Is(rows.fill(), io.EOF)
}

// fill makes sure the current batch has an unread row, reading result data
// (and skipping empty batches) as needed. It returns io.EOF when all data has
// been read. A read error is kept in rows.err and returned on every later
// call, so it reaches the caller instead of ending the result set early.
func (rows *clickzettaRows) fill() error {
	if rows.currentBatchSize > 0 && rows.currentBatchIndex < rows.currentBatchSize {
		return nil
	}
	if rows.err != nil {
		return rows.err
	}
	for rows.hasUnreadResultData() {
		if err := rows.GetResultRows(); err != nil {
			rows.err = err
			return err
		}
		rows.currentBatchIndex = 0
		rows.currentBatchSize = len(rows.response.Data.Data)
		if rows.currentBatchSize > 0 {
			return nil
		}
	}
	return io.EOF
}

func (rows *clickzettaRows) hasUnreadResultData() bool {
	if rows == nil || rows.response == nil {
		return false
	}
	data := &rows.response.Data
	switch data.DataType {
	case Memory:
		return !data.MemoryRead
	case File:
		return data.CurrentFileIndex < len(data.FileList)
	default:
		return false
	}
}
