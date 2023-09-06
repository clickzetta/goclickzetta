package goclickzetta

import (
	"database/sql/driver"
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
	logger.Infoln("Rows.Next")
	if rows.HasNextResultSet() {
		err := rows.NextResultSet()
		if err != nil {
			return err
		}
	} else {
		return io.EOF
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
	if rows.currentBatchSize > 0 && rows.currentBatchIndex < rows.currentBatchSize {
		return nil
	}
	rows.currentBatchIndex = 0
	rows.currentBatchSize = len(rows.response.Data.Data)
	return nil
}

func (rows *clickzettaRows) HasNextResultSet() bool {
	logger.WithContext(rows.cn.ctx).Debugln("Rows.HasNextResultSet")
	if rows.currentBatchSize > 0 && rows.currentBatchIndex < rows.currentBatchSize {
		return true
	}
	err := rows.GetResultRows()
	if err != nil {
		return false
	}
	if len(rows.response.Data.Data) == 0 {
		return false
	}
	return true
}
