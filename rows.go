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
	diagnostics       queryDiagnostics
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
	if !rows.HasNextResultSet() {
		return io.EOF
	}
	if err := rows.NextResultSet(); err != nil {
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
	readStart := rows.diagnostics.trace.start()
	err := rows.response.Data.read(rows.diagnostics)
	if err != nil {
		if rows.diagnostics.trace.enabled {
			rows.diagnostics.trace.record(rows.response.Data.JobId, "rows_data_read_error", readStart, "error_type", safeErrorType(err))
		}
		return err
	}
	if rows.diagnostics.trace.enabled {
		rows.diagnostics.trace.record(
			rows.response.Data.JobId,
			"rows_data_read",
			readStart,
			"data_type", rows.response.Data.DataType,
			"rows", len(rows.response.Data.Data),
			"row_count_total", rows.response.Data.RowCount,
			"file_index", rows.response.Data.CurrentFileIndex,
			"files", len(rows.response.Data.FileList),
		)
	}
	return nil
}

func (rows *clickzettaRows) NextResultSet() error {
	logger.WithContext(rows.cn.ctx).Debugln("Rows.NextResultSet")
	if rows.currentBatchSize > 0 && rows.currentBatchIndex < rows.currentBatchSize {
		return nil
	}
	for rows.hasUnreadResultData() {
		if err := rows.GetResultRows(); err != nil {
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

func (rows *clickzettaRows) HasNextResultSet() bool {
	logger.WithContext(rows.cn.ctx).Debugln("Rows.HasNextResultSet")
	if rows.currentBatchSize > 0 && rows.currentBatchIndex < rows.currentBatchSize {
		return true
	}
	return rows.hasUnreadResultData()
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
