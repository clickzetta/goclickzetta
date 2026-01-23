package goclickzetta

import (
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
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

// LazyStreamingReader lazily downloads and reads files from presigned URLs
// Only downloads the next file when the current one is exhausted
type LazyStreamingReader struct {
	urls          []string
	currentIdx    int
	currentReader *ipc.Reader
	currentResp   *http.Response
	schema        *arrow.Schema
	err           error
	closed        bool
}

// NewLazyStreamingReader creates a new lazy streaming reader from presigned URLs
func NewLazyStreamingReader(urls []string) (*LazyStreamingReader, error) {
	if len(urls) == 0 {
		return nil, fmt.Errorf("no urls provided")
	}

	reader := &LazyStreamingReader{
		urls:       urls,
		currentIdx: -1,
	}

	// Load first file to get schema
	if err := reader.loadNextFile(); err != nil {
		return nil, err
	}

	return reader, nil
}

func (r *LazyStreamingReader) loadNextFile() error {
	// Close previous resources
	if r.currentReader != nil {
		r.currentReader.Release()
		r.currentReader = nil
	}
	if r.currentResp != nil {
		r.currentResp.Body.Close()
		r.currentResp = nil
	}

	r.currentIdx++
	if r.currentIdx >= len(r.urls) {
		return io.EOF
	}

	resp, err := http.Get(r.urls[r.currentIdx])
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("failed to download file, status code: %d", resp.StatusCode)
	}

	reader, err := ipc.NewReader(resp.Body)
	if err != nil {
		resp.Body.Close()
		return err
	}

	r.currentResp = resp
	r.currentReader = reader

	if r.schema == nil {
		r.schema = reader.Schema()
	}

	return nil
}

func (r *LazyStreamingReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *LazyStreamingReader) Next() bool {
	if r.closed || r.err != nil {
		return false
	}

	for {
		if r.currentReader != nil && r.currentReader.Next() {
			return true
		}

		if r.currentReader != nil {
			if err := r.currentReader.Err(); err != nil {
				r.err = err
				return false
			}
		}

		// Try to load next file
		if err := r.loadNextFile(); err != nil {
			if err != io.EOF {
				r.err = err
			}
			return false
		}
	}
}

func (r *LazyStreamingReader) Record() arrow.Record {
	if r.closed || r.currentReader == nil {
		return nil
	}
	return r.currentReader.Record()
}

func (r *LazyStreamingReader) RecordBatch() arrow.Record {
	if r.closed || r.currentReader == nil {
		return nil
	}
	return r.currentReader.Record()
}

func (r *LazyStreamingReader) Err() error {
	return r.err
}

func (r *LazyStreamingReader) Retain() {
	if r.currentReader != nil {
		r.currentReader.Retain()
	}
}

func (r *LazyStreamingReader) Release() {
	if r.closed {
		return
	}
	r.closed = true

	if r.currentReader != nil {
		r.currentReader.Release()
		r.currentReader = nil
	}
	if r.currentResp != nil {
		r.currentResp.Body.Close()
		r.currentResp = nil
	}
}

// LazyMemoryReader lazily reads base64 encoded arrow data
// Only decodes the next chunk when the current one is exhausted
type LazyMemoryReader struct {
	dataChunks    []string
	currentIdx    int
	currentReader *ipc.Reader
	schema        *arrow.Schema
	err           error
	closed        bool
}

// NewLazyMemoryReader creates a new lazy memory reader from base64 encoded data chunks
func NewLazyMemoryReader(dataChunks []string) (*LazyMemoryReader, error) {
	if len(dataChunks) == 0 {
		return nil, fmt.Errorf("no data chunks provided")
	}

	reader := &LazyMemoryReader{
		dataChunks: dataChunks,
		currentIdx: -1,
	}

	// Load first chunk to get schema
	if err := reader.loadNextChunk(); err != nil {
		return nil, err
	}

	return reader, nil
}

func (r *LazyMemoryReader) loadNextChunk() error {
	// Release previous reader
	if r.currentReader != nil {
		r.currentReader.Release()
		r.currentReader = nil
	}

	r.currentIdx++
	if r.currentIdx >= len(r.dataChunks) {
		return io.EOF
	}

	buffer := base64.NewDecoder(base64.StdEncoding, strings.NewReader(r.dataChunks[r.currentIdx]))
	reader, err := ipc.NewReader(buffer)
	if err != nil {
		return err
	}

	r.currentReader = reader

	if r.schema == nil {
		r.schema = reader.Schema()
	}

	return nil
}

func (r *LazyMemoryReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *LazyMemoryReader) Next() bool {
	if r.closed || r.err != nil {
		return false
	}

	for {
		if r.currentReader != nil && r.currentReader.Next() {
			return true
		}

		if r.currentReader != nil {
			if err := r.currentReader.Err(); err != nil {
				r.err = err
				return false
			}
		}

		// Try to load next chunk
		if err := r.loadNextChunk(); err != nil {
			if err != io.EOF {
				r.err = err
			}
			return false
		}
	}
}

func (r *LazyMemoryReader) Record() arrow.Record {
	if r.closed || r.currentReader == nil {
		return nil
	}
	return r.currentReader.Record()
}

func (r *LazyMemoryReader) RecordBatch() arrow.Record {
	if r.closed || r.currentReader == nil {
		return nil
	}
	return r.currentReader.Record()
}

func (r *LazyMemoryReader) Err() error {
	return r.err
}

func (r *LazyMemoryReader) Retain() {
	if r.currentReader != nil {
		r.currentReader.Retain()
	}
}

func (r *LazyMemoryReader) Release() {
	if r.closed {
		return
	}
	r.closed = true

	if r.currentReader != nil {
		r.currentReader.Release()
		r.currentReader = nil
	}
}

// EmptyRecordReader is a RecordReader that returns no records
// Used when query returns empty result set
type EmptyRecordReader struct {
	schema *arrow.Schema
	closed bool
}

// NewEmptyRecordReader creates a new empty record reader with the given schema
func NewEmptyRecordReader(schema *arrow.Schema) *EmptyRecordReader {
	return &EmptyRecordReader{
		schema: schema,
		closed: false,
	}
}

// NewEmptyRecordReaderFromFields creates a new empty record reader from schema fields
func NewEmptyRecordReaderFromFields(fields []execResponseColumnType) *EmptyRecordReader {
	arrowFields := make([]arrow.Field, len(fields))
	for i, f := range fields {
		arrowFields[i] = arrow.Field{
			Name:     f.Name,
			Type:     mapClickzettaTypeToArrow(f.Type, f.Precision, f.Scale),
			Nullable: f.Nullable,
		}
	}
	schema := arrow.NewSchema(arrowFields, nil)
	return &EmptyRecordReader{
		schema: schema,
		closed: false,
	}
}

func (r *EmptyRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *EmptyRecordReader) Next() bool {
	return false
}

func (r *EmptyRecordReader) Record() arrow.Record {
	return nil
}

func (r *EmptyRecordReader) RecordBatch() arrow.Record {
	return nil
}

func (r *EmptyRecordReader) Err() error {
	return nil
}

func (r *EmptyRecordReader) Retain() {
}

func (r *EmptyRecordReader) Release() {
	r.closed = true
}

// mapClickzettaTypeToArrow maps Clickzetta type to Arrow type
func mapClickzettaTypeToArrow(typeName string, precision, scale int64) arrow.DataType {
	switch typeName {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "INT", "INTEGER":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64
	case "DECIMAL":
		return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
	case "STRING", "VARCHAR", "CHAR":
		return arrow.BinaryTypes.String
	case "BINARY", "VARBINARY":
		return arrow.BinaryTypes.Binary
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	case "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ":
		return arrow.FixedWidthTypes.Timestamp_us
	default:
		return arrow.BinaryTypes.String
	}
}
