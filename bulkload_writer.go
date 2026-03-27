package goclickzetta

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/clickzetta/goclickzetta/protos/bulkload/util"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// FileFormat represents the output file format.
type FileFormat string

var (
	PARQUET   FileFormat = "parquet"
	ORC       FileFormat = "orc"
	AVRO      FileFormat = "avro"
	CSV       FileFormat = "csv"
	ARROW_FMT FileFormat = "arrow"
)

// Row represents a single row of data to write.
type Row struct {
	Columns          map[string]*util.DataType
	TableName        string
	ColumnNameValues map[string]interface{}
}

func (row *Row) SetBoolean(columnName string, value interface{}) error {
	v, ok := value.(bool)
	if !ok {
		return fmt.Errorf("cannot convert value to bool")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetTinyInt(columnName string, value interface{}) error {
	v, ok := value.(int8)
	if !ok {
		return fmt.Errorf("cannot convert value to int8")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetSmallInt(columnName string, value interface{}) error {
	v, ok := value.(int16)
	if !ok {
		return fmt.Errorf("cannot convert value to int16")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetInt(columnName string, value interface{}) error {
	v, ok := value.(int32)
	if !ok {
		return fmt.Errorf("cannot convert value to int32")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetBigint(columnName string, value interface{}) error {
	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("cannot convert value to int64")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetFloat(columnName string, value interface{}) error {
	v, ok := value.(float32)
	if !ok {
		return fmt.Errorf("cannot convert value to float32")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetDouble(columnName string, value interface{}) error {
	v, ok := value.(float64)
	if !ok {
		return fmt.Errorf("cannot convert value to float64")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetDecimal(columnName string, value interface{}) error {
	v, ok := value.(decimal.Decimal)
	if !ok {
		return fmt.Errorf("cannot convert value to decimal")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetString(columnName string, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("cannot convert value to string")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetDate(columnName string, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("cannot convert value to date string")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetTimestamp(columnName string, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("cannot convert value to timestamp string")
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

// Default writer config
const (
	defaultMaxRowsPerFile  = 1000000  // 1M rows
	defaultMaxBytesPerFile = 64 << 20 // 64MB
	defaultBatchFlushSize  = 16 * 1024 * 1024
	defaultBatchFlushRows  = 40000
)

// BulkloadWriter writes data to local parquet files.
// Files are uploaded to table volume asynchronously on file roll to minimize
// local disk usage without blocking row writes.
type BulkloadWriter struct {
	Connection    *ClickzettaConn
	MetaData      *BulkloadMetadata
	PartitionId   int64
	StreamOptions *BulkloadOptions

	FinishedFiles       []string // local file paths (cleared after upload)
	UploadedVolumePaths []string // remote volume paths (accumulated)
	TxnId               string   // transaction id for PUT subdirectory
	FileNameUUID        string
	FileId              int
	Closed              bool
	LocalBaseDir        string
	CurrentTotalRows    int
	CurrentTotalBytes   int

	CurrentRecordBatch     map[string][]interface{}
	CurrentRecordBatchSize int
	CurrentRecordBatchRows int
	EstimateRowStaticSize  int
	ArrowSchema            *arrow.Schema
	Writer                 *pqarrow.FileWriter
	PartitionSpec          map[string]string

	// Async upload
	uploadCh   chan []string // channel to send local file batches for upload
	uploadDone chan struct{} // closed when upload goroutine exits
	uploadMu   sync.Mutex    // protects UploadedVolumePaths and uploadErr
	uploadErr  error         // first error from upload goroutine

	// Configurable thresholds (0 means use defaults)
	MaxRowsPerFile  int
	MaxBytesPerFile int
	BatchFlushSize  int
	BatchFlushRows  int
}

func (bw *BulkloadWriter) getMaxRowsPerFile() int {
	if bw.MaxRowsPerFile > 0 {
		return bw.MaxRowsPerFile
	}
	return defaultMaxRowsPerFile
}

func (bw *BulkloadWriter) getMaxBytesPerFile() int {
	if bw.MaxBytesPerFile > 0 {
		return bw.MaxBytesPerFile
	}
	return defaultMaxBytesPerFile
}

func (bw *BulkloadWriter) getBatchFlushSize() int {
	if bw.BatchFlushSize > 0 {
		return bw.BatchFlushSize
	}
	return defaultBatchFlushSize
}

func (bw *BulkloadWriter) getBatchFlushRows() int {
	if bw.BatchFlushRows > 0 {
		return bw.BatchFlushRows
	}
	return defaultBatchFlushRows
}

func (bw *BulkloadWriter) Init() error {
	spec, err := bw.ParsePartitionSpec()
	if err != nil {
		return err
	}
	bw.PartitionSpec = spec
	bw.EstimateRowStaticSize = bw.EstimateRowSize()
	bw.FileNameUUID = uuid.New().String()
	bw.FileId = 0
	bw.TxnId = fmt.Sprintf("%s_%s_%d", bw.MetaData.StreamInfo.GetStreamId(), uuid.New().String(), time.Now().UnixMilli())
	if err := bw.ConstructArrowSchema(); err != nil {
		return err
	}
	bw.FinishedFiles = []string{}
	bw.UploadedVolumePaths = []string{}
	// Use configured load URI or fall back to system temp dir
	baseDir := bw.StreamOptions.LoadUri
	if baseDir == "" {
		baseDir = os.TempDir()
	}
	bw.LocalBaseDir = filepath.Join(baseDir, "goclickzetta_bulkload",
		bw.MetaData.StreamInfo.GetStreamId(), strconv.FormatInt(bw.PartitionId, 10))
	if err := os.MkdirAll(bw.LocalBaseDir, 0777); err != nil {
		return err
	}
	// Start async upload goroutine
	bw.uploadCh = make(chan []string, 4)
	bw.uploadDone = make(chan struct{})
	go bw.uploadLoop()
	return nil
}

func (bw *BulkloadWriter) CreateRow() *Row {
	return &Row{
		Columns:          bw.MetaData.Table.Schema,
		TableName:        bw.MetaData.StreamInfo.GetIdentifier().TableName,
		ColumnNameValues: map[string]interface{}{},
	}
}

// Close flushes remaining data, waits for all async uploads to finish, and cleans up.
func (bw *BulkloadWriter) Close() error {
	if bw.Closed {
		return nil
	}
	if err := bw.CloseCurrentFile(); err != nil {
		return err
	}
	// Signal upload goroutine to finish and wait
	close(bw.uploadCh)
	<-bw.uploadDone

	// Check for async upload errors
	bw.uploadMu.Lock()
	err := bw.uploadErr
	bw.uploadMu.Unlock()
	if err != nil {
		bw.cleanupAllLocal()
		return err
	}

	bw.Closed = true
	logger.Info("Writer closed for stream:", bw.MetaData.StreamInfo.GetStreamId(), "partition:", bw.PartitionId,
		"uploaded files:", len(bw.UploadedVolumePaths))
	return nil
}

// GetCommittables returns the committable with already-uploaded volume paths.
func (bw *BulkloadWriter) GetCommittables() []BulkLoadCommittable {
	bw.uploadMu.Lock()
	paths := bw.UploadedVolumePaths
	bw.uploadMu.Unlock()

	if len(paths) == 0 {
		return nil
	}
	return []BulkLoadCommittable{{
		StreamId:    bw.MetaData.StreamInfo.GetStreamId(),
		PartitionId: int(bw.PartitionId),
		DstFiles:    paths,
	}}
}

func (bw *BulkloadWriter) WriteRow(row *Row) error {
	if bw.Closed {
		return errors.New("bulkload writer is closed")
	}
	// Check for async upload errors before writing
	bw.uploadMu.Lock()
	err := bw.uploadErr
	bw.uploadMu.Unlock()
	if err != nil {
		return fmt.Errorf("async upload failed: %w", err)
	}
	if err := bw.CheckFileStatus(); err != nil {
		return err
	}
	if bw.CurrentRecordBatch == nil {
		bw.CurrentRecordBatch = map[string][]interface{}{}
	}
	for k, v := range bw.PartitionSpec {
		_ = row.SetString(k, v)
	}
	currentRowSize := bw.EstimateRowStaticSize
	for col, val := range row.ColumnNameValues {
		bw.CurrentRecordBatch[col] = append(bw.CurrentRecordBatch[col], val)
		if s, ok := val.(string); ok {
			currentRowSize += len(s)
		}
	}
	bw.CurrentRecordBatchSize += currentRowSize
	bw.CurrentTotalRows++
	bw.CurrentRecordBatchRows++

	if bw.CurrentRecordBatchSize >= bw.getBatchFlushSize() || bw.CurrentRecordBatchRows >= bw.getBatchFlushRows() {
		bufferSize, err := bw.FlushRecordBatch()
		if err != nil {
			return err
		}
		bw.CurrentTotalBytes += bufferSize
	}
	return nil
}

func (bw *BulkloadWriter) Abort() error {
	if bw.Writer != nil {
		_ = bw.Writer.Close()
	}
	// Clean up current file
	removeFile(bw.CurrentFileName())
	// Clean up all finished files
	for _, f := range bw.FinishedFiles {
		removeFile(f)
	}
	bw.CurrentRecordBatch = nil
	bw.Closed = true
	return nil
}

func (bw *BulkloadWriter) EstimateRowSize() int {
	size := 0
	for _, column := range bw.MetaData.Table.Schema {
		switch column.GetCategory() {
		case util.DataTypeCategory_BOOLEAN, util.DataTypeCategory_INT8:
			size += 1
		case util.DataTypeCategory_INT16:
			size += 2
		case util.DataTypeCategory_INT32, util.DataTypeCategory_FLOAT32:
			size += 4
		case util.DataTypeCategory_INT64, util.DataTypeCategory_FLOAT64, util.DataTypeCategory_DATE, util.DataTypeCategory_TIMESTAMP_LTZ:
			size += 8
		case util.DataTypeCategory_DECIMAL:
			size += 16
		case util.DataTypeCategory_STRING, util.DataTypeCategory_VARCHAR, util.DataTypeCategory_CHAR:
			size += 16
		case util.DataTypeCategory_VECTOR_TYPE:
			vi := column.GetVectorInfo()
			dim := int(vi.GetDimension())
			elemSize := vectorNumberTypeWidth(vi.GetNumberType())
			size += dim * elemSize
		default:
			size += 8
		}
	}
	return size
}

func (bw *BulkloadWriter) ParsePartitionSpec() (map[string]string, error) {
	spec := bw.MetaData.GetPartitionSpec()
	if spec == "" {
		return nil, nil
	}
	m := map[string]string{}
	for _, part := range strings.Split(spec, ",") {
		kv := strings.Split(part, "=")
		if len(kv) != 2 {
			return nil, errors.New("invalid partition spec: " + part)
		}
		m[kv[0]] = kv[1]
	}
	return m, nil
}

func (bw *BulkloadWriter) CheckFileStatus() error {
	if bw.Writer != nil {
		if bw.CurrentTotalRows >= bw.getMaxRowsPerFile() || bw.CurrentTotalBytes >= bw.getMaxBytesPerFile() {
			return bw.CloseCurrentFile()
		}
	} else {
		writer, err := bw.CreateNextFileWriter()
		if err != nil {
			return err
		}
		bw.Writer = writer
	}
	return nil
}

func (bw *BulkloadWriter) CreateNextFileWriter() (*pqarrow.FileWriter, error) {
	fileName := bw.CurrentFileName()
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	props := parquet.NewWriterProperties()
	return pqarrow.NewFileWriter(bw.ArrowSchema, file, props, pqarrow.DefaultWriterProps())
}

func (bw *BulkloadWriter) CloseCurrentFile() error {
	if bw.Writer == nil {
		return nil
	}
	if _, err := bw.FlushRecordBatch(); err != nil {
		return err
	}
	if err := bw.Writer.Close(); err != nil {
		return err
	}
	bw.FinishedFiles = append(bw.FinishedFiles, bw.CurrentFileName())
	bw.CurrentTotalBytes = 0
	bw.CurrentTotalRows = 0
	bw.FileId++
	bw.Writer = nil

	// Send finished files to async upload goroutine
	files := bw.FinishedFiles
	bw.FinishedFiles = nil
	bw.uploadCh <- files
	return nil
}

func (bw *BulkloadWriter) CurrentFileName() string {
	return filepath.Join(bw.LocalBaseDir, bw.FileNameUUID+"_"+strconv.Itoa(bw.FileId)+".parquet")
}

func (bw *BulkloadWriter) FlushRecordBatch() (int, error) {
	if bw.CurrentRecordBatchRows == 0 {
		return 0, nil
	}
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	rb := array.NewRecordBuilder(mem, bw.ArrowSchema)
	defer rb.Release()
	for i, column := range bw.ArrowSchema.Fields() {
		values := bw.CurrentRecordBatch[column.Name]
		for _, value := range values {
			if value == "" {
				rb.Field(i).AppendNull()
			} else {
				if err := AppendValueToArrowField(rb.Field(i), value, bw.MetaData.Table.Schema[column.Name]); err != nil {
					return 0, err
				}
			}
		}
	}
	record := rb.NewRecord()
	if err := bw.Writer.Write(record); err != nil {
		return 0, err
	}
	bw.CurrentRecordBatch = nil
	bw.CurrentRecordBatchSize = 0
	bw.CurrentRecordBatchRows = 0
	return mem.CurrentAlloc(), nil
}

func (bw *BulkloadWriter) ConstructArrowSchema() error {
	fields := bw.MetaData.Table.TableMeta.GetDataFields()
	arrowFields := make([]arrow.Field, len(fields))
	for i, col := range fields {
		at, err := ConvertToArrowDataType(col.GetType())
		if err != nil {
			return err
		}
		arrowFields[i] = arrow.Field{Name: col.GetName(), Type: at}
	}
	bw.ArrowSchema = arrow.NewSchema(arrowFields, nil)
	return nil
}

// removeFile removes a file, ignoring errors if it doesn't exist.
func removeFile(path string) error {
	if path == "" {
		return nil
	}
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// uploadLoop is the background goroutine that processes file upload requests.
func (bw *BulkloadWriter) uploadLoop() {
	defer close(bw.uploadDone)
	for files := range bw.uploadCh {
		// Check if a previous upload already failed
		bw.uploadMu.Lock()
		prevErr := bw.uploadErr
		bw.uploadMu.Unlock()
		if prevErr != nil {
			// Previous upload failed, just clean up local files
			for _, f := range files {
				cleanupLocalFile(f)
			}
			continue
		}

		if err := bw.uploadFiles(files); err != nil {
			bw.uploadMu.Lock()
			if bw.uploadErr == nil {
				bw.uploadErr = err
			}
			bw.uploadMu.Unlock()
		}
	}
}

// uploadFiles uploads a batch of local files to table volume, deletes local files after success.
func (bw *BulkloadWriter) uploadFiles(files []string) error {
	if len(files) == 0 {
		return nil
	}

	tableName := bw.StreamOptions.Table

	// 1. PUT to get presigned URLs
	confSQL := generateConfSetSQL(bw.StreamOptions.PreferInternalEndpoint, bw.StreamOptions.Properties)
	putSQL := generatePutToTableVolumeSQL(tableName, bw.TxnId, files)
	fullSQL := confSQL + putSQL

	ctx := context.Background()
	rows, err := bw.Connection.QueryContext(ctx, fullSQL, nil)
	if err != nil {
		// Clean up all local files on PUT failure
		for _, f := range files {
			cleanupLocalFile(f)
		}
		return fmt.Errorf("PUT to table volume failed: %w", err)
	}
	defer rows.Close()

	putResult, err := parsePutResultRows(rows)
	if err != nil {
		for _, f := range files {
			cleanupLocalFile(f)
		}
		return fmt.Errorf("parse PUT result failed: %w", err)
	}

	// 2. Upload each file, delete local immediately after success
	for i, presignedURL := range putResult.PresignedURLs {
		if i >= len(files) {
			break
		}
		localFile := files[i]
		if err := uploadFileViaPresignedURL(presignedURL, localFile); err != nil {
			// Clean up remaining local files
			for j := i; j < len(files); j++ {
				cleanupLocalFile(files[j])
			}
			return fmt.Errorf("upload file %s failed: %w", localFile, err)
		}
		cleanupLocalFile(localFile)
		logger.Info("Uploaded and cleaned local file:", localFile)
	}

	// 3. Record volume paths (thread-safe)
	bw.uploadMu.Lock()
	bw.UploadedVolumePaths = append(bw.UploadedVolumePaths, putResult.VolumePaths...)
	bw.uploadMu.Unlock()
	return nil
}

// cleanupAllLocal removes all remaining local files.
func (bw *BulkloadWriter) cleanupAllLocal() {
	for _, f := range bw.FinishedFiles {
		cleanupLocalFile(f)
	}
	bw.FinishedFiles = nil
}

// CleanupAllVolume removes all uploaded volume files. Called on abort or commit failure.
func (bw *BulkloadWriter) CleanupAllVolume() {
	bw.uploadMu.Lock()
	paths := bw.UploadedVolumePaths
	bw.UploadedVolumePaths = nil
	bw.uploadMu.Unlock()

	if len(paths) == 0 {
		return
	}
	tableName := bw.StreamOptions.Table
	cleanupSQL := generateCleanUpTableVolumeSQL(tableName, paths)
	if cleanupSQL != "" {
		logger.Info("Cleaning up volume files for writer partition:", bw.PartitionId)
		_, _ = bw.Connection.exec(context.Background(), cleanupSQL, true, false, false, nil)
	}
}
