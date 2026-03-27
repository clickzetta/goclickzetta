package goclickzetta

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/clickzetta/goclickzetta/protos/bulkload/ingestion"
	"github.com/clickzetta/goclickzetta/protos/bulkload/util"
	"github.com/google/uuid"
)

// BulkLoadOperation defines the operation type for bulkload.
type BulkLoadOperation = string

var (
	APPEND    BulkLoadOperation = "APPEND"
	UPSERT    BulkLoadOperation = "UPSERT"
	OVERWRITE BulkLoadOperation = "OVERWRITE"
)

// BulkLoadState defines the state of a bulkload stream.
type BulkLoadState = string

var (
	CREATED          BulkLoadState = "CREATED"
	SEALED           BulkLoadState = "SEALED"
	COMMIT_SUBMITTED BulkLoadState = "COMMIT_SUBMITTED"
	COMMIT_SUCCESS   BulkLoadState = "COMMIT_SUCCESS"
	COMMIT_FAILED    BulkLoadState = "COMMIT_FAILED"
	ABORTED          BulkLoadState = "ABORTED"
)

// BulkloadOptions holds options for creating a bulkload stream.
type BulkloadOptions struct {
	Table                  string
	Operation              BulkLoadOperation
	PartitionSpec          string
	RecordKeys             []string
	PartialUpdateColumns   []string // optional: columns to update in UPSERT mode, empty means update all
	PreferInternalEndpoint bool
	Properties             map[string]string // optional: extra SQL set properties for COPY/MERGE
	LoadUri                string            // optional: local directory for temp parquet files, defaults to os.TempDir()
	MaxRowsPerFile         int               // optional: max rows per parquet file, defaults to 1M
	MaxBytesPerFile        int               // optional: max bytes per parquet file, defaults to 64MB
}

// BulkloadCommitOptions holds commit-related options.
type BulkloadCommitOptions struct {
	Workspace      string
	VirtualCluster string
}

// CZTable holds table metadata.
type CZTable struct {
	SchemaName string
	TableName  string
	TableMeta  *ingestion.StreamSchema
	Schema     map[string]*util.DataType
}

// BulkloadMetadata holds stream metadata.
type BulkloadMetadata struct {
	InstanceId int64
	StreamInfo *ingestion.BulkLoadStreamInfo
	Table      CZTable
}

func (bm *BulkloadMetadata) GetOperation() BulkLoadOperation {
	switch bm.StreamInfo.GetOperation() {
	case ingestion.BulkLoadStreamOperation_BL_APPEND:
		return APPEND
	case ingestion.BulkLoadStreamOperation_BL_UPSERT:
		return UPSERT
	default:
		return OVERWRITE
	}
}

func (bm *BulkloadMetadata) GetState() BulkLoadState {
	switch bm.StreamInfo.GetStreamState() {
	case ingestion.BulkLoadStreamState_BL_CREATED:
		return CREATED
	case ingestion.BulkLoadStreamState_BL_SEALED:
		return SEALED
	case ingestion.BulkLoadStreamState_BL_COMMIT_SUBMITTED:
		return COMMIT_SUBMITTED
	case ingestion.BulkLoadStreamState_BL_COMMIT_SUCCESS:
		return COMMIT_SUCCESS
	case ingestion.BulkLoadStreamState_BL_COMMIT_FAILED:
		return COMMIT_FAILED
	default:
		return ABORTED
	}
}

func (bm *BulkloadMetadata) GetSQLErrorMsg() string {
	return bm.StreamInfo.GetSqlErrorMsg()
}

func (bm *BulkloadMetadata) GetPartitionSpec() string {
	return bm.StreamInfo.GetPartitionSpec()
}

func (bm *BulkloadMetadata) GetRecordKeys() []string {
	return bm.StreamInfo.GetRecordKeys()
}

// BulkLoadCommittable holds the local file list produced by a writer.
type BulkLoadCommittable struct {
	StreamId    string
	PartitionId int
	Files       []string // local file paths
	DstFiles    []string // volume file paths (set after PUT)
}

// BulkloadStream represents a bulkload stream with writer and committer support.
type BulkloadStream struct {
	MetaData      *BulkloadMetadata
	Connection    *ClickzettaConn
	CommitOptions *BulkloadCommitOptions
	StreamOptions *BulkloadOptions
	Closed        bool

	mu        sync.Mutex
	writers   map[int]*BulkloadWriter
	committer *BulkloadCommitter
}

func (stream *BulkloadStream) GetStreamId() string {
	return stream.MetaData.StreamInfo.StreamId
}

// CreateWriter creates a writer for the given partition.
func (stream *BulkloadStream) CreateWriter(partitionId int) (*BulkloadWriter, error) {
	if stream.Closed {
		return nil, errors.New("bulkload stream is closed")
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.writers == nil {
		stream.writers = make(map[int]*BulkloadWriter)
	}
	if w, ok := stream.writers[partitionId]; ok {
		return w, nil
	}

	writer := &BulkloadWriter{
		Connection:      stream.Connection,
		MetaData:        stream.MetaData,
		PartitionId:     int64(partitionId),
		StreamOptions:   stream.StreamOptions,
		MaxRowsPerFile:  stream.StreamOptions.MaxRowsPerFile,
		MaxBytesPerFile: stream.StreamOptions.MaxBytesPerFile,
	}
	if err := writer.Init(); err != nil {
		return nil, err
	}
	stream.writers[partitionId] = writer
	logger.Info("Created writer for stream:", stream.GetStreamId(), "partition:", partitionId)
	return writer, nil
}

// OpenWriter is an alias for CreateWriter for backward compatibility.
func (stream *BulkloadStream) OpenWriter(partitionId int64) (*BulkloadWriter, error) {
	return stream.CreateWriter(int(partitionId))
}

// CreateCommitter creates a committer for this stream.
func (stream *BulkloadStream) CreateCommitter() *BulkloadCommitter {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.committer != nil {
		return stream.committer
	}
	stream.committer = &BulkloadCommitter{
		Connection:    stream.Connection,
		MetaData:      stream.MetaData,
		StreamOptions: stream.StreamOptions,
	}
	return stream.committer
}

// Close closes all writers, collects committables, and commits the stream.
func (stream *BulkloadStream) Close() error {
	if stream.Closed {
		return nil
	}
	defer func() { stream.Closed = true }()

	// 1. Close all writers first (flush remaining data + upload to volume)
	stream.mu.Lock()
	var writerCloseErr error
	for _, w := range stream.writers {
		if err := w.Close(); err != nil && writerCloseErr == nil {
			writerCloseErr = err
		}
	}
	stream.mu.Unlock()
	if writerCloseErr != nil {
		// Upload failed — clean up all volume files from all writers
		stream.cleanupAllWriterVolumes()
		return writerCloseErr
	}

	// 2. Collect committables from all writers (volume paths already set)
	var committables []BulkLoadCommittable
	stream.mu.Lock()
	for _, w := range stream.writers {
		committables = append(committables, w.GetCommittables()...)
	}
	stream.mu.Unlock()

	// 3. Commit (COPY SQL only, files already uploaded)
	committer := stream.CreateCommitter()
	if err := committer.Commit(committables); err != nil {
		// Commit failed — volume cleanup is handled inside Commit
		return err
	}
	return nil
}

// cleanupAllWriterVolumes cleans up all uploaded volume files from all writers.
func (stream *BulkloadStream) cleanupAllWriterVolumes() {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	for _, w := range stream.writers {
		w.CleanupAllVolume()
	}
}

// Abort aborts the stream.
func (stream *BulkloadStream) Abort() error {
	logger.Info("Aborting bulkload stream:", stream.GetStreamId())
	stream.mu.Lock()
	defer stream.mu.Unlock()
	// Clean up all local files and remote volume files
	for _, w := range stream.writers {
		w.cleanupAllLocal()
		w.CleanupAllVolume()
	}
	return nil
}

// BulkloadCommitter handles the V2 commit lifecycle via SQL.
type BulkloadCommitter struct {
	Connection    *ClickzettaConn
	MetaData      *BulkloadMetadata
	StreamOptions *BulkloadOptions
}

// PrepareCommit uploads local files to table volume via PUT SQL + presigned URL.
func (c *BulkloadCommitter) PrepareCommit(committables []BulkLoadCommittable) error {
	if len(committables) == 0 {
		return errors.New("committables is empty")
	}

	tableName := c.StreamOptions.Table
	txnId := fmt.Sprintf("%s_%s_%d", c.MetaData.StreamInfo.GetStreamId(), uuid.New().String(), time.Now().UnixMilli())

	var allFiles []string
	for _, comm := range committables {
		allFiles = append(allFiles, comm.Files...)
	}
	if len(allFiles) == 0 {
		return errors.New("no files to commit")
	}

	// 1. Execute PUT SQL to get presigned URLs
	confSQL := generateConfSetSQL(c.StreamOptions.PreferInternalEndpoint, c.StreamOptions.Properties)
	putSQL := generatePutToTableVolumeSQL(tableName, txnId, allFiles)
	fullSQL := confSQL + putSQL

	logger.Info("Executing PUT SQL for stream:", c.MetaData.StreamInfo.GetStreamId())
	ctx := context.Background()
	rows, err := c.Connection.QueryContext(ctx, fullSQL, nil)
	if err != nil {
		return fmt.Errorf("PUT to table volume failed: %w", err)
	}
	defer rows.Close()

	putResult, err := parsePutResultRows(rows)
	if err != nil {
		return err
	}

	// 2. Upload files via presigned URLs
	for i, presignedURL := range putResult.PresignedURLs {
		if i >= len(allFiles) {
			break
		}
		if err := uploadFileViaPresignedURL(presignedURL, allFiles[i]); err != nil {
			return fmt.Errorf("upload file %s failed: %w", allFiles[i], err)
		}
		logger.Info("Uploaded file:", allFiles[i])
	}

	// 3. Set DstFiles on committables using volume paths
	fileIdx := 0
	for i := range committables {
		n := len(committables[i].Files)
		if fileIdx+n <= len(putResult.VolumePaths) {
			committables[i].DstFiles = putResult.VolumePaths[fileIdx : fileIdx+n]
		}
		fileIdx += n
	}
	return nil
}

// Commit prepares (PUT) and commits (COPY/MERGE) the data.
func (c *BulkloadCommitter) Commit(committables []BulkLoadCommittable) error {
	// Collect all volume dst files (already uploaded by writers)
	var allDstFiles []string
	for _, comm := range committables {
		allDstFiles = append(allDstFiles, comm.DstFiles...)
	}
	if len(allDstFiles) == 0 {
		return errors.New("no files to commit")
	}

	tableName := c.StreamOptions.Table
	operation := c.StreamOptions.Operation

	// Generate and execute COPY/MERGE SQL
	confSQL := generateConfSetSQL(false, c.StreamOptions.Properties)
	var commitSQL string
	if operation == UPSERT {
		commitSQL = generateMergeIntoSQL(tableName, c.StreamOptions.RecordKeys, c.StreamOptions.PartialUpdateColumns, c.MetaData.Table, allDstFiles)
	} else {
		opType := "INTO"
		if operation == OVERWRITE {
			opType = "OVERWRITE"
		}
		commitSQL = generateCopyTableVolumeSQL(tableName, opType, allDstFiles)
	}
	fullSQL := confSQL + commitSQL

	logger.Info("Executing commit SQL for stream:", c.MetaData.StreamInfo.GetStreamId())
	resp, err := c.Connection.exec(context.Background(), fullSQL, true, false, false, nil)
	if err != nil {
		c.cleanupVolumeFiles(committables)
		return fmt.Errorf("commit failed: %w", err)
	}
	if !resp.Success {
		c.cleanupVolumeFiles(committables)
		return fmt.Errorf("commit failed: %s", resp.Message)
	}

	// MERGE INTO doesn't support PURGE, clean up volume files after success
	if operation == UPSERT {
		c.cleanupVolumeFiles(committables)
	}

	logger.Info("Commit succeeded for stream:", c.MetaData.StreamInfo.GetStreamId())
	return nil
}

// cleanupVolumeFiles removes uploaded files from table volume.
func (c *BulkloadCommitter) cleanupVolumeFiles(committables []BulkLoadCommittable) {
	var allDstFiles []string
	for _, comm := range committables {
		allDstFiles = append(allDstFiles, comm.DstFiles...)
	}
	if len(allDstFiles) == 0 {
		return
	}
	tableName := c.StreamOptions.Table
	cleanupSQL := generateCleanUpTableVolumeSQL(tableName, allDstFiles)
	if cleanupSQL != "" {
		logger.Info("Cleaning up volume files for stream:", c.MetaData.StreamInfo.GetStreamId())
		_, _ = c.Connection.exec(context.Background(), cleanupSQL, true, false, false, nil)
	}
}

// --- SQL generation helpers ---

func generateConfSetSQL(preferInternal bool, properties map[string]string) string {
	sql := "set cz.sql.allow.insert.table.with.pk=true;\n" +
		"set cz.sql.copy.write.history.table.enabled=false;\n" +
		"set cz.storage.parquet.enable.read.vector.from.binary=true;\n"
	if preferInternal {
		sql = "set cz.sql.volume.file.transfer.force.external=false;\n" + sql
	}
	for k, v := range properties {
		sql += fmt.Sprintf("set %s=%s;\n", k, v)
	}
	return strings.ToLower(sql)
}

func generatePutToTableVolumeSQL(tableName string, txnId string, files []string) string {
	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = "'" + f + "'"
	}
	parallel := len(files)
	if parallel > 8 {
		parallel = 8
	}
	return strings.ToLower(fmt.Sprintf("PUT %s TO TABLE VOLUME %s SUBDIRECTORY '%s' PARALLEL = %d;",
		strings.Join(quoted, ","), tableName, txnId, parallel))
}

func generateCopyTableVolumeSQL(tableName string, opType string, dstFiles []string) string {
	quoted := make([]string, len(dstFiles))
	for i, f := range dstFiles {
		quoted[i] = "'" + f + "'"
	}
	return strings.ToLower(fmt.Sprintf("COPY %s %s FROM(SELECT * FROM TABLE VOLUME %s USING parquet FILES (%s)) PURGE = true;",
		opType, tableName, tableName, strings.Join(quoted, ",")))
}

func generateMergeIntoSQL(tableName string, recordKeys []string, partialUpdateColumns []string, table CZTable, dstFiles []string) string {
	quoted := make([]string, len(dstFiles))
	for i, f := range dstFiles {
		quoted[i] = "'" + f + "'"
	}

	// Build ON condition from primary keys
	var pkConditions []string
	for _, pk := range recordKeys {
		pkConditions = append(pkConditions, fmt.Sprintf("DST.`%s` = SRC.`%s`", pk, pk))
	}

	// Build sets for filtering
	pkSet := make(map[string]bool)
	for _, pk := range recordKeys {
		pkSet[strings.ToLower(pk)] = true
	}
	partialSet := make(map[string]bool)
	for _, col := range partialUpdateColumns {
		partialSet[strings.ToLower(col)] = true
	}
	hasPartial := len(partialUpdateColumns) > 0

	// Build column lists
	var updateCols, insertCols, srcCols []string
	for _, field := range table.TableMeta.GetDataFields() {
		name := field.GetName()
		nameLower := strings.ToLower(name)

		// UPDATE SET: only partial update columns (or all non-PK if no partial specified)
		if hasPartial {
			if partialSet[nameLower] {
				updateCols = append(updateCols, fmt.Sprintf("DST.`%s` = SRC.`%s`", name, name))
			}
		} else {
			updateCols = append(updateCols, fmt.Sprintf("DST.`%s` = SRC.`%s`", name, name))
		}
		// INSERT always includes all columns
		insertCols = append(insertCols, fmt.Sprintf("`%s`", name))
		srcCols = append(srcCols, fmt.Sprintf("SRC.`%s`", name))
	}

	return strings.ToLower(fmt.Sprintf(
		"MERGE INTO %s AS DST USING (SELECT * FROM TABLE VOLUME %s USING parquet FILES (%s)) SRC ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		tableName, tableName, strings.Join(quoted, ","),
		strings.Join(pkConditions, " AND "),
		strings.Join(updateCols, ", "),
		strings.Join(insertCols, ", "),
		strings.Join(srcCols, ", ")))
}

func generateCleanUpTableVolumeSQL(tableName string, dstFiles []string) string {
	if len(dstFiles) == 0 {
		return ""
	}
	// Remove quotes and build regex pattern
	var cleaned []string
	for _, f := range dstFiles {
		f = strings.Trim(f, "'")
		if f != "" {
			cleaned = append(cleaned, f)
		}
	}
	if len(cleaned) == 0 {
		return ""
	}
	return strings.ToLower(fmt.Sprintf("REMOVE TABLE VOLUME %s REGEXP '%s';", tableName, strings.Join(cleaned, "|")))
}

// putResultInfo holds parsed PUT SQL response.
type putResultInfo struct {
	PresignedURLs []string
	VolumePaths   []string
}

// putResponse represents the JSON returned by PUT SQL.
type putResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Ticket struct {
		PresignedUrls []string `json:"presignedUrls"`
		Paths         []string `json:"paths"`
	} `json:"ticket"`
}

func parsePutResultRows(rows driver.Rows) (*putResultInfo, error) {
	cols := rows.Columns()
	if len(cols) == 0 {
		return nil, errors.New("PUT returned no columns")
	}
	result := &putResultInfo{}
	dest := make([]driver.Value, len(cols))
	for {
		if err := rows.Next(dest); err != nil {
			break
		}
		jsonStr := fmt.Sprintf("%v", dest[0])
		var resp putResponse
		if err := json.Unmarshal([]byte(jsonStr), &resp); err != nil {
			return nil, fmt.Errorf("failed to parse PUT response: %w", err)
		}
		if !strings.EqualFold(resp.Status, "SUCCESS") {
			return nil, fmt.Errorf("PUT failed: %s", resp.Error)
		}
		result.PresignedURLs = append(result.PresignedURLs, resp.Ticket.PresignedUrls...)
		result.VolumePaths = append(result.VolumePaths, resp.Ticket.Paths...)
	}
	return result, nil
}

func uploadFileViaPresignedURL(presignedURL string, localFilePath string) error {
	file, err := os.Open(localFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", presignedURL, file)
	if err != nil {
		return err
	}
	req.ContentLength = stat.Size()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 300 {
		return fmt.Errorf("upload returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func cleanupLocalFile(path string) {
	if path == "" {
		return
	}
	if err := removeFile(path); err != nil {
		logger.Warn("Failed to cleanup local file:", path, err)
	}
	// Try to remove parent dir if empty
	dir := filepath.Dir(path)
	os.Remove(dir) // ignore error — fails if not empty, which is fine
}
