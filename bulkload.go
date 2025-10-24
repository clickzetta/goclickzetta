package goclickzetta

import (
	"context"
	"errors"
	"fmt"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/clickzetta/goclickzetta/protos/bulkload/ingestion"
	"github.com/clickzetta/goclickzetta/protos/bulkload/util"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/tencentyun/cos-go-sdk-v5"
)

type BulkloadStream struct {
	MetaData      *BulkloadMetadata
	Connection    *ClickzettaConn
	CommitOptions *BulkloadCommitOptions
	StreamOptions *BulkloadOptions
	Closed        bool
}

func (stream *BulkloadStream) Close() error {
	if stream.Closed {
		return nil
	}
	return stream.Commit()
}

func (stream *BulkloadStream) GetStreamId() string {
	return stream.MetaData.StreamInfo.StreamId
}

func (stream *BulkloadStream) OpenWriter(partitionId int64) (*BulkloadWriter, error) {
	if stream.Closed {
		return nil, errors.New("Bulkload stream is closed")
	}
	logger.Info("Opening writer for bulkload stream:", stream.MetaData.StreamInfo.GetStreamId())
	config, err := stream.Connection.OpenBulkloadStreamWriter(stream.MetaData.StreamInfo.GetStreamId(), *stream.StreamOptions, uint32(partitionId))
	if err != nil {
		logger.Error("Failed to open writer for bulkload stream:", stream.MetaData.StreamInfo.GetStreamId(), err)
		return nil, err
	}
	writer := &BulkloadWriter{
		Connection:    stream.Connection,
		MetaData:      stream.MetaData,
		BLConfig:      config,
		PartitionId:   partitionId,
		StreamOptions: stream.StreamOptions,
	}
	err = writer.Init()
	if err != nil {
		logger.Error("Failed to init writer for bulkload stream:", stream.MetaData.StreamInfo.GetStreamId(), err)
		return nil, err
	}
	return writer, nil
}

func (stream *BulkloadStream) Commit() error {
	if stream.Closed {
		return nil
	}
	logger.Info("Committing bulkload stream:", stream.MetaData.StreamInfo.GetStreamId())

	_, err := stream.Connection.CommitBulkloadStream(stream.MetaData.StreamInfo.GetStreamId(), COMMIT_STREAM, *stream.StreamOptions)
	if err != nil {
		logger.Error("Failed to commit bulkload stream:", stream.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	state := COMMIT_SUBMITTED
	sqlErrorMsg := ""
	retryCount := 0
	for {
		bulkload, err := stream.Connection.GetBulkloadStream(stream.MetaData.StreamInfo.GetStreamId(), *stream.StreamOptions)
		if err != nil {
			logger.Error("Failed to get bulkload stream:", stream.MetaData.StreamInfo.GetStreamId(), err)
			return err
		}
		currentStream := BulkloadStream{
			MetaData:   bulkload,
			Connection: stream.Connection,
		}
		state = currentStream.MetaData.GetState()
		sqlErrorMsg = currentStream.MetaData.GetSQLErrorMsg()
		logger.Info("Get bulkload stream state, retry times:", retryCount, "state:", state)
		if state == COMMIT_SUCCESS || state == COMMIT_FAILED {
			break
		} else {
			retryCount++
			time.Sleep(2 * time.Second)
		}

	}
	if state != COMMIT_SUCCESS {
		logger.Error("Failed to commit bulkload stream:", stream.MetaData.StreamInfo.GetStreamId(), sqlErrorMsg)
		return errors.New(sqlErrorMsg)
	}
	stream.Closed = true
	return nil
}

func (stream *BulkloadStream) Abort() error {
	logger.Info("Aborting bulkload stream:", stream.MetaData.StreamInfo.GetStreamId())
	meta, err := stream.Connection.CommitBulkloadStream(stream.MetaData.StreamInfo.GetStreamId(), ABORT_STREAM, *stream.StreamOptions)
	if err != nil {
		logger.Error("Failed to abort bulkload stream:", stream.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	if meta.GetState() != ABORTED {
		logger.Error("Failed to abort bulkload stream:", stream.MetaData.StreamInfo.GetStreamId())
		return errors.New("Failed to abort bulkload stream")
	}
	return nil
}

type BulkloadOptions struct {
	Table         string
	Operation     BulkLoadOperation
	PartitionSpec string
	RecordKeys    []string
}

func (opts *BulkloadOptions) properties() map[string]interface{} {
	return map[string]interface{}{
		"operation":       opts.Operation,
		"partition_specs": opts.PartitionSpec,
		"record_keys":     opts.RecordKeys,
	}
}

type BulkLoadCommitMode string

var (
	COMMIT_STREAM BulkLoadCommitMode
	ABORT_STREAM  BulkLoadCommitMode
)

type BulkLoadConfig struct {
	BLConfig *ingestion.BulkLoadStreamWriterConfig
}

func (bc *BulkLoadConfig) GetBulkLoadConfig() (StagingConfig, error) {
	stagingPath := bc.BLConfig.GetStagingPath()
	if stagingPath.GetOssPath().GetPath() != "" {
		return StagingConfig{
			Path:     stagingPath.GetOssPath().GetPath(),
			ID:       stagingPath.GetOssPath().GetStsAkId(),
			Secret:   stagingPath.GetOssPath().GetStsAkSecret(),
			Token:    stagingPath.GetOssPath().GetStsToken(),
			Endpoint: stagingPath.GetOssPath().GetOssEndpoint(),
			Type:     "oss",
		}, nil
	} else {
		return StagingConfig{
			Path:     stagingPath.GetCosPath().GetPath(),
			ID:       stagingPath.GetCosPath().GetStsAkId(),
			Secret:   stagingPath.GetCosPath().GetStsAkSecret(),
			Token:    stagingPath.GetCosPath().GetStsToken(),
			Endpoint: stagingPath.GetCosPath().GetCosRegion(),
			Type:     "cos",
		}, nil
	}
}

func (bc *BulkLoadConfig) GetFileFormat() FileFormat {
	if bc.BLConfig.GetFileFormat() == util.FileFormatType_TEXT {
		return TEXT
	} else if bc.BLConfig.GetFileFormat() == util.FileFormatType_PARQUET {
		return PARQUET
	} else if bc.BLConfig.GetFileFormat() == util.FileFormatType_ORC {
		return ORC
	} else if bc.BLConfig.GetFileFormat() == util.FileFormatType_AVRO {
		return AVRO
	} else if bc.BLConfig.GetFileFormat() == util.FileFormatType_CSV {
		return CSV
	} else if bc.BLConfig.GetFileFormat() == util.FileFormatType_ARROW {
		return ARROW
	} else if bc.BLConfig.GetFileFormat() == util.FileFormatType_HIVE_RESULT {
		return HIVE_RESULT
	} else if bc.BLConfig.GetFileFormat() == util.FileFormatType_DUMMY {
		return DUMMY
	} else if bc.BLConfig.GetFileFormat() == util.FileFormatType_MEMORY {
		return MEMORY
	} else if bc.BLConfig.GetFileFormat() == util.FileFormatType_ICEBERG {
		return ICEBERG
	} else {
		return TEXT
	}

}

func (bc *BulkLoadConfig) GetMaxRowsPerFile() int64 {
	if bc.BLConfig.GetMaxNumRowsPerFile() > 0 {
		return bc.BLConfig.GetMaxNumRowsPerFile()
	}
	return 64 << 20
}

func (bc *BulkLoadConfig) GetMaxBytesPerFile() int64 {
	if bc.BLConfig.GetMaxSizeInBytesPerFile() > 0 {
		return bc.BLConfig.GetMaxSizeInBytesPerFile()
	}
	return 256 << 20
}

type FileFormat string

var (
	TEXT        FileFormat = "text"
	PARQUET     FileFormat = "parquet"
	ORC         FileFormat = "orc"
	AVRO        FileFormat = "avro"
	CSV         FileFormat = "csv"
	ARROW       FileFormat = "arrow"
	HIVE_RESULT FileFormat = "hive_result"
	DUMMY       FileFormat = "dummy"
	MEMORY      FileFormat = "memory"
	ICEBERG     FileFormat = "iceberg"
)

type StagingConfig struct {
	Path     string
	ID       string
	Secret   string
	Token    string
	Endpoint string
	Type     string
}

type BulkloadMetadata struct {
	InstanceId int64
	StreamInfo *ingestion.BulkLoadStreamInfo
	Table      CZTable
}

type BulkLoadOperation string

var (
	APPEND    BulkLoadOperation = "APPEND"
	UPSERT    BulkLoadOperation = "UPSERT"
	OVERWRITE BulkLoadOperation = "OVERWRITE"
)

type BulkLoadState string

var (
	CREATED          BulkLoadState = "CREATED"
	SEALED           BulkLoadState = "SEALED"
	COMMIT_SUBMITTED BulkLoadState = "COMMIT_SUBMITTED"
	COMMIT_SUCCESS   BulkLoadState = "COMMIT_SUCCESS"
	COMMIT_FAILED    BulkLoadState = "COMMIT_FAILED"
	ABORTED          BulkLoadState = "ABORTED"
)

func (bm *BulkloadMetadata) GetOperation() BulkLoadOperation {
	if bm.StreamInfo.GetOperation() == ingestion.BulkLoadStreamOperation_BL_APPEND {
		return APPEND
	} else if bm.StreamInfo.GetOperation() == ingestion.BulkLoadStreamOperation_BL_UPSERT {
		return UPSERT
	}
	return OVERWRITE
}

func (bm *BulkloadMetadata) GetState() BulkLoadState {
	if bm.StreamInfo.GetStreamState() == ingestion.BulkLoadStreamState_BL_CREATED {
		return CREATED
	} else if bm.StreamInfo.GetStreamState() == ingestion.BulkLoadStreamState_BL_SEALED {
		return SEALED
	} else if bm.StreamInfo.GetStreamState() == ingestion.BulkLoadStreamState_BL_COMMIT_SUBMITTED {
		return COMMIT_SUBMITTED
	} else if bm.StreamInfo.GetStreamState() == ingestion.BulkLoadStreamState_BL_COMMIT_SUCCESS {
		return COMMIT_SUCCESS
	} else if bm.StreamInfo.GetStreamState() == ingestion.BulkLoadStreamState_BL_COMMIT_FAILED {
		return COMMIT_FAILED
	}

	return ABORTED
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

type BulkloadCommitOptions struct {
	Workspace      string
	VirtualCluster string
}

type CZTable struct {
	SchemaName string
	TableName  string
	TableMeta  *ingestion.StreamSchema
	Schema     map[string]*util.DataType
}

type BulkloadWriter struct {
	Connection             *ClickzettaConn
	MetaData               *BulkloadMetadata
	BLConfig               *BulkLoadConfig
	PartitionId            int64
	StreamOptions          *BulkloadOptions
	StageConfig            *StagingConfig
	PartitionSpec          map[string]string
	FileSystem             fs.FS
	FinishedFiles          []string
	FinishedFileSizes      []int
	FileNameUUID           string
	FileId                 int
	Closed                 bool
	CurrentTotalRows       int
	CurrentTotalBytes      int
	CurrentRecordBatch     map[string][]interface{}
	CurrentRecordBatchSize int
	CurrentRecordBatchRows int
	EstimateRowStaticSize  int
	OSSBucket              *oss.Bucket
	COSClient              *cos.Client
	LocalLocation          string
}

type Row struct {
	Columns          map[string]*util.DataType
	TableName        string
	ColumnNameValues map[string]interface{}
}

func (row *Row) SetBoolean(columnName string, value interface{}) error {
	v, ok := value.(bool)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to bool")
		return fmt.Errorf("Row set value failed, can not convert value: %s to bool", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetTinyInt(columnName string, value interface{}) error {
	v, ok := value.(int8)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to int8")
		return fmt.Errorf("Row set value failed, can not convert value: %s to int8", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetSmallInt(columnName string, value interface{}) error {
	v, ok := value.(int16)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to int16")
		return fmt.Errorf("Row set value failed, can not convert value: %s to int16", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetInt(columnName string, value interface{}) error {
	v, ok := value.(int32)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to int32")
		return fmt.Errorf("Row set value failed, can not convert value: %s to int32", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetBigint(columnName string, value interface{}) error {
	v, ok := value.(int64)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to int64")
		return fmt.Errorf("Row set value failed, can not convert value: %s to int64", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetFloat(columnName string, value interface{}) error {
	v, ok := value.(float32)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to float32")
		return fmt.Errorf("Row set value failed, can not convert value: %s to float32", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetDouble(columnName string, value interface{}) error {
	v, ok := value.(float64)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to float64")
		return fmt.Errorf("Row set value failed, can not convert value: %s to float64", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetDecimal(columnName string, value interface{}) error {
	v, ok := value.(decimal.Decimal)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to decimal")
		return fmt.Errorf("Row set value failed, can not convert value: %s to decimal", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetString(columnName string, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to string")
		return fmt.Errorf("Row set value failed, can not convert value: %s to string", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetDate(columnName string, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to date")
		return fmt.Errorf("Row set value failed, can not convert value: %s to date", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (row *Row) SetTimestamp(columnName string, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		logger.Error("Row set value failed, can not convert value:", value, "to timestamp")
		return fmt.Errorf("Row set value failed, can not convert value: %s to timestamp", value)
	}
	row.ColumnNameValues[columnName] = v
	return nil
}

func (bw *BulkloadWriter) Init() error {
	res, err := bw.BLConfig.GetBulkLoadConfig()
	if err != nil {
		logger.Error("Failed to get bulkload config:", bw.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	bw.StageConfig = &res
	err = bw.ProcessStagingType()
	if err != nil {
		logger.Error("Failed to process staging type:", bw.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	spec, err := bw.ParsePartitionSpec()
	if err != nil {
		logger.Error("Failed to parse partition spec:", bw.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	bw.PartitionSpec = spec
	bw.EstimateRowStaticSize = bw.EstimateRowSize()
	bw.FileNameUUID = uuid.New().String()
	bw.FileId = 0
	err = bw.ConstructArrowSchema()
	if err != nil {
		logger.Error("Failed to construct arrow schema:", bw.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	bw.FinishedFiles = []string{}
	bw.FinishedFileSizes = []int{}

	return nil
}

func (bw *BulkloadWriter) ProcessStagingType() error {
	bw.LocalLocation = os.TempDir() + bw.StageConfig.Path

	if strings.ToLower(bw.StageConfig.Type) == "oss" {
		option := oss.ClientOption(oss.SecurityToken(bw.StageConfig.Token))
		client, err := oss.New(bw.StageConfig.Endpoint, bw.StageConfig.ID, bw.StageConfig.Secret, option)
		if err != nil {
			logger.WithContext(nil).Errorf("error: %v", err)
			return err
		}
		bucketName := strings.SplitN(bw.StageConfig.Path, "/", 4)[2]

		bucket, err := client.Bucket(bucketName)
		if err != nil {
			logger.WithContext(nil).Errorf("error: %v", err)
			return err
		}
		bw.OSSBucket = bucket
	} else if strings.ToLower(bw.StageConfig.Type) == "cos" {
		url, err := url.Parse(bw.StageConfig.Endpoint)
		if err != nil {
			logger.WithContext(nil).Errorf("error: %v", err)
			return err
		}
		client := cos.NewClient(&cos.BaseURL{BucketURL: url}, &http.Client{
			Transport: &cos.AuthorizationTransport{
				SecretID:     bw.StageConfig.ID,
				SecretKey:    bw.StageConfig.Secret,
				SessionToken: bw.StageConfig.Token,
			},
		})
		bw.COSClient = client
	}
	return nil
}

func (bw *BulkloadWriter) CreateRow() *Row {
	return &Row{
		Columns:          bw.MetaData.Table.Schema,
		TableName:        bw.MetaData.StreamInfo.GetIdentifier().TableName,
		ColumnNameValues: map[string]interface{}{},
	}
}

func (bw *BulkloadWriter) Close() error {
	if bw.Closed {
		return nil
	}
	err := bw.Finish()
	if err != nil {
		logger.Error("Failed to finish bulkload stream writer:", bw.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	return nil
}

func (bw *BulkloadWriter) WriteRow(row *Row) error {
	if bw.Closed {
		return errors.New("Bulkload writer is closed")
	}
	err := bw.CheckFileStatus()
	if err != nil {
		logger.Error("Failed to check file status:", bw.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	if bw.CurrentRecordBatch == nil {
		bw.CurrentRecordBatch = map[string][]interface{}{}
	}
	for partitionK, partitionV := range bw.PartitionSpec {
		err := row.SetString(partitionK, partitionV)
		if err != nil {
			logger.Error("Failed to set partition spec:", bw.MetaData.StreamInfo.GetStreamId(), err)
			return err
		}
	}
	currentRowSize := bw.EstimateRowStaticSize
	for columnName, columnValue := range row.ColumnNameValues {
		bw.CurrentRecordBatch[columnName] = append(bw.CurrentRecordBatch[columnName], columnValue)
		v, ok := columnValue.(string)
		if ok {
			currentRowSize += len(v)
		}
	}
	bw.CurrentRecordBatchSize += currentRowSize
	bw.CurrentTotalRows++
	bw.CurrentRecordBatchRows++

	if bw.CurrentRecordBatchSize >= 16*1024*1024 || bw.CurrentRecordBatchRows >= 40000 {
		bufferSize, err := bw.FlushRecordBatch()
		if err != nil {
			logger.Error("Failed to flush record batch:", bw.MetaData.StreamInfo.GetStreamId(), err)
			return err
		}
		bw.CurrentTotalBytes += bufferSize
		bw.CurrentRecordBatchSize = 0
	}
	return nil
}

func (bw *BulkloadWriter) Finish() error {
	if bw.Closed {
		return errors.New("Bulkload writer is closed")
	}
	err := bw.CloseCurrentFile()
	if err != nil {
		logger.Error("Failed to close current file:", bw.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	tempSizes := []uint64{}
	for _, fileSize := range bw.FinishedFileSizes {
		tempSizes = append(tempSizes, uint64(fileSize))
	}
	resp, err := bw.Connection.FinishBulkloadStreamWriter(bw.MetaData.StreamInfo.GetStreamId(), *bw.StreamOptions, uint32(bw.PartitionId), bw.FinishedFiles, tempSizes)
	if err != nil {
		logger.Error("Failed to finish bulkload stream writer:", bw.MetaData.StreamInfo.GetStreamId(), err)
		return err
	}
	if resp.Code != ingestion.Code_SUCCESS {
		logger.Error("Failed to finish bulkload stream writer:", bw.MetaData.StreamInfo.GetStreamId(), resp.GetErrorMessage())
		return errors.New(resp.GetErrorMessage())
	}
	logger.Info("Finished writing bulkload stream:", bw.MetaData.StreamInfo.GetStreamId())
	bw.Closed = true
	bw.FinishedFiles = nil
	bw.FinishedFileSizes = nil
	return nil
}

func (bw *BulkloadWriter) Abort() error {
	bw.CurrentRecordBatchSize = 0
	bw.CurrentRecordBatchRows = 0
	bw.CurrentTotalBytes = 0
	bw.CurrentTotalRows = 0
	bw.CurrentRecordBatch = map[string][]interface{}{}
	bw.Closed = true
	return nil
}

func (bw *BulkloadWriter) EstimateRowSize() int {
	size := 0
	for _, column := range bw.MetaData.Table.Schema {
		if column.GetCategory() == util.DataTypeCategory_INT8 {
			size += 1
		} else if column.GetCategory() == util.DataTypeCategory_INT16 {
			size += 2
		} else if column.GetCategory() == util.DataTypeCategory_INT32 {
			size += 4
		} else if column.GetCategory() == util.DataTypeCategory_INT64 {
			size += 8
		} else if column.GetCategory() == util.DataTypeCategory_FLOAT32 {
			size += 4
		} else if column.GetCategory() == util.DataTypeCategory_FLOAT64 {
			size += 8
		} else if column.GetCategory() == util.DataTypeCategory_DECIMAL {
			size += 16
		} else if column.GetCategory() == util.DataTypeCategory_STRING ||
			column.GetCategory() == util.DataTypeCategory_VARCHAR ||
			column.GetCategory() == util.DataTypeCategory_CHAR {
			size += 16
		} else if column.GetCategory() == util.DataTypeCategory_DATE {
			size += 8
		} else if column.GetCategory() == util.DataTypeCategory_TIMESTAMP_LTZ {
			size += 8
		} else if column.GetCategory() == util.DataTypeCategory_BOOLEAN {
			size += 1
		} else {
			size += 8
		}
	}
	return size
}

func (bw *BulkloadWriter) ParsePartitionSpec() (map[string]string, error) {
	partitionSpec := bw.MetaData.GetPartitionSpec()
	if partitionSpec == "" {
		return nil, nil
	}
	partitionSpecMap := map[string]string{}
	partitionSpecs := strings.Split(partitionSpec, ",")
	for _, partitionSpec := range partitionSpecs {
		partitionSpecKV := strings.Split(partitionSpec, "=")
		if len(partitionSpecKV) != 2 {
			return nil, errors.New("Invalid partition spec:" + partitionSpec)
		}
		partitionSpecMap[partitionSpecKV[0]] = partitionSpecKV[1]
	}
	return partitionSpecMap, nil
}

func (bw *BulkloadWriter) CheckFileStatus() error {
	return nil

}

func (bw *BulkloadWriter) CloseCurrentFile() error {
	return nil
}

func (bw *BulkloadWriter) UploadLocalFile() (string, error) {
	fileName := bw.CurrentFileName()
	objectStorgePath := strings.SplitN(bw.StageConfig.Path, "/", 4)[3] + strings.Split(fileName, "/")[len(strings.Split(fileName, "/"))-1]
	logger.Info("Uploading file:", fileName, "to", objectStorgePath)
	if strings.ToLower(bw.StageConfig.Type) == "oss" {
		err := bw.OSSBucket.PutObjectFromFile(objectStorgePath, fileName)
		if err != nil {
			logger.Error("Failed to upload file: %v to oss:", fileName, err)
			return "", err
		}

	} else if strings.ToLower(bw.StageConfig.Type) == "cos" {
		fileName := bw.CurrentFileName()
		_, err := bw.COSClient.Object.PutFromFile(context.Background(), objectStorgePath, fileName, nil)
		if err != nil {
			logger.Error("Failed to upload file: %v to cos:", fileName, err)
			return "", err
		}
	}
	err := os.Remove(fileName)
	if err != nil {
		logger.Error("Failed to remove local file:", fileName, err)
		return "", err
	}
	return bw.StageConfig.Path + strings.Split(fileName, "/")[len(strings.Split(fileName, "/"))-1], nil
}

func (bw *BulkloadWriter) CurrentFileName() string {
	return bw.LocalLocation + bw.FileNameUUID + "_" + strconv.Itoa(bw.FileId) + "." + string(bw.BLConfig.GetFileFormat())
}

func (bw *BulkloadWriter) FlushRecordBatch() (int, error) {
	if bw.CurrentRecordBatchRows == 0 {
		return 0, nil
	}
	return 0, nil

}

func (bw *BulkloadWriter) ConstructArrowSchema() error {
	return nil
}

func ConvertToArrowValue(value interface{}, tpe *util.DataType) (string, error) {
	category := tpe.GetCategory()
	switch category {
	case util.DataTypeCategory_BOOLEAN:
		v, ok := value.(bool)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to bool", value)
		}
		return strconv.FormatBool(v), nil
	case util.DataTypeCategory_INT8:
		v, ok := value.(int8)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to int8", value)
		}
		return strconv.Itoa(int(v)), nil
	case util.DataTypeCategory_INT16:
		v, ok := value.(int16)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to int16", value)
		}
		return strconv.Itoa(int(v)), nil
	case util.DataTypeCategory_INT32:
		v, ok := value.(int32)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to int32", value)
		}
		return strconv.Itoa(int(v)), nil
	case util.DataTypeCategory_INT64:
		v, ok := value.(int64)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to int64", value)
		}
		return strconv.Itoa(int(v)), nil
	case util.DataTypeCategory_FLOAT32:
		v, ok := value.(float32)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to float32", value)
		}
		return strconv.FormatFloat(float64(v), 'f', -1, 64), nil
	case util.DataTypeCategory_FLOAT64:
		v, ok := value.(float64)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to float64", value)
		}
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case util.DataTypeCategory_DECIMAL:
		v, ok := value.(decimal.Decimal)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to float64", value)
		}
		return v.String(), nil
	case util.DataTypeCategory_STRING:
		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to string", value)
		}
		return v, nil
	case util.DataTypeCategory_DATE:
		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to string", value)
		}
		return v, nil
	case util.DataTypeCategory_CHAR:
		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to string", value)
		}
		return v, nil
	case util.DataTypeCategory_VARCHAR:
		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to string", value)
		}
		return v, nil
	case util.DataTypeCategory_TIMESTAMP_LTZ:
		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("ConvertToArrowValue:Failed to convert value: %s to string", value)
		}
		return v, nil
	default:
		return "", errors.New("Unsupported data type:" + tpe.String())
	}
}
