package goclickzetta

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/tencentyun/cos-go-sdk-v5"
)

type queryDataType int

const (
	Memory queryDataType = iota
	File   queryDataType = iota
)

type objectStorageType int

const (
	OSS objectStorageType = iota
	COS objectStorageType = iota
)

type httpResponseMessage struct {
	HttpResponseMessageStatus    httpResponseMessageStatus    `json:"status"`
	HttpResponseMessageResultSet httpResponseMessageResultSet `json:"resultSet"`
}

type httpResponseMessageStatus struct {
	JobId       responseJobId `json:"jobId"`
	State       string        `json:"state"`
	RunningTime string        `json:"runningTime"`
	Status      string        `json:"status"`
	ErrorCode   string        `json:"errorCode"`
	Message     string        `json:"message"`
}

type httpResponseMessageResultSet struct {
	MetaData              metaData              `json:"metadata"`
	ObjectStorageLocation objectStorageLocation `json:"location"`
	MemoryData            memoryData            `json:"data"`
}

type responseJobId struct {
	ID         string `json:"id"`
	Workspace  string `json:"workspace"`
	InstanceId string `json:"instanceId"`
}

type metaData struct {
	Format string  `json:"format"`
	Type   string  `json:"type"`
	Fields []field `json:"fields"`
}

type field struct {
	Name      string    `json:"name"`
	FieldType fieldType `json:"type"`
	Comment   string    `json:"comment"`
}

type fieldType struct {
	Category        string          `json:"category"`
	Nullable        bool            `json:"nullable"`
	FieldId         int64           `json:"fieldId"`
	TimestampInfo   timestampInfo   `json:"timestampInfo"`
	CharTypeInfo    charTypeInfo    `json:"charTypeInfo"`
	DecimalTypeInfo decimalTypeInfo `json:"decimalTypeInfo"`
}

type charTypeInfo struct {
	Length int64 `json:"length"`
}

type decimalTypeInfo struct {
	Precision string `json:"precision"`
	Scale     string `json:"scale"`
}

type timestampInfo struct {
	TsUnit string `json:"tsUnit"`
}

type objectStorageLocation struct {
	Location               []string           `json:"location"`
	FileSystem             string             `json:"fileSystem"`
	ID                     string             `json:"stsAkId"`
	Secret                 string             `json:"stsAkSecret"`
	Token                  string             `json:"stsToken"`
	OSSEndpoint            string             `json:"ossEndpoint"`
	OSSInternalEndpoint    string             `json:"ossInternalEndpoint"`
	COSObjectStorageRegion string             `json:"objectStorageRegion"`
	LocationFiles          []locationFileInfo `json:"locationFiles"`
}

type locationFileInfo struct {
	FilePath string `json:"filePath"`
	FileSize string `json:"fileSize"`
}

type memoryData struct {
	Data []string `json:"data"`
}

type execResponseData struct {
	// query data
	Data []interface{}
	// query data type
	DataType queryDataType
	// query data file list
	FileList []string
	// query object storage type
	ObjectStorageType   objectStorageType
	OSSBucket           *oss.Bucket
	COSClient           *cos.Client
	CurrentFileIndex    int
	MemoryRead          bool
	Schema              []execResponseColumnType
	HTTPResponseMessage httpResponseMessage
	RowCount            int64
	JobId               string
	QuerySQL            string
}

type objectStorageErrorSummary struct {
	errorType  string
	statusCode int
	errorCode  string
	requestID  string
	traceID    string
}

func summarizeObjectStorageError(err error) objectStorageErrorSummary {
	summary := objectStorageErrorSummary{errorType: safeErrorType(err)}

	var ossPointer *oss.ServiceError
	if errors.As(err, &ossPointer) && ossPointer != nil {
		return objectStorageErrorSummary{
			errorType:  summary.errorType,
			statusCode: ossPointer.StatusCode,
			errorCode:  sanitizeDiagnosticToken(ossPointer.Code),
			requestID:  sanitizeDiagnosticToken(ossPointer.RequestID),
		}
	}
	var ossValue oss.ServiceError
	if errors.As(err, &ossValue) {
		return objectStorageErrorSummary{
			errorType:  summary.errorType,
			statusCode: ossValue.StatusCode,
			errorCode:  sanitizeDiagnosticToken(ossValue.Code),
			requestID:  sanitizeDiagnosticToken(ossValue.RequestID),
		}
	}

	var cosError *cos.ErrorResponse
	if errors.As(err, &cosError) && cosError != nil {
		summary.errorCode = sanitizeDiagnosticToken(cosError.Code)
		summary.requestID = sanitizeDiagnosticToken(cosError.RequestID)
		summary.traceID = sanitizeDiagnosticToken(cosError.TraceID)
		if cosError.Response != nil {
			summary.statusCode = cosError.Response.StatusCode
			if summary.requestID == "" {
				summary.requestID = sanitizeDiagnosticToken(cosError.Response.Header.Get("X-Cos-Request-Id"))
			}
			if summary.traceID == "" {
				summary.traceID = sanitizeDiagnosticToken(cosError.Response.Header.Get("X-Cos-Trace-Id"))
			}
		}
	}
	return summary
}

func sanitizeDiagnosticToken(value string) string {
	return strings.ReplaceAll(sanitizeDiagnosticText(value, maxResponseErrorCodeBytes), " ", "_")
}

func (summary objectStorageErrorSummary) fields() []interface{} {
	fields := []interface{}{"error_type", summary.errorType}
	if summary.statusCode != 0 {
		fields = append(fields, "status_code", summary.statusCode)
	}
	if summary.errorCode != "" {
		fields = append(fields, "error_code", summary.errorCode)
	}
	if summary.requestID != "" {
		fields = append(fields, "request_id", summary.requestID)
	}
	if summary.traceID != "" {
		fields = append(fields, "trace_id", summary.traceID)
	}
	return fields
}

func (summary objectStorageErrorSummary) String() string {
	fields := summary.fields()
	parts := make([]string, 0, len(fields)/2)
	for index := 0; index+1 < len(fields); index += 2 {
		parts = append(parts, fmt.Sprintf("%v=%v", fields[index], fields[index+1]))
	}
	return strings.Join(parts, " ")
}

func parseObjectStoragePath(value string) (bucket string, objectKey string, ok bool) {
	parts := strings.SplitN(value, "/", 4)
	if len(parts) != 4 || !strings.HasSuffix(parts[0], ":") || parts[1] != "" || parts[2] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[2], parts[3], true
}

func validateObjectStorageLocations(locations []string) (string, error) {
	bucket := ""
	for index, location := range locations {
		currentBucket, _, ok := parseObjectStoragePath(location)
		if !ok || bucket != "" && currentBucket != bucket {
			return "", &ClickzettaError{
				Number:         -1,
				Message:        fmt.Sprintf("invalid object storage file path at index %d", index),
				MessageArgs:    make([]interface{}, 0),
				IncludeQueryID: false,
			}
		}
		bucket = currentBucket
	}
	return bucket, nil
}

func (qd *execResponseData) init(diagnostics queryDiagnostics) error {
	if err := qd.parseSchema(); err != nil {
		diagnostics.errorf("parse response schema failed, error_type=%s", safeErrorType(err))
		return err
	}
	if len(qd.HTTPResponseMessage.HttpResponseMessageResultSet.MemoryData.Data) != 0 {
		qd.DataType = Memory
		qd.MemoryRead = false
	} else if len(qd.HTTPResponseMessage.HttpResponseMessageResultSet.ObjectStorageLocation.Location) != 0 {
		qd.DataType = File
		loc := qd.HTTPResponseMessage.HttpResponseMessageResultSet.ObjectStorageLocation
		qd.FileList = make([]string, 0)
		for _, file := range loc.Location {
			qd.FileList = append(qd.FileList, file)
		}
		bucketName, err := validateObjectStorageLocations(qd.FileList)
		if err != nil {
			diagnostics.errorf("invalid object storage location list, error_type=%s", safeErrorType(err))
			return err
		}
		if loc.FileSystem == "OSS" {
			qd.ObjectStorageType = OSS
			option := oss.ClientOption(oss.SecurityToken(loc.Token))
			client, err := oss.New(loc.OSSEndpoint, loc.ID, loc.Secret, option)
			if err != nil {
				diagnostics.errorf("initialize OSS client failed, %s", summarizeObjectStorageError(err))
				return err
			}

			bucket, err := client.Bucket(bucketName)
			if err != nil {
				diagnostics.errorf("initialize OSS bucket failed, %s", summarizeObjectStorageError(err))
				return err
			}
			qd.OSSBucket = bucket
		} else if loc.FileSystem == "COS" {
			qd.ObjectStorageType = COS
			cosUrl := "https://" + bucketName + ".cos." + loc.COSObjectStorageRegion + ".myqcloud.com"
			url, err := url.Parse(cosUrl)
			if err != nil {
				diagnostics.errorf("initialize COS endpoint failed, error_type=%s", safeErrorType(err))
				return err
			}
			client := cos.NewClient(&cos.BaseURL{BucketURL: url}, &http.Client{
				Transport: &cos.AuthorizationTransport{
					SecretID:     loc.ID,
					SecretKey:    loc.Secret,
					SessionToken: loc.Token,
				},
			})
			qd.COSClient = client
		} else {
			return &ClickzettaError{
				Number:         -1,
				Message:        "object storage type is not supported",
				SQLState:       "",
				QueryID:        "",
				MessageArgs:    make([]interface{}, 0),
				IncludeQueryID: false,
			}
		}
	}
	return nil
}

func (qd *execResponseData) parseSchema() error {
	fields := make([]execResponseColumnType, 0)
	for _, field := range qd.HTTPResponseMessage.HttpResponseMessageResultSet.MetaData.Fields {
		dataType := getclickzettaType(strings.ToUpper(field.FieldType.Category))
		if dataType == DECIMAL || field.FieldType.DecimalTypeInfo.Precision != "" || field.FieldType.DecimalTypeInfo.Scale != "" {
			precision, err := strconv.ParseInt(field.FieldType.DecimalTypeInfo.Precision, 10, 64)
			if err != nil {
				return fmt.Errorf("parse decimal precision: %w", err)
			}
			scale, err := strconv.ParseInt(field.FieldType.DecimalTypeInfo.Scale, 10, 64)
			if err != nil {
				return fmt.Errorf("parse decimal scale: %w", err)
			}
			fields = append(fields, execResponseColumnType{
				Name:      field.Name,
				Length:    0,
				Type:      field.FieldType.Category,
				Precision: precision,
				Scale:     scale,
				Nullable:  field.FieldType.Nullable,
				TsUnit:    "",
			})
		} else if dataType == TIMESTAMP_LTZ || dataType == TIMESTAMP_NTZ || field.FieldType.TimestampInfo.TsUnit != "" {
			fields = append(fields, execResponseColumnType{
				Name:      field.Name,
				Length:    0,
				Type:      field.FieldType.Category,
				Precision: 0,
				Scale:     0,
				Nullable:  field.FieldType.Nullable,
				TsUnit:    field.FieldType.TimestampInfo.TsUnit,
			})
		} else {
			fields = append(fields, execResponseColumnType{
				Name:      field.Name,
				Length:    field.FieldType.CharTypeInfo.Length,
				Type:      field.FieldType.Category,
				Precision: 0,
				Scale:     0,
				Nullable:  field.FieldType.Nullable,
				TsUnit:    "",
			})
		}
	}
	qd.Schema = fields
	return nil
}

func (qd *execResponseData) readMemoryData(diagnostics queryDiagnostics) error {
	qd.Data = make([]interface{}, 0)
	for index, data := range qd.HTTPResponseMessage.HttpResponseMessageResultSet.MemoryData.Data {
		readerStart := diagnostics.trace.start()
		buffer := base64.NewDecoder(base64.StdEncoding, strings.NewReader(data))
		reader, err := ipc.NewReader(buffer)
		if err != nil {
			if diagnostics.trace.enabled {
				diagnostics.trace.record(qd.JobId, "memory_ipc_reader_error", readerStart, "chunk", index, "error_type", safeErrorType(err))
			}
			diagnostics.errorf("create memory IPC reader failed, chunk=%d, error_type=%s", index, safeErrorType(err))
			return err
		}
		if diagnostics.trace.enabled {
			diagnostics.trace.record(qd.JobId, "memory_ipc_reader", readerStart, "chunk", index)
		}
		err = arrowToRows(diagnostics, qd, reader)
		reader.Release()
		if err != nil {
			diagnostics.errorf("decode memory Arrow rows failed, chunk=%d, error_type=%s", index, safeErrorType(err))
			return err
		}
	}
	return nil
}

func arrowToRows(diagnostics queryDiagnostics, qd *execResponseData, reader *ipc.Reader) error {
	arrowStart := diagnostics.trace.start()
	tempDataList := make(map[int][]interface{}, 0)
	recordCount := 0
	valueCount := 0

	for reader.Next() {
		recordCount++
		record := reader.Record()

		for index, column := range record.Columns() {
			if diagnostics.trace.enabled {
				valueCount += column.Len()
			}
			if tempDataList[index] == nil {
				des := make([]interface{}, column.Len())
				timeLocation := time.Local
				err := arrowToValue(des, qd.Schema[index], column, timeLocation, false)
				if err != nil {
					diagnostics.errorf("decode Arrow column failed, error_type=%s", safeErrorType(err))
					return err
				}
				tempDataList[index] = des

			} else {
				des := make([]interface{}, column.Len())
				timeLocation := time.Local
				err := arrowToValue(des, qd.Schema[index], column, timeLocation, false)
				if err != nil {
					diagnostics.errorf("decode Arrow column failed, error_type=%s", safeErrorType(err))
					return err
				}
				tempDataList[index] = append(tempDataList[index], des...)

			}
		}
	}
	if err := reader.Err(); err != nil {
		if diagnostics.trace.enabled {
			diagnostics.trace.record(qd.JobId, "arrow_to_rows_error", arrowStart, "error_type", safeErrorType(err))
		}
		return err
	}

	for i := 0; i < len(tempDataList[0]); i++ {
		row := make([]interface{}, 0)
		for j := 0; j < len(tempDataList); j++ {
			row = append(row, tempDataList[j][i])
		}
		qd.Data = append(qd.Data, row)
	}
	if diagnostics.trace.enabled {
		diagnostics.trace.record(qd.JobId, "arrow_to_rows", arrowStart, "records", recordCount, "values", valueCount, "rows", len(qd.Data))
	}
	return nil
}

func (qd *execResponseData) read(diagnostics queryDiagnostics) error {
	if qd.DataType == Memory {
		if qd.MemoryRead {
			qd.Data = make([]interface{}, 0)
			return nil
		}
		qd.MemoryRead = true
		memoryStart := diagnostics.trace.start()
		err := qd.readMemoryData(diagnostics)
		if err != nil {
			if diagnostics.trace.enabled {
				diagnostics.trace.record(qd.JobId, "read_memory_error", memoryStart, "error_type", safeErrorType(err))
			}
			return err
		}
		qd.RowCount = int64(len(qd.Data))
		if diagnostics.trace.enabled {
			diagnostics.trace.record(qd.JobId, "read_memory", memoryStart, "chunks", len(qd.HTTPResponseMessage.HttpResponseMessageResultSet.MemoryData.Data), "rows", len(qd.Data))
		}
		return nil
	} else if qd.DataType == File {
		if qd.FileList == nil || len(qd.FileList) == 0 {
			return &ClickzettaError{
				Number:         -1,
				Message:        "object storage file list is empty",
				SQLState:       "",
				QueryID:        "",
				MessageArgs:    make([]interface{}, 0),
				IncludeQueryID: false,
			}
		}
		qd.Data = make([]interface{}, 0)
		if qd.CurrentFileIndex >= len(qd.FileList) {
			return nil
		}
		fileIndex := qd.CurrentFileIndex
		fileName := qd.FileList[fileIndex]
		_, objectKey, validPath := parseObjectStoragePath(fileName)
		qd.CurrentFileIndex++
		if !validPath {
			diagnostics.errorf("invalid object storage file path, file_index=%d", fileIndex)
			return &ClickzettaError{
				Number:         -1,
				Message:        "invalid object storage file path",
				MessageArgs:    make([]interface{}, 0),
				IncludeQueryID: false,
			}
		}
		if qd.ObjectStorageType == OSS {
			downloadStart := diagnostics.trace.start()
			stream, err := qd.OSSBucket.GetObject(objectKey)
			if err != nil {
				summary := summarizeObjectStorageError(err)
				if diagnostics.trace.enabled {
					fields := []interface{}{"file_index", fileIndex, "storage", "OSS"}
					fields = append(fields, summary.fields()...)
					diagnostics.trace.record(qd.JobId, "file_download_error", downloadStart, fields...)
				}
				diagnostics.errorf("download object storage file failed, file_index=%d, storage=OSS, %s", fileIndex, summary)
				return err
			}
			defer stream.Close()
			if diagnostics.trace.enabled {
				diagnostics.trace.record(qd.JobId, "file_download", downloadStart, "file_index", fileIndex, "storage", "OSS")
			}
			readerStart := diagnostics.trace.start()
			reader, err := ipc.NewReader(stream)
			if err != nil {
				if diagnostics.trace.enabled {
					diagnostics.trace.record(qd.JobId, "file_ipc_reader_error", readerStart, "file_index", fileIndex, "storage", "OSS", "error_type", safeErrorType(err))
				}
				diagnostics.errorf("create file IPC reader failed, file_index=%d, storage=OSS, error_type=%s", fileIndex, safeErrorType(err))
				return err
			}
			defer reader.Release()
			if diagnostics.trace.enabled {
				diagnostics.trace.record(qd.JobId, "file_ipc_reader", readerStart, "file_index", fileIndex, "storage", "OSS")
			}
			err = arrowToRows(diagnostics, qd, reader)
			if err != nil {
				return err
			}

		} else if qd.ObjectStorageType == COS {
			downloadStart := diagnostics.trace.start()
			res, err := qd.COSClient.Object.Get(context.Background(), objectKey, nil)
			if err != nil {
				summary := summarizeObjectStorageError(err)
				if diagnostics.trace.enabled {
					fields := []interface{}{"file_index", fileIndex, "storage", "COS"}
					fields = append(fields, summary.fields()...)
					diagnostics.trace.record(qd.JobId, "file_download_error", downloadStart, fields...)
				}
				diagnostics.errorf("download object storage file failed, file_index=%d, storage=COS, %s", fileIndex, summary)
				return err
			}
			defer res.Body.Close()
			if diagnostics.trace.enabled {
				diagnostics.trace.record(qd.JobId, "file_download", downloadStart, "file_index", fileIndex, "storage", "COS", "status", res.StatusCode)
			}
			readerStart := diagnostics.trace.start()
			reader, err := ipc.NewReader(res.Body)
			if err != nil {
				if diagnostics.trace.enabled {
					diagnostics.trace.record(qd.JobId, "file_ipc_reader_error", readerStart, "file_index", fileIndex, "storage", "COS", "error_type", safeErrorType(err))
				}
				diagnostics.errorf("create file IPC reader failed, file_index=%d, storage=COS, error_type=%s", fileIndex, safeErrorType(err))
				return err
			}
			defer reader.Release()
			if diagnostics.trace.enabled {
				diagnostics.trace.record(qd.JobId, "file_ipc_reader", readerStart, "file_index", fileIndex, "storage", "COS")
			}
			err = arrowToRows(diagnostics, qd, reader)
			if err != nil {
				return err
			}

		}
		qd.RowCount = qd.RowCount + int64(len(qd.Data))
	}
	return nil
}

type execResponseColumnType struct {
	Name      string `json:"name"`
	Length    int64  `json:"length"`
	Type      string `json:"type"`
	Precision int64  `json:"precision"`
	Scale     int64  `json:"scale"`
	Nullable  bool   `json:"nullable"`
	TsUnit    string `json:"tsUnit"`
}

type execResponse struct {
	Data    execResponseData `json:"Data"`
	Message string           `json:"message"`
	Code    string           `json:"code"`
	Success bool             `json:"success"`
}
