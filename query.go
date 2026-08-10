package goclickzetta

import (
	"encoding/base64"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type queryDataType int

const (
	Memory queryDataType = iota
	File   queryDataType = iota
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
	PresignedUrls          []string           `json:"presignedUrls"`
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
	// query data file list (presigned URLs)
	FileList            []string
	CurrentFileIndex    int
	MemoryRead          bool
	Schema              []execResponseColumnType
	HTTPResponseMessage httpResponseMessage
	RowCount            int64
	JobId               string
	QuerySQL            string
}

func hasField(obj interface{}, field string) bool {
	t := reflect.TypeOf(obj)
	_, ok := t.FieldByName(field)
	return ok
}

func (qd *execResponseData) init(diagnostics queryDiagnostics) error {
	qd.parseSchema(diagnostics)
	if len(qd.HTTPResponseMessage.HttpResponseMessageResultSet.MemoryData.Data) != 0 {
		qd.DataType = Memory
		qd.MemoryRead = false
	} else if len(qd.HTTPResponseMessage.HttpResponseMessageResultSet.ObjectStorageLocation.Location) != 0 {
		qd.DataType = File
		loc := qd.HTTPResponseMessage.HttpResponseMessageResultSet.ObjectStorageLocation
		qd.FileList = make([]string, 0)
		for _, file := range loc.PresignedUrls {
			qd.FileList = append(qd.FileList, file)
		}
	}
	return nil
}

func (qd *execResponseData) parseSchema(diagnostics queryDiagnostics) {
	fields := make([]execResponseColumnType, 0)
	for _, field := range qd.HTTPResponseMessage.HttpResponseMessageResultSet.MetaData.Fields {
		if hasField(field.FieldType, "CharTypeInfo") {
			fields = append(fields, execResponseColumnType{
				Name:      field.Name,
				Length:    field.FieldType.CharTypeInfo.Length,
				Type:      field.FieldType.Category,
				Precision: 0,
				Scale:     0,
				Nullable:  field.FieldType.Nullable,
				TsUnit:    "",
			})
		} else if hasField(field.FieldType, "DecimalTypeInfo") {
			precision, err := strconv.ParseInt(field.FieldType.DecimalTypeInfo.Precision, 10, 64)
			if err != nil {
				diagnostics.errorf("error: %v", err)
				return
			}
			scale, err := strconv.ParseInt(field.FieldType.DecimalTypeInfo.Scale, 10, 64)
			if err != nil {
				diagnostics.errorf("error: %v", err)
				return
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
		} else if hasField(field.FieldType, "TimestampInfo") {
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
				Length:    0,
				Type:      field.FieldType.Category,
				Precision: 0,
				Scale:     0,
				Nullable:  field.FieldType.Nullable,
				TsUnit:    "",
			})
		}
	}
	qd.Schema = fields
}

func (qd *execResponseData) readMemoryData(diagnostics queryDiagnostics) error {
	qd.Data = make([]interface{}, 0)
	for index, data := range qd.HTTPResponseMessage.HttpResponseMessageResultSet.MemoryData.Data {
		readerStart := diagnostics.trace.start()
		buffer := base64.NewDecoder(base64.StdEncoding, strings.NewReader(data))
		reader, err := ipc.NewReader(buffer)
		if err != nil {
			if diagnostics.trace.enabled {
				diagnostics.trace.record(qd.JobId, "memory_ipc_reader_error", readerStart, "chunk", index, "error", err)
			}
			diagnostics.errorf("error: %v", err)
			return err
		}
		if diagnostics.trace.enabled {
			diagnostics.trace.record(qd.JobId, "memory_ipc_reader", readerStart, "chunk", index)
		}
		err = arrowToRows(diagnostics, qd, reader)
		if err != nil {
			diagnostics.errorf("error: %v", err)
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
					diagnostics.errorf("error: %v", err)
					return err
				}
				tempDataList[index] = des

			} else {
				des := make([]interface{}, column.Len())
				timeLocation := time.Local
				err := arrowToValue(des, qd.Schema[index], column, timeLocation, false)
				if err != nil {
					diagnostics.errorf("error: %v", err)
					return err
				}
				tempDataList[index] = append(tempDataList[index], des...)

			}
		}
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
				diagnostics.trace.record(qd.JobId, "read_memory_error", memoryStart, "error", err)
			}
			diagnostics.errorf("error: %v", err)
			return err
		}
		qd.RowCount = int64(len(qd.Data))
		if diagnostics.trace.enabled {
			diagnostics.trace.record(qd.JobId, "read_memory", memoryStart, "chunks", len(qd.HTTPResponseMessage.HttpResponseMessageResultSet.MemoryData.Data), "rows", len(qd.Data))
		}
		return nil
	} else if qd.DataType == File {
		if len(qd.FileList) == 0 {
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
		presignedUrl := qd.FileList[fileIndex]
		qd.CurrentFileIndex++

		httpStart := diagnostics.trace.start()
		resp, err := http.Get(presignedUrl)
		if err != nil {
			if diagnostics.trace.enabled {
				diagnostics.trace.record(qd.JobId, "file_http_get_error", httpStart, "file_index", fileIndex, "error", err)
			}
			diagnostics.errorf("error: %v", err)
			return err
		}
		defer resp.Body.Close()
		if diagnostics.trace.enabled {
			diagnostics.trace.record(qd.JobId, "file_http_get", httpStart, "file_index", fileIndex, "status", resp.StatusCode)
		}

		if resp.StatusCode != http.StatusOK {
			diagnostics.errorf("failed to download file, status code: %d", resp.StatusCode)
			return &ClickzettaError{
				Number:         -1,
				Message:        "failed to download file from presigned url",
				SQLState:       "",
				QueryID:        "",
				MessageArgs:    make([]interface{}, 0),
				IncludeQueryID: false,
			}
		}

		readerStart := diagnostics.trace.start()
		reader, err := ipc.NewReader(resp.Body)
		if err != nil {
			if diagnostics.trace.enabled {
				diagnostics.trace.record(qd.JobId, "file_ipc_reader_error", readerStart, "file_index", fileIndex, "error", err)
			}
			diagnostics.errorf("error: %v", err)
			return err
		}
		if diagnostics.trace.enabled {
			diagnostics.trace.record(qd.JobId, "file_ipc_reader", readerStart, "file_index", fileIndex)
		}
		err = arrowToRows(diagnostics, qd, reader)
		if err != nil {
			diagnostics.errorf("error: %v", err)
			return err
		}

		qd.RowCount = qd.RowCount + int64(len(qd.Data))
	}
	return nil
}

// GetFileURLs returns the list of presigned URLs for lazy loading
func (qd *execResponseData) GetFileURLs() []string {
	if qd.DataType == File {
		return qd.FileList
	}
	return nil
}

// GetMemoryDataChunks returns the list of base64 encoded data chunks for lazy loading
func (qd *execResponseData) GetMemoryDataChunks() []string {
	if qd.DataType == Memory {
		return qd.HTTPResponseMessage.HttpResponseMessageResultSet.MemoryData.Data
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
