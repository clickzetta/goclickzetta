package goclickzetta

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/url"
	"reflect"
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

func hasField(obj interface{}, field string) bool {
	t := reflect.TypeOf(obj)
	_, ok := t.FieldByName(field)
	return ok
}

func (qd *execResponseData) init() error {
	qd.parseSchema()
	if len(qd.HTTPResponseMessage.HttpResponseMessageResultSet.MemoryData.Data) != 0 {
		qd.DataType = Memory
		qd.MemoryRead = false
	} else if len(qd.HTTPResponseMessage.HttpResponseMessageResultSet.ObjectStorageLocation.Location) != 0 {
		qd.DataType = File
		loc := qd.HTTPResponseMessage.HttpResponseMessageResultSet.ObjectStorageLocation
		if loc.FileSystem == "OSS" {
			qd.ObjectStorageType = OSS
			qd.FileList = make([]string, 0)
			for _, file := range loc.LocationFiles {
				qd.FileList = append(qd.FileList, file.FilePath)
			}
			option := oss.ClientOption(oss.SecurityToken(loc.Token))
			client, err := oss.New(loc.OSSEndpoint, loc.ID, loc.Secret, option)
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
			bucketName := strings.SplitN(qd.FileList[0], "/", 4)[2]

			bucket, err := client.Bucket(bucketName)
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
			qd.OSSBucket = bucket
		} else if loc.FileSystem == "COS" {
			qd.ObjectStorageType = COS
			qd.FileList = make([]string, 0)
			for _, file := range loc.LocationFiles {
				qd.FileList = append(qd.FileList, file.FilePath)
			}
			fileInfo := strings.SplitN(loc.Location[0], "/", 4)
			cosUrl := "https://" + fileInfo[2] + ".cos." + loc.COSObjectStorageRegion + ".myqcloud.com"
			url, err := url.Parse(cosUrl)
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
			client := cos.NewClient(&cos.BaseURL{BucketURL: url}, &http.Client{
				Transport: &cos.AuthorizationTransport{
					SecretID:     loc.ID,
					SecretKey:    loc.Secret,
					SessionToken: loc.Token,
				},
			})
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
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

func (qd *execResponseData) parseSchema() {
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
				logger.WithContext(nil).Errorf("error: %v", err)
				return
			}
			scale, err := strconv.ParseInt(field.FieldType.DecimalTypeInfo.Scale, 10, 64)
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
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

func (qd *execResponseData) readMemoryData() error {
	qd.Data = make([]interface{}, 0)
	for _, data := range qd.HTTPResponseMessage.HttpResponseMessageResultSet.MemoryData.Data {
		buffer := base64.NewDecoder(base64.StdEncoding, strings.NewReader(data))
		reader, err := ipc.NewReader(buffer)
		if err != nil {
			logger.WithContext(nil).Errorf("error: %v", err)
			return err
		}
		err = arrowToRows(qd, reader)
		if err != nil {
			logger.WithContext(nil).Errorf("error: %v", err)
			return err
		}
	}
	return nil
}

func arrowToRows(qd *execResponseData, reader *ipc.Reader) error {
	tempDataList := make(map[int][]interface{}, 0)

	for reader.Next() {
		record := reader.Record()

		for index, column := range record.Columns() {
			if tempDataList[index] == nil {
				des := make([]interface{}, column.Len())
				timeLocation := time.Local
				err := arrowToValue(des, qd.Schema[index], column, timeLocation, false)
				if err != nil {
					logger.WithContext(nil).Errorf("error: %v", err)
					return err
				}
				tempDataList[index] = des

			} else {
				des := make([]interface{}, column.Len())
				timeLocation := time.Local
				err := arrowToValue(des, qd.Schema[index], column, timeLocation, false)
				if err != nil {
					logger.WithContext(nil).Errorf("error: %v", err)
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
	return nil
}

func (qd *execResponseData) read() error {
	if qd.DataType == Memory {
		if qd.MemoryRead {
			qd.Data = make([]interface{}, 0)
			return nil
		}
		qd.MemoryRead = true
		err := qd.readMemoryData()
		if err != nil {
			logger.WithContext(nil).Errorf("error: %v", err)
			return err
		}
		qd.RowCount = int64(len(qd.Data))
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
		fileName := qd.FileList[qd.CurrentFileIndex]
		fileInfo := strings.SplitN(fileName, "/", 4)
		qd.CurrentFileIndex++
		if qd.ObjectStorageType == OSS {
			stream, err := qd.OSSBucket.GetObject(fileInfo[3])
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
			reader, err := ipc.NewReader(stream)
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
			err = arrowToRows(qd, reader)
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}

		} else if qd.ObjectStorageType == COS {
			res, err := qd.COSClient.Object.Get(context.Background(), fileInfo[3], nil)
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
			reader, err := ipc.NewReader(res.Body)
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
			err = arrowToRows(qd, reader)
			if err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
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
