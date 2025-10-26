package goclickzetta

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
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
		qd.FileList = make([]string, 0)
		for _, file := range loc.Location {
			qd.FileList = append(qd.FileList, file)
		}
		if loc.FileSystem == "OSS" {
			qd.ObjectStorageType = OSS
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
		if err := textToRows(qd, buffer); err != nil {
			logger.WithContext(nil).Errorf("error: %v", err)
			return err
		}
	}
	return nil
}

// textToRows parses CSV-like rows (comma separated) from r.
// It appends parsed rows into qd.Data as [][]interface{}.
func textToRows(qd *execResponseData, r io.Reader) error {
	cr := csv.NewReader(r)
	cr.Comma = ','
	cr.LazyQuotes = true
	cr.FieldsPerRecord = -1 // allow variable columns
	limit := len(qd.Schema)
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// build row with schema-based conversion and padding
		row := make([]interface{}, limit)
		for i := 0; i < limit; i++ {
			var cell string
			if i < len(rec) {
				cell = rec[i]
			} else {
				cell = ""
			}
			if cell == "\\N" || cell == "" {
				row[i] = nil
				continue
			}
			v, convErr := convertTextValue(qd.Schema[i], cell)
			if convErr != nil {
				row[i] = cell
			} else {
				row[i] = v
			}
		}
		qd.Data = append(qd.Data, row)
	}
	return nil
}

// removed custom splitter; csv.Reader handles RFC4180 quoting/escaping

// convertTextValue converts a string cell to a typed value according to schema
func convertTextValue(col execResponseColumnType, s string) (interface{}, error) {
	t := strings.ToUpper(col.Type)
	switch t {
	case "BOOLEAN":
		if s == "" {
			return nil, nil
		}
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, err
		}
		return b, nil
	case "TINYINT", "INT8":
		if s == "" {
			return nil, nil
		}
		n, err := strconv.ParseInt(s, 10, 8)
		if err != nil {
			return nil, err
		}
		return int8(n), nil
	case "SMALLINT", "INT16":
		if s == "" {
			return nil, nil
		}
		n, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return nil, err
		}
		return int16(n), nil
	case "INT", "INT32":
		if s == "" {
			return nil, nil
		}
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(n), nil
	case "BIGINT", "INT64":
		if s == "" {
			return nil, nil
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	case "FLOAT", "FLOAT32":
		if s == "" {
			return nil, nil
		}
		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, err
		}
		return float32(f), nil
	case "DOUBLE", "FLOAT64":
		if s == "" {
			return nil, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	case "STRING", "VARCHAR", "CHAR":
		return s, nil
	case "JSON", "ARRAY", "MAP", "STRUCT":
		if len(s) > 0 && (s[0] == '[' || s[0] == '{') {
			var v interface{}
			if err := json.Unmarshal([]byte(s), &v); err == nil {
				return v, nil
			}
		}
		return s, nil
	case "DATE":
		if s == "" {
			return nil, nil
		}
		// assume yyyy-mm-dd
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return nil, err
		}
		return t.UTC(), nil
	case "TIMESTAMP_LTZ":
		if s == "" {
			return nil, nil
		}
		// try common layouts
		layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"}
		var parsed time.Time
		var perr error
		for _, layout := range layouts {
			if tt, e := time.Parse(layout, s); e == nil {
				parsed = tt
				perr = nil
				break
			} else {
				perr = e
			}
		}
		if perr != nil {
			return nil, perr
		}
		return parsed.UTC(), nil
	default:
		return s, nil
	}
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
			if err := textToRows(qd, stream); err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
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
			if err := textToRows(qd, res.Body); err != nil {
				logger.WithContext(nil).Errorf("error: %v", err)
				return err
			}
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
