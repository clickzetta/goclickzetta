package goclickzetta

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/tencentyun/cos-go-sdk-v5"
)

func TestRowsNextPropagatesReadError(t *testing.T) {
	rows := &clickzettaRows{
		cn: newTestConn(nil),
		response: &execResponse{Data: execResponseData{
			DataType:   File,
			FileList:   []string{"invalid-location"},
			MemoryRead: true,
		}},
	}

	err := rows.Next(make([]driver.Value, 1))
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("Rows.Next() error = %v, want non-EOF read error", err)
	}
}

func TestRowsNextSkipsEmptyObjectStorageFile(t *testing.T) {
	emptyPayload := makeIPCStringPayload(t, nil)
	dataPayload := makeIPCStringPayload(t, []string{"row-from-second-file"})
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestCount++
		response.Header().Set("x-oss-request-id", "test-request")
		if requestCount == 1 {
			_, _ = response.Write(emptyPayload)
			return
		}
		_, _ = response.Write(dataPayload)
	}))
	defer server.Close()

	client, err := oss.New(server.URL, "test-id", "test-secret", oss.UseCname(true))
	if err != nil {
		t.Fatalf("oss.New: %v", err)
	}
	bucket, err := client.Bucket("bucket")
	if err != nil {
		t.Fatalf("client.Bucket: %v", err)
	}
	rows := &clickzettaRows{
		cn: newTestConn(nil),
		response: &execResponse{Data: execResponseData{
			DataType:          File,
			FileList:          []string{"oss://bucket/empty", "oss://bucket/data"},
			ObjectStorageType: OSS,
			OSSBucket:         bucket,
			Schema:            []execResponseColumnType{{Name: "value", Type: "STRING"}},
		}},
	}
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Rows.Next: %v", err)
	}
	if got := dest[0]; got != "row-from-second-file" {
		t.Fatalf("Rows.Next value = %v, want row from second file", got)
	}
	if requestCount != 2 {
		t.Fatalf("object requests = %d, want 2", requestCount)
	}
}

func TestArrowToRowsReturnsReaderError(t *testing.T) {
	payload := makeIPCStringPayload(t, []string{"value"})
	for trimBytes := 9; trimBytes < len(payload); trimBytes++ {
		reader, err := ipc.NewReader(bytes.NewReader(payload[:len(payload)-trimBytes]))
		if err != nil {
			continue
		}
		err = arrowToRows(
			queryDiagnostics{},
			&execResponseData{Schema: []execResponseColumnType{{Name: "value", Type: "STRING"}}},
			reader,
		)
		reader.Release()
		if err != nil {
			return
		}
	}
	t.Fatal("could not produce a readable schema followed by a truncated record")
}

func TestParseSchemaReturnsDecimalError(t *testing.T) {
	data := execResponseData{HTTPResponseMessage: httpResponseMessage{
		HttpResponseMessageResultSet: httpResponseMessageResultSet{MetaData: metaData{Fields: []field{{
			Name: "amount",
			FieldType: fieldType{
				Category:        "DECIMAL",
				DecimalTypeInfo: decimalTypeInfo{Precision: "invalid", Scale: "2"},
			},
		}}}},
	}}

	if err := data.init(queryDiagnostics{}); err == nil {
		t.Fatal("init() error = nil, want invalid decimal schema error")
	}
	if data.Schema != nil {
		t.Fatalf("Schema = %#v, want nil after parse failure", data.Schema)
	}
}

func TestParseSchemaUsesMatchingTypeInfo(t *testing.T) {
	data := execResponseData{HTTPResponseMessage: httpResponseMessage{
		HttpResponseMessageResultSet: httpResponseMessageResultSet{MetaData: metaData{Fields: []field{
			{
				Name: "amount",
				FieldType: fieldType{
					Category:        "DECIMAL",
					DecimalTypeInfo: decimalTypeInfo{Precision: "18", Scale: "2"},
				},
			},
			{
				Name: "event_time",
				FieldType: fieldType{
					Category:      "TIMESTAMP_NTZ",
					TimestampInfo: timestampInfo{TsUnit: "MICROSECOND"},
				},
			},
		}}},
	}}

	if err := data.parseSchema(); err != nil {
		t.Fatalf("parseSchema: %v", err)
	}
	if got := data.Schema[0]; got.Precision != 18 || got.Scale != 2 {
		t.Fatalf("decimal schema = %#v, want precision=18 scale=2", got)
	}
	if got := data.Schema[1]; got.TsUnit != "MICROSECOND" {
		t.Fatalf("timestamp schema = %#v, want TsUnit=MICROSECOND", got)
	}
}

func TestValidateObjectStorageLocationsRejectsIncompletePath(t *testing.T) {
	if _, err := validateObjectStorageLocations([]string{"oss://bucket"}); err == nil {
		t.Fatal("validateObjectStorageLocations() error = nil, want incomplete path error")
	}
	if _, err := validateObjectStorageLocations([]string{"oss://bucket/one", "oss://other/two"}); err == nil {
		t.Fatal("validateObjectStorageLocations() error = nil, want mixed bucket error")
	}
}

func TestObjectStorageErrorSummaryKeepsSafeDiagnostics(t *testing.T) {
	ossSummary := summarizeObjectStorageError(oss.ServiceError{
		StatusCode: 403,
		Code:       "AccessDenied",
		Message:    "sensitive-message",
		RequestID:  "oss-request-id",
		Endpoint:   "https://example.invalid/?token=sensitive-token",
	})
	assertSafeStorageSummary(t, ossSummary.String(), []string{"status_code=403", "error_code=AccessDenied", "request_id=oss-request-id"})

	requestURL, err := url.Parse("https://example.invalid/object?token=sensitive-token")
	if err != nil {
		t.Fatal(err)
	}
	cosSummary := summarizeObjectStorageError(&cos.ErrorResponse{
		Code:      "NoSuchKey",
		Message:   "sensitive-message",
		RequestID: "cos-request-id",
		TraceID:   "cos-trace-id",
		Response: &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     make(http.Header),
			Request:    &http.Request{Method: http.MethodGet, URL: requestURL},
		},
	})
	assertSafeStorageSummary(t, cosSummary.String(), []string{"status_code=404", "error_code=NoSuchKey", "request_id=cos-request-id", "trace_id=cos-trace-id"})
}

func assertSafeStorageSummary(t *testing.T, summary string, expected []string) {
	t.Helper()
	for _, value := range expected {
		if !strings.Contains(summary, value) {
			t.Fatalf("summary %q omitted %q", summary, value)
		}
	}
	for _, sensitive := range []string{"sensitive-message", "sensitive-token", "example.invalid"} {
		if strings.Contains(summary, sensitive) {
			t.Fatalf("summary leaked %q: %s", sensitive, summary)
		}
	}
}

func makeIPCStringPayload(t *testing.T, values []string) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	buffer := &bytes.Buffer{}
	writer := ipc.NewWriter(buffer, ipc.WithSchema(schema))
	if values != nil {
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		builder.AppendValues(values, nil)
		column := builder.NewArray()
		builder.Release()
		record := array.NewRecord(schema, []arrow.Array{column}, int64(len(values)))
		if err := writer.Write(record); err != nil {
			record.Release()
			column.Release()
			t.Fatalf("writer.Write: %v", err)
		}
		record.Release()
		column.Release()
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}
	return append([]byte(nil), buffer.Bytes()...)
}
