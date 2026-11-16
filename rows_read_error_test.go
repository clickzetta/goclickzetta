package goclickzetta

import (
	"bytes"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// makeIPCStringPayload encodes values as an Arrow IPC stream with one STRING
// column. nil values produce a stream with a schema and no record batches.
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

func fileRows(t *testing.T, urls []string) *clickzettaRows {
	t.Helper()
	return &clickzettaRows{
		cn: newTestConn(nil),
		response: &execResponse{Success: true, Data: execResponseData{
			DataType: File,
			FileList: urls,
			Schema:   []execResponseColumnType{{Name: "value", Type: "STRING"}},
		}},
	}
}

func wantReadError(t *testing.T, err error) {
	t.Helper()
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("Rows.Next() error = %v, want a read error (not EOF)", err)
	}
}

// A result file that cannot be downloaded must surface as an error. Before
// the fix Next returned io.EOF, so database/sql reported a clean end of the
// result set and the caller silently lost the rows in that file.
func TestRowsNextPropagatesFileDownloadError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	rows := fileRows(t, []string{server.URL + "/expired"})
	wantReadError(t, rows.Next(make([]driver.Value, 1)))
}

// A failure on a later file must not be hidden behind the rows already read.
func TestRowsNextPropagatesErrorAfterFirstFile(t *testing.T) {
	first := makeIPCStringPayload(t, []string{"a"})
	mux := http.NewServeMux()
	mux.HandleFunc("/1", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(first) })
	mux.HandleFunc("/2", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
	server := httptest.NewServer(mux)
	defer server.Close()

	rows := fileRows(t, []string{server.URL + "/1", server.URL + "/2"})
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("first Rows.Next: %v", err)
	}
	if dest[0] != "a" {
		t.Fatalf("first row = %v, want a", dest[0])
	}
	wantReadError(t, rows.Next(dest))
}

// An empty result file is not the end of the result set; the rows in the
// files after it must still be returned.
func TestRowsNextSkipsEmptyResultFile(t *testing.T) {
	empty := makeIPCStringPayload(t, nil)
	data := makeIPCStringPayload(t, []string{"row-from-second-file"})
	mux := http.NewServeMux()
	mux.HandleFunc("/1", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(empty) })
	mux.HandleFunc("/2", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) })
	server := httptest.NewServer(mux)
	defer server.Close()

	rows := fileRows(t, []string{server.URL + "/1", server.URL + "/2"})
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Rows.Next: %v", err)
	}
	if dest[0] != "row-from-second-file" {
		t.Fatalf("Rows.Next value = %v, want row from second file", dest[0])
	}
	if err := rows.Next(dest); !errors.Is(err, io.EOF) {
		t.Fatalf("second Rows.Next = %v, want io.EOF", err)
	}
}

// A column the driver cannot decode must fail the query instead of returning
// an empty result set.
func TestRowsNextPropagatesDecodeError(t *testing.T) {
	payload := base64.StdEncoding.EncodeToString(makeIPCStringPayload(t, []string{"x"}))
	rows := &clickzettaRows{
		cn: newTestConn(nil),
		response: &execResponse{Success: true, Data: execResponseData{
			DataType: Memory,
			Schema:   []execResponseColumnType{{Name: "value", Type: "SOMETHING_NEW"}},
			HTTPResponseMessage: httpResponseMessage{
				HttpResponseMessageResultSet: httpResponseMessageResultSet{
					MemoryData: memoryData{Data: []string{payload}},
				},
			},
		}},
	}
	wantReadError(t, rows.Next(make([]driver.Value, 1)))
}

// A truncated IPC stream must be reported; reader.Next() returning false is
// not proof that every record batch was read.
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
	t.Fatal("arrowToRows never reported a truncated record batch")
}
