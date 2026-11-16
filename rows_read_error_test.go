package goclickzetta

import (
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// ossFileRows serves each object key from files through a fake OSS endpoint
// and returns rows that read them in order. A missing key or one mapped to "\x00" answers 500.
func ossFileRows(t *testing.T, keys []string, files map[string]string) (*clickzettaRows, *[]string) {
	t.Helper()
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/"), "bucket/")
		requested = append(requested, key)
		w.Header().Set("x-oss-request-id", "test-request")
		body, ok := files[key]
		if !ok || body == "\x00" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	client, err := oss.New(server.URL, "test-id", "test-secret", oss.UseCname(true))
	if err != nil {
		t.Fatalf("oss.New: %v", err)
	}
	bucket, err := client.Bucket("bucket")
	if err != nil {
		t.Fatalf("client.Bucket: %v", err)
	}
	fileList := make([]string, len(keys))
	for i, key := range keys {
		fileList[i] = "oss://bucket/" + key
	}
	rows := &clickzettaRows{
		cn: newTestConn(nil),
		response: &execResponse{Success: true, Data: execResponseData{
			DataType:          File,
			FileList:          fileList,
			ObjectStorageType: OSS,
			OSSBucket:         bucket,
			Schema:            []execResponseColumnType{{Name: "value", Type: "STRING"}},
		}},
	}
	return rows, &requested
}

func readAll(rows *clickzettaRows) ([]string, error) {
	var got []string
	dest := make([]driver.Value, 1)
	for {
		err := rows.Next(dest)
		if errors.Is(err, io.EOF) {
			return got, nil
		}
		if err != nil {
			return got, err
		}
		got = append(got, dest[0].(string))
	}
}

// Every result file must be returned exactly once. Before the fix,
// HasNextResultSet read the next file and NextResultSet then read the one
// after it over the top, so every other file after the first was dropped.
func TestRowsNextReadsEveryResultFile(t *testing.T) {
	rows, _ := ossFileRows(t, []string{"f1", "f2", "f3", "f4"}, map[string]string{
		"f1": "a\n", "f2": "b\n", "f3": "c\n", "f4": "d\n",
	})
	got, err := readAll(rows)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Join(got, ",") != "a,b,c,d" {
		t.Fatalf("rows = %v, want [a b c d]", got)
	}
}

// A result file that cannot be downloaded must surface as an error instead
// of ending the result set early.
func TestRowsNextPropagatesFileDownloadError(t *testing.T) {
	rows, _ := ossFileRows(t, []string{"f1", "f2"}, map[string]string{
		"f1": "a\n", "f2": "\x00",
	})
	got, err := readAll(rows)
	if err == nil {
		t.Fatalf("read returned %v with no error, want the download error", got)
	}
}

// An empty result file is not the end of the result set.
func TestRowsNextSkipsEmptyResultFile(t *testing.T) {
	rows, _ := ossFileRows(t, []string{"f1", "f2"}, map[string]string{
		"f1": "", "f2": "b\n",
	})
	got, err := readAll(rows)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Join(got, ",") != "b" {
		t.Fatalf("rows = %v, want [b]", got)
	}
}

// Malformed in-memory data must fail the query instead of returning no rows.
func TestRowsNextPropagatesMemoryDecodeError(t *testing.T) {
	rows := &clickzettaRows{
		cn: newTestConn(nil),
		response: &execResponse{Success: true, Data: execResponseData{
			DataType: Memory,
			Schema:   []execResponseColumnType{{Name: "value", Type: "STRING"}},
			HTTPResponseMessage: httpResponseMessage{
				HttpResponseMessageResultSet: httpResponseMessageResultSet{
					MemoryData: memoryData{Data: []string{base64.StdEncoding.EncodeToString([]byte("\"unterminated"))[:5] + "!!!"}},
				},
			},
		}},
	}
	err := rows.Next(make([]driver.Value, 1))
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("Rows.Next() error = %v, want a decode error", err)
	}
}
