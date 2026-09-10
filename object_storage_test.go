package goclickzetta

import (
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
)

// The result set arrives one of two ways: inline base64 CSV for a small result,
// or a list of object storage files for anything large. Only the inline path had
// coverage, which left the entire download path - the one every real query over
// a few tens of thousands of rows takes - untested. These tests cover the
// dispatch and the bookkeeping around it; the download itself needs a live
// bucket and is covered by TestObjectStorageResultSetStreamsEveryRow.

// newResponseData builds an execResponseData from a response body, going through
// JSON on purpose so the field tags are exercised the same way a real response
// exercises them.
func newResponseData(t *testing.T, body string) *execResponseData {
	t.Helper()
	qd := &execResponseData{}
	if err := json.Unmarshal([]byte(body), &qd.HTTPResponseMessage); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	return qd
}

func TestExecResponseDataInitSelectsMemoryPath(t *testing.T) {
	rows := base64.StdEncoding.EncodeToString([]byte("1,alpha\n2,beta\n"))
	qd := newResponseData(t, `{"resultSet":{
		"metadata":{"fields":[
			{"name":"id","type":{"category":"BIGINT"}},
			{"name":"name","type":{"category":"STRING"}}]},
		"data":{"data":["`+rows+`"]}}}`)

	if err := qd.init(); err != nil {
		t.Fatalf("init() error = %v", err)
	}
	if qd.DataType != Memory {
		t.Errorf("DataType = %v, want Memory", qd.DataType)
	}
	if qd.MemoryRead {
		t.Error("MemoryRead = true before the first read")
	}
	if err := qd.read(); err != nil {
		t.Fatalf("read() error = %v", err)
	}
	if qd.RowCount != 2 || len(qd.Data) != 2 {
		t.Fatalf("read() got RowCount=%d rows=%d, want 2 and 2", qd.RowCount, len(qd.Data))
	}
	first, ok := qd.Data[0].([]interface{})
	if !ok {
		t.Fatalf("row 0 is %T, want []interface{}", qd.Data[0])
	}
	if first[0] != int64(1) || first[1] != "alpha" {
		t.Errorf("row 0 = %#v, want [1 alpha]", first)
	}
}

// TestExecResponseDataReadMemoryIsSingleShot pins the contract Rows.Next relies
// on: the inline batch is handed over once, and the second call reports an empty
// batch so iteration terminates instead of replaying the same rows forever.
func TestExecResponseDataReadMemoryIsSingleShot(t *testing.T) {
	rows := base64.StdEncoding.EncodeToString([]byte("7\n"))
	qd := newResponseData(t, `{"resultSet":{
		"metadata":{"fields":[{"name":"id","type":{"category":"BIGINT"}}]},
		"data":{"data":["`+rows+`"]}}}`)
	if err := qd.init(); err != nil {
		t.Fatalf("init() error = %v", err)
	}
	if err := qd.read(); err != nil {
		t.Fatalf("first read() error = %v", err)
	}
	if len(qd.Data) != 1 {
		t.Fatalf("first read() rows = %d, want 1", len(qd.Data))
	}
	if err := qd.read(); err != nil {
		t.Fatalf("second read() error = %v", err)
	}
	if len(qd.Data) != 0 {
		t.Errorf("second read() rows = %d, want 0", len(qd.Data))
	}
}

func TestExecResponseDataInitBuildsOSSBucket(t *testing.T) {
	qd := newResponseData(t, `{"resultSet":{
		"metadata":{"fields":[{"name":"id","type":{"category":"BIGINT"}}]},
		"location":{
			"location":["oss://result-bucket/jobs/j-1/part-0.csv","oss://result-bucket/jobs/j-1/part-1.csv"],
			"fileSystem":"OSS",
			"stsAkId":"ak","stsAkSecret":"sk","stsToken":"token",
			"ossEndpoint":"oss-cn-hangzhou.aliyuncs.com"}}}`)

	if err := qd.init(); err != nil {
		t.Fatalf("init() error = %v", err)
	}
	if qd.DataType != File {
		t.Errorf("DataType = %v, want File", qd.DataType)
	}
	if qd.ObjectStorageType != OSS {
		t.Errorf("ObjectStorageType = %v, want OSS", qd.ObjectStorageType)
	}
	if len(qd.FileList) != 2 {
		t.Fatalf("FileList = %v, want 2 entries", qd.FileList)
	}
	if qd.OSSBucket == nil {
		t.Fatal("OSSBucket = nil, want a bucket client")
	}
	// The bucket name is the third path segment of the first file, so a
	// different URL layout would silently address the wrong bucket.
	if qd.OSSBucket.BucketName != "result-bucket" {
		t.Errorf("BucketName = %q, want %q", qd.OSSBucket.BucketName, "result-bucket")
	}
}

func TestExecResponseDataInitBuildsCOSClient(t *testing.T) {
	qd := newResponseData(t, `{"resultSet":{
		"metadata":{"fields":[{"name":"id","type":{"category":"BIGINT"}}]},
		"location":{
			"location":["cos://result-bucket-1250000000/jobs/j-1/part-0.csv"],
			"fileSystem":"COS",
			"stsAkId":"ak","stsAkSecret":"sk","stsToken":"token",
			"objectStorageRegion":"ap-shanghai"}}}`)

	if err := qd.init(); err != nil {
		t.Fatalf("init() error = %v", err)
	}
	if qd.ObjectStorageType != COS {
		t.Errorf("ObjectStorageType = %v, want COS", qd.ObjectStorageType)
	}
	if qd.COSClient == nil {
		t.Fatal("COSClient = nil, want a client")
	}
	wantHost := "result-bucket-1250000000.cos.ap-shanghai.myqcloud.com"
	if got := qd.COSClient.BaseURL.BucketURL.Host; got != wantHost {
		t.Errorf("BucketURL.Host = %q, want %q", got, wantHost)
	}
}

func TestExecResponseDataInitRejectsUnknownFileSystem(t *testing.T) {
	qd := newResponseData(t, `{"resultSet":{
		"metadata":{"fields":[{"name":"id","type":{"category":"BIGINT"}}]},
		"location":{"location":["s3://b/f.csv"],"fileSystem":"S3"}}}`)

	err := qd.init()
	if err == nil {
		t.Fatal("init() error = nil, want an unsupported storage error")
	}
	if !strings.Contains(err.Error(), "object storage type is not supported") {
		t.Errorf("init() error = %q, want it to name the unsupported storage", err)
	}
}

// TestExecResponseDataReadFileRejectsEmptyList covers the response shape where
// the server announces File mode but sends no files: without the guard the read
// would index into an empty slice.
func TestExecResponseDataReadFileRejectsEmptyList(t *testing.T) {
	qd := &execResponseData{DataType: File}
	err := qd.read()
	if err == nil {
		t.Fatal("read() error = nil, want an empty file list error")
	}
	if !strings.Contains(err.Error(), "object storage file list is empty") {
		t.Errorf("read() error = %q, want it to name the empty file list", err)
	}
}

// TestExecResponseDataReadFileStopsAfterLastFile pins the loop that walks the
// file list: once the index passes the end, read hands back an empty batch so
// Rows.Next stops instead of re-downloading or panicking.
func TestExecResponseDataReadFileStopsAfterLastFile(t *testing.T) {
	qd := &execResponseData{
		DataType:         File,
		FileList:         []string{"oss://b/dir/part-0.csv"},
		CurrentFileIndex: 1,
		RowCount:         42,
	}
	if err := qd.read(); err != nil {
		t.Fatalf("read() error = %v", err)
	}
	if len(qd.Data) != 0 {
		t.Errorf("read() rows = %d, want 0", len(qd.Data))
	}
	if qd.RowCount != 42 {
		t.Errorf("RowCount = %d, want it left at 42", qd.RowCount)
	}
}

// TestObjectStorageResultSetStreamsEveryRow runs a result set large enough that
// the server hands back object storage files instead of inline data, then checks
// every row survives the download and the CSV parse. A row lost at a chunk
// boundary, or a batch replayed because the file index did not advance, both
// show up here and nowhere else: every other test in the package stays under the
// inline threshold.
func TestObjectStorageResultSetStreamsEveryRow(t *testing.T) {
	// The server picks inline or object storage by result size, not row count:
	// a million bare integers still arrive inline, so each row carries padding
	// to push the result over the threshold.
	const (
		outer = 200
		inner = 1000
		total = outer * inner
	)
	query := fmt.Sprintf(
		"SELECT cast(a.i * %d + b.i AS bigint) AS id, repeat('x', 50) AS pad "+
			"FROM (SELECT explode(sequence(1, %d)) AS i) a "+
			"CROSS JOIN (SELECT explode(sequence(1, %d)) AS i) b",
		inner, outer, inner)

	cfg := integrationConfig(t)
	c, err := ClickzettaDriver{}.OpenWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("OpenWithConfig() error = %v", err)
	}
	conn := c.(*ClickzettaConn)
	defer conn.Close()

	rows, err := conn.QueryContext(context.Background(), query, nil)
	if err != nil {
		t.Fatalf("QueryContext() error = %v", err)
	}
	defer rows.Close()

	czRows, ok := rows.(*clickzettaRows)
	if !ok {
		t.Fatalf("rows is %T, want *clickzettaRows", rows)
	}
	// If the server ever raises the inline threshold past this result size the
	// test would keep passing while covering nothing, so say so instead.
	if czRows.response.Data.DataType != File {
		t.Fatalf("%d rows came back inline; raise the row count so the result goes through object storage", total)
	}
	if fs := czRows.response.Data.HTTPResponseMessage.HttpResponseMessageResultSet.ObjectStorageLocation.FileSystem; fs != "OSS" && fs != "COS" {
		t.Fatalf("fileSystem = %q, want OSS or COS", fs)
	}

	var (
		count    int64
		sum      int64
		min, max int64 = math.MaxInt64, math.MinInt64
	)
	dest := make([]driver.Value, 2)
	for {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next() error after %d rows: %v", count, err)
		}
		id, ok := dest[0].(int64)
		if !ok {
			t.Fatalf("row %d id is %T (%v), want int64", count, dest[0], dest[0])
		}
		if pad, ok := dest[1].(string); !ok || len(pad) != 50 {
			t.Fatalf("row %d pad is %T %q, want a 50 character string", count, dest[1], dest[1])
		}
		count++
		sum += id
		if id < min {
			min = id
		}
		if id > max {
			max = id
		}
	}

	var wantSum int64
	for a := int64(1); a <= outer; a++ {
		for b := int64(1); b <= inner; b++ {
			wantSum += a*inner + b
		}
	}
	if count != total {
		t.Errorf("read %d rows, want %d", count, total)
	}
	if sum != wantSum {
		t.Errorf("id sum = %d, want %d; rows were dropped, duplicated or misparsed", sum, wantSum)
	}
	if min != 1*inner+1 || max != outer*inner+inner {
		t.Errorf("id range = [%d,%d], want [%d,%d]", min, max, 1*inner+1, outer*inner+inner)
	}
	if czRows.response.Data.RowCount != count {
		t.Errorf("RowCount = %d, want %d", czRows.response.Data.RowCount, count)
	}
}
