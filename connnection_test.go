package goclickzetta

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestBuildConnection(t *testing.T) {
	ctx := context.TODO()
	cfg := integrationConfig(t)
	conn, err := buildClickzettaConn(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if conn.cfg.Token == "" {
		t.Fatal("token is empty")
	}
}

func TestConnectionQuery(t *testing.T) {
	ctx := context.TODO()
	cfg := integrationConfig(t)
	conn, err := buildClickzettaConn(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	data, err := conn.Query("select 1+1;", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	result := make([]driver.Value, 1)
	for {
		err = data.Next(result)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(result)
	}
}

func TestConnectionExec(t *testing.T) {
	ctx := context.TODO()
	cfg := integrationConfig(t)
	conn, err := buildClickzettaConn(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if conn.cfg.Token == "" {
		t.Error("token is empty")
	}
	tableName := fmt.Sprintf("goclickzetta_conn_it_%d", time.Now().UnixNano())
	ddl := fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY, first_name STRING, last_name STRING, email STRING, phone STRING, address STRING)", tableName)
	t.Cleanup(func() {
		if _, cleanupErr := conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName), nil); cleanupErr != nil {
			t.Errorf("drop connection integration table: %v", cleanupErr)
		}
	})
	result, err := conn.Exec(ddl, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, ok := result.(ClickzettaResult)
	if !ok {
		t.Fatal("result is not ClickzettaResult")
	}
	if res.GetStatus() != queryStatus(QueryStatusComplete) {
		fmt.Println(res.GetError().Error())
		t.Fatal("query status is failed")
	}
	assert.Equal(t, res.GetStatus(), queryStatus(QueryStatusComplete))
	del := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	result, err = conn.Exec(del, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, ok = result.(ClickzettaResult)
	if !ok {
		t.Fatal("result is not ClickzettaResult")
	}
	if res.GetStatus() != queryStatus(QueryStatusComplete) {
		fmt.Println(res.GetError().Error())
		t.Fatal("query status is failed")
	}
}

func TestConnectionPing(t *testing.T) {
	ctx := context.TODO()
	cfg := integrationConfig(t)
	conn, err := buildClickzettaConn(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	err = conn.Ping(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMergeDriverFlagHints(t *testing.T) {
	// flags 与语句内联 hint 都可能带 escape mode 这个键,最终发给服务端的必须是
	// 驱动实际编码用的规范名,否则两边对字面量的理解会不一致。
	hints := map[string]interface{}{
		stringLiteralEscapeModeHint: "2",
		"cz.sql.adhoc.result.type":  "embedded",
	}
	flags := DriverFlags{
		stringLiteralEscapeModeHint: "2",
		"workspace":                 "ws",
		"virtualCluster":            "vc",
		"schema":                    "sc",
		"catalog":                   "cat",
		traceTimingFlag:             "true",
		"cz.sql.custom":             "value",
	}
	mergeDriverFlagHints(hints, flags, escapeQuote)

	if got := hints[stringLiteralEscapeModeHint]; got != string(escapeQuote) {
		t.Errorf("hints[%s] = %v, want %q", stringLiteralEscapeModeHint, got, string(escapeQuote))
	}
	if got := hints["cz.sql.custom"]; got != "value" {
		t.Errorf("hints[cz.sql.custom] = %v, want %q", got, "value")
	}
	if got := hints["cz.sql.adhoc.result.type"]; got != "embedded" {
		t.Errorf("hints[cz.sql.adhoc.result.type] = %v, want %q", got, "embedded")
	}
	// 结构化请求参数不应作为 hint 下发。
	if _, ok := hints[traceTimingFlag]; ok {
		t.Errorf("hints[%s] present, want it handled as a request parameter", traceTimingFlag)
	}
}
