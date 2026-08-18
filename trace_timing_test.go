package goclickzetta

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/valyala/fastjson"
)

func enableTimingTraceForTest(t *testing.T) *bytes.Buffer {
	t.Helper()

	previousLogger := logger
	previousEnvironmentFlag := traceTimingEnvironmentEnabled
	buffer := &bytes.Buffer{}
	testLogger := CreateDefaultLogger()
	testLogger.SetOutput(buffer)
	if err := testLogger.SetLogLevel("debug"); err != nil {
		t.Fatalf("set debug log level: %v", err)
	}
	logger = testLogger
	traceTimingEnvironmentEnabled = true

	t.Cleanup(func() {
		logger = previousLogger
		traceTimingEnvironmentEnabled = previousEnvironmentFlag
	})
	return buffer
}

func TestTimingTraceDoesNotLogResponsePayload(t *testing.T) {
	logs := enableTimingTraceForTest(t)
	response := []byte(`{
		"status":{"state":"SUCCEED"},
		"resultSet":{
			"location":{"stsAkSecret":"sensitive-secret","stsToken":"sensitive-token"},
			"data":{"data":["sensitive-result-data"]}
		}
	}`)
	conn := newTestConn(&unlimitedMockClient{body: response})

	if _, err := conn.ExecContext(context.Background(), "select 'sensitive-query'", nil); err != nil {
		t.Fatalf("ExecContext: %v", err)
	}

	output := logs.String()
	if !strings.Contains(output, "phase=submit_job") {
		t.Fatalf("expected submit timing log, got %q", output)
	}
	for _, sensitiveValue := range []string{"sensitive-secret", "sensitive-token", "sensitive-result-data", "sensitive-query"} {
		if strings.Contains(output, sensitiveValue) {
			t.Fatalf("timing log leaked %q: %s", sensitiveValue, output)
		}
	}
}

func TestTimingTraceRecordsFailureTotalsWithoutPayload(t *testing.T) {
	logs := enableTimingTraceForTest(t)
	response := []byte(`{
		"status":{"state":"FAILED","errorCode":"TEST_FAILURE","message":"safe failure"},
		"resultSet":{"location":{"stsAkSecret":"sensitive-secret"}}
	}`)
	conn := newTestConn(&unlimitedMockClient{body: response})

	if _, err := conn.ExecContext(context.Background(), "select 1", nil); err == nil {
		t.Fatal("expected ExecContext to fail")
	}

	output := logs.String()
	if !strings.Contains(output, "phase=exec_internal_total") {
		t.Fatalf("expected exec_internal_total on failure, got %q", output)
	}
	if strings.Contains(output, "sensitive-secret") || strings.Contains(output, string(response)) {
		t.Fatalf("failure log leaked raw response: %s", output)
	}
}

func TestTraceTimingConfigurationPrecedence(t *testing.T) {
	previousEnvironmentFlag := traceTimingEnvironmentEnabled
	traceTimingEnvironmentEnabled = false
	t.Cleanup(func() {
		traceTimingEnvironmentEnabled = previousEnvironmentFlag
	})

	dsnEnabled := "true"
	conn := newTestConn(nil)
	conn.cfg.Params = map[string]*string{traceTimingFlag: &dsnEnabled}
	if !conn.traceTimingEnabled(context.Background()) {
		t.Fatal("expected DSN trace_timing=true to enable timing")
	}

	ctx := WithDriverFlags(context.Background(), DriverFlags{traceTimingFlag: "false"})
	if conn.traceTimingEnabled(ctx) {
		t.Fatal("expected context trace_timing=false to override DSN setting")
	}

	dsnDisabled := "false"
	conn.cfg.Params[traceTimingFlag] = &dsnDisabled
	ctx = WithDriverFlags(context.Background(), DriverFlags{traceTimingFlag: "true"})
	if !conn.traceTimingEnabled(ctx) {
		t.Fatal("expected context trace_timing=true to override DSN setting")
	}
}

func TestFormatJobFailureOmitsResponsePayload(t *testing.T) {
	response, err := fastjson.Parse(`{
		"status":{"errorCode":"CZLH-TEST","message":"failed\nreason"},
		"resultSet":{"location":{"stsAkSecret":"sensitive-secret"}}
	}`)
	if err != nil {
		t.Fatal(err)
	}

	message := formatJobFailure("job-1", response)
	if !strings.Contains(message, "CZLH-TEST") || !strings.Contains(message, "failed reason") {
		t.Fatalf("unexpected failure message: %s", message)
	}
	if strings.Contains(message, "sensitive-secret") {
		t.Fatalf("failure message leaked response payload: %s", message)
	}
}
