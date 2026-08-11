package goclickzetta

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
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
			"location":{
				"location":["oss://bucket/path"],
				"stsAkId":"sensitive-ak",
				"stsAkSecret":"sensitive-secret",
				"stsToken":"sensitive-token"
			},
			"data":{"data":["sensitive-arrow-data"]}
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
	for _, sensitiveValue := range []string{
		"sensitive-ak",
		"sensitive-secret",
		"sensitive-token",
		"sensitive-arrow-data",
		"sensitive-query",
	} {
		if strings.Contains(output, sensitiveValue) {
			t.Fatalf("timing log leaked %q: %s", sensitiveValue, output)
		}
	}
}

func TestTimingTraceRecordsFailureTotals(t *testing.T) {
	logs := enableTimingTraceForTest(t)
	response := []byte(`{"status":{"state":"FAILED","errorCode":"TEST_FAILURE"}}`)
	conn := newTestConn(&unlimitedMockClient{body: response})

	if _, err := conn.ExecContext(context.Background(), "select 1", nil); err == nil {
		t.Fatal("expected ExecContext to fail")
	}

	output := logs.String()
	if !strings.Contains(output, "phase=exec_internal_total") {
		t.Fatalf("expected exec_internal_total on failure, got %q", output)
	}
	if strings.Contains(output, string(response)) {
		t.Fatalf("failure log leaked raw response: %s", output)
	}
}

func TestRetryTimingTraceRecordsFailureTotal(t *testing.T) {
	logs := enableTimingTraceForTest(t)
	conn := newTestConn(&unlimitedMockClient{body: []byte(`{"unexpected":true}`)})
	response := &execResponse{}

	_, err := conn.retryGetResult(context.Background(), jobId{ID: "test-job"}, map[string]string{}, 1, response, 1)
	if err == nil {
		t.Fatal("expected retryGetResult to fail")
	}
	if output := logs.String(); !strings.Contains(output, "phase=retry_get_result_total") {
		t.Fatalf("expected retry_get_result_total on failure, got %q", output)
	}
}

func TestTimingTraceDoesNotLogRawError(t *testing.T) {
	logs := enableTimingTraceForTest(t)
	trace := timingTrace{enabled: true, entry: logger.WithContext(context.Background())}
	trace.record("test-job", "download_error", trace.start(), "error_type", safeErrorType(errors.New("https://example.invalid/?token=sensitive-token")))

	output := logs.String()
	if strings.Contains(output, "sensitive-token") {
		t.Fatalf("timing log leaked raw error: %s", output)
	}
	if !strings.Contains(output, "error_type=*errors.errorString") {
		t.Fatalf("timing log omitted safe error type: %s", output)
	}
}

func TestDSNParameterLogDoesNotContainValues(t *testing.T) {
	logs := enableTimingTraceForTest(t)
	cfg := &Config{}
	if err := parseDSNParams(cfg, "trace_timing=true&token=sensitive-token&workspace=sensitive-workspace"); err != nil {
		t.Fatalf("parseDSNParams: %v", err)
	}

	output := logs.String()
	for _, key := range []string{"trace_timing", "token", "workspace"} {
		if !strings.Contains(output, key) {
			t.Fatalf("DSN log omitted parameter name %q: %s", key, output)
		}
	}
	for _, value := range []string{"sensitive-token", "sensitive-workspace"} {
		if strings.Contains(output, value) {
			t.Fatalf("DSN log leaked parameter value %q: %s", value, output)
		}
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

	ctx = WithDriverFlags(context.Background(), DriverFlags{"traceTiming": "true"})
	if conn.traceTimingEnabled(ctx) {
		t.Fatal("deprecated traceTiming alias must not enable timing")
	}
}
