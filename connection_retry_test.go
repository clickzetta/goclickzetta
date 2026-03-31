package goclickzetta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fastjson"
)

// mockInternalClient implements InternalClient for testing retry logic
type mockInternalClient struct {
	postResponses []mockResponse
	callIndex     int
}

type mockResponse struct {
	body []byte
	err  error
}

func (m *mockInternalClient) Post(_ context.Context, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration) (*http.Response, error) {
	if m.callIndex >= len(m.postResponses) {
		return nil, errors.New("no more mock responses")
	}
	resp := m.postResponses[m.callIndex]
	m.callIndex++
	if resp.err != nil {
		return nil, resp.err
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(resp.body)),
	}, nil
}

func (m *mockInternalClient) Get(_ context.Context, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (m *mockInternalClient) Close() error { return nil }

// unlimitedMockClient returns the same response forever (useful for timeout tests)
type unlimitedMockClient struct {
	body      []byte
	callCount atomic.Int32
}

func (m *unlimitedMockClient) Post(_ context.Context, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration) (*http.Response, error) {
	m.callCount.Add(1)
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(m.body)),
	}, nil
}

func (m *unlimitedMockClient) Get(_ context.Context, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (m *unlimitedMockClient) Close() error { return nil }

// ==================== Helpers ====================

func newTestConn(mock InternalClient) *ClickzettaConn {
	return &ClickzettaConn{
		ctx:      context.Background(),
		cfg:      &Config{Service: "http://localhost"},
		internal: mock,
	}
}

func newTestConnWithRetries(mock InternalClient, maxRetries int) *ClickzettaConn {
	v := fmt.Sprintf("%d", maxRetries)
	return &ClickzettaConn{
		ctx: context.Background(),
		cfg: &Config{
			Service:   "http://localhost",
			Workspace: "ws",
			Params:    map[string]*string{"sdk.query.max.retries": &v},
		},
		internal: mock,
	}
}

// ==================== TestGetResponseErrorCode ====================

func TestGetResponseErrorCode(t *testing.T) {
	tests := []struct {
		name string
		json string
		want string
	}{
		{"nil input", "", ""},
		{"status.errorCode", `{"status":{"errorCode":"CZLH-60005"}}`, "CZLH-60005"},
		{"respStatus.errorCode", `{"respStatus":{"errorCode":"CZLH-60023"}}`, "CZLH-60023"},
		{"status takes priority", `{"status":{"errorCode":"CZLH-60005"},"respStatus":{"errorCode":"CZLH-60023"}}`, "CZLH-60005"},
		{"no error code", `{"status":{"state":"SUCCEED"}}`, ""},
		{"empty object", `{}`, ""},
		{"empty errorCode string", `{"status":{"errorCode":""}}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var val *fastjson.Value
			if tt.json != "" {
				var err error
				val, err = fastjson.Parse(tt.json)
				if err != nil {
					t.Fatalf("bad test json: %v", err)
				}
			}
			got := getResponseErrorCode(val)
			if got != tt.want {
				t.Errorf("getResponseErrorCode() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ==================== TestIsGetJobResultFailed ====================

func TestIsGetJobResultFailed(t *testing.T) {
	tests := []struct {
		name string
		json string
		want bool
	}{
		{"nil input", "", true},
		{"CZLH-60023 REQUEST_NOT_SUBMITTED in respStatus", `{"respStatus":{"errorCode":"CZLH-60023"}}`, true},
		{"CZLH-60023 REQUEST_NOT_SUBMITTED in status", `{"status":{"errorCode":"CZLH-60023"}}`, true},
		{"CZLH-60022 REQUEST_STATUS_UNKNOWN", `{"status":{"errorCode":"CZLH-60022"}}`, true},
		{"normal response no error", `{"status":{"state":"SUCCEED"}}`, false},
		{"JOB_NOT_EXIST is not a temporary failure", `{"status":{"errorCode":"CZLH-60005"}}`, false},
		{"JOB_ALREADY_EXIST is not a temporary failure", `{"status":{"errorCode":"CZLH-60007"}}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var val *fastjson.Value
			if tt.json != "" {
				var err error
				val, err = fastjson.Parse(tt.json)
				if err != nil {
					t.Fatalf("bad test json: %v", err)
				}
			}
			got := isGetJobResultFailed(val)
			if got != tt.want {
				t.Errorf("isGetJobResultFailed() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ==================== TestSleepWithBackoff ====================

func TestSleepWithBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled context avoids actual sleeping

	tests := []struct {
		input int
		want  int
	}{
		{50, 100},
		{100, 200},
		{200, 400},
		{500, 1000},
		{1500, 3000},
		{3000, 3000}, // capped
		{5000, 3000}, // capped
	}
	for _, tt := range tests {
		got := sleepWithBackoff(ctx, tt.input)
		if got != tt.want {
			t.Errorf("sleepWithBackoff(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

// ==================== TestGetMaxRetries ====================

func TestGetMaxRetries(t *testing.T) {
	// default
	conn := newTestConn(&mockInternalClient{})
	if got := conn.getMaxRetries(); got != defaultMaxRetries {
		t.Errorf("default getMaxRetries() = %d, want %d", got, defaultMaxRetries)
	}

	// configured
	conn = newTestConnWithRetries(&mockInternalClient{}, 5)
	if got := conn.getMaxRetries(); got != 5 {
		t.Errorf("configured getMaxRetries() = %d, want 5", got)
	}

	// invalid value falls back to default
	bad := "abc"
	conn = &ClickzettaConn{
		ctx: context.Background(),
		cfg: &Config{
			Service: "http://localhost",
			Params:  map[string]*string{"sdk.query.max.retries": &bad},
		},
	}
	if got := conn.getMaxRetries(); got != defaultMaxRetries {
		t.Errorf("invalid getMaxRetries() = %d, want %d", got, defaultMaxRetries)
	}

	// zero falls back to default
	conn = newTestConnWithRetries(&mockInternalClient{}, 0)
	if got := conn.getMaxRetries(); got != defaultMaxRetries {
		t.Errorf("zero getMaxRetries() = %d, want %d", got, defaultMaxRetries)
	}
}

// ==================== TestRetryGetResult ====================

func TestRetryGetResult_NetworkErrorThenSuccess(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{err: errors.New("connection refused")},
			{body: succeedBody},
		},
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-123"}

	result, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 0, resp, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got: %s", result.Message)
	}
}

func TestRetryGetResult_JobNotExist(t *testing.T) {
	body := []byte(`{"respStatus":{"errorCode":"CZLH-60005"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{{body: body}},
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-456"}

	_, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 0, resp, 3)
	if err == nil {
		t.Fatal("expected error for JOB_NOT_EXIST")
	}
	if !isJobNotExistError(err) {
		t.Errorf("expected jobNotExistError, got: %T: %v", err, err)
	}
}

func TestRetryGetResult_TemporaryFailureThenSuccess(t *testing.T) {
	tempFail := []byte(`{"respStatus":{"errorCode":"CZLH-60023"}}`)
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{body: tempFail},
			{body: tempFail},
			{body: succeedBody},
		},
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-789"}

	result, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 0, resp, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got: %s", result.Message)
	}
}

func TestRetryGetResult_Timeout(t *testing.T) {
	mock := &unlimitedMockClient{
		body: []byte(`{"status":{"state":"RUNNING"}}`),
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-timeout"}

	// timeout=1 second with unlimited RUNNING responses
	result, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 1, resp, 100)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	var dte driverTimeoutError
	if !errors.As(err, &dte) {
		t.Errorf("expected driverTimeoutError, got: %T: %v", err, err)
	}
	if result.Success {
		t.Error("expected Success=false on timeout")
	}
	// should have polled multiple times
	if mock.callCount.Load() < 2 {
		t.Errorf("expected multiple poll attempts, got %d", mock.callCount.Load())
	}
}

func TestRetryGetResult_NetworkErrorExceedsMaxRetries(t *testing.T) {
	// maxRetries=2, need exceptionRetryCount > 2 (i.e. 3 failures) to exit
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{err: errors.New("network error 1")},
			{err: errors.New("network error 2")},
			{err: errors.New("network error 3")},
		},
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-maxretry"}

	result, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 0, resp, 2)
	if err == nil {
		t.Fatal("expected error after max retries")
	}
	if result.Success {
		t.Error("expected Success=false")
	}
}

func TestRetryGetResult_JobFailed(t *testing.T) {
	failedBody := []byte(`{"status":{"state":"FAILED","errorMessage":"OOM"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{{body: failedBody}},
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-failed"}

	result, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 0, resp, 3)
	if err == nil {
		t.Fatal("expected error for FAILED job")
	}
	if result.Success {
		t.Error("expected Success=false for FAILED job")
	}
}

func TestRetryGetResult_JobCancelled(t *testing.T) {
	cancelledBody := []byte(`{"status":{"state":"CANCELLED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{{body: cancelledBody}},
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-cancelled"}

	result, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 0, resp, 3)
	if err == nil {
		t.Fatal("expected error for CANCELLED job")
	}
	if result.Success {
		t.Error("expected Success=false for CANCELLED job")
	}
}

func TestRetryGetResult_RunningThenSucceed(t *testing.T) {
	runningBody := []byte(`{"status":{"state":"RUNNING"}}`)
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{body: runningBody},
			{body: runningBody},
			{body: succeedBody},
		},
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-running"}

	result, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 0, resp, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got: %s", result.Message)
	}
}

func TestRetryGetResult_ExceptionRetryResetOnSuccess(t *testing.T) {
	// 2 network errors, then RUNNING (resets counter), then 2 more network errors, then SUCCEED
	runningBody := []byte(`{"status":{"state":"RUNNING"}}`)
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{err: errors.New("net error 1")},
			{err: errors.New("net error 2")},
			{body: runningBody}, // resets exceptionRetryCount
			{err: errors.New("net error 3")},
			{err: errors.New("net error 4")},
			{body: succeedBody},
		},
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-reset"}

	// maxRetries=2: without reset, 4 total errors would exceed limit
	result, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 0, resp, 2)
	if err != nil {
		t.Fatalf("unexpected error (exception counter should have been reset): %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got: %s", result.Message)
	}
}

func TestRetryGetResult_NoStatusField(t *testing.T) {
	// response with no "status" field at all
	body := []byte(`{"something":"else"}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{{body: body}},
	}
	conn := newTestConn(mock)
	resp := &execResponse{}
	id := jobId{ID: "test-nostatus"}

	_, err := conn.retryGetResult(context.Background(), id, map[string]string{}, 0, resp, 3)
	if err == nil {
		t.Fatal("expected error for missing status field")
	}
}

// ==================== TestExecInternal (submit retry logic) ====================

func TestExecInternal_SubmitSucceedImmediately(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithRetries(mock, 3)

	result, err := conn.execInternal(context.Background(), "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got: %s", result.Message)
	}
	if mock.callIndex != 1 {
		t.Errorf("expected 1 call, got %d", mock.callIndex)
	}
}

func TestExecInternal_SubmitFailedImmediately(t *testing.T) {
	failedBody := []byte(`{"status":{"state":"FAILED","errorCode":"","errorMessage":"syntax error"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{{body: failedBody}},
	}
	conn := newTestConnWithRetries(mock, 3)

	result, err := conn.execInternal(context.Background(), "bad sql\n;", nil)
	if err == nil {
		t.Fatal("expected error for FAILED job")
	}
	if result.Success {
		t.Error("expected Success=false")
	}
}

func TestExecInternal_NeedReExecute(t *testing.T) {
	reExecBody := []byte(`{"status":{"errorCode":"CZLH-57015"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{{body: reExecBody}},
	}
	conn := newTestConnWithRetries(mock, 3)

	_, err := conn.execInternal(context.Background(), "select 1\n;", nil)
	if err == nil {
		t.Fatal("expected reExecuteError")
	}
	if _, ok := err.(*reExecuteError); !ok {
		t.Errorf("expected *reExecuteError, got %T: %v", err, err)
	}
}

func TestExecInternal_SubmitNetworkErrorRetryThenSucceed(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{err: errors.New("connection reset")},
			{err: errors.New("connection reset")},
			{body: succeedBody},
		},
	}
	conn := newTestConnWithRetries(mock, 5)

	result, err := conn.execInternal(context.Background(), "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success after retry, got: %s", result.Message)
	}
	if mock.callIndex != 3 {
		t.Errorf("expected 3 calls, got %d", mock.callIndex)
	}
}

func TestExecInternal_RequestNotSubmittedRetry(t *testing.T) {
	notSubmitted := []byte(`{"status":{"errorCode":"CZLH-60023"}}`)
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{body: notSubmitted},
			{body: notSubmitted},
			{body: succeedBody},
		},
	}
	conn := newTestConnWithRetries(mock, 5)

	result, err := conn.execInternal(context.Background(), "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success after retry")
	}
}

func TestExecInternal_JobAlreadyExistFirstAttempt(t *testing.T) {
	alreadyExist := []byte(`{"status":{"errorCode":"CZLH-60007"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{{body: alreadyExist}},
	}
	conn := newTestConnWithRetries(mock, 3)

	result, err := conn.execInternal(context.Background(), "select 1\n;", nil)
	if err == nil {
		t.Fatal("expected error for JOB_ALREADY_EXIST on first attempt")
	}
	if result.Success {
		t.Error("expected Success=false")
	}
}

func TestExecInternal_SubmitThenPollRunningThenSucceed(t *testing.T) {
	// first call: submit returns RUNNING
	// second call: getJobResult returns SUCCEED
	runningBody := []byte(`{"status":{"state":"RUNNING"}}`)
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{body: runningBody}, // submit response
			{body: succeedBody}, // getJobResult response
		},
	}
	conn := newTestConnWithRetries(mock, 3)

	result, err := conn.execInternal(context.Background(), "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got: %s", result.Message)
	}
}

func TestExecInternal_AllRetriesExhausted(t *testing.T) {
	notSubmitted := []byte(`{"status":{"errorCode":"CZLH-60023"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{body: notSubmitted},
			{body: notSubmitted},
			{body: notSubmitted},
		},
	}
	conn := newTestConnWithRetries(mock, 3)

	result, err := conn.execInternal(context.Background(), "select 1\n;", nil)
	if err == nil {
		t.Fatal("expected error after all retries exhausted")
	}
	if result.Success {
		t.Error("expected Success=false")
	}
}

// ==================== TestExec (re-execute with new jobId) ====================

func TestExec_ReExecuteWithNewJobId(t *testing.T) {
	// First submit: returns CZLH-57015 (need re-execute)
	// Second submit (new jobId): returns SUCCEED
	reExecBody := []byte(`{"status":{"errorCode":"CZLH-57015"}}`)
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &mockInternalClient{
		postResponses: []mockResponse{
			{body: reExecBody},  // first attempt
			{body: succeedBody}, // second attempt with new jobId
		},
	}
	conn := newTestConnWithRetries(mock, 3)

	result, err := conn.exec(context.Background(), "select 1", false, false, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success after re-execute, got: %s", result.Message)
	}
	if mock.callIndex != 2 {
		t.Errorf("expected 2 submit calls (re-execute), got %d", mock.callIndex)
	}
}

// ==================== TestHandleFinishedJob ====================

func TestHandleFinishedJob(t *testing.T) {
	conn := newTestConn(&mockInternalClient{})
	id := jobId{ID: "test-handle"}

	tests := []struct {
		name        string
		status      string
		wantSuccess bool
		wantErr     bool
	}{
		{"SUCCEED", "SUCCEED", true, false},
		{"FAILED", "FAILED", false, true},
		{"CANCELLED", "CANCELLED", false, true},
		{"unknown", "WEIRD", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"status":{"state":"%s"}}`, tt.status))
			val, _ := fastjson.ParseBytes(body)
			resp := &execResponse{}

			result, err := conn.handleFinishedJob(tt.status, id, resp, body, val)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result.Success != tt.wantSuccess {
				t.Errorf("Success = %v, want %v", result.Success, tt.wantSuccess)
			}
		})
	}
}

// ==================== TestErrorTypes ====================

func TestReExecuteError(t *testing.T) {
	err := &reExecuteError{jobID: "j1", errorCode: "CZLH-57015"}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
	// should not be a jobNotExistError
	if isJobNotExistError(err) {
		t.Error("reExecuteError should not be jobNotExistError")
	}
}

func TestJobNotExistError(t *testing.T) {
	err := &jobNotExistError{jobID: "j2"}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
	if !isJobNotExistError(err) {
		t.Error("expected isJobNotExistError to return true")
	}
	// regular error should not match
	if isJobNotExistError(errors.New("something")) {
		t.Error("regular error should not be jobNotExistError")
	}
}

// ==================== capturingMockClient ====================

// capturingMockClient captures the request body for assertion
type capturingMockClient struct {
	postResponses []mockResponse
	callIndex     int
	lastBody      []byte
}

func (m *capturingMockClient) Post(_ context.Context, _ *url.URL, _ map[string]string, body []byte, _ time.Duration) (*http.Response, error) {
	m.lastBody = make([]byte, len(body))
	copy(m.lastBody, body)
	if m.callIndex >= len(m.postResponses) {
		return nil, errors.New("no more mock responses")
	}
	resp := m.postResponses[m.callIndex]
	m.callIndex++
	if resp.err != nil {
		return nil, resp.err
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(resp.body)),
	}, nil
}

func (m *capturingMockClient) Get(_ context.Context, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (m *capturingMockClient) Close() error { return nil }

// helper to build a test conn with full config fields
func newTestConnWithConfig(mock InternalClient, cfg *Config) *ClickzettaConn {
	retries := "3"
	if cfg.Params == nil {
		cfg.Params = map[string]*string{}
	}
	if _, ok := cfg.Params["sdk.query.max.retries"]; !ok {
		cfg.Params["sdk.query.max.retries"] = &retries
	}
	if cfg.Service == "" {
		cfg.Service = "http://localhost"
	}
	return &ClickzettaConn{
		ctx:      context.Background(),
		cfg:      cfg,
		internal: mock,
	}
}

// helper to extract a nested JSON string value using fastjson
func jsonStr(data []byte, keys ...string) string {
	v, err := fastjson.ParseBytes(data)
	if err != nil {
		return ""
	}
	for _, k := range keys {
		v = v.Get(k)
		if v == nil {
			return ""
		}
	}
	s := v.String()
	// strip quotes
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// helper to extract a JSON string array using fastjson
func jsonStrArray(data []byte, keys ...string) []string {
	v, err := fastjson.ParseBytes(data)
	if err != nil {
		return nil
	}
	for _, k := range keys {
		v = v.Get(k)
		if v == nil {
			return nil
		}
	}
	arr, err := v.Array()
	if err != nil {
		return nil
	}
	result := make([]string, len(arr))
	for i, item := range arr {
		s := item.String()
		if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
			s = s[1 : len(s)-1]
		}
		result[i] = s
	}
	return result
}

// ==================== TestExecInternal_ContextFlags ====================

func TestExecInternal_DefaultValues(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
	})

	_, err := conn.execInternal(context.Background(), "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// verify jobId.workspace
	ws := jsonStr(mock.lastBody, "jobDesc", "jobId", "workspace")
	if ws != "default_ws" {
		t.Errorf("jobId.workspace = %q, want %q", ws, "default_ws")
	}

	// verify virtualCluster
	vc := jsonStr(mock.lastBody, "jobDesc", "virtualCluster")
	if vc != "default_vc" {
		t.Errorf("virtualCluster = %q, want %q", vc, "default_vc")
	}

	// verify defaultNamespace = [workspace, schema]
	ns := jsonStrArray(mock.lastBody, "jobDesc", "sqlJob", "defaultNamespace")
	if len(ns) != 2 || ns[0] != "default_ws" || ns[1] != "public" {
		t.Errorf("defaultNamespace = %v, want [default_ws, public]", ns)
	}
}

func TestExecInternal_ContextWorkspaceAffectsJobId(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
	})

	ctx := WithDriverFlags(context.Background(), DriverFlags{
		"workspace": "ctx_ws",
	})
	_, err := conn.execInternal(ctx, "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// jobId.workspace should use context value
	ws := jsonStr(mock.lastBody, "jobDesc", "jobId", "workspace")
	if ws != "ctx_ws" {
		t.Errorf("jobId.workspace = %q, want %q", ws, "ctx_ws")
	}

	// defaultNamespace catalog should also be ctx_ws (no separate catalog flag)
	ns := jsonStrArray(mock.lastBody, "jobDesc", "sqlJob", "defaultNamespace")
	if len(ns) != 2 || ns[0] != "ctx_ws" || ns[1] != "public" {
		t.Errorf("defaultNamespace = %v, want [ctx_ws, public]", ns)
	}
}

func TestExecInternal_ContextCatalogOverridesWorkspace(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
	})

	ctx := WithDriverFlags(context.Background(), DriverFlags{
		"workspace": "ctx_ws",
		"catalog":   "ctx_catalog",
	})
	_, err := conn.execInternal(ctx, "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// jobId.workspace should use context workspace
	ws := jsonStr(mock.lastBody, "jobDesc", "jobId", "workspace")
	if ws != "ctx_ws" {
		t.Errorf("jobId.workspace = %q, want %q", ws, "ctx_ws")
	}

	// defaultNamespace catalog should use context catalog (independent of workspace)
	ns := jsonStrArray(mock.lastBody, "jobDesc", "sqlJob", "defaultNamespace")
	if len(ns) != 2 || ns[0] != "ctx_catalog" || ns[1] != "public" {
		t.Errorf("defaultNamespace = %v, want [ctx_catalog, public]", ns)
	}
}

func TestExecInternal_ContextCatalogOnlyWithoutWorkspace(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
	})

	// only set catalog, not workspace
	ctx := WithDriverFlags(context.Background(), DriverFlags{
		"catalog": "my_catalog",
	})
	_, err := conn.execInternal(ctx, "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// jobId.workspace should remain default (no workspace flag)
	ws := jsonStr(mock.lastBody, "jobDesc", "jobId", "workspace")
	if ws != "default_ws" {
		t.Errorf("jobId.workspace = %q, want %q", ws, "default_ws")
	}

	// defaultNamespace catalog should use context catalog
	ns := jsonStrArray(mock.lastBody, "jobDesc", "sqlJob", "defaultNamespace")
	if len(ns) != 2 || ns[0] != "my_catalog" || ns[1] != "public" {
		t.Errorf("defaultNamespace = %v, want [my_catalog, public]", ns)
	}
}

func TestExecInternal_ContextVirtualCluster(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
	})

	ctx := WithDriverFlags(context.Background(), DriverFlags{
		"virtualCluster": "ctx_vc",
	})
	_, err := conn.execInternal(ctx, "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	vc := jsonStr(mock.lastBody, "jobDesc", "virtualCluster")
	if vc != "ctx_vc" {
		t.Errorf("virtualCluster = %q, want %q", vc, "ctx_vc")
	}
}

func TestExecInternal_ContextSchema(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
	})

	ctx := WithDriverFlags(context.Background(), DriverFlags{
		"schema": "ctx_schema",
	})
	_, err := conn.execInternal(ctx, "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ns := jsonStrArray(mock.lastBody, "jobDesc", "sqlJob", "defaultNamespace")
	if len(ns) != 2 || ns[0] != "default_ws" || ns[1] != "ctx_schema" {
		t.Errorf("defaultNamespace = %v, want [default_ws, ctx_schema]", ns)
	}
}

func TestExecInternal_AllContextFlagsTogether(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
	})

	ctx := WithDriverFlags(context.Background(), DriverFlags{
		"workspace":      "ctx_ws",
		"catalog":        "ctx_catalog",
		"virtualCluster": "ctx_vc",
		"schema":         "ctx_schema",
	})
	_, err := conn.execInternal(ctx, "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// jobId.workspace = ctx_ws
	ws := jsonStr(mock.lastBody, "jobDesc", "jobId", "workspace")
	if ws != "ctx_ws" {
		t.Errorf("jobId.workspace = %q, want %q", ws, "ctx_ws")
	}

	// virtualCluster = ctx_vc
	vc := jsonStr(mock.lastBody, "jobDesc", "virtualCluster")
	if vc != "ctx_vc" {
		t.Errorf("virtualCluster = %q, want %q", vc, "ctx_vc")
	}

	// defaultNamespace = [ctx_catalog, ctx_schema]
	ns := jsonStrArray(mock.lastBody, "jobDesc", "sqlJob", "defaultNamespace")
	if len(ns) != 2 || ns[0] != "ctx_catalog" || ns[1] != "ctx_schema" {
		t.Errorf("defaultNamespace = %v, want [ctx_catalog, ctx_schema]", ns)
	}
}

func TestExecInternal_ConfigCatalogParam(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	catalogVal := "config_catalog"
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
		Params:         map[string]*string{"catalog": &catalogVal},
	})

	_, err := conn.execInternal(context.Background(), "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// defaultNamespace catalog should use Config.Params["catalog"]
	ns := jsonStrArray(mock.lastBody, "jobDesc", "sqlJob", "defaultNamespace")
	if len(ns) != 2 || ns[0] != "config_catalog" || ns[1] != "public" {
		t.Errorf("defaultNamespace = %v, want [config_catalog, public]", ns)
	}
}

func TestExecInternal_ContextCatalogOverridesConfigCatalog(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	catalogVal := "config_catalog"
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
		Params:         map[string]*string{"catalog": &catalogVal},
	})

	ctx := WithDriverFlags(context.Background(), DriverFlags{
		"catalog": "ctx_catalog",
	})
	_, err := conn.execInternal(ctx, "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// context catalog should override config catalog
	ns := jsonStrArray(mock.lastBody, "jobDesc", "sqlJob", "defaultNamespace")
	if len(ns) != 2 || ns[0] != "ctx_catalog" || ns[1] != "public" {
		t.Errorf("defaultNamespace = %v, want [ctx_catalog, public]", ns)
	}
}

func TestExecInternal_FlagsNotLeakIntoHints(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
	})

	ctx := WithDriverFlags(context.Background(), DriverFlags{
		"workspace":      "ctx_ws",
		"catalog":        "ctx_catalog",
		"virtualCluster": "ctx_vc",
		"schema":         "ctx_schema",
		"custom_hint":    "hint_value",
	})
	_, err := conn.execInternal(ctx, "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	body := mock.lastBody
	parsed, _ := fastjson.ParseBytes(body)
	hints := parsed.Get("jobDesc", "sqlJob", "sqlConfig", "hint")
	if hints == nil {
		t.Fatal("hints not found in request")
	}

	// reserved flags should NOT appear in hints
	for _, key := range []string{"workspace", "catalog", "virtualCluster", "schema"} {
		if hints.Get(key) != nil {
			t.Errorf("reserved flag %q should not appear in hints", key)
		}
	}

	// custom_hint SHOULD appear in hints
	customHint := hints.Get("custom_hint")
	if customHint == nil {
		t.Error("custom_hint should appear in hints")
	} else {
		val := customHint.String()
		if val != `"hint_value"` {
			t.Errorf("custom_hint = %s, want %q", val, "hint_value")
		}
	}
}

func TestExecInternal_EmptyContextFlagsUseDefaults(t *testing.T) {
	succeedBody := []byte(`{"status":{"state":"SUCCEED"}}`)
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: succeedBody}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Workspace:      "default_ws",
		VirtualCluster: "default_vc",
		Schema:         "public",
	})

	// set flags with empty values — should not override defaults
	ctx := WithDriverFlags(context.Background(), DriverFlags{
		"workspace":      "",
		"catalog":        "",
		"virtualCluster": "",
		"schema":         "",
	})
	_, err := conn.execInternal(ctx, "select 1\n;", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ws := jsonStr(mock.lastBody, "jobDesc", "jobId", "workspace")
	if ws != "default_ws" {
		t.Errorf("jobId.workspace = %q, want %q (empty flag should not override)", ws, "default_ws")
	}

	vc := jsonStr(mock.lastBody, "jobDesc", "virtualCluster")
	if vc != "default_vc" {
		t.Errorf("virtualCluster = %q, want %q (empty flag should not override)", vc, "default_vc")
	}

	ns := jsonStrArray(mock.lastBody, "jobDesc", "sqlJob", "defaultNamespace")
	if len(ns) != 2 || ns[0] != "default_ws" || ns[1] != "public" {
		t.Errorf("defaultNamespace = %v, want [default_ws, public] (empty flags should not override)", ns)
	}
}
