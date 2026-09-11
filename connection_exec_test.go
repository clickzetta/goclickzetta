package goclickzetta

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The retry tests next door drive the happy paths and the documented error
// codes. What was left uncovered is everything that goes wrong before a request
// is built, and the three ways a response can be unusable after it arrives: an
// unreadable body, a body that is not JSON, and a body that is JSON but does not
// fit the result message.

// recordingClient captures what the driver posted, keyed by request path, and
// replays canned responses. The last response is reused once the list runs out,
// so a polling loop does not run into "no more mock responses".
type recordingClient struct {
	responses []mockResponse
	index     int
	bodies    [][]byte
	paths     []string
}

func (c *recordingClient) Post(_ context.Context, u *url.URL, _ map[string]string, body []byte, _ time.Duration) (*http.Response, error) {
	c.bodies = append(c.bodies, append([]byte(nil), body...))
	c.paths = append(c.paths, u.Path)
	resp := c.responses[len(c.responses)-1]
	if c.index < len(c.responses) {
		resp = c.responses[c.index]
	}
	c.index++
	if resp.err != nil {
		return nil, resp.err
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(resp.body))}, nil
}

func (c *recordingClient) Get(_ context.Context, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (c *recordingClient) Close() error { return nil }

func (c *recordingClient) lastBody(t *testing.T) string {
	t.Helper()
	if len(c.bodies) == 0 {
		t.Fatal("nothing was posted")
	}
	return string(c.bodies[len(c.bodies)-1])
}

// succeedingClient answers every request with an immediately finished job.
func succeedingClient() *recordingClient {
	return &recordingClient{responses: []mockResponse{{body: []byte(`{"status":{"state":"SUCCEED"}}`)}}}
}

// errBody is a response body that fails on read, which is what a connection cut
// mid-response looks like to io.ReadAll.
type errBody struct{}

func (errBody) Read([]byte) (int, error) { return 0, errors.New("connection reset by peer") }
func (errBody) Close() error             { return nil }

// unreadableBodyClient hands back a response whose body cannot be read.
type unreadableBodyClient struct{ calls int }

func (c *unreadableBodyClient) Post(_ context.Context, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration) (*http.Response, error) {
	c.calls++
	return &http.Response{StatusCode: 200, Body: errBody{}}, nil
}

func (c *unreadableBodyClient) Get(_ context.Context, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (c *unreadableBodyClient) Close() error { return nil }

// ==================== execInternal: rejected before any request ====================

func TestExecInternalRejectsInvalidUTF8(t *testing.T) {
	client := succeedingClient()
	conn := newTestConn(client)
	// A lone continuation byte. encoding/json would replace it with U+FFFD
	// without reporting anything, so the server would run a different statement.
	_, err := conn.execInternal(context.Background(), "SELECT '\xbf'", jobId{ID: "j-1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
		t.Fatalf("execInternal() error = %v, want it to reject invalid UTF-8", err)
	}
	if len(client.bodies) != 0 {
		t.Errorf("the statement was posted anyway: %d requests", len(client.bodies))
	}
}

func TestExecInternalRejectsQueryWithNoStatement(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		// Nothing but separators: splitSQL returns an empty slice and indexing
		// the last statement would panic.
		{"one separator", ";"},
		{"two separators", ";;"},
		// Whitespace between separators does count as a statement, so this one
		// gets as far as reading "  " as a hint and gives up there instead. It
		// still has to fail, and still without posting anything.
		{"whitespace between separators", " ; ; "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := succeedingClient()
			conn := newTestConn(client)
			_, err := conn.execInternal(context.Background(), tt.query, jobId{ID: "j-1"}, nil)
			if err == nil {
				t.Fatalf("execInternal(%q) succeeded, want an error", tt.query)
			}
			if len(client.bodies) != 0 {
				t.Errorf("execInternal(%q) posted a request anyway", tt.query)
			}
		})
	}
}

func TestExecInternalRejectsMalformedHintPrefix(t *testing.T) {
	conn := newTestConn(succeedingClient())
	// Everything before the last statement is read as key=value hints.
	_, err := conn.execInternal(context.Background(), "not_a_hint;SELECT 1", jobId{ID: "j-1"}, nil)
	if !errors.Is(err, driver.ErrSkip) {
		t.Fatalf("execInternal() error = %v, want driver.ErrSkip", err)
	}
}

func TestExecInternalRejectsBadJobTimeoutHint(t *testing.T) {
	conn := newTestConn(succeedingClient())
	_, err := conn.execInternal(context.Background(), "sdk.job.timeout=soon;SELECT 1", jobId{ID: "j-1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid sdk.job.timeout hint") {
		t.Fatalf("execInternal() error = %v, want it to name the bad hint", err)
	}
}

func TestExecInternalReportsBindingErrors(t *testing.T) {
	conn := newTestConn(succeedingClient())
	_, err := conn.execInternal(context.Background(), "SELECT 1", jobId{ID: "j-1"},
		[]driver.NamedValue{{Ordinal: 1, Value: int64(7)}})
	if err == nil || !strings.Contains(err.Error(), "bind query parameters") {
		t.Fatalf("execInternal() error = %v, want a binding error", err)
	}
}

func TestExecInternalRejectsUnknownEscapeMode(t *testing.T) {
	client := succeedingClient()
	conn := newTestConn(client)
	mode := "sideways"
	conn.cfg.Params = map[string]*string{stringLiteralEscapeModeHint: &mode}
	_, err := conn.execInternal(context.Background(), "SELECT 1", jobId{ID: "j-1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("execInternal() error = %v, want it to reject the escape mode", err)
	}
	if len(client.bodies) != 0 {
		t.Errorf("the statement was posted under an unknown escape mode")
	}
}

// ==================== execInternal: what reaches the server ====================

// The hint prefix, the result format and the escape mode all end up in the
// submitted request, and nothing had ever looked at that request.
func TestExecInternalSendsHintsFromThePrefix(t *testing.T) {
	client := succeedingClient()
	conn := newTestConn(client)
	_, err := conn.execInternal(context.Background(),
		"cz.sql.custom.hint=on;sdk.job.timeout=42;SELECT 1", jobId{ID: "j-1"}, nil)
	if err != nil {
		t.Fatalf("execInternal() error = %v", err)
	}
	body := client.lastBody(t)
	if !strings.Contains(body, `"cz.sql.custom.hint":"on"`) {
		t.Errorf("request does not carry the custom hint: %s", body)
	}
	// sdk.job.timeout is consumed by the driver as the polling deadline, so it
	// is not forwarded as a server hint.
	if strings.Contains(body, "sdk.job.timeout") {
		t.Errorf("sdk.job.timeout was forwarded to the server: %s", body)
	}
	// Only the last statement is the query; the hints are stripped from it.
	if !strings.Contains(body, `"query":["SELECT 1\n;"]`) {
		t.Errorf("request query is not the last statement: %s", body)
	}
}

func TestExecInternalSelectsResultFormatFromParams(t *testing.T) {
	tests := []struct {
		name  string
		param *string
		want  string
	}{
		{"unset", nil, `"cz.sql.adhoc.default.format":"csv"`},
		{"arrow", stringPtr("arrow"), `"cz.sql.adhoc.default.format":"arrow"`},
		// An empty value is treated as not configured.
		{"empty", stringPtr(""), `"cz.sql.adhoc.default.format":"csv"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := succeedingClient()
			conn := newTestConn(client)
			if tt.param != nil {
				conn.cfg.Params = map[string]*string{"resultFormat": tt.param}
			}
			if _, err := conn.execInternal(context.Background(), "SELECT 1", jobId{ID: "j-1"}, nil); err != nil {
				t.Fatalf("execInternal() error = %v", err)
			}
			if body := client.lastBody(t); !strings.Contains(body, tt.want) {
				t.Errorf("request does not contain %s: %s", tt.want, body)
			}
		})
	}
}

func TestExecInternalForwardsServerDSNParams(t *testing.T) {
	for _, value := range []string{"true", "false"} {
		t.Run(value, func(t *testing.T) {
			client := succeedingClient()
			conn := newTestConn(client)
			conn.cfg.Params = map[string]*string{"separate_params": &value}
			if _, err := conn.execInternal(context.Background(), "SELECT 1", jobId{ID: "j-1"}, nil); err != nil {
				t.Fatalf("execInternal() error = %v", err)
			}
			want := `"separate_params":"` + value + `"`
			if body := client.lastBody(t); !strings.Contains(body, want) {
				t.Errorf("request does not forward DSN hint %s: %s", want, body)
			}
		})
	}
}

// The escape mode can come from a driver flag or from a DSN param, the flag
// wins, and whichever one applies is both used to encode the bindings and
// declared to the server.
func TestExecInternalDeclaresTheEscapeModeItUsed(t *testing.T) {
	tests := []struct {
		name        string
		flag        string
		param       string
		wantMode    string
		wantLiteral string
	}{
		{"default", "", "", "backslash", `'o\'brien'`},
		{"from param", "", "quote", "quote", `'o''brien'`},
		{"from flag", "quote", "", "quote", `'o''brien'`},
		{"flag beats param", "quote", "backslash", "quote", `'o''brien'`},
		// The numeric aliases are accepted, and the canonical name is what goes
		// out, because only the names are known to be understood.
		{"numeric alias", "", "2", "quote", `'o''brien'`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := succeedingClient()
			conn := newTestConn(client)
			ctx := context.Background()
			if tt.flag != "" {
				ctx = WithDriverFlags(ctx, DriverFlags{stringLiteralEscapeModeHint: tt.flag})
			}
			if tt.param != "" {
				value := tt.param
				conn.cfg.Params = map[string]*string{stringLiteralEscapeModeHint: &value}
			}
			_, err := conn.execInternal(ctx, "SELECT ?", jobId{ID: "j-1"},
				[]driver.NamedValue{{Ordinal: 1, Value: "o'brien"}})
			if err != nil {
				t.Fatalf("execInternal() error = %v", err)
			}
			body := client.lastBody(t)
			wantHint := `"` + stringLiteralEscapeModeHint + `":"` + tt.wantMode + `"`
			if !strings.Contains(body, wantHint) {
				t.Errorf("request does not declare %s: %s", wantHint, body)
			}
			// The literal is JSON-encoded in the request, so a backslash in the
			// SQL shows up doubled here.
			wantLiteral := strings.ReplaceAll(tt.wantLiteral, `\`, `\\`)
			if !strings.Contains(body, wantLiteral) {
				t.Errorf("request does not contain the literal %s: %s", wantLiteral, body)
			}
		})
	}
}

func stringPtr(s string) *string { return &s }

// ==================== retryGetResult: unusable responses ====================

// A body that fails on read is retried like any other transport fault, and gives
// up with the read error once the retries are spent.
func TestRetryGetResultRetriesUnreadableBodies(t *testing.T) {
	client := &unreadableBodyClient{}
	conn := newTestConnWithRetries(client, 1)
	resp, err := conn.retryGetResult(context.Background(), jobId{ID: "j-1"},
		map[string]string{}, 0, &execResponse{}, 1)
	if err == nil || !strings.Contains(err.Error(), "connection reset by peer") {
		t.Fatalf("retryGetResult() error = %v, want the read error", err)
	}
	if resp.Success {
		t.Error("resp.Success is true after a failed poll")
	}
	if resp.Message != "connection reset by peer" {
		t.Errorf("resp.Message = %q, want the read error", resp.Message)
	}
	// One attempt plus one retry, then it stops.
	if client.calls != 2 {
		t.Errorf("polled %d times, want 2", client.calls)
	}
}

// A body that is not JSON is also a retryable fault: a proxy returning an error
// page is transient often enough to be worth another attempt.
func TestRetryGetResultRetriesUnparsableBodies(t *testing.T) {
	client := &recordingClient{responses: []mockResponse{
		{body: []byte(`<html>502 Bad Gateway</html>`)},
		{body: []byte(`{"status":{"state":"SUCCEED"}}`)},
	}}
	conn := newTestConnWithRetries(client, 3)
	resp, err := conn.retryGetResult(context.Background(), jobId{ID: "j-1"},
		map[string]string{}, 0, &execResponse{}, 3)
	if err != nil {
		t.Fatalf("retryGetResult() error = %v, want the retry to recover", err)
	}
	if !resp.Success {
		t.Error("resp.Success is false after a successful retry")
	}
	if client.index != 2 {
		t.Errorf("polled %d times, want 2", client.index)
	}
}

func TestRetryGetResultGivesUpOnUnparsableBodies(t *testing.T) {
	client := &recordingClient{responses: []mockResponse{{body: []byte(`{`)}}}
	conn := newTestConnWithRetries(client, 1)
	_, err := conn.retryGetResult(context.Background(), jobId{ID: "j-1"},
		map[string]string{}, 0, &execResponse{}, 1)
	if err == nil || !strings.Contains(err.Error(), "cannot parse JSON") {
		t.Fatalf("retryGetResult() error = %v, want the parse error", err)
	}
}

// A finished job whose payload does not fit the result message is a hard error:
// there is nothing to retry, the server said the job succeeded.
func TestRetryGetResultReportsUndecodableResults(t *testing.T) {
	// jobId is an object in the result message, so a string there fails to
	// unmarshal while still being valid JSON.
	client := &recordingClient{responses: []mockResponse{
		{body: []byte(`{"status":{"state":"SUCCEED","jobId":"not-an-object"}}`)},
	}}
	conn := newTestConnWithRetries(client, 1)
	_, err := conn.retryGetResult(context.Background(), jobId{ID: "j-1"},
		map[string]string{}, 0, &execResponse{}, 1)
	if err == nil {
		t.Fatal("retryGetResult() succeeded, want the decode to be reported")
	}
	if client.index != 1 {
		t.Errorf("polled %d times, want it not to retry a decode failure", client.index)
	}
}

// The same decode failure on the submit response, which has its own copy of the
// unmarshal.
func TestExecInternalReportsUndecodableSubmitResults(t *testing.T) {
	client := &recordingClient{responses: []mockResponse{
		{body: []byte(`{"status":{"state":"SUCCEED","jobId":"not-an-object"}}`)},
	}}
	conn := newTestConnWithRetries(client, 1)
	_, err := conn.execInternal(context.Background(), "SELECT 1", jobId{ID: "j-1"}, nil)
	if err == nil {
		t.Fatal("execInternal() succeeded, want the decode to be reported")
	}
}

// ==================== timeout, cancellation and tracing ====================

// pathAwareClient answers by request path, so a test can let the polling
// succeed while the cancel that follows a timeout fails.
type pathAwareClient struct {
	byPath   map[string]mockResponse
	cancels  int
	fallback mockResponse
}

func (c *pathAwareClient) Post(_ context.Context, u *url.URL, _ map[string]string, _ []byte, _ time.Duration) (*http.Response, error) {
	if u.Path == string(CancelJobPath) {
		c.cancels++
	}
	resp, ok := c.byPath[u.Path]
	if !ok {
		resp = c.fallback
	}
	if resp.err != nil {
		return nil, resp.err
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(resp.body))}, nil
}

func (c *pathAwareClient) Get(_ context.Context, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (c *pathAwareClient) Close() error { return nil }

// A job that outlives its sdk.job.timeout is cancelled, and a cancel that itself
// fails does not replace the timeout the caller needs to see.
func TestRetryGetResultCancelsOnTimeoutAndKeepsTheTimeoutError(t *testing.T) {
	client := &pathAwareClient{
		byPath: map[string]mockResponse{
			string(GetJobResultPath): {body: []byte(`{"status":{"state":"RUNNING"}}`)},
			string(CancelJobPath):    {err: errors.New("cancel refused")},
		},
	}
	conn := newTestConn(client)
	_, err := conn.retryGetResult(context.Background(), jobId{ID: "j-1"},
		map[string]string{}, 1, &execResponse{}, 100)

	var timeout driverTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("retryGetResult() error = %T %v, want a timeout error", err, err)
	}
	if client.cancels == 0 {
		t.Error("the job was never cancelled")
	}
	if strings.Contains(err.Error(), "cancel refused") {
		t.Errorf("the cancel failure replaced the timeout: %v", err)
	}
}

// With trace_timing on, the driver takes a second path through every step it
// times. The timings go to the log, so what is asserted here is that the traced
// path produces the same result as the untraced one.
func TestTracedQueryBehavesLikeAnUntracedOne(t *testing.T) {
	run := func(ctx context.Context) (*execResponse, error) {
		client := &recordingClient{responses: []mockResponse{
			{body: []byte(`{"status":{"state":"RUNNING"}}`)},
			{body: []byte(`{"status":{"state":"SUCCEED"}}`)},
		}}
		return newTestConn(client).execInternal(ctx, "SELECT 1", jobId{ID: "j-1"}, nil)
	}

	plain, plainErr := run(context.Background())
	traced, tracedErr := run(WithDriverFlags(context.Background(), DriverFlags{traceTimingFlag: "true"}))

	if plainErr != nil || tracedErr != nil {
		t.Fatalf("execInternal() errors = %v (plain), %v (traced)", plainErr, tracedErr)
	}
	if plain.Success != traced.Success || plain.Data.JobId != traced.Data.JobId {
		t.Errorf("tracing changed the result: %+v vs %+v", plain, traced)
	}
}

// The traced path also has to survive the failures, since every error branch
// records its own event before returning.
func TestTracingSurvivesFailures(t *testing.T) {
	ctx := WithDriverFlags(context.Background(), DriverFlags{traceTimingFlag: "true"})

	t.Run("unreadable body", func(t *testing.T) {
		conn := newTestConnWithRetries(&unreadableBodyClient{}, 1)
		if _, err := conn.retryGetResult(ctx, jobId{ID: "j-1"}, map[string]string{}, 0, &execResponse{}, 1); err == nil {
			t.Fatal("retryGetResult() succeeded, want the read error")
		}
	})

	t.Run("unparsable body", func(t *testing.T) {
		client := &recordingClient{responses: []mockResponse{{body: []byte(`{`)}}}
		conn := newTestConnWithRetries(client, 1)
		if _, err := conn.retryGetResult(ctx, jobId{ID: "j-1"}, map[string]string{}, 0, &execResponse{}, 1); err == nil {
			t.Fatal("retryGetResult() succeeded, want the parse error")
		}
	})

	t.Run("submit transport error", func(t *testing.T) {
		client := &recordingClient{responses: []mockResponse{{err: errors.New("no route to host")}}}
		conn := newTestConnWithRetries(client, 1)
		if _, err := conn.execInternal(ctx, "SELECT 1", jobId{ID: "j-1"}, nil); err == nil {
			t.Fatal("execInternal() succeeded, want the transport error")
		}
	})
}

// ==================== ExecContext and QueryContext ====================

// A connection with no internal client is one that was closed, or one built by
// hand. Every entry point has to answer ErrBadConn so database/sql discards it
// instead of retrying on it.
func TestEntryPointsRejectAConnectionWithNoClient(t *testing.T) {
	conn := &ClickzettaConn{ctx: context.Background(), cfg: &Config{}}

	if _, err := conn.PrepareContext(context.Background(), "SELECT 1"); !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("PrepareContext() error = %v, want driver.ErrBadConn", err)
	}
	if _, err := conn.ExecContext(context.Background(), "SELECT 1", nil); !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("ExecContext() error = %v, want driver.ErrBadConn", err)
	}
	if _, err := conn.QueryContext(context.Background(), "SELECT 1", nil); !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("QueryContext() error = %v, want driver.ErrBadConn", err)
	}
}

// A failed exec still hands back a Result, carrying the job id and the failure,
// so a caller that ignores the error does not dereference nil.
func TestExecContextReturnsAResultOnFailure(t *testing.T) {
	client := &recordingClient{responses: []mockResponse{
		{body: []byte(`{"status":{"state":"FAILED","errorMessage":"syntax error"}}`)},
	}}
	conn := newTestConnWithRetries(client, 1)

	result, err := conn.ExecContext(context.Background(), "SELEKT 1", nil)
	if err == nil {
		t.Fatal("ExecContext() succeeded, want the job failure")
	}
	if result == nil {
		t.Fatal("ExecContext() returned no result alongside the error")
	}
	czResult, ok := result.(*clickzettaResult)
	if !ok {
		t.Fatalf("result is %T, want *clickzettaResult", result)
	}
	if czResult.status != queryStatus(QueryFailed) {
		t.Errorf("result status = %q, want %q", czResult.status, QueryFailed)
	}
	if czResult.err == nil {
		t.Error("result carries no error")
	}
}

// A cancelled job reaches the caller as an error rather than as rows that hold
// no data.
//
// This also pins down that QueryContext's else branch, the one building a
// ClickzettaError with SQLState "FAILED" out of a response whose Success is
// false, cannot be reached through conn.exec: every path in execInternal and
// retryGetResult that clears Success also returns a non-nil error, so the
// earlier `if err != nil` always wins. The same is true of the equivalent branch
// in ExecContext. They are defensive, not live, and the message here is the one
// exec produced.
func TestQueryContextReportsACancelledJob(t *testing.T) {
	client := &recordingClient{responses: []mockResponse{
		{body: []byte(`{"status":{"state":"CANCELLED"}}`)},
	}}
	conn := newTestConnWithRetries(client, 1)

	rows, err := conn.QueryContext(context.Background(), "SELECT 1", nil)
	if rows != nil {
		t.Error("QueryContext() returned rows for a cancelled job")
	}
	if err == nil {
		t.Fatal("QueryContext() succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "job cancelled") {
		t.Errorf("error = %q, want it to mention the cancellation", err)
	}
}

// An object storage result the driver cannot set up fails the query rather than
// handing back rows that cannot be read.
func TestQueryContextReportsResultSetupFailures(t *testing.T) {
	client := &recordingClient{responses: []mockResponse{{body: []byte(`{
		"status":{"state":"SUCCEED"},
		"resultSet":{
			"metadata":{"fields":[{"name":"id","type":{"category":"BIGINT"}}]},
			"location":{"location":["oss://bucket/one.csv"],"fileSystem":"HDFS"}
		}
	}`)}}}
	conn := newTestConnWithRetries(client, 1)

	rows, err := conn.QueryContext(context.Background(), "SELECT 1", nil)
	if rows != nil {
		t.Error("QueryContext() returned rows despite the unusable result location")
	}
	if err == nil || !strings.Contains(err.Error(), "object storage type is not supported") {
		t.Fatalf("QueryContext() error = %v, want the unsupported file system", err)
	}
}

// Close tears the connection down even when the client complains, and stays
// safe to call twice.
func TestCloseReportsNothingWhenTheClientFails(t *testing.T) {
	conn := newTestConn(&failingCloseClient{})
	if err := conn.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
	if conn.cfg != nil || conn.ctx != nil {
		t.Error("Close() left the connection usable")
	}
}

type failingCloseClient struct{ recordingClient }

func (c *failingCloseClient) Close() error { return errors.New("transport already gone") }
