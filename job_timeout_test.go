package goclickzetta

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fastjson"
)

// slowSubmitClient holds the submit request for submitDelay (the way the
// server holds a HYBRID submit) and answers every request with RUNNING. It
// returns early when the request context ends, like a real HTTP client.
type slowSubmitClient struct {
	submitDelay time.Duration
	submits     atomic.Int32
	submitBody  atomic.Value // []byte
}

func (m *slowSubmitClient) Post(ctx context.Context, u *url.URL, _ map[string]string, body []byte, _ time.Duration) (*http.Response, error) {
	if strings.HasSuffix(u.Path, string(SubmitJobRequestPath)) {
		m.submits.Add(1)
		m.submitBody.Store(append([]byte(nil), body...))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.submitDelay):
		}
	} else if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader([]byte(`{"status":{"state":"RUNNING"}}`))),
	}, nil
}

func (m *slowSubmitClient) Get(_ context.Context, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (m *slowSubmitClient) Close() error { return nil }

func submittedHoldSeconds(t *testing.T, body []byte) int {
	t.Helper()
	v, err := fastjson.ParseBytes(body)
	if err != nil {
		t.Fatalf("parse submit body: %v", err)
	}
	return v.GetInt("jobDesc", "hybridPollingTimeout")
}

// sdk.job.timeout must count from the submit. Before the fix the clock only
// started when polling began, so the time the server held the submit was added
// to every timeout (sdk.job.timeout=5 fired after about 35 seconds).
func TestExecInternal_JobTimeoutCountsSubmitTime(t *testing.T) {
	mock := &slowSubmitClient{submitDelay: 1500 * time.Millisecond}
	conn := newTestConnWithRetries(mock, 3)

	start := time.Now()
	_, err := conn.execInternal(context.Background(), "sdk.job.timeout=1;select 1", nil)
	elapsed := time.Since(start)

	var timeoutErr driverTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("error = %T: %v, want driverTimeoutError", err, err)
	}
	// The submit alone takes 1.5s, already past the 1s limit, so the first
	// poll must report the timeout. Counting from polling would take 2.5s.
	if elapsed > 2*time.Second {
		t.Errorf("timed out after %v, want the submit time counted (about 1.5s)", elapsed)
	}
}

// A timeout shorter than the default 30s hold must shorten the hold, or the
// job cannot be stopped before the server answers the submit.
func TestExecInternal_ShortJobTimeoutShortensSubmitHold(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  int
	}{
		{"select 1", 30},
		{"sdk.job.timeout=5;select 1", 5},
		{"sdk.job.timeout=30;select 1", 30},
		{"sdk.job.timeout=600;select 1", 30},
	} {
		mock := &capturingMockClient{postResponses: []mockResponse{{body: []byte(`{"status":{"state":"SUCCEED"}}`)}}}
		conn := newTestConnWithConfig(mock, &Config{Workspace: "ws", VirtualCluster: "vc", Schema: "public"})
		if _, err := conn.execInternal(context.Background(), tc.query, nil); err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		if got := submittedHoldSeconds(t, mock.lastBody); got != tc.want {
			t.Errorf("%s: hybridPollingTimeout = %d, want %d", tc.query, got, tc.want)
		}
	}
}

// The submit request must use the caller's context. Before the fix it used
// the connection's context, so context.WithTimeout below 30s was ignored until
// the server released the submit.
func TestExecInternal_SubmitHonorsCallerContext(t *testing.T) {
	mock := &slowSubmitClient{submitDelay: 5 * time.Second}
	conn := newTestConnWithRetries(mock, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := conn.execInternal(ctx, "select 1", nil)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Errorf("returned after %v, want shortly after the 200ms deadline", elapsed)
	}
	// A cancelled context is the caller giving up, not a network failure:
	// the submit must not be retried.
	if n := mock.submits.Load(); n != 1 {
		t.Errorf("submit attempts = %d, want 1", n)
	}
}
