package goclickzetta

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// These tests used to fetch https://www.baidu.com, which made them fail on any
// machine without internet, depend on a third party staying up, and assert
// nothing beyond "no error". A local server covers the same code and lets the
// request itself be inspected.

func newTestHTTPClient(t *testing.T) *httpClient {
	t.Helper()
	transport := newHTTPTransport()
	client := &httpClient{
		client:    &http.Client{Transport: transport},
		transport: transport,
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return client
}

func TestClientGet(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		gotHeader = r.Header.Get("X-Test")
		_, _ = io.WriteString(w, "pong")
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	resp, err := newTestHTTPClient(t).Get(context.Background(), u, map[string]string{"X-Test": "abc"}, 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "pong" {
		t.Errorf("body = %q, want %q", body, "pong")
	}
	if gotHeader != "abc" {
		t.Errorf("server saw X-Test = %q, want %q", gotHeader, "abc")
	}
}

func TestClientPost(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	payload := []byte(`{"query":"SELECT 1"}`)
	resp, err := newTestHTTPClient(t).Post(context.Background(), u, nil, payload, 0)
	if err != nil {
		t.Fatalf("Post() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("StatusCode = %d, want 202", resp.StatusCode)
	}
	if string(gotBody) != string(payload) {
		t.Errorf("server saw body %q, want %q", gotBody, payload)
	}
}

// TestClientRespectsContextCancellation covers the timeout argument threading:
// a cancelled context has to abort the request instead of blocking on the
// server.
func TestClientRespectsContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := newTestHTTPClient(t).Get(ctx, u, nil, 0); err == nil {
		t.Fatal("Get() error = nil, want the cancelled context error")
	}
}
