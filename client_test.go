package goclickzetta

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// clientTestRequest is what the test server saw.
type clientTestRequest struct {
	method string
	header string
	body   string
}

// newRecordingServer starts a local server that records the request it
// receives and answers 200 "ok". The tests stay off the public network so the
// unit suite runs the same in CI and offline.
func newRecordingServer(t *testing.T) (*url.URL, *clientTestRequest) {
	t.Helper()
	got := &clientTestRequest{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got.method = r.Method
		got.header = r.Header.Get("X-Test")
		got.body = string(body)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return u, got
}

func newTestHTTPClient() *httpClient {
	transport := newHTTPTransport()
	return &httpClient{client: &http.Client{Transport: transport}, transport: transport}
}

func readOK(t *testing.T, resp *http.Response) {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("response = %d %q, want 200 \"ok\"", resp.StatusCode, body)
	}
}

func TestClientGet(t *testing.T) {
	u, got := newRecordingServer(t)
	client := newTestHTTPClient()
	resp, err := client.Get(context.Background(), u, map[string]string{"X-Test": "get"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	readOK(t, resp)
	if got.method != http.MethodGet || got.header != "get" {
		t.Errorf("server saw %s with X-Test=%q, want GET with X-Test=get", got.method, got.header)
	}
	if err := client.Close(); err != nil {
		t.Error(err)
	}
}

func TestClientPost(t *testing.T) {
	u, got := newRecordingServer(t)
	client := newTestHTTPClient()
	resp, err := client.Post(context.Background(), u, map[string]string{"X-Test": "post"}, []byte(`{"a":1}`), 0)
	if err != nil {
		t.Fatal(err)
	}
	readOK(t, resp)
	if got.method != http.MethodPost || got.header != "post" || got.body != `{"a":1}` {
		t.Errorf("server saw %s with X-Test=%q body=%q, want POST with X-Test=post body={\"a\":1}", got.method, got.header, got.body)
	}
	if err := client.Close(); err != nil {
		t.Error(err)
	}
}
