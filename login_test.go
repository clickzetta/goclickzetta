package goclickzetta

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/valyala/fastjson"
)

// Logging in is the first thing a connection does and the first thing that goes
// wrong: a mistyped password, an instance the user has no grant on, a service
// URL pointing at something that is not the portal. None of those paths had any
// coverage, so the only thing that was known to work was the happy one.
//
// buildClickzettaConn builds its own httpClient rather than taking an
// InternalClient, so these tests point Config.Service at an httptest server
// instead of injecting a mock.

func TestFormatLoginFailure(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"nil response", "", "empty login response"},
		{"empty object", `{}`, "response contained no data token"},
		{"top level message", `{"message":"user or password error"}`, "message: user or password error"},
		{"errorMessage key", `{"errorMessage":"instance not found"}`, "message: instance not found"},
		{"msg key", `{"msg":"short form"}`, "message: short form"},
		{"code only", `{"status":{"errorCode":"CZLH-40001"}}`, "error_code: CZLH-40001"},
		{
			"code and message",
			`{"status":{"errorCode":"CZLH-40001"},"message":"user or password error"}`,
			"error_code: CZLH-40001, message: user or password error",
		},
		{
			// The top level keys are checked first; the status container is the
			// fallback for a response that carries the text only there.
			"message from status container",
			`{"status":{"errorCode":"CZLH-40001","message":"nested"}}`,
			"error_code: CZLH-40001, message: nested",
		},
		{
			// A data object without a token is the shape that sends
			// buildClickzettaConn down this path with nothing to report.
			"data without token", `{"data":{"expires":1}}`, "response contained no data token",
		},
		{
			// Control characters are folded to spaces and runs of whitespace
			// collapse, so a multi-line server message stays on one line.
			"message is flattened", "{\"message\":\"first line\\nsecond\\tline\"}", "message: first line second line",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var value *fastjson.Value
			if tt.body != "" {
				parsed, err := fastjson.Parse(tt.body)
				if err != nil {
					t.Fatalf("bad test json: %v", err)
				}
				value = parsed
			}
			if got := formatLoginFailure(value); got != tt.want {
				t.Errorf("formatLoginFailure() = %q, want %q", got, tt.want)
			}
		})
	}
}

// A server message longer than the cap is truncated rather than logged whole,
// and the cut lands on a rune boundary so the result stays valid UTF-8.
func TestFormatLoginFailureTruncatesLongMessage(t *testing.T) {
	// Three-byte runes, so a cut at an arbitrary byte would land inside one.
	long := strings.Repeat("字", maxResponseErrorMessageBytes)
	body, err := json.Marshal(map[string]string{"message": long})
	if err != nil {
		t.Fatal(err)
	}
	value, err := fastjson.ParseBytes(body)
	if err != nil {
		t.Fatal(err)
	}
	got := formatLoginFailure(value)
	if !strings.HasSuffix(got, "...") {
		t.Errorf("formatLoginFailure() = %q, want it to end in the truncation marker", got)
	}
	message := strings.TrimPrefix(got, "message: ")
	if len(message) > maxResponseErrorMessageBytes {
		t.Errorf("message is %d bytes, want at most %d", len(message), maxResponseErrorMessageBytes)
	}
	if !utf8.ValidString(strings.TrimSuffix(message, "...")) {
		t.Error("truncation split a multi-byte rune")
	}
}

// loginConfig is the minimum a login needs: where the portal is, and the three
// values that go into the request body.
func loginConfig(service string) Config {
	return Config{
		Service:        service,
		UserName:       "tester",
		Password:       "secret",
		Instance:       "inst-1",
		Workspace:      "ws",
		VirtualCluster: "vc",
	}
}

func TestBuildClickzettaConnStoresTheToken(t *testing.T) {
	var gotPath string
	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decoding the login body: %v", err)
		}
		w.Write([]byte(`{"data":{"token":"tok-1"}}`))
	}))
	defer server.Close()

	conn, err := buildClickzettaConn(context.Background(), loginConfig(server.URL))
	if err != nil {
		t.Fatalf("buildClickzettaConn() error = %v", err)
	}
	defer conn.Close()

	if gotPath != string(GetTokenPath) {
		t.Errorf("login posted to %q, want %q", gotPath, GetTokenPath)
	}
	want := map[string]string{"username": "tester", "password": "secret", "instanceName": "inst-1"}
	for key, value := range want {
		if gotBody[key] != value {
			t.Errorf("login body %s = %q, want %q", key, gotBody[key], value)
		}
	}
	// The token arrives as a JSON string, quotes included, and is stored bare.
	if conn.cfg.Token != "tok-1" {
		t.Errorf("conn.cfg.Token = %q, want %q", conn.cfg.Token, "tok-1")
	}
}

// A token in the config means the caller already authenticated, so the portal is
// never contacted. The handler fails the test if it is.
func TestBuildClickzettaConnSkipsLoginWithAToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("login was attempted even though a token was configured")
	}))
	defer server.Close()

	cfg := loginConfig(server.URL)
	cfg.Token = "preset"
	conn, err := buildClickzettaConn(context.Background(), cfg)
	if err != nil {
		t.Fatalf("buildClickzettaConn() error = %v", err)
	}
	defer conn.Close()
	if conn.cfg.Token != "preset" {
		t.Errorf("conn.cfg.Token = %q, want it left alone", conn.cfg.Token)
	}
}

func TestBuildClickzettaConnReportsLoginFailures(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		wantPart string
	}{
		{
			"wrong password",
			http.StatusOK,
			`{"status":{"errorCode":"CZLH-40001"},"message":"user or password error"}`,
			"error_code: CZLH-40001, message: user or password error",
		},
		{
			"no token in the response",
			http.StatusOK,
			`{"data":{}}`,
			"response contained no data token",
		},
		{
			// A service URL pointing at something that is not the portal: the
			// body is not JSON at all.
			"response is not json",
			http.StatusOK,
			`<html>404</html>`,
			"cannot parse JSON",
		},
		{
			// The status code is not consulted, only the body, so an error page
			// with a JSON body is reported through the same path.
			"error status with a json body",
			http.StatusUnauthorized,
			`{"message":"unauthorized"}`,
			"message: unauthorized",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			conn, err := buildClickzettaConn(context.Background(), loginConfig(server.URL))
			if err == nil {
				conn.Close()
				t.Fatal("buildClickzettaConn() succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tt.wantPart) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantPart)
			}
		})
	}
}

// Nothing listening at all: the transport error is returned as it is, not
// swallowed into a generic login failure.
func TestBuildClickzettaConnReportsTransportErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := server.URL
	server.Close()

	conn, err := buildClickzettaConn(context.Background(), loginConfig(url))
	if err == nil {
		conn.Close()
		t.Fatal("buildClickzettaConn() succeeded against a closed server, want an error")
	}
	if !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "refused") {
		t.Errorf("error = %q, want it to describe the failed connection", err)
	}
}
