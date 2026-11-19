package goclickzetta

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// tokenServer answers loginSingle with a new token each time (t1, t2, ...)
// unless failLogin is set, and records the token every submit carried.
type tokenServer struct {
	mu         sync.Mutex
	logins     int
	failLogin  bool
	submitSeen []string
}

func (s *tokenServer) handler(w http.ResponseWriter, r *http.Request) {
	_, _ = io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case strings.HasSuffix(r.URL.Path, string(GetTokenPath)):
		if s.failLogin {
			_, _ = w.Write([]byte(`{"code":"LOGIN_FAILED","message":"service unavailable"}`))
			return
		}
		s.logins++
		fmt.Fprintf(w, `{"data":{"token":"t%d"}}`, s.logins)
	case strings.HasSuffix(r.URL.Path, string(SubmitJobRequestPath)):
		s.submitSeen = append(s.submitSeen, r.Header.Get("X-ClickZetta-Token"))
		_, _ = w.Write([]byte(`{"status":{"state":"SUCCEED"}}`))
	default:
		http.NotFound(w, r)
	}
}

func newTokenTestConn(t *testing.T, s *tokenServer, token string, params map[string]string) *ClickzettaConn {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(s.handler))
	t.Cleanup(server.Close)
	p := map[string]*string{}
	for k, v := range params {
		v := v
		p[k] = &v
	}
	conn, err := buildClickzettaConn(context.Background(), Config{
		UserName: "u", Password: "p", Instance: "i", Workspace: "ws", VirtualCluster: "vc",
		Schema: "public", Service: server.URL, Protocol: "http", Token: token, Params: p,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func (s *tokenServer) snapshot() (int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logins, append([]string(nil), s.submitSeen...)
}

func runSelect(t *testing.T, conn *ClickzettaConn) {
	t.Helper()
	if _, err := conn.execInternal(context.Background(), "select 1", nil); err != nil {
		t.Fatal(err)
	}
}

// Like JDBC, a login token is renewed once token_expire_time_ms has passed.
func TestToken_RefreshedAfterExpireTime(t *testing.T) {
	s := &tokenServer{}
	conn := newTokenTestConn(t, s, "", map[string]string{"token_expire_time_ms": "100"})

	runSelect(t, conn)
	runSelect(t, conn)
	time.Sleep(150 * time.Millisecond)
	runSelect(t, conn)

	logins, seen := s.snapshot()
	if logins != 2 {
		t.Errorf("logins = %d, want 2 (connect + one refresh)", logins)
	}
	if want := []string{"t1", "t1", "t2"}; fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Errorf("tokens sent = %v, want %v", seen, want)
	}
}

// The default period is JDBC's one hour, so a fresh connection does not
// log in again.
func TestToken_DefaultPeriodDoesNotRefreshEarly(t *testing.T) {
	s := &tokenServer{}
	conn := newTokenTestConn(t, s, "", nil)
	runSelect(t, conn)
	runSelect(t, conn)
	if logins, _ := s.snapshot(); logins != 1 {
		t.Errorf("logins = %d, want 1", logins)
	}
}

// A caller-provided magic_token is the caller's to renew: never log in.
func TestToken_MagicTokenIsNeverRefreshed(t *testing.T) {
	s := &tokenServer{}
	conn := newTokenTestConn(t, s, "magic", map[string]string{"token_expire_time_ms": "1"})
	time.Sleep(10 * time.Millisecond)
	runSelect(t, conn)

	logins, seen := s.snapshot()
	if logins != 0 {
		t.Errorf("logins = %d, want 0", logins)
	}
	if fmt.Sprint(seen) != "[magic]" {
		t.Errorf("tokens sent = %v, want [magic]", seen)
	}
}

// As in JDBC, a failed refresh is logged and the statement goes on with the
// token it has; the server rejects it if it really expired.
func TestToken_FailedRefreshKeepsOldToken(t *testing.T) {
	s := &tokenServer{}
	conn := newTokenTestConn(t, s, "", map[string]string{"token_expire_time_ms": "50"})
	s.mu.Lock()
	s.failLogin = true
	s.mu.Unlock()
	time.Sleep(80 * time.Millisecond)
	runSelect(t, conn)

	if _, seen := s.snapshot(); fmt.Sprint(seen) != "[t1]" {
		t.Errorf("tokens sent = %v, want [t1]", seen)
	}
}
