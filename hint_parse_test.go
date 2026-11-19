package goclickzetta

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/valyala/fastjson"
)

// submittedHints runs query through execInternal and returns the hint map the
// driver sent with the submit.
func submittedHints(t *testing.T, query string, params map[string]*string) (map[string]string, error) {
	t.Helper()
	mock := &capturingMockClient{postResponses: []mockResponse{{body: []byte(`{"status":{"state":"SUCCEED"}}`)}}}
	conn := newTestConnWithConfig(mock, &Config{Workspace: "ws", VirtualCluster: "vc", Schema: "public", Params: params})
	if _, err := conn.execInternal(context.Background(), query, nil); err != nil {
		return nil, err
	}
	v, err := fastjson.ParseBytes(mock.lastBody)
	if err != nil {
		t.Fatalf("parse submit body: %v", err)
	}
	hints := map[string]string{}
	v.Get("jobDesc", "sqlJob", "sqlConfig", "hint").GetObject().Visit(func(k []byte, val *fastjson.Value) {
		hints[string(k)] = string(val.GetStringBytes())
	})
	return hints, nil
}

func TestExecInternal_PrefixHintForms(t *testing.T) {
	for _, tc := range []struct {
		name, query, key, want string
	}{
		{"plain", "cz.sql.timezone=UTC;select 1", "cz.sql.timezone", "UTC"},
		// JDBC users write the parameter with the SET keyword.
		{"SET keyword", "SET cz.sql.timezone=UTC;select 1", "cz.sql.timezone", "UTC"},
		{"lower-case set", "set cz.sql.timezone=UTC;select 1", "cz.sql.timezone", "UTC"},
		{"spaces around =", "cz.sql.timezone = UTC ;select 1", "cz.sql.timezone", "UTC"},
		{"on its own line", "cz.a=1;\n  cz.sql.timezone=UTC;\nselect 1", "cz.sql.timezone", "UTC"},
		// Only the first '=' separates the name from the value.
		{"value contains =", "cz.x=a=b;select 1", "cz.x", "a=b"},
	} {
		hints, err := submittedHints(t, tc.query, nil)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got, ok := hints[tc.key]; !ok || got != tc.want {
			t.Errorf("%s: hint %q = %q (present %v), want %q; all hints: %v", tc.name, tc.key, got, ok, tc.want, hints)
		}
	}
}

// A setting without a name, or a statement that is not a setting before the
// last one, must fail with an error the caller can read. driver.ErrSkip is
// only meaningful to database/sql as "fall back to Prepare", which then runs
// the same broken parse again.
func TestExecInternal_InvalidPrefixHintReturnsReadableError(t *testing.T) {
	for _, query := range []string{
		"=UTC;select 1",
		"select 1;select 2",
	} {
		_, err := submittedHints(t, query, nil)
		if err == nil {
			t.Errorf("%q: no error, want one", query)
			continue
		}
		if errors.Is(err, driver.ErrSkip) {
			t.Errorf("%q: error is driver.ErrSkip, want a descriptive error", query)
		}
	}
}

// cz.* parameters in the DSN are SQL parameters for the server. Precedence:
// DSN < SQL prefix < WithDriverFlags.
func TestExecInternal_DSNSQLParametersAreSent(t *testing.T) {
	tz := "UTC"
	other := "x"
	params := map[string]*string{"cz.sql.timezone": &tz, "unrelated": &other}

	hints, err := submittedHints(t, "select 1", params)
	if err != nil {
		t.Fatal(err)
	}
	if hints["cz.sql.timezone"] != "UTC" {
		t.Errorf("DSN cz.sql.timezone not sent; hints: %v", hints)
	}
	if _, ok := hints["unrelated"]; ok {
		t.Errorf("non-cz DSN parameter sent as a hint: %v", hints)
	}

	hints, err = submittedHints(t, "cz.sql.timezone=Asia/Shanghai;select 1", params)
	if err != nil {
		t.Fatal(err)
	}
	if hints["cz.sql.timezone"] != "Asia/Shanghai" {
		t.Errorf("SQL prefix should override DSN, got %q", hints["cz.sql.timezone"])
	}
}

// A malformed getJob response arrives after the job was submitted. Returning
// driver.ErrBadConn there makes database/sql run the statement again on a new
// connection, so a DML could be executed twice.
func TestExecContext_MalformedGetJobIsNotErrBadConn(t *testing.T) {
	mock := &mockInternalClient{postResponses: []mockResponse{
		{body: []byte(`{"status":{"state":"RUNNING"}}`)},
		{body: []byte(`{"something":"else"}`)},
	}}
	conn := newTestConnWithRetries(mock, 3)
	_, err := conn.ExecContext(context.Background(), "insert into t values (1)", nil)
	if err == nil {
		t.Fatal("expected an error for the response without status")
	}
	if errors.Is(err, driver.ErrBadConn) {
		t.Errorf("error is driver.ErrBadConn (database/sql would retry the DML): %v", err)
	}
}
