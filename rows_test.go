package goclickzetta

import (
	"context"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"io"
	"testing"
)

func newMemoryRows(t *testing.T, csv string) *clickzettaRows {
	t.Helper()
	encoded := base64.StdEncoding.EncodeToString([]byte(csv))
	return &clickzettaRows{
		queryID: "job-rows-1",
		status:  QueryStatusComplete,
		cn:      &ClickzettaConn{ctx: context.Background()},
		response: &execResponse{
			Success: true,
			Data: execResponseData{
				DataType: Memory,
				Schema:   []execResponseColumnType{{Name: "id", Type: "INT"}, {Name: "name", Type: "STRING"}},
				HTTPResponseMessage: httpResponseMessage{HttpResponseMessageResultSet: httpResponseMessageResultSet{
					MemoryData: memoryData{Data: []string{encoded}},
				}},
			},
		},
	}
}

func TestRowsColumnsAndNext(t *testing.T) {
	rows := newMemoryRows(t, "1,alice\n2,bob\n")
	if rows.GetQueryID() != "job-rows-1" || rows.GetStatus() != QueryStatusComplete {
		t.Fatalf("row metadata = (%q, %q), want (job-rows-1, %q)", rows.GetQueryID(), rows.GetStatus(), QueryStatusComplete)
	}
	if got, want := rows.Columns(), []string{"id", "name"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Columns() = %v, want %v", got, want)
	}

	dest := make([]driver.Value, 2)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	if dest[0] != int32(1) || dest[1] != "alice" {
		t.Fatalf("first row = %#v, want [1 alice]", dest)
	}
	if err := rows.Next(dest); err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	if dest[0] != int32(2) || dest[1] != "bob" {
		t.Fatalf("second row = %#v, want [2 bob]", dest)
	}
	if err := rows.Next(dest); !errors.Is(err, io.EOF) {
		t.Fatalf("exhausted Next() error = %v, want io.EOF", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestRowsNextResultSetReturnsEOFWhenExhausted(t *testing.T) {
	rows := newMemoryRows(t, "1,alice\n")
	dest := make([]driver.Value, 2)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if err := rows.NextResultSet(); !errors.Is(err, io.EOF) {
		t.Fatalf("NextResultSet() error = %v, want io.EOF", err)
	}
}

func TestRowsEmptyResult(t *testing.T) {
	rows := newMemoryRows(t, "")
	if rows.HasNextResultSet() {
		t.Fatal("HasNextResultSet() = true for an empty result")
	}
	if err := rows.Next(make([]driver.Value, 2)); !errors.Is(err, io.EOF) {
		t.Fatalf("Next() error = %v, want io.EOF", err)
	}
}
