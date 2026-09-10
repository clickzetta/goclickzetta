package goclickzetta

import (
	"strings"
	"testing"

	"github.com/clickzetta/goclickzetta/protos/bulkload/ingestion"
)

// TestGateWayCallReturnsErrorInsteadOfNilPair covers the two envelopes that used
// to yield (nil, nil). Every caller of GateWayCall dereferences the returned
// value right away, so a nil value with a nil error is a segfault rather than a
// failed call: a missing table or a disabled operation would take the whole
// process down instead of surfacing as an error.
func TestGateWayCallReturnsErrorInsteadOfNilPair(t *testing.T) {
	tests := []struct {
		name     string
		response string
		wantErr  string
	}{
		{
			name: "gateway error carries no status field",
			// This is verbatim what the server answers for a table that does
			// not exist.
			response: `{"code":"200","message":"StatusRuntimeException: UNKNOWN: Error: Get schema for public.t failed. tablePtr has no value. Error details:NotFound: Table not found.","requestId":"r-1","data":null}`,
			wantErr:  "no status field",
		},
		{
			name:     "success envelope carries no message",
			response: `{"status":{"code":"SUCCESS"}}`,
			wantErr:  "carried no message",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := newTestConn(&mockInternalClient{
				postResponses: []mockResponse{{body: []byte(tc.response)}},
			})
			req := ingestion.CreateBulkLoadStreamRequest{}
			got, err := conn.GateWayCall(&req, ingestion.MethodEnum_CREATE_BULK_LOAD_STREAM_V2)
			if got != nil {
				t.Errorf("GateWayCall() value = %v, want nil", got)
			}
			if err == nil {
				t.Fatal("GateWayCall() error = nil, want an error; a nil pair segfaults every caller")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("GateWayCall() error = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

// TestGateWayCallSurfacesNonSuccessStatus keeps the ordinary failure path
// distinguishable from the two above: here the envelope is well-formed and the
// server simply reports a non-SUCCESS code.
func TestGateWayCallSurfacesNonSuccessStatus(t *testing.T) {
	conn := newTestConn(&mockInternalClient{
		postResponses: []mockResponse{{body: []byte(`{"status":{"code":"FAILED","message":"denied","request_id":"r-2"},"message":"{}"}`)}},
	})
	req := ingestion.CreateBulkLoadStreamRequest{}
	got, err := conn.GateWayCall(&req, ingestion.MethodEnum_CREATE_BULK_LOAD_STREAM_V2)
	if got != nil {
		t.Errorf("GateWayCall() value = %v, want nil", got)
	}
	if err == nil || !strings.Contains(err.Error(), "gateway call error") {
		t.Fatalf("GateWayCall() error = %v, want a gateway call error", err)
	}
}
