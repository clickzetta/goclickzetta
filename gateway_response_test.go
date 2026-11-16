package goclickzetta

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/clickzetta/goclickzetta/protos/bulkload/ingestion"
)

func gatewayBody(t *testing.T, status string, message string) []byte {
	t.Helper()
	m, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(`{"status":` + status + `,"message":` + string(m) + `}`)
}

// A gateway response without "status" must be an error. Before the fix
// GateWayCall returned (nil, nil) and the caller dereferenced the nil value.
func TestGateWayCallMissingStatusIsError(t *testing.T) {
	conn := newTestConn(&mockInternalClient{postResponses: []mockResponse{{body: []byte(`{"error":"table not found"}`)}}})
	val, err := conn.GateWayCall(&ingestion.CreateBulkLoadStreamRequest{}, ingestion.MethodEnum_CREATE_BULK_LOAD_STREAM_V2)
	if err == nil {
		t.Fatalf("GateWayCall = (%v, nil), want an error", val)
	}
	if !strings.Contains(err.Error(), "table not found") {
		t.Errorf("error = %v, want it to include the response body", err)
	}
}

// Same path through CreateBulkloadStream: no panic, an error instead.
func TestCreateBulkloadStreamMissingStatusIsError(t *testing.T) {
	conn := newTestConn(&mockInternalClient{postResponses: []mockResponse{{body: []byte(`{"error":"x"}`)}}})
	if _, err := conn.CreateBulkloadStream(BulkloadOptions{Table: "t"}); err == nil {
		t.Fatal("CreateBulkloadStream succeeded, want an error")
	}
}

// Fields a newer server adds must not break older clients.
func TestGateWayCallIgnoresUnknownStatusFields(t *testing.T) {
	body := gatewayBody(t, `{"code":"SUCCESS","brand_new_field":1}`, `{}`)
	conn := newTestConn(&mockInternalClient{postResponses: []mockResponse{{body: body}}})
	if _, err := conn.GateWayCall(&ingestion.CreateBulkLoadStreamRequest{}, ingestion.MethodEnum_CREATE_BULK_LOAD_STREAM_V2); err != nil {
		t.Fatalf("GateWayCall: %v", err)
	}
}

func TestCreateBulkloadStreamIgnoresUnknownResponseFields(t *testing.T) {
	msg := `{"status":{"code":"SUCCESS"},"info":{"stream_id":"s1","brand_new_field":"x",` +
		`"identifier":{"instance_id":"7","table_name":"t"},` +
		`"stream_schema":{"data_fields":[{"name":"id","type":{"category":"INT64","nullable":false}}]}},"another_new_field":true}`
	body := gatewayBody(t, `{"code":"SUCCESS"}`, msg)
	conn := newTestConn(&mockInternalClient{postResponses: []mockResponse{{body: body}}})
	stream, err := conn.CreateBulkloadStream(BulkloadOptions{Table: "t"})
	if err != nil {
		t.Fatalf("CreateBulkloadStream: %v", err)
	}
	if got := stream.MetaData.StreamInfo.GetStreamId(); got != "s1" {
		t.Errorf("stream id = %q, want s1", got)
	}
	if _, ok := stream.MetaData.Table.Schema["id"]; !ok {
		t.Errorf("schema = %v, want column id", stream.MetaData.Table.Schema)
	}
}
