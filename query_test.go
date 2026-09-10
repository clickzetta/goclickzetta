package goclickzetta

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestParseSchemaDecodesStringEncodedTypeInfo goes through JSON on purpose. The
// server sends precision, scale and length as strings, so a numeric field type
// fails the whole response, and the sibling tests that build the structs by hand
// cannot catch that.
func TestParseSchemaDecodesStringEncodedTypeInfo(t *testing.T) {
	const payload = `{"resultSet":{"metadata":{"fields":[
	  {"name":"amount","type":{"category":"DECIMAL","nullable":true,
	    "decimalTypeInfo":{"precision":"10","scale":"2"}}},
	  {"name":"code","type":{"category":"CHAR","nullable":false,
	    "charTypeInfo":{"length":"8"}}},
	  {"name":"created","type":{"category":"TIMESTAMP_LTZ","nullable":true,
	    "timestampInfo":{"tsUnit":"MICROSECONDS"}}},
	  {"name":"note","type":{"category":"STRING","nullable":true}}]}}}`

	var data execResponseData
	if err := json.Unmarshal([]byte(payload), &data.HTTPResponseMessage); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if err := data.parseSchema(); err != nil {
		t.Fatalf("parseSchema() error = %v", err)
	}

	want := []execResponseColumnType{
		{Name: "amount", Type: "DECIMAL", Precision: 10, Scale: 2, Nullable: true},
		{Name: "code", Type: "CHAR", Length: 8},
		{Name: "created", Type: "TIMESTAMP_LTZ", Nullable: true, TsUnit: "MICROSECONDS"},
		{Name: "note", Type: "STRING", Nullable: true},
	}
	if !reflect.DeepEqual(data.Schema, want) {
		t.Errorf("parseSchema() =\n%+v\nwant\n%+v", data.Schema, want)
	}
}
