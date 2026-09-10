package goclickzetta

import (
	"encoding/json"
	"testing"
)

func TestParseSchemaKeepsTypeInfo(t *testing.T) {
	// 服务端只给列送一个 typeInfo 子对象,解析必须按"哪个到了"分派。早先用反射
	// 判断结构体是否声明了该字段,恒为真,于是每一列都当成 charTypeInfo,
	// DECIMAL 的 scale 恒为 0,解码时 2.5 被还原成 25。
	const payload = `{
	  "resultSet": {
	    "metadata": {
	      "fields": [
	        {"name": "amount", "type": {"category": "DECIMAL", "nullable": true,
	          "decimalTypeInfo": {"precision": "10", "scale": "2"}}},
	        {"name": "code", "type": {"category": "CHAR", "nullable": false,
	          "charTypeInfo": {"length": "8"}}},
	        {"name": "ts", "type": {"category": "TIMESTAMP_LTZ", "nullable": true,
	          "timestampInfo": {"tsUnit": "MICROSECONDS"}}},
	        {"name": "note", "type": {"category": "STRING", "nullable": true}}
	      ]
	    }
	  }
	}`
	var message httpResponseMessage
	if err := json.Unmarshal([]byte(payload), &message); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	qd := &execResponseData{HTTPResponseMessage: message}
	qd.parseSchema(queryDiagnostics{})

	want := []execResponseColumnType{
		{Name: "amount", Type: "DECIMAL", Precision: 10, Scale: 2, Nullable: true},
		{Name: "code", Type: "CHAR", Length: 8, Nullable: false},
		{Name: "ts", Type: "TIMESTAMP_LTZ", TsUnit: "MICROSECONDS", Nullable: true},
		{Name: "note", Type: "STRING", Nullable: true},
	}
	if len(qd.Schema) != len(want) {
		t.Fatalf("parseSchema() produced %d columns, want %d", len(qd.Schema), len(want))
	}
	for i, w := range want {
		if got := qd.Schema[i]; got != w {
			t.Errorf("column %d = %+v, want %+v", i, got, w)
		}
	}
}
