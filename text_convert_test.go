package goclickzetta

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// 本分支的结果集是文本的,每个单元格由 convertTextValue 按列元数据定型。这是唯一
// 活着的转换路径:converter.go 在这个分支上没有生产调用方。

func TestConvertTextValueByType(t *testing.T) {
	cases := []struct {
		name    string
		colType string
		cell    string
		want    interface{}
		wantErr bool
	}{
		{name: "BOOLEAN_true", colType: "BOOLEAN", cell: "true", want: true},
		{name: "BOOLEAN_false", colType: "BOOLEAN", cell: "false", want: false},
		{name: "BOOLEAN_numeric", colType: "BOOLEAN", cell: "1", want: true},
		{name: "BOOLEAN_invalid", colType: "BOOLEAN", cell: "yes", wantErr: true},

		{name: "TINYINT", colType: "TINYINT", cell: "-128", want: int8(-128)},
		{name: "TINYINT_overflow", colType: "TINYINT", cell: "128", wantErr: true},
		{name: "INT8_alias", colType: "INT8", cell: "7", want: int8(7)},

		{name: "SMALLINT", colType: "SMALLINT", cell: "-32768", want: int16(-32768)},
		{name: "SMALLINT_overflow", colType: "SMALLINT", cell: "32768", wantErr: true},
		{name: "INT16_alias", colType: "INT16", cell: "7", want: int16(7)},

		{name: "INT", colType: "INT", cell: "-2147483648", want: int32(-2147483648)},
		{name: "INT_overflow", colType: "INT", cell: "2147483648", wantErr: true},
		{name: "INT32_alias", colType: "INT32", cell: "7", want: int32(7)},

		{name: "BIGINT", colType: "BIGINT", cell: "-9223372036854775808", want: int64(-9223372036854775808)},
		{name: "BIGINT_overflow", colType: "BIGINT", cell: "9223372036854775808", wantErr: true},
		{name: "INT64_alias", colType: "INT64", cell: "7", want: int64(7)},

		{name: "FLOAT", colType: "FLOAT", cell: "-1.5", want: float32(-1.5)},
		{name: "FLOAT_invalid", colType: "FLOAT", cell: "abc", wantErr: true},
		{name: "FLOAT32_alias", colType: "FLOAT32", cell: "0.5", want: float32(0.5)},

		{name: "DOUBLE", colType: "DOUBLE", cell: "-2.5", want: -2.5},
		{name: "DOUBLE_exponent", colType: "DOUBLE", cell: "1.5e-10", want: 1.5e-10},
		{name: "DOUBLE_invalid", colType: "DOUBLE", cell: "abc", wantErr: true},
		{name: "FLOAT64_alias", colType: "FLOAT64", cell: "0.5", want: 0.5},

		{name: "STRING", colType: "STRING", cell: "hello", want: "hello"},
		{name: "VARCHAR", colType: "VARCHAR", cell: "xy", want: "xy"},
		{name: "CHAR", colType: "CHAR", cell: "abc", want: "abc"},
		// 字符串分支不看内容,数字样子的文本也照原样交回去。
		{name: "STRING_numeric_text", colType: "STRING", cell: "007", want: "007"},

		// DECIMAL 不在 switch 里,落到 default,所以保留服务端渲染的尾零。改成
		// float64 会把 2.50 变成 2.5,声明的 scale 就丢了。
		{name: "DECIMAL_keeps_trailing_zero", colType: "DECIMAL", cell: "2.50", want: "2.50"},
		{name: "DECIMAL_scale0", colType: "DECIMAL", cell: "7", want: "7"},
		// BINARY 同样落到 default,以十六进制文本交回。
		{name: "BINARY_hex_text", colType: "BINARY", cell: "4142", want: "4142"},
		{name: "unknown_type", colType: "SOMETHING_NEW", cell: "raw", want: "raw"},

		// 类型名大小写不敏感:服务端 category 的拼写变了不该改变定型结果。
		{name: "lowercase_bigint", colType: "bigint", cell: "7", want: int64(7)},
		{name: "mixedcase_double", colType: "Double", cell: "2.5", want: 2.5},

		{name: "DATE", colType: "DATE", cell: "2024-01-02",
			want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
		{name: "DATE_invalid", colType: "DATE", cell: "2024/01/02", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertTextValue(execResponseColumnType{Type: tc.colType}, tc.cell)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("convertTextValue(%s, %q) = %#v, want error", tc.colType, tc.cell, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("convertTextValue(%s, %q): %v", tc.colType, tc.cell, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("convertTextValue(%s, %q) = %#v (%T), want %#v (%T)",
					tc.colType, tc.cell, got, got, tc.want, tc.want)
			}
		})
	}
}

func TestConvertTextValueTimestampLayouts(t *testing.T) {
	cases := []struct {
		name string
		cell string
		want time.Time
	}{
		{name: "space_seconds", cell: "2024-01-02 03:04:05",
			want: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
		{name: "space_micros", cell: "2024-01-02 03:04:05.123456",
			want: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)},
		{name: "space_nanos", cell: "2024-01-02 03:04:05.123456789",
			want: time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC)},
		{name: "rfc3339_zulu", cell: "2024-01-02T03:04:05Z",
			want: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
		// 带偏移量的写法会被归一到 UTC,瞬时值保留。服务端目前不发偏移量,这一条
		// 钉住的是"发了就按发的算"。
		{name: "rfc3339_offset", cell: "2024-01-02T03:04:05+08:00",
			want: time.Date(2024, 1, 1, 19, 4, 5, 0, time.UTC)},
		{name: "rfc3339_nano_offset", cell: "2024-01-02T03:04:05.123456789+08:00",
			want: time.Date(2024, 1, 1, 19, 4, 5, 123456789, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertTextValue(execResponseColumnType{Type: "TIMESTAMP_LTZ"}, tc.cell)
			if err != nil {
				t.Fatalf("convert %q: %v", tc.cell, err)
			}
			ts, ok := got.(time.Time)
			if !ok {
				t.Fatalf("convert %q = %#v (%T), want time.Time", tc.cell, got, got)
			}
			if !ts.Equal(tc.want) {
				t.Errorf("convert %q = %v, want %v", tc.cell, ts, tc.want)
			}
			// 无条件归一到 UTC,是 TestDataTypeTimestampIgnoresSessionZone 记录的
			// 缺口的来源。
			if ts.Location() != time.UTC {
				t.Errorf("convert %q location = %v, want UTC", tc.cell, ts.Location())
			}
		})
	}

	if _, err := convertTextValue(execResponseColumnType{Type: "TIMESTAMP_LTZ"}, "not a time"); err == nil {
		t.Error("无法识别的时间文本应当报错")
	}
}

func TestConvertTextValueNestedTypes(t *testing.T) {
	cases := []struct {
		name    string
		colType string
		cell    string
		want    interface{}
	}{
		// JSON 数字统一是 float64,所以 ARRAY<INT> 的元素读回来不是整型。
		{name: "ARRAY", colType: "ARRAY", cell: "[1,2,3]",
			want: []interface{}{float64(1), float64(2), float64(3)}},
		{name: "MAP", colType: "MAP", cell: `{"a":1}`,
			want: map[string]interface{}{"a": float64(1)}},
		{name: "STRUCT", colType: "STRUCT", cell: `{"x":1,"s":"y"}`,
			want: map[string]interface{}{"x": float64(1), "s": "y"}},
		{name: "JSON_object", colType: "JSON", cell: `{"a":[1,{"b":null}]}`,
			want: map[string]interface{}{"a": []interface{}{float64(1), map[string]interface{}{"b": nil}}}},
		{name: "ARRAY_empty", colType: "ARRAY", cell: "[]", want: []interface{}{}},
		{name: "MAP_empty", colType: "MAP", cell: "{}", want: map[string]interface{}{}},
		{name: "ARRAY_nested", colType: "ARRAY", cell: "[[1],[2]]",
			want: []interface{}{[]interface{}{float64(1)}, []interface{}{float64(2)}}},

		// 首字节不是 [ 或 { 就当普通文本,不试解析。
		{name: "JSON_scalar_stays_text", colType: "JSON", cell: "42", want: "42"},
		{name: "JSON_quoted_stays_text", colType: "JSON", cell: `"a"`, want: `"a"`},
		// 看着像 JSON 但解析不了的,原文交回,不报错:结果集不该因为一个坏单元格
		// 整体失败。
		{name: "ARRAY_malformed_stays_text", colType: "ARRAY", cell: "[1,2", want: "[1,2"},
		{name: "MAP_malformed_stays_text", colType: "MAP", cell: `{"a":}`, want: `{"a":}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertTextValue(execResponseColumnType{Type: tc.colType}, tc.cell)
			if err != nil {
				t.Fatalf("convert %s %q: %v", tc.colType, tc.cell, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("convert %s %q = %#v (%T), want %#v (%T)",
					tc.colType, tc.cell, got, got, tc.want, tc.want)
			}
		})
	}
}

// textToRows 负责把 CSV 文本装成行:定型交给 convertTextValue,这里测的是空值、
// 列数不齐和转换失败时的行为。
func TestTextToRows(t *testing.T) {
	schema := func(types ...string) []execResponseColumnType {
		cols := make([]execResponseColumnType, 0, len(types))
		for i, tp := range types {
			cols = append(cols, execResponseColumnType{Name: fmt.Sprintf("c%d", i), Type: tp})
		}
		return cols
	}

	cases := []struct {
		name   string
		schema []execResponseColumnType
		body   string
		want   [][]interface{}
	}{
		{
			name:   "typed_row",
			schema: schema("BIGINT", "STRING", "BOOLEAN"),
			body:   "1,hello,true\n",
			want:   [][]interface{}{{int64(1), "hello", true}},
		},
		{
			name:   "multiple_rows",
			schema: schema("INT"),
			body:   "1\n2\n3\n",
			want:   [][]interface{}{{int32(1)}, {int32(2)}, {int32(3)}},
		},
		{
			// \N 是 NULL 的哨兵值。
			name:   "null_sentinel",
			schema: schema("BIGINT", "STRING"),
			body:   "\\N,x\n",
			want:   [][]interface{}{{nil, "x"}},
		},
		{
			// 空字段也当 NULL,所以空串和 NULL 在这一层就分不开了。
			name:   "empty_cell_is_null",
			schema: schema("STRING", "STRING"),
			body:   ",x\n",
			want:   [][]interface{}{{nil, "x"}},
		},
		{
			// 列数不够就用 NULL 补齐,不报错。
			name:   "short_row_padded_with_nulls",
			schema: schema("BIGINT", "STRING", "BOOLEAN"),
			body:   "1\n",
			want:   [][]interface{}{{int64(1), nil, nil}},
		},
		{
			// 多出来的列按 schema 截断。
			name:   "extra_columns_truncated",
			schema: schema("BIGINT"),
			body:   "1,2,3\n",
			want:   [][]interface{}{{int64(1)}},
		},
		{
			// 定型失败时保留原文,而不是让整个结果集报错。
			name:   "convert_error_falls_back_to_raw_text",
			schema: schema("BIGINT", "BOOLEAN"),
			body:   "not-a-number,not-a-bool\n",
			want:   [][]interface{}{{"not-a-number", "not-a-bool"}},
		},
		{
			// 引号里的逗号和换行不算分隔符。
			name:   "quoted_field_with_comma_and_newline",
			schema: schema("STRING", "BIGINT"),
			body:   "\"a,b\nc\",7\n",
			want:   [][]interface{}{{"a,b\nc", int64(7)}},
		},
		{
			// 已知缺口的根因:整行只有一个 NULL 列时,这一行在文本里是空行,
			// csv.Reader 直接丢掉,于是结果集为空。
			// 见 TestDataTypeNullReadBack/SingleNullColumnRowIsDropped。
			name:   "single_null_column_row_disappears",
			schema: schema("INT"),
			body:   "\n",
			want:   nil,
		},
		{
			// 同一个空行规则在多列时不生效,因为行里有逗号。
			name:   "all_null_multi_column_row_survives",
			schema: schema("INT", "INT"),
			body:   ",\n",
			want:   [][]interface{}{{nil, nil}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qd := &execResponseData{Schema: tc.schema}
			if err := textToRows(qd, strings.NewReader(tc.body)); err != nil {
				t.Fatalf("textToRows: %v", err)
			}
			got := make([][]interface{}, 0, len(qd.Data))
			for i, raw := range qd.Data {
				row, ok := raw.([]interface{})
				if !ok {
					t.Fatalf("row %d is %T, want []interface{}", i, raw)
				}
				got = append(got, row)
			}
			if len(tc.want) == 0 {
				if len(got) != 0 {
					t.Fatalf("got %d rows, want 0: %#v", len(got), got)
				}
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("rows = %#v, want %#v", got, tc.want)
			}
		})
	}
}
