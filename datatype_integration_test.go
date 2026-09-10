package goclickzetta

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// 这一组测试覆盖每种数据类型的读回与往返。之前 DECIMAL 精度丢失和 CHAR 元数据
// 解析失败都能一路溜过去,是因为集成测试只碰过 BIGINT 和 STRING。

// readBackCase 是一条读回断言:把 expr 交给服务端,断言驱动交回的 Go 值。
type readBackCase struct {
	name string
	expr string
	want interface{}
	// skip 非空表示这是一个已知缺口,原因写在里面。缺口修好之后删掉这一行,
	// 断言就会开始生效。
	skip string
}

func TestDataTypeReadBack(t *testing.T) {
	db := openDataTypeDB(t)

	cases := []readBackCase{
		// 整型全部以十进制文本交回,不是整数:arrowToValue 只在 higherPrecision
		// 为真时才返回 int64,而两个调用点都硬写了 false。
		// 见 TestFormatIntValue/scale0_text。
		{name: "TINYINT", expr: "cast(1 as tinyint)", want: "1"},
		{name: "SMALLINT", expr: "cast(2 as smallint)", want: "2"},
		{name: "INT", expr: "cast(3 as int)", want: "3"},
		{name: "BIGINT", expr: "cast(-4 as bigint)", want: "-4"},

		{name: "FLOAT", expr: "cast(1.5 as float)", want: float32(1.5)},
		{name: "DOUBLE", expr: "cast(-2.5 as double)", want: -2.5},
		{name: "BOOLEAN", expr: "true", want: true},
		{name: "STRING", expr: "'hello'", want: "hello"},

		// DECIMAL 保留声明的 scale,尾零不能被吃掉:客户端拿 2.5 还是 2.50 是有
		// 区别的。
		{name: "DECIMAL_10_2", expr: "cast(2.5 as decimal(10,2))", want: "2.50"},
		{name: "DECIMAL_38_10", expr: "cast(1.23456789 as decimal(38,10))", want: "1.2345678900"},
		{name: "DECIMAL_scale0", expr: "cast(7 as decimal(10,0))", want: "7"},
		{name: "DECIMAL_negative", expr: "cast(-2.5 as decimal(10,2))", want: "-2.50"},

		// CHAR 的 length 元数据服务端用 JSON 字符串编码,解析失败会让整个结果集
		// 反序列化报错,而不只是丢掉宽度。
		{name: "CHAR", expr: "cast('abc' as char(8))", want: "abc"},
		{name: "VARCHAR", expr: "cast('xy' as varchar(16))", want: "xy"},

		{name: "DATE", expr: "date'2024-01-02'", want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},

		{name: "ARRAY", expr: "array(1,2,3)", want: []int32{1, 2, 3}},
		{name: "MAP", expr: "map('a',1)", want: map[string]int32{"a": 1}},

		// BINARY 在 clickzettaTypes 里没有对应项,arrowToValue 整列报
		// "unsupported data type",上层把它当批次失败,于是结果集是空的。
		{name: "BINARY", expr: "unhex('4142')", want: nil,
			skip: "BINARY 不受支持,见 TestArrowToValueErrorPaths/BINARY_unsupported"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}
			var got interface{}
			if err := db.QueryRow("SELECT " + tc.expr).Scan(&got); err != nil {
				t.Fatalf("query %s: %v", tc.expr, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s = %#v (%T), want %#v (%T)", tc.expr, got, got, tc.want, tc.want)
			}
		})
	}
}

// openDataTypeDB 打开一个跑集成测试用的连接池。没有配 CZ_TEST_DSN 就跳过。
func openDataTypeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("clickzetta", getTestDSN(t))
	if err != nil {
		t.Fatalf("open integration db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close integration db: %v", err)
		}
	})
	return db
}

// STRUCT 的字段顺序不定,单独比对。
func TestDataTypeStructReadBack(t *testing.T) {
	db := openDataTypeDB(t)
	var got interface{}
	if err := db.QueryRow("SELECT named_struct('x',1,'s','y')").Scan(&got); err != nil {
		t.Fatalf("query struct: %v", err)
	}
	want := map[string]interface{}{"x": int32(1), "s": "y"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("named_struct = %#v (%T), want %#v", got, got, want)
	}
}

// TIMESTAMP_LTZ 走 arrow 时瞬时值是对的:服务端发的是微秒时间戳,不依赖会话时区
// 的文本渲染。text 结果集那条路径不是这样,所以这里单独钉住。
func TestDataTypeTimestampPreservesInstant(t *testing.T) {
	db := openDataTypeDB(t)

	var zone string
	if err := db.QueryRow("SELECT current_timezone()").Scan(&zone); err != nil {
		t.Fatalf("read session timezone: %v", err)
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		t.Skipf("会话时区 %q 在本机无法解析: %v", zone, err)
	}

	// 字面量按会话时区的墙上时间理解,所以期望值要用会话时区构造。
	var got time.Time
	if err := db.QueryRow("SELECT timestamp'2024-01-02 03:04:05.123456'").Scan(&got); err != nil {
		t.Fatalf("query timestamp: %v", err)
	}
	want := time.Date(2024, 1, 2, 3, 4, 5, 123456000, loc)
	if !got.Equal(want) {
		t.Errorf("timestamp = %v, want %v (会话时区 %s)", got, want, zone)
	}
	if got.Location() != time.UTC {
		t.Errorf("location = %v, want UTC (归一到 UTC,瞬时值不变)", got.Location())
	}
}

func TestDataTypeNullReadBack(t *testing.T) {
	db := openDataTypeDB(t)
	for _, expr := range []string{
		"cast(null as decimal(10,2))",
		"cast(null as string)",
		"cast(null as int)",
		"cast(null as date)",
		"cast(null as boolean)",
		"cast(null as double)",
	} {
		var got interface{}
		if err := db.QueryRow("SELECT " + expr).Scan(&got); err != nil {
			t.Errorf("query %s: %v", expr, err)
			continue
		}
		if got != nil {
			t.Errorf("%s = %#v, want nil", expr, got)
		}
	}

	// arrow 结果集里空串和 NULL 是分得开的。
	var empty interface{}
	if err := db.QueryRow("SELECT cast('' as string)").Scan(&empty); err != nil {
		t.Fatalf("query empty string: %v", err)
	}
	if empty != "" {
		t.Errorf("空串 = %#v, want \"\"", empty)
	}
}

// roundTripColumn 描述一列:建表用的类型、绑定进去的值、以及读回来该等于什么。
type roundTripColumn struct {
	name     string
	sqlType  string
	bind     interface{}
	want     interface{}
	wantNull interface{} // 绑 nil 时读回来的值,通常是 nil
	skip     string
}

func TestDataTypeRoundTrip(t *testing.T) {
	db := openDataTypeDB(t)

	// 表里刻意不放 BINARY 列:arrowToValue 对整列报错,会让整个批次失败,连别的
	// 列一起读不出来。见 TestArrowToValueErrorPaths/BINARY_unsupported。
	cols := []roundTripColumn{
		{name: "c_tinyint", sqlType: "TINYINT", bind: int8(1), want: "1"},
		{name: "c_smallint", sqlType: "SMALLINT", bind: int16(2), want: "2"},
		{name: "c_int", sqlType: "INT", bind: int32(3), want: "3"},
		{name: "c_bigint", sqlType: "BIGINT", bind: int64(-4), want: "-4"},
		{name: "c_float", sqlType: "FLOAT", bind: float32(1.5), want: float32(1.5)},
		{name: "c_double", sqlType: "DOUBLE", bind: -2.5, want: -2.5},
		{name: "c_decimal", sqlType: "DECIMAL(10,2)", bind: "2.50", want: "2.50"},
		{name: "c_boolean", sqlType: "BOOLEAN", bind: true, want: true},
		// 反斜杠和单引号一起走一遍插值转义。
		{name: "c_string", sqlType: "STRING", bind: "a'b\\c", want: "a'b\\c"},
		{name: "c_char", sqlType: "CHAR(8)", bind: "abc", want: "abc"},
		{name: "c_varchar", sqlType: "VARCHAR(16)", bind: "xy", want: "xy"},
		{name: "c_date", sqlType: "DATE", bind: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
		{name: "c_ts", sqlType: "TIMESTAMP", bind: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC),
			want: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)},
	}

	tableName := fmt.Sprintf("goclickzetta_dt_it_%d", time.Now().UnixNano())
	createDataTypeTable(t, db, tableName, cols)

	t.Run("BoundValues", func(t *testing.T) {
		assertRoundTrip(t, db, tableName, cols, false)
	})
	t.Run("BoundNulls", func(t *testing.T) {
		assertRoundTrip(t, db, tableName, cols, true)
	})
}

func createDataTypeTable(t *testing.T, db *sql.DB, tableName string, cols []roundTripColumn) {
	t.Helper()
	defs := make([]string, 0, len(cols))
	for _, c := range cols {
		defs = append(defs, c.name+" "+c.sqlType)
	}
	ddl := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(defs, ", "))
	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("create table: %v\nDDL: %s", err, ddl)
	}
	t.Cleanup(func() {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tableName); err != nil {
			t.Errorf("drop table %s: %v", tableName, err)
		}
	})
}

// assertRoundTrip 用占位符绑定写一行,再读回来逐列比对。nulls 为真时所有列都绑
// nil,这样写入路径的 NULL 编码和读取路径的 NULL 解码一起被覆盖。
func assertRoundTrip(t *testing.T, db *sql.DB, tableName string, cols []roundTripColumn, nulls bool) {
	t.Helper()

	names := make([]string, 0, len(cols))
	marks := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.name)
		marks = append(marks, "?")
		if nulls {
			args = append(args, nil)
		} else {
			args = append(args, c.bind)
		}
	}

	marker := "vals"
	if nulls {
		marker = "nulls"
	}
	insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(names, ", "), strings.Join(marks, ", "))
	if _, err := db.Exec(insert, args...); err != nil {
		t.Fatalf("insert %s: %v\nSQL: %s", marker, err, insert)
	}

	// nulls 那一行所有列都是 NULL,用 c_int IS NULL 区分两行。
	predicate := "c_int IS NOT NULL"
	if nulls {
		predicate = "c_int IS NULL"
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(names, ", "), tableName, predicate)

	dest := make([]interface{}, len(cols))
	scan := make([]interface{}, len(cols))
	for i := range dest {
		scan[i] = &dest[i]
	}
	if err := db.QueryRow(query).Scan(scan...); err != nil {
		t.Fatalf("select %s: %v\nSQL: %s", marker, err, query)
	}

	for i, c := range cols {
		if c.skip != "" {
			t.Logf("%s: 跳过 (%s)", c.name, c.skip)
			continue
		}
		want := c.want
		if nulls {
			want = c.wantNull
		}
		if !reflect.DeepEqual(dest[i], want) {
			t.Errorf("%s (%s) = %#v (%T), want %#v (%T)", c.name, c.sqlType, dest[i], dest[i], want, want)
		}
	}
}
