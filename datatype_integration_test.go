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
		{name: "TINYINT", expr: "cast(1 as tinyint)", want: int8(1)},
		{name: "SMALLINT", expr: "cast(2 as smallint)", want: int16(2)},
		{name: "INT", expr: "cast(3 as int)", want: int32(3)},
		{name: "BIGINT", expr: "cast(4 as bigint)", want: int64(4)},
		{name: "FLOAT", expr: "cast(1.5 as float)", want: float32(1.5)},
		{name: "DOUBLE", expr: "cast(2.5 as double)", want: 2.5},
		{name: "BOOLEAN", expr: "true", want: true},
		{name: "STRING", expr: "'hello'", want: "hello"},

		// DECIMAL 走文本结果时保留声明的 scale,尾零不能被吃掉:客户端拿 2.5
		// 还是 2.50 是有区别的。
		{name: "DECIMAL_10_2", expr: "cast(2.5 as decimal(10,2))", want: "2.50"},
		{name: "DECIMAL_38_10", expr: "cast(1.23456789 as decimal(38,10))", want: "1.2345678900"},
		{name: "DECIMAL_scale0", expr: "cast(7 as decimal(10,0))", want: "7"},
		{name: "DECIMAL_negative", expr: "cast(-2.5 as decimal(10,2))", want: "-2.50"},

		// CHAR 的 length 元数据服务端用 JSON 字符串编码,解析失败会让整个
		// 结果集反序列化报错,而不只是丢掉宽度。
		{name: "CHAR", expr: "cast('abc' as char(8))", want: "abc"},
		{name: "VARCHAR", expr: "cast('xy' as varchar(16))", want: "xy"},

		{name: "DATE", expr: "date'2024-01-02'", want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
		// 墙上时间对得上,但 location 被无条件标成 UTC,而会话时区并不是 UTC。
		// 详见 TestDataTypeTimestampIgnoresSessionZone。
		{name: "TIMESTAMP_LTZ", expr: "timestamp'2024-01-02 03:04:05.123456'",
			want: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)},

		// BINARY 以十六进制文本回来,而不是 []byte。
		{name: "BINARY", expr: "unhex('4142')", want: "4142"},

		// 嵌套类型以 JSON 文本回来,再交给 json.Unmarshal,所以里面的数字一律是
		// float64:ARRAY<INT> 的元素读回来不是 int32。
		{name: "ARRAY", expr: "array(1,2,3)", want: []interface{}{float64(1), float64(2), float64(3)}},
		{name: "MAP", expr: "map('a',1)", want: map[string]interface{}{"a": float64(1)}},
		{name: "STRUCT", expr: "named_struct('x',1,'s','y')",
			want: map[string]interface{}{"x": float64(1), "s": "y"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}
			// 带一列常量:单列全 NULL 的结果行会被文本解析丢掉(见
			// TestDataTypeNullReadBack),多一列常量让所有用例走同一条路径。
			var guard int32
			var got interface{}
			if err := db.QueryRow("SELECT 1, "+tc.expr).Scan(&guard, &got); err != nil {
				t.Fatalf("query %s: %v", tc.expr, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s = %#v (%T), want %#v (%T)", tc.expr, got, got, tc.want, tc.want)
			}
		})
	}
}

// openDataTypeDB 打开一个跑集成测试用的连接池。没有配 CLICKZETTA_DSN 就跳过。
func openDataTypeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("clickzetta", integrationDSN(t))
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

// TestDataTypeNullReadBack 单独测 NULL,因为它有两个已知缺口,混在类型表里会
// 掩盖掉正常类型的失败。
func TestDataTypeNullReadBack(t *testing.T) {
	db := openDataTypeDB(t)

	// 多列时 NULL 是好的。
	t.Run("NullAlongsideValue", func(t *testing.T) {
		for _, expr := range []string{
			"cast(null as decimal(10,2))",
			"cast(null as string)",
			"cast(null as int)",
			"cast(null as date)",
			"cast(null as binary)",
		} {
			var guard int32
			var got interface{}
			if err := db.QueryRow("SELECT 1, "+expr).Scan(&guard, &got); err != nil {
				t.Errorf("query %s: %v", expr, err)
				continue
			}
			if got != nil {
				t.Errorf("%s = %#v, want nil", expr, got)
			}
		}
	})

	// 已知缺口 1:结果只有一列且该列为 NULL 时,这一行在文本结果里是一个空行,
	// csv.Reader 把空行丢掉,于是整个结果集变空。
	t.Run("SingleNullColumnRowIsDropped", func(t *testing.T) {
		var got interface{}
		err := db.QueryRow("SELECT cast(null as int)").Scan(&got)
		if err == nil {
			t.Fatal("单列 NULL 现在能读回来了,缺口已修,请删掉这个用例并把 NULL 并入类型表")
		}
		if err != sql.ErrNoRows {
			t.Fatalf("want %v, got %v", sql.ErrNoRows, err)
		}
	})

	// 已知缺口 2:文本结果里空串和 NULL 都是空字段,驱动一律当 NULL,所以空串
	// 读回来是 nil。
	t.Run("EmptyStringReadsAsNull", func(t *testing.T) {
		var guard int32
		var got interface{}
		if err := db.QueryRow("SELECT 1, cast('' as string)").Scan(&guard, &got); err != nil {
			t.Fatalf("query empty string: %v", err)
		}
		if got != nil {
			t.Fatalf("空串现在能和 NULL 区分了,缺口已修,请更新这个用例: got %#v", got)
		}
	})
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

	cols := []roundTripColumn{
		{name: "c_tinyint", sqlType: "TINYINT", bind: int8(1), want: int8(1)},
		{name: "c_smallint", sqlType: "SMALLINT", bind: int16(2), want: int16(2)},
		{name: "c_int", sqlType: "INT", bind: int32(3), want: int32(3)},
		{name: "c_bigint", sqlType: "BIGINT", bind: int64(-4), want: int64(-4)},
		{name: "c_float", sqlType: "FLOAT", bind: float32(1.5), want: float32(1.5)},
		{name: "c_double", sqlType: "DOUBLE", bind: -2.5, want: -2.5},
		{name: "c_decimal", sqlType: "DECIMAL(10,2)", bind: "2.50", want: "2.50"},
		{name: "c_boolean", sqlType: "BOOLEAN", bind: true, want: true},
		{name: "c_string", sqlType: "STRING", bind: "a'b\\c", want: "a'b\\c"},
		{name: "c_char", sqlType: "CHAR(8)", bind: "abc", want: "abc"},
		{name: "c_varchar", sqlType: "VARCHAR(16)", bind: "xy", want: "xy"},
		{name: "c_date", sqlType: "DATE", bind: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
		{name: "c_ts", sqlType: "TIMESTAMP", bind: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC),
			want: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC),
			skip: "时间戳往返不保瞬时值,见 TestDataTypeTimestampIgnoresSessionZone"},
		{name: "c_binary", sqlType: "BINARY", bind: []byte{0x41, 0x42}, want: "4142"},
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
	defs := make([]string, 0, len(cols)+1)
	defs = append(defs, "row_id BIGINT PRIMARY KEY")
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

	names := make([]string, 0, len(cols)+1)
	marks := make([]string, 0, len(cols)+1)
	args := make([]interface{}, 0, len(cols)+1)
	names = append(names, "row_id")
	marks = append(marks, "?")
	rowID := int64(1)
	if nulls {
		rowID = 2
	}
	args = append(args, rowID)
	for _, c := range cols {
		names = append(names, c.name)
		marks = append(marks, "?")
		if nulls {
			args = append(args, nil)
		} else {
			args = append(args, c.bind)
		}
	}

	// 每个子测试用自己的一行,读的时候按标记列过滤,避免两个子测试互相看见。
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
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(names[1:], ", "), tableName, predicate)

	dest := make([]interface{}, len(cols))
	scan := make([]interface{}, len(cols))
	for i := range dest {
		scan[i] = &dest[i]
	}
	row := db.QueryRow(query)
	if err := row.Scan(scan...); err != nil {
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

// TestDataTypeTimestampIgnoresSessionZone 钉住一个已知缺口:文本结果里的
// TIMESTAMP_LTZ 是服务端按会话时区渲染的墙上时间,不带偏移量,而驱动解析完
// 直接 .UTC(),等于把会话时区的墙上时间当成 UTC 的。
//
// 后果是 time.Time 往返不保瞬时值:绑一个 UTC 时刻进去,读回来的 time.Time 墙上
// 时间是会话时区的,location 却是 UTC,两者相差一个会话偏移量。会话时区就是 UTC
// 时看不出问题,所以这个测试在那种情况下跳过。
//
// 缺口修好之后这个测试会失败,那时把它删掉,并把 TestDataTypeRoundTrip 里 c_ts
// 的 skip 一起去掉。
func TestDataTypeTimestampIgnoresSessionZone(t *testing.T) {
	db := openDataTypeDB(t)

	var guard int32
	var zone string
	if err := db.QueryRow("SELECT 1, current_timezone()").Scan(&guard, &zone); err != nil {
		t.Fatalf("read session timezone: %v", err)
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		t.Skipf("会话时区 %q 在本机无法解析: %v", zone, err)
	}
	instant := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)
	_, offset := instant.In(loc).Zone()
	if offset == 0 {
		t.Skipf("会话时区 %q 偏移量为 0,这个缺口观察不到", zone)
	}

	var got time.Time
	if err := db.QueryRow("SELECT 1, cast(? as timestamp)", instant).Scan(&guard, &got); err != nil {
		t.Fatalf("round trip timestamp: %v", err)
	}

	if got.Equal(instant) {
		t.Fatalf("时间戳往返现在保瞬时值了,缺口已修:删掉本测试并去掉 c_ts 的 skip (got %v)", got)
	}
	// 当前行为:墙上时间是会话时区渲染出来的,location 却被标成 UTC。
	wantWall := instant.In(loc).Format("2006-01-02 15:04:05.999999")
	if gotWall := got.Format("2006-01-02 15:04:05.999999"); gotWall != wantWall {
		t.Errorf("墙上时间 = %s, want %s (会话时区 %s)", gotWall, wantWall, zone)
	}
	if got.Location() != time.UTC {
		t.Errorf("location = %v, want UTC (当前实现无条件标 UTC)", got.Location())
	}
	if d := got.Sub(instant); d != time.Duration(offset)*time.Second {
		t.Errorf("瞬时值偏差 = %v, want %v", d, time.Duration(offset)*time.Second)
	}
}
