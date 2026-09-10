package goclickzetta

import (
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

// formatIntValue 决定整型和定点数最终以什么 Go 类型交给调用方,scale 和
// higherPrecision 两个开关组合出四种行为,这里把四种都钉住。
func TestFormatIntValue(t *testing.T) {
	cases := []struct {
		name            string
		val             int64
		scale           int64
		higherPrecision bool
		want            interface{}
	}{
		// scale 为 0 且不要求高精度时返回的是十进制文本,不是整数。整型列因此
		// 一路读回来都是 string,见 TestDataTypeReadBack 里记录的缺口。
		{name: "scale0_text", val: 42, scale: 0, want: "42"},
		{name: "scale0_text_negative", val: -42, scale: 0, want: "-42"},
		{name: "scale0_int64", val: 42, scale: 0, higherPrecision: true, want: int64(42)},
		{name: "scale0_int64_min", val: -9223372036854775808, scale: 0, higherPrecision: true,
			want: int64(-9223372036854775808)},

		// scale 不为 0 时按 scale 补齐小数位,尾零保留。
		{name: "scale2_text", val: 250, scale: 2, want: "2.50"},
		{name: "scale2_text_negative", val: -250, scale: 2, want: "-2.50"},
		{name: "scale4_text_rounds", val: 12345, scale: 4, want: "1.2345"},
		{name: "scale2_text_zero", val: 0, scale: 2, want: "0.00"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var divisor float64
			if tc.scale != 0 && !tc.higherPrecision {
				divisor = 1
				for i := int64(0); i < tc.scale; i++ {
					divisor *= 10
				}
			}
			got := formatIntValue(tc.val, tc.scale, tc.higherPrecision, divisor)
			if got != tc.want {
				t.Errorf("formatIntValue(%d, %d, %v) = %#v (%T), want %#v (%T)",
					tc.val, tc.scale, tc.higherPrecision, got, got, tc.want, tc.want)
			}
		})
	}

	// 高精度 + 非零 scale 走 big.Float,单独比对,因为 big.Float 不能用 == 比。
	t.Run("scale2_bigfloat", func(t *testing.T) {
		got, ok := formatIntValue(250, 2, true, 0).(*big.Float)
		if !ok {
			t.Fatalf("want *big.Float")
		}
		if got.Text('f', 2) != "2.50" {
			t.Errorf("got %s, want 2.50", got.Text('f', 2))
		}
	})
}

func TestIntToBigFloat(t *testing.T) {
	cases := []struct {
		val   int64
		scale int64
		want  string
	}{
		{val: 250, scale: 2, want: "2.50"},
		{val: -250, scale: 2, want: "-2.50"},
		{val: 7, scale: 0, want: "7.00"},
		{val: 1, scale: 4, want: "0.00"},
	}
	for _, tc := range cases {
		got := intToBigFloat(tc.val, tc.scale)
		if s := got.Text('f', 2); s != tc.want {
			t.Errorf("intToBigFloat(%d, %d) = %s, want %s", tc.val, tc.scale, s, tc.want)
		}
	}
}

// decimalToBigInt / decimalToBigFloat 处理 128 位定点数,重点是负数和超出
// int64 的值不能在中途被截断。
func TestDecimalConversions(t *testing.T) {
	t.Run("BigInt", func(t *testing.T) {
		cases := []struct {
			name string
			num  decimal128.Num
			want string
		}{
			{name: "positive", num: decimal128.FromI64(250), want: "250"},
			{name: "negative", num: decimal128.FromI64(-250), want: "-250"},
			{name: "zero", num: decimal128.FromI64(0), want: "0"},
			{name: "int64_max", num: decimal128.FromI64(9223372036854775807), want: "9223372036854775807"},
			// 超过 int64 的值:高 64 位必须参与运算,否则结果会绕回去。
			{name: "beyond_int64", num: decimal128.New(1, 0), want: "18446744073709551616"},
			{name: "beyond_int64_negative", num: decimal128.New(-1, 0), want: "-18446744073709551616"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if got := decimalToBigInt(tc.num).String(); got != tc.want {
					t.Errorf("decimalToBigInt = %s, want %s", got, tc.want)
				}
			})
		}
	})

	t.Run("BigFloat", func(t *testing.T) {
		divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(2), nil))
		cases := []struct {
			num  decimal128.Num
			want string
		}{
			{num: decimal128.FromI64(250), want: "2.50"},
			{num: decimal128.FromI64(-250), want: "-2.50"},
			{num: decimal128.FromI64(0), want: "0.00"},
		}
		for _, tc := range cases {
			if got := decimalToBigFloat(tc.num, divisor).Text('f', 2); got != tc.want {
				t.Errorf("decimalToBigFloat(%v) = %s, want %s", tc.num, got, tc.want)
			}
		}
	})
}

// arrowToValue 是 arrow 结果集唯一的定型入口,一列一列地把 arrow 数组翻成
// []interface{}。下面按 clickzetta 类型逐个分支覆盖,包括 NULL 和不认识的类型。

// arrowCol 用 builder 造一列 arrow 数据。b 里塞值,返回的数组由调用方 Release。
func buildArray(t *testing.T, dt arrow.DataType, fill func(b array.Builder)) arrow.Array {
	t.Helper()
	b := array.NewBuilder(memory.NewGoAllocator(), dt)
	t.Cleanup(b.Release)
	fill(b)
	arr := b.NewArray()
	t.Cleanup(arr.Release)
	return arr
}

func TestArrowToValueScalarTypes(t *testing.T) {
	cases := []struct {
		name    string
		colType string
		scale   int64
		arr     func(t *testing.T) arrow.Array
		want    []interface{}
	}{
		{
			// 整型不带 scale 时以十进制文本交回,不是 int64。
			name: "INT64_as_text", colType: "INT64",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.PrimitiveTypes.Int64, func(b array.Builder) {
					ib := b.(*array.Int64Builder)
					ib.Append(1)
					ib.AppendNull()
					ib.Append(-9223372036854775808)
				})
			},
			want: []interface{}{"1", nil, "-9223372036854775808"},
		},
		{
			name: "INT32_as_text", colType: "INT32",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.PrimitiveTypes.Int32, func(b array.Builder) {
					b.(*array.Int32Builder).AppendValues([]int32{7, -7}, nil)
				})
			},
			want: []interface{}{"7", "-7"},
		},
		{
			name: "INT16_as_text", colType: "INT16",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.PrimitiveTypes.Int16, func(b array.Builder) {
					b.(*array.Int16Builder).AppendValues([]int16{-32768}, nil)
				})
			},
			want: []interface{}{"-32768"},
		},
		{
			name: "INT8_as_text", colType: "INT8",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.PrimitiveTypes.Int8, func(b array.Builder) {
					b.(*array.Int8Builder).AppendValues([]int8{-128}, nil)
				})
			},
			want: []interface{}{"-128"},
		},
		{
			name: "BOOLEAN", colType: "BOOLEAN",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.FixedWidthTypes.Boolean, func(b array.Builder) {
					bb := b.(*array.BooleanBuilder)
					bb.Append(true)
					bb.Append(false)
					bb.AppendNull()
				})
			},
			want: []interface{}{true, false, nil},
		},
		{
			name: "FLOAT64", colType: "FLOAT64",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.PrimitiveTypes.Float64, func(b array.Builder) {
					fb := b.(*array.Float64Builder)
					fb.Append(-2.5)
					fb.AppendNull()
				})
			},
			want: []interface{}{-2.5, nil},
		},
		{
			name: "FLOAT32", colType: "FLOAT32",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.PrimitiveTypes.Float32, func(b array.Builder) {
					b.(*array.Float32Builder).AppendValues([]float32{1.5}, nil)
				})
			},
			want: []interface{}{float32(1.5)},
		},
		{
			// STRING / VARCHAR / CHAR / JSON 共用一个分支,原样交回。
			name: "STRING", colType: "STRING",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.BinaryTypes.String, func(b array.Builder) {
					sb := b.(*array.StringBuilder)
					sb.Append("hello")
					sb.Append("")
					sb.AppendNull()
				})
			},
			// 空串和 NULL 在 arrow 里是分得开的。
			want: []interface{}{"hello", "", nil},
		},
		{
			name: "CHAR", colType: "CHAR",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.BinaryTypes.String, func(b array.Builder) {
					b.(*array.StringBuilder).Append("abc")
				})
			},
			want: []interface{}{"abc"},
		},
		{
			name: "JSON", colType: "JSON",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.BinaryTypes.String, func(b array.Builder) {
					b.(*array.StringBuilder).Append(`{"a":1}`)
				})
			},
			want: []interface{}{`{"a":1}`},
		},
		{
			// DATE 以 1970 起的天数传输,还原成 UTC 零点。
			name: "DATE", colType: "DATE",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, arrow.FixedWidthTypes.Date32, func(b array.Builder) {
					db := b.(*array.Date32Builder)
					db.Append(arrow.Date32(19724)) // 2024-01-02
					db.AppendNull()
				})
			},
			want: []interface{}{time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), nil},
		},
		{
			// TIMESTAMP_LTZ 按微秒解释并归一到 UTC,瞬时值保留。
			name: "TIMESTAMP_LTZ", colType: "TIMESTAMP_LTZ",
			arr: func(t *testing.T) arrow.Array {
				return buildArray(t, &arrow.TimestampType{Unit: arrow.Microsecond}, func(b array.Builder) {
					tb := b.(*array.TimestampBuilder)
					tb.Append(arrow.Timestamp(1704164645123456))
					tb.AppendNull()
				})
			},
			want: []interface{}{time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC), nil},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arr := tc.arr(t)
			dest := make([]interface{}, arr.Len())
			meta := execResponseColumnType{Name: "c", Type: tc.colType, Scale: tc.scale}
			if err := arrowToValue(dest, meta, arr, time.UTC, false); err != nil {
				t.Fatalf("arrowToValue: %v", err)
			}
			if !reflect.DeepEqual(dest, tc.want) {
				t.Errorf("dest = %#v, want %#v", dest, tc.want)
			}
		})
	}
}

// DECIMAL 是之前出过精度丢失问题的分支:scale 必须从列元数据来,渲染时尾零要留住。
func TestArrowToValueDecimal(t *testing.T) {
	decArray := func(t *testing.T, precision, scale int32, vals ...int64) arrow.Array {
		dt := &arrow.Decimal128Type{Precision: precision, Scale: scale}
		return buildArray(t, dt, func(b array.Builder) {
			db := b.(*array.Decimal128Builder)
			for _, v := range vals {
				db.Append(decimal128.FromI64(v))
			}
			db.AppendNull()
		})
	}

	t.Run("scale2", func(t *testing.T) {
		arr := decArray(t, 10, 2, 250, -250, 0)
		dest := make([]interface{}, arr.Len())
		meta := execResponseColumnType{Type: "DECIMAL", Precision: 10, Scale: 2}
		if err := arrowToValue(dest, meta, arr, time.UTC, false); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		want := []interface{}{"2.50", "-2.50", "0.00", nil}
		if !reflect.DeepEqual(dest, want) {
			t.Errorf("dest = %#v, want %#v", dest, want)
		}
	})

	t.Run("scale0", func(t *testing.T) {
		arr := decArray(t, 10, 0, 7, -7)
		dest := make([]interface{}, arr.Len())
		meta := execResponseColumnType{Type: "DECIMAL", Precision: 10, Scale: 0}
		if err := arrowToValue(dest, meta, arr, time.UTC, false); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		want := []interface{}{"7", "-7", nil}
		if !reflect.DeepEqual(dest, want) {
			t.Errorf("dest = %#v, want %#v", dest, want)
		}
	})

	// scale 报 0 而数据本身带小数位时,渲染出来就是没有小数点的整数文本。元数据
	// 解析错了会长这样,所以这条钉住的是"结果完全跟着元数据走"。
	t.Run("scale_mismatch_follows_metadata", func(t *testing.T) {
		arr := decArray(t, 10, 2, 250)
		dest := make([]interface{}, arr.Len())
		meta := execResponseColumnType{Type: "DECIMAL", Precision: 10, Scale: 0}
		if err := arrowToValue(dest, meta, arr, time.UTC, false); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		if dest[0] != "250" {
			t.Errorf("dest[0] = %#v, want \"250\"", dest[0])
		}
	})

	t.Run("higherPrecision_scale0_bigint", func(t *testing.T) {
		arr := decArray(t, 10, 0, 7)
		dest := make([]interface{}, arr.Len())
		meta := execResponseColumnType{Type: "DECIMAL", Precision: 10, Scale: 0}
		if err := arrowToValue(dest, meta, arr, time.UTC, true); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		got, ok := dest[0].(*big.Int)
		if !ok {
			t.Fatalf("dest[0] = %#v (%T), want *big.Int", dest[0], dest[0])
		}
		if got.String() != "7" {
			t.Errorf("got %s, want 7", got)
		}
	})

	t.Run("higherPrecision_scale2_bigfloat", func(t *testing.T) {
		arr := decArray(t, 10, 2, 250)
		dest := make([]interface{}, arr.Len())
		meta := execResponseColumnType{Type: "DECIMAL", Precision: 10, Scale: 2}
		if err := arrowToValue(dest, meta, arr, time.UTC, true); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		got, ok := dest[0].(*big.Float)
		if !ok {
			t.Fatalf("dest[0] = %#v (%T), want *big.Float", dest[0], dest[0])
		}
		if got.Text('f', 2) != "2.50" {
			t.Errorf("got %s, want 2.50", got.Text('f', 2))
		}
	})

	// 整型列在高精度模式下才交回真正的 int64。
	t.Run("higherPrecision_int64", func(t *testing.T) {
		arr := buildArray(t, arrow.PrimitiveTypes.Int64, func(b array.Builder) {
			b.(*array.Int64Builder).AppendValues([]int64{42}, nil)
		})
		dest := make([]interface{}, arr.Len())
		if err := arrowToValue(dest, execResponseColumnType{Type: "INT64"}, arr, time.UTC, true); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		if dest[0] != int64(42) {
			t.Errorf("dest[0] = %#v (%T), want int64(42)", dest[0], dest[0])
		}
	})
}

// 嵌套类型会按元素类型收成同构切片/映射,而不是 []interface{}。代价是 NULL 元素
// 被压成零值,这一点也一起钉住。
func TestArrowToValueNestedTypes(t *testing.T) {
	t.Run("ARRAY_int32", func(t *testing.T) {
		arr := buildArray(t, arrow.ListOf(arrow.PrimitiveTypes.Int32), func(b array.Builder) {
			lb := b.(*array.ListBuilder)
			vb := lb.ValueBuilder().(*array.Int32Builder)
			lb.Append(true)
			vb.AppendValues([]int32{1, 2, 3}, nil)
			lb.Append(true) // 空数组
			lb.AppendNull()
		})
		dest := make([]interface{}, arr.Len())
		if err := arrowToValue(dest, execResponseColumnType{Type: "ARRAY"}, arr, time.UTC, false); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		want := []interface{}{[]int32{1, 2, 3}, []int32{}, nil}
		if !reflect.DeepEqual(dest, want) {
			t.Errorf("dest = %#v, want %#v", dest, want)
		}
	})

	t.Run("ARRAY_string_null_element_becomes_empty", func(t *testing.T) {
		arr := buildArray(t, arrow.ListOf(arrow.BinaryTypes.String), func(b array.Builder) {
			lb := b.(*array.ListBuilder)
			vb := lb.ValueBuilder().(*array.StringBuilder)
			lb.Append(true)
			vb.Append("a")
			vb.AppendNull()
		})
		dest := make([]interface{}, arr.Len())
		if err := arrowToValue(dest, execResponseColumnType{Type: "ARRAY"}, arr, time.UTC, false); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		// 元素里的 NULL 和空串在这里分不开了。
		want := []interface{}{[]string{"a", ""}}
		if !reflect.DeepEqual(dest, want) {
			t.Errorf("dest = %#v, want %#v", dest, want)
		}
	})

	t.Run("ARRAY_int64_null_element_becomes_zero", func(t *testing.T) {
		arr := buildArray(t, arrow.ListOf(arrow.PrimitiveTypes.Int64), func(b array.Builder) {
			lb := b.(*array.ListBuilder)
			vb := lb.ValueBuilder().(*array.Int64Builder)
			lb.Append(true)
			vb.Append(5)
			vb.AppendNull()
		})
		dest := make([]interface{}, arr.Len())
		if err := arrowToValue(dest, execResponseColumnType{Type: "ARRAY"}, arr, time.UTC, false); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		// NULL 元素压成 0,和真的 0 分不开。
		want := []interface{}{[]int64{5, 0}}
		if !reflect.DeepEqual(dest, want) {
			t.Errorf("dest = %#v, want %#v", dest, want)
		}
	})

	t.Run("ARRAY_fixed_size_list", func(t *testing.T) {
		arr := buildArray(t, arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64), func(b array.Builder) {
			lb := b.(*array.FixedSizeListBuilder)
			vb := lb.ValueBuilder().(*array.Float64Builder)
			lb.Append(true)
			vb.AppendValues([]float64{1.5, 2.5}, nil)
		})
		dest := make([]interface{}, arr.Len())
		if err := arrowToValue(dest, execResponseColumnType{Type: "ARRAY"}, arr, time.UTC, false); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		want := []interface{}{[]float64{1.5, 2.5}}
		if !reflect.DeepEqual(dest, want) {
			t.Errorf("dest = %#v, want %#v", dest, want)
		}
	})

	t.Run("MAP_string_to_int32", func(t *testing.T) {
		arr := buildArray(t, arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), func(b array.Builder) {
			mb := b.(*array.MapBuilder)
			kb := mb.KeyBuilder().(*array.StringBuilder)
			vb := mb.ItemBuilder().(*array.Int32Builder)
			mb.Append(true)
			kb.Append("a")
			vb.Append(1)
			kb.Append("b")
			vb.Append(2)
			mb.AppendNull()
		})
		dest := make([]interface{}, arr.Len())
		if err := arrowToValue(dest, execResponseColumnType{Type: "MAP"}, arr, time.UTC, false); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		want := []interface{}{map[string]int32{"a": 1, "b": 2}, nil}
		if !reflect.DeepEqual(dest, want) {
			t.Errorf("dest = %#v, want %#v", dest, want)
		}
	})

	t.Run("STRUCT", func(t *testing.T) {
		dt := arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			arrow.Field{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		)
		arr := buildArray(t, dt, func(b array.Builder) {
			sb := b.(*array.StructBuilder)
			sb.Append(true)
			sb.FieldBuilder(0).(*array.Int32Builder).Append(1)
			sb.FieldBuilder(1).(*array.StringBuilder).Append("y")
			sb.AppendNull()
		})
		dest := make([]interface{}, arr.Len())
		if err := arrowToValue(dest, execResponseColumnType{Type: "STRUCT"}, arr, time.UTC, false); err != nil {
			t.Fatalf("arrowToValue: %v", err)
		}
		want := []interface{}{map[string]interface{}{"x": int32(1), "s": "y"}, nil}
		if !reflect.DeepEqual(dest, want) {
			t.Errorf("dest = %#v, want %#v", dest, want)
		}
	})
}

func TestArrowToValueErrorPaths(t *testing.T) {
	// BINARY 不在 clickzettaTypes 里,整列直接报错。上层把这个错当成批次失败,
	// 结果是查一个 BINARY 列拿到空结果集,不是一个能看懂的错误。
	t.Run("BINARY_unsupported", func(t *testing.T) {
		arr := buildArray(t, arrow.BinaryTypes.Binary, func(b array.Builder) {
			b.(*array.BinaryBuilder).Append([]byte{0x41, 0x42})
		})
		dest := make([]interface{}, arr.Len())
		err := arrowToValue(dest, execResponseColumnType{Type: "BINARY"}, arr, time.UTC, false)
		if err == nil {
			t.Fatal("BINARY 现在支持了,请把它并入类型表并更新冒烟测试")
		}
		if err.Error() != "unsupported data type" {
			t.Errorf("err = %v, want \"unsupported data type\"", err)
		}
	})

	t.Run("unknown_type_name", func(t *testing.T) {
		arr := buildArray(t, arrow.BinaryTypes.String, func(b array.Builder) {
			b.(*array.StringBuilder).Append("x")
		})
		dest := make([]interface{}, arr.Len())
		if err := arrowToValue(dest, execResponseColumnType{Type: "SOMETHING_NEW"}, arr, time.UTC, false); err == nil {
			t.Error("不认识的类型名应当报错")
		}
	})

	// destcol 长度和 arrow 数组不一致时报错,但已经能填的值仍然填进去了。
	t.Run("length_mismatch_reports_and_still_fills", func(t *testing.T) {
		arr := buildArray(t, arrow.FixedWidthTypes.Boolean, func(b array.Builder) {
			bb := b.(*array.BooleanBuilder)
			bb.Append(true)
			bb.Append(false)
		})
		dest := make([]interface{}, 2)
		if err := arrowToValue(dest, execResponseColumnType{Type: "BOOLEAN"}, arr, time.UTC, false); err != nil {
			t.Fatalf("等长时不该报错: %v", err)
		}
		short := make([]interface{}, 1)
		err := arrowToValue(short, execResponseColumnType{Type: "BOOLEAN"}, arr, time.UTC, false)
		if err == nil {
			t.Fatal("长度不一致应当报错")
		}
		if err.Error() != "array interface length mismatch" {
			t.Errorf("err = %v", err)
		}
	})

	// 类型名对上了但 arrow 数组的物理类型不对,报的是带类型名的错,便于定位。
	t.Run("ARRAY_wrong_arrow_type", func(t *testing.T) {
		arr := buildArray(t, arrow.BinaryTypes.String, func(b array.Builder) {
			b.(*array.StringBuilder).Append("x")
		})
		dest := make([]interface{}, arr.Len())
		err := arrowToValue(dest, execResponseColumnType{Type: "ARRAY"}, arr, time.UTC, false)
		if err == nil || !strings.Contains(err.Error(), "unsupported ARRAY arrow type") {
			t.Errorf("err = %v, want unsupported ARRAY arrow type", err)
		}
	})

	t.Run("STRUCT_wrong_arrow_type", func(t *testing.T) {
		arr := buildArray(t, arrow.BinaryTypes.String, func(b array.Builder) {
			b.(*array.StringBuilder).Append("x")
		})
		dest := make([]interface{}, arr.Len())
		err := arrowToValue(dest, execResponseColumnType{Type: "STRUCT"}, arr, time.UTC, false)
		if err == nil || !strings.Contains(err.Error(), "unsupported STRUCT arrow type") {
			t.Errorf("err = %v, want unsupported STRUCT arrow type", err)
		}
	})
}

// TIMESTAMP_NTZ 不带时区,墙上时间要原样保留,不能像 LTZ 那样归一到 UTC。
func TestArrowToValueTimestampNTZ(t *testing.T) {
	arr := buildArray(t, &arrow.TimestampType{Unit: arrow.Microsecond}, func(b array.Builder) {
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(1704164645123456))
	})
	dest := make([]interface{}, arr.Len())
	if err := arrowToValue(dest, execResponseColumnType{Type: "TIMESTAMP_NTZ"}, arr, time.UTC, false); err != nil {
		t.Fatalf("arrowToValue: %v", err)
	}
	got, ok := dest[0].(time.Time)
	if !ok {
		t.Fatalf("dest[0] = %#v (%T), want time.Time", dest[0], dest[0])
	}
	if w := got.Format("2006-01-02 15:04:05.999999"); w != "2024-01-02 03:04:05.123456" {
		t.Errorf("墙上时间 = %s, want 2024-01-02 03:04:05.123456", w)
	}
}

// getMapKeyString 把任意 key 数组的一项转成 map 的字符串键。
func TestGetMapKeyString(t *testing.T) {
	t.Run("string_keys", func(t *testing.T) {
		keys := buildArray(t, arrow.BinaryTypes.String, func(b array.Builder) {
			b.(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)
		})
		if got := getMapKeyString(keys, 1); got != "b" {
			t.Errorf("got %q, want b", got)
		}
	})
	t.Run("int_keys_are_stringified", func(t *testing.T) {
		keys := buildArray(t, arrow.PrimitiveTypes.Int32, func(b array.Builder) {
			b.(*array.Int32Builder).AppendValues([]int32{7}, nil)
		})
		if got := getMapKeyString(keys, 0); got != "7" {
			t.Errorf("got %q, want 7", got)
		}
	})
}

// arrowMapToTypedGoMap 按 value 的类型收成同构 map,下面把各个 value 类型分支
// 走一遍。
func TestArrowMapToTypedGoMap(t *testing.T) {
	buildMap := func(t *testing.T, valueType arrow.DataType, fill func(kb *array.StringBuilder, items array.Builder)) *array.Map {
		t.Helper()
		arr := buildArray(t, arrow.MapOf(arrow.BinaryTypes.String, valueType), func(b array.Builder) {
			mb := b.(*array.MapBuilder)
			mb.Append(true)
			fill(mb.KeyBuilder().(*array.StringBuilder), mb.ItemBuilder())
		})
		return arr.(*array.Map)
	}

	t.Run("string_values_null_becomes_empty", func(t *testing.T) {
		m := buildMap(t, arrow.BinaryTypes.String, func(kb *array.StringBuilder, items array.Builder) {
			vb := items.(*array.StringBuilder)
			kb.Append("a")
			vb.Append("x")
			kb.Append("b")
			vb.AppendNull()
		})
		want := map[string]string{"a": "x", "b": ""}
		if got := arrowMapToTypedGoMap(m, 0); !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
	})

	t.Run("int64_values", func(t *testing.T) {
		m := buildMap(t, arrow.PrimitiveTypes.Int64, func(kb *array.StringBuilder, items array.Builder) {
			kb.Append("a")
			items.(*array.Int64Builder).Append(7)
		})
		want := map[string]int64{"a": 7}
		if got := arrowMapToTypedGoMap(m, 0); !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
	})

	t.Run("float64_values", func(t *testing.T) {
		m := buildMap(t, arrow.PrimitiveTypes.Float64, func(kb *array.StringBuilder, items array.Builder) {
			kb.Append("a")
			items.(*array.Float64Builder).Append(2.5)
		})
		want := map[string]float64{"a": 2.5}
		if got := arrowMapToTypedGoMap(m, 0); !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
	})

	t.Run("bool_values", func(t *testing.T) {
		m := buildMap(t, arrow.FixedWidthTypes.Boolean, func(kb *array.StringBuilder, items array.Builder) {
			kb.Append("a")
			items.(*array.BooleanBuilder).Append(true)
		})
		want := map[string]bool{"a": true}
		if got := arrowMapToTypedGoMap(m, 0); !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
	})

	// value 类型没有专门分支时退回 map[string]interface{}。
	t.Run("fallback_to_interface_values", func(t *testing.T) {
		m := buildMap(t, arrow.PrimitiveTypes.Float32, func(kb *array.StringBuilder, items array.Builder) {
			kb.Append("a")
			items.(*array.Float32Builder).Append(1.5)
		})
		want := map[string]interface{}{"a": float32(1.5)}
		if got := arrowMapToTypedGoMap(m, 0); !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v (%T), want %#v", got, got, want)
		}
	})

	t.Run("null_map_is_nil", func(t *testing.T) {
		arr := buildArray(t, arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), func(b array.Builder) {
			b.(*array.MapBuilder).AppendNull()
		})
		if got := arrowMapToTypedGoMap(arr.(*array.Map), 0); got != nil {
			t.Errorf("got %#v, want nil", got)
		}
	})
}

// TestArrowValueToInterfaceNestedList 钉住一个 bug:arrowValueToInterface 里
// `case *array.List:` 的分支体是空的,Go 不会往下贯穿,于是落到函数末尾 return
// nil。嵌在 STRUCT / MAP 里的 ARRAY 字段因此读回来是 nil,而不是它的元素。
// 相邻的 *array.LargeList 分支才带着解码逻辑,两个 case 本该合成一个。
//
// 修好之后这个测试会失败,那时把断言改成期望真正的元素。
func TestArrowValueToInterfaceNestedList(t *testing.T) {
	dt := arrow.StructOf(
		arrow.Field{Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	)
	arr := buildArray(t, dt, func(b array.Builder) {
		sb := b.(*array.StructBuilder)
		lb := sb.FieldBuilder(0).(*array.ListBuilder)
		sb.Append(true)
		lb.Append(true)
		lb.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	})

	got := arrowStructToGoMap(arr.(*array.Struct), 0)
	if v, ok := got["items"]; !ok {
		t.Fatalf("结构体里没有 items 字段: %#v", got)
	} else if v != nil {
		t.Fatalf("嵌套 ARRAY 现在能解出来了,bug 已修:请把断言改成期望元素 (got %#v)", v)
	}
}

// 嵌在结构体里的 MAP 走的是 arrowMapToGoMap,值一律是 interface{}。
func TestArrowStructWithNestedMap(t *testing.T) {
	dt := arrow.StructOf(
		arrow.Field{Name: "m", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), Nullable: true},
	)
	arr := buildArray(t, dt, func(b array.Builder) {
		sb := b.(*array.StructBuilder)
		mb := sb.FieldBuilder(0).(*array.MapBuilder)
		sb.Append(true)
		mb.Append(true)
		mb.KeyBuilder().(*array.StringBuilder).Append("a")
		mb.ItemBuilder().(*array.Int32Builder).Append(1)
	})

	got := arrowStructToGoMap(arr.(*array.Struct), 0)
	want := map[string]interface{}{"m": map[string]interface{}{"a": int32(1)}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}
