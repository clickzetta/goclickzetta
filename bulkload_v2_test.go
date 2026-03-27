package goclickzetta

import (
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/clickzetta/goclickzetta/protos/bulkload/ingestion"
	"github.com/clickzetta/goclickzetta/protos/bulkload/util"
	"github.com/shopspring/decimal"
	"github.com/zeebo/assert"
)

// --- BulkloadMetadata tests ---

func newTestMetadata(op ingestion.BulkLoadStreamOperation, state ingestion.BulkLoadStreamState) *BulkloadMetadata {
	return &BulkloadMetadata{
		StreamInfo: &ingestion.BulkLoadStreamInfo{
			Operation:     op,
			StreamState:   state,
			StreamId:      "test-stream-1",
			PartitionSpec: "pt=v1",
			RecordKeys:    []string{"id"},
			SqlErrorMsg:   "some error",
		},
	}
}

func TestBulkloadMetadata_GetOperation(t *testing.T) {
	assert.Equal(t, APPEND, newTestMetadata(ingestion.BulkLoadStreamOperation_BL_APPEND, 0).GetOperation())
	assert.Equal(t, UPSERT, newTestMetadata(ingestion.BulkLoadStreamOperation_BL_UPSERT, 0).GetOperation())
	assert.Equal(t, OVERWRITE, newTestMetadata(ingestion.BulkLoadStreamOperation_BL_OVERWRITE, 0).GetOperation())
}

func TestBulkloadMetadata_GetState(t *testing.T) {
	tests := []struct {
		state  ingestion.BulkLoadStreamState
		expect BulkLoadState
	}{
		{ingestion.BulkLoadStreamState_BL_CREATED, CREATED},
		{ingestion.BulkLoadStreamState_BL_SEALED, SEALED},
		{ingestion.BulkLoadStreamState_BL_COMMIT_SUBMITTED, COMMIT_SUBMITTED},
		{ingestion.BulkLoadStreamState_BL_COMMIT_SUCCESS, COMMIT_SUCCESS},
		{ingestion.BulkLoadStreamState_BL_COMMIT_FAILED, COMMIT_FAILED},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expect, newTestMetadata(0, tt.state).GetState())
	}
}

func TestBulkloadMetadata_Accessors(t *testing.T) {
	m := newTestMetadata(0, 0)
	assert.Equal(t, "some error", m.GetSQLErrorMsg())
	assert.Equal(t, "pt=v1", m.GetPartitionSpec())
	assert.DeepEqual(t, []string{"id"}, m.GetRecordKeys())
}

// --- Row setter tests ---

func newTestRow() *Row {
	return &Row{
		Columns:          map[string]*util.DataType{},
		TableName:        "test_table",
		ColumnNameValues: map[string]interface{}{},
	}
}

func TestRow_SettersHappyPath(t *testing.T) {
	row := newTestRow()
	assert.NoError(t, row.SetBoolean("b", true))
	assert.NoError(t, row.SetTinyInt("ti", int8(1)))
	assert.NoError(t, row.SetSmallInt("si", int16(2)))
	assert.NoError(t, row.SetInt("i", int32(3)))
	assert.NoError(t, row.SetBigint("bi", int64(4)))
	assert.NoError(t, row.SetFloat("f", float32(1.1)))
	assert.NoError(t, row.SetDouble("d", float64(2.2)))
	assert.NoError(t, row.SetDecimal("dec", decimal.NewFromFloat(3.3)))
	assert.NoError(t, row.SetString("s", "hello"))
	assert.NoError(t, row.SetDate("dt", "2024-01-01"))
	assert.NoError(t, row.SetTimestamp("ts", "2024-01-01 00:00:00"))

	assert.Equal(t, true, row.ColumnNameValues["b"])
	assert.Equal(t, int8(1), row.ColumnNameValues["ti"])
	assert.Equal(t, int64(4), row.ColumnNameValues["bi"])
	assert.Equal(t, "hello", row.ColumnNameValues["s"])
}

func TestRow_SettersWrongType(t *testing.T) {
	row := newTestRow()
	assert.Error(t, row.SetBoolean("b", "not_bool"))
	assert.Error(t, row.SetTinyInt("ti", int64(1)))
	assert.Error(t, row.SetSmallInt("si", int64(1)))
	assert.Error(t, row.SetInt("i", int64(1)))
	assert.Error(t, row.SetBigint("bi", int32(1)))
	assert.Error(t, row.SetFloat("f", float64(1.0)))
	assert.Error(t, row.SetDouble("d", float32(1.0)))
	assert.Error(t, row.SetDecimal("dec", 1.0))
	assert.Error(t, row.SetString("s", 123))
	assert.Error(t, row.SetDate("dt", 123))
	assert.Error(t, row.SetTimestamp("ts", 123))
}

// --- ConvertToArrowDataType tests ---

func TestConvertToArrowDataType(t *testing.T) {
	tests := []struct {
		name     string
		dataType *util.DataType
		expected arrow.DataType
	}{
		{"boolean", &util.DataType{Category: util.DataTypeCategory_BOOLEAN}, arrow.FixedWidthTypes.Boolean},
		{"int8", &util.DataType{Category: util.DataTypeCategory_INT8}, arrow.PrimitiveTypes.Int8},
		{"int16", &util.DataType{Category: util.DataTypeCategory_INT16}, arrow.PrimitiveTypes.Int16},
		{"int32", &util.DataType{Category: util.DataTypeCategory_INT32}, arrow.PrimitiveTypes.Int32},
		{"int64", &util.DataType{Category: util.DataTypeCategory_INT64}, arrow.PrimitiveTypes.Int64},
		{"float32", &util.DataType{Category: util.DataTypeCategory_FLOAT32}, arrow.PrimitiveTypes.Float32},
		{"float64", &util.DataType{Category: util.DataTypeCategory_FLOAT64}, arrow.PrimitiveTypes.Float64},
		{"string", &util.DataType{Category: util.DataTypeCategory_STRING}, arrow.BinaryTypes.String},
		{"char", &util.DataType{Category: util.DataTypeCategory_CHAR}, arrow.BinaryTypes.String},
		{"varchar", &util.DataType{Category: util.DataTypeCategory_VARCHAR}, arrow.BinaryTypes.String},
		{"date", &util.DataType{Category: util.DataTypeCategory_DATE}, arrow.FixedWidthTypes.Date32},
		{"decimal", &util.DataType{
			Category: util.DataTypeCategory_DECIMAL,
			Info:     &util.DataType_DecimalTypeInfo{DecimalTypeInfo: &util.DecimalTypeInfo{Precision: 10, Scale: 2}},
		}, &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{"timestamp_s", &util.DataType{
			Category: util.DataTypeCategory_TIMESTAMP_LTZ,
			Info:     &util.DataType_TimestampInfo{TimestampInfo: &util.TimestampInfo{TsUnit: util.TimestampUnit_SECONDS}},
		}, arrow.FixedWidthTypes.Timestamp_s},
		{"timestamp_ms", &util.DataType{
			Category: util.DataTypeCategory_TIMESTAMP_LTZ,
			Info:     &util.DataType_TimestampInfo{TimestampInfo: &util.TimestampInfo{TsUnit: util.TimestampUnit_MILLISECONDS}},
		}, arrow.FixedWidthTypes.Timestamp_ms},
		{"timestamp_us", &util.DataType{
			Category: util.DataTypeCategory_TIMESTAMP_LTZ,
			Info:     &util.DataType_TimestampInfo{TimestampInfo: &util.TimestampInfo{TsUnit: util.TimestampUnit_MICROSECONDS}},
		}, arrow.FixedWidthTypes.Timestamp_us},
		{"timestamp_ns", &util.DataType{
			Category: util.DataTypeCategory_TIMESTAMP_LTZ,
			Info:     &util.DataType_TimestampInfo{TimestampInfo: &util.TimestampInfo{TsUnit: util.TimestampUnit_NANOSECONDS}},
		}, arrow.FixedWidthTypes.Timestamp_ns},
		{"json", &util.DataType{Category: util.DataTypeCategory_JSON}, arrow.BinaryTypes.String},
		{"binary", &util.DataType{Category: util.DataTypeCategory_BINARY}, arrow.BinaryTypes.Binary},
		{"bitmap", &util.DataType{Category: util.DataTypeCategory_BITMAP}, arrow.BinaryTypes.Binary},
		{"timestamp_ntz", &util.DataType{Category: util.DataTypeCategory_TIMESTAMP_NTZ}, &arrow.TimestampType{Unit: arrow.Microsecond}},
		{"vector_f32_4", &util.DataType{
			Category: util.DataTypeCategory_VECTOR_TYPE,
			Info:     &util.DataType_VectorInfo{VectorInfo: &util.VectorTypeInfo{NumberType: util.VectorNumberType_F32, Dimension: 4}},
		}, &arrow.FixedSizeBinaryType{ByteWidth: 16}},
		{"vector_i8_8", &util.DataType{
			Category: util.DataTypeCategory_VECTOR_TYPE,
			Info:     &util.DataType_VectorInfo{VectorInfo: &util.VectorTypeInfo{NumberType: util.VectorNumberType_I8, Dimension: 8}},
		}, &arrow.FixedSizeBinaryType{ByteWidth: 8}},
		{"array_int32", &util.DataType{
			Category: util.DataTypeCategory_ARRAY,
			Info: &util.DataType_ArrayTypeInfo{ArrayTypeInfo: &util.ArrayTypeInfo{
				ElementType: &util.DataType{Category: util.DataTypeCategory_INT32},
			}},
		}, arrow.ListOfField(arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Int32})},
		{"map_string_int64", &util.DataType{
			Category: util.DataTypeCategory_MAP,
			Info: &util.DataType_MapTypeInfo{MapTypeInfo: &util.MapTypeInfo{
				KeyType:   &util.DataType{Category: util.DataTypeCategory_STRING},
				ValueType: &util.DataType{Category: util.DataTypeCategory_INT64},
			}},
		}, arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
		{"struct_simple", &util.DataType{
			Category: util.DataTypeCategory_STRUCT,
			Info: &util.DataType_StructTypeInfo{StructTypeInfo: &util.StructTypeInfo{
				Fields: []*util.StructTypeInfo_Field{
					{Name: "name", Type: &util.DataType{Category: util.DataTypeCategory_STRING}},
					{Name: "age", Type: &util.DataType{Category: util.DataTypeCategory_INT32}},
				},
			}},
		}, arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32},
		)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertToArrowDataType(tt.dataType)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestConvertToArrowDataType_Unsupported(t *testing.T) {
	_, err := ConvertToArrowDataType(&util.DataType{Category: util.DataTypeCategory_VOID})
	assert.Error(t, err)
}

// --- ConvertToArrowValue tests ---

func TestConvertToArrowValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		dataType *util.DataType
		expected string
	}{
		{"bool", true, &util.DataType{Category: util.DataTypeCategory_BOOLEAN}, "true"},
		{"int8", int8(42), &util.DataType{Category: util.DataTypeCategory_INT8}, "42"},
		{"int16", int16(300), &util.DataType{Category: util.DataTypeCategory_INT16}, "300"},
		{"int32", int32(100000), &util.DataType{Category: util.DataTypeCategory_INT32}, "100000"},
		{"int64", int64(999999), &util.DataType{Category: util.DataTypeCategory_INT64}, "999999"},
		{"float32", float32(1.5), &util.DataType{Category: util.DataTypeCategory_FLOAT32}, "1.5"},
		{"float64", float64(2.5), &util.DataType{Category: util.DataTypeCategory_FLOAT64}, "2.5"},
		{"decimal", decimal.NewFromFloat(3.14), &util.DataType{Category: util.DataTypeCategory_DECIMAL}, "3.14"},
		{"string", "hello", &util.DataType{Category: util.DataTypeCategory_STRING}, "hello"},
		{"date", "2024-01-01", &util.DataType{Category: util.DataTypeCategory_DATE}, "2024-01-01"},
		{"char", "c", &util.DataType{Category: util.DataTypeCategory_CHAR}, "c"},
		{"varchar", "vc", &util.DataType{Category: util.DataTypeCategory_VARCHAR}, "vc"},
		{"timestamp", "2024-01-01 00:00:00", &util.DataType{Category: util.DataTypeCategory_TIMESTAMP_LTZ}, "2024-01-01 00:00:00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertToArrowValue(tt.value, tt.dataType)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestConvertToArrowValue_WrongType(t *testing.T) {
	_, err := ConvertToArrowValue("not_bool", &util.DataType{Category: util.DataTypeCategory_BOOLEAN})
	assert.Error(t, err)
	_, err = ConvertToArrowValue("not_int", &util.DataType{Category: util.DataTypeCategory_INT64})
	assert.Error(t, err)
}

// --- AppendValueToArrowField tests ---

func TestAppendValueToArrowField_AllTypes(t *testing.T) {
	mem := memory.DefaultAllocator

	tests := []struct {
		name    string
		builder array.Builder
		value   interface{}
		tpe     *util.DataType
	}{
		{"bool", array.NewBooleanBuilder(mem), true, &util.DataType{Category: util.DataTypeCategory_BOOLEAN}},
		{"int8", array.NewInt8Builder(mem), int8(1), &util.DataType{Category: util.DataTypeCategory_INT8}},
		{"int16", array.NewInt16Builder(mem), int16(2), &util.DataType{Category: util.DataTypeCategory_INT16}},
		{"int32", array.NewInt32Builder(mem), int32(3), &util.DataType{Category: util.DataTypeCategory_INT32}},
		{"int64", array.NewInt64Builder(mem), int64(4), &util.DataType{Category: util.DataTypeCategory_INT64}},
		{"float32", array.NewFloat32Builder(mem), float32(1.1), &util.DataType{Category: util.DataTypeCategory_FLOAT32}},
		{"float64", array.NewFloat64Builder(mem), float64(2.2), &util.DataType{Category: util.DataTypeCategory_FLOAT64}},
		{"decimal", array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 10, Scale: 2}), decimal.NewFromFloat(3.14), &util.DataType{Category: util.DataTypeCategory_DECIMAL}},
		{"string", array.NewStringBuilder(mem), "hello", &util.DataType{Category: util.DataTypeCategory_STRING}},
		{"char", array.NewStringBuilder(mem), "c", &util.DataType{Category: util.DataTypeCategory_CHAR}},
		{"varchar", array.NewStringBuilder(mem), "vc", &util.DataType{Category: util.DataTypeCategory_VARCHAR}},
		{"date", array.NewDate32Builder(mem), "2024-01-01", &util.DataType{Category: util.DataTypeCategory_DATE}},
		{"timestamp", array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond}), "2024-01-01 00:00:00", &util.DataType{
			Category: util.DataTypeCategory_TIMESTAMP_LTZ,
			Info:     &util.DataType_TimestampInfo{TimestampInfo: &util.TimestampInfo{TsUnit: util.TimestampUnit_MICROSECONDS}},
		}},
		{"json", array.NewStringBuilder(mem), `{"key":"val"}`, &util.DataType{Category: util.DataTypeCategory_JSON}},
		{"binary", array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary), []byte{0x01, 0x02}, &util.DataType{Category: util.DataTypeCategory_BINARY}},
		{"vector_f32", array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 8}), []byte{0, 0, 128, 63, 0, 0, 0, 64}, &util.DataType{
			Category: util.DataTypeCategory_VECTOR_TYPE,
			Info:     &util.DataType_VectorInfo{VectorInfo: &util.VectorTypeInfo{NumberType: util.VectorNumberType_F32, Dimension: 2}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AppendValueToArrowField(tt.builder, tt.value, tt.tpe)
			assert.NoError(t, err)
			assert.Equal(t, 1, tt.builder.Len())
			tt.builder.Release()
		})
	}
}

// --- BulkloadStream unit tests ---

func TestBulkloadStream_ClosedStreamRejectsWriter(t *testing.T) {
	stream := &BulkloadStream{Closed: true}
	_, err := stream.CreateWriter(0)
	assert.Error(t, err)
}

func TestBulkloadStream_GetStreamId(t *testing.T) {
	stream := &BulkloadStream{
		MetaData: &BulkloadMetadata{
			StreamInfo: &ingestion.BulkLoadStreamInfo{StreamId: "abc-123"},
		},
	}
	assert.Equal(t, "abc-123", stream.GetStreamId())
}

func TestBulkloadStream_CreateCommitterReuseSame(t *testing.T) {
	stream := &BulkloadStream{
		MetaData:      &BulkloadMetadata{StreamInfo: &ingestion.BulkLoadStreamInfo{StreamId: "s1"}},
		StreamOptions: &BulkloadOptions{Table: "t1"},
	}
	c1 := stream.CreateCommitter()
	c2 := stream.CreateCommitter()
	assert.True(t, c1 == c2)
}

func TestBulkloadStream_DoubleCloseIsNoop(t *testing.T) {
	stream := &BulkloadStream{Closed: true}
	assert.NoError(t, stream.Close())
}

// --- BulkloadCommitter tests ---

func TestBulkloadCommitter_PrepareCommitEmptyReturnsError(t *testing.T) {
	c := &BulkloadCommitter{}
	err := c.PrepareCommit(nil)
	assert.Error(t, err)
	err = c.PrepareCommit([]BulkLoadCommittable{})
	assert.Error(t, err)
}

// --- BulkloadWriter tests ---

func TestBulkloadWriter_ParsePartitionSpec(t *testing.T) {
	w := &BulkloadWriter{
		MetaData: &BulkloadMetadata{
			StreamInfo: &ingestion.BulkLoadStreamInfo{PartitionSpec: "pt=v1,region=us"},
		},
	}
	m, err := w.ParsePartitionSpec()
	assert.NoError(t, err)
	assert.Equal(t, "v1", m["pt"])
	assert.Equal(t, "us", m["region"])
}

func TestBulkloadWriter_ParsePartitionSpecEmpty(t *testing.T) {
	w := &BulkloadWriter{
		MetaData: &BulkloadMetadata{
			StreamInfo: &ingestion.BulkLoadStreamInfo{PartitionSpec: ""},
		},
	}
	m, err := w.ParsePartitionSpec()
	assert.NoError(t, err)
	assert.True(t, m == nil)
}

func TestBulkloadWriter_ParsePartitionSpecInvalid(t *testing.T) {
	w := &BulkloadWriter{
		MetaData: &BulkloadMetadata{
			StreamInfo: &ingestion.BulkLoadStreamInfo{PartitionSpec: "bad_spec"},
		},
	}
	_, err := w.ParsePartitionSpec()
	assert.Error(t, err)
}

func TestBulkloadWriter_EstimateRowSize(t *testing.T) {
	w := &BulkloadWriter{
		MetaData: &BulkloadMetadata{
			Table: CZTable{
				Schema: map[string]*util.DataType{
					"a": {Category: util.DataTypeCategory_INT64},
					"b": {Category: util.DataTypeCategory_STRING},
					"c": {Category: util.DataTypeCategory_BOOLEAN},
				},
			},
		},
	}
	size := w.EstimateRowSize()
	// int64=8, string=16, boolean=1
	assert.Equal(t, 25, size)
}

func TestBulkloadWriter_GetCommittablesEmpty(t *testing.T) {
	w := &BulkloadWriter{
		MetaData: &BulkloadMetadata{StreamInfo: &ingestion.BulkLoadStreamInfo{StreamId: "s1"}},
	}
	assert.True(t, w.GetCommittables() == nil)
}

func TestBulkloadWriter_GetCommittablesWithFiles(t *testing.T) {
	w := &BulkloadWriter{
		MetaData:            &BulkloadMetadata{StreamInfo: &ingestion.BulkLoadStreamInfo{StreamId: "s1"}},
		PartitionId:         2,
		UploadedVolumePaths: []string{"vol/file1", "vol/file2"},
	}
	comms := w.GetCommittables()
	assert.Equal(t, 1, len(comms))
	assert.Equal(t, "s1", comms[0].StreamId)
	assert.Equal(t, 2, comms[0].PartitionId)
	assert.Equal(t, 2, len(comms[0].DstFiles))
}

func TestBulkloadWriter_ClosedWriterRejectsWrite(t *testing.T) {
	w := &BulkloadWriter{Closed: true}
	err := w.WriteRow(&Row{})
	assert.Error(t, err)
}

func TestBulkloadWriter_AbortMarksClosed(t *testing.T) {
	w := &BulkloadWriter{
		MetaData:     &BulkloadMetadata{StreamInfo: &ingestion.BulkLoadStreamInfo{}},
		LocalBaseDir: os.TempDir(),
		FileNameUUID: "uuid",
	}
	assert.NoError(t, w.Abort())
	assert.True(t, w.Closed)
}

func TestBulkloadWriter_DoubleCloseIsNoop(t *testing.T) {
	w := &BulkloadWriter{Closed: true}
	assert.NoError(t, w.Close())
}

// --- BulkloadOptions tests ---

func TestBulkloadOptions_PreferInternalEndpoint(t *testing.T) {
	opts := BulkloadOptions{
		Table:                  "t1",
		Operation:              UPSERT,
		RecordKeys:             []string{"id"},
		PreferInternalEndpoint: true,
	}
	assert.True(t, opts.PreferInternalEndpoint)
}

// --- vectorNumberTypeWidth tests ---

func TestVectorNumberTypeWidth(t *testing.T) {
	assert.Equal(t, 1, vectorNumberTypeWidth(util.VectorNumberType_I8))
	assert.Equal(t, 4, vectorNumberTypeWidth(util.VectorNumberType_I32))
	assert.Equal(t, 4, vectorNumberTypeWidth(util.VectorNumberType_F32))
	// unsupported types fallback to 4
	assert.Equal(t, 4, vectorNumberTypeWidth(util.VectorNumberType_I16))
	assert.Equal(t, 4, vectorNumberTypeWidth(util.VectorNumberType_I64))
	assert.Equal(t, 4, vectorNumberTypeWidth(util.VectorNumberType_F64))
	assert.Equal(t, 4, vectorNumberTypeWidth(util.VectorNumberType_BF64))
}

// --- ARRAY/MAP/STRUCT ConvertToArrowDataType nested tests ---

func TestConvertToArrowDataType_NestedArray(t *testing.T) {
	// ARRAY<ARRAY<STRING>>
	dt := &util.DataType{
		Category: util.DataTypeCategory_ARRAY,
		Info: &util.DataType_ArrayTypeInfo{ArrayTypeInfo: &util.ArrayTypeInfo{
			ElementType: &util.DataType{
				Category: util.DataTypeCategory_ARRAY,
				Info: &util.DataType_ArrayTypeInfo{ArrayTypeInfo: &util.ArrayTypeInfo{
					ElementType: &util.DataType{Category: util.DataTypeCategory_STRING},
				}},
			},
		}},
	}
	got, err := ConvertToArrowDataType(dt)
	assert.NoError(t, err)
	// outer list -> inner list -> string
	outerList, ok := got.(*arrow.ListType)
	assert.True(t, ok)
	innerList, ok := outerList.Elem().(*arrow.ListType)
	assert.True(t, ok)
	assert.Equal(t, arrow.BinaryTypes.String, innerList.Elem())
}

func TestConvertToArrowDataType_MapWithStructValue(t *testing.T) {
	// MAP<STRING, STRUCT<x:INT32, y:FLOAT64>>
	dt := &util.DataType{
		Category: util.DataTypeCategory_MAP,
		Info: &util.DataType_MapTypeInfo{MapTypeInfo: &util.MapTypeInfo{
			KeyType: &util.DataType{Category: util.DataTypeCategory_STRING},
			ValueType: &util.DataType{
				Category: util.DataTypeCategory_STRUCT,
				Info: &util.DataType_StructTypeInfo{StructTypeInfo: &util.StructTypeInfo{
					Fields: []*util.StructTypeInfo_Field{
						{Name: "x", Type: &util.DataType{Category: util.DataTypeCategory_INT32}},
						{Name: "y", Type: &util.DataType{Category: util.DataTypeCategory_FLOAT64}},
					},
				}},
			},
		}},
	}
	got, err := ConvertToArrowDataType(dt)
	assert.NoError(t, err)
	mapType, ok := got.(*arrow.MapType)
	assert.True(t, ok)
	assert.Equal(t, arrow.BinaryTypes.String, mapType.KeyType())
	structType, ok := mapType.ItemType().(*arrow.StructType)
	assert.True(t, ok)
	assert.Equal(t, 2, structType.NumFields())
	assert.Equal(t, "x", structType.Field(0).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Int32, structType.Field(0).Type)
	assert.Equal(t, "y", structType.Field(1).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Float64, structType.Field(1).Type)
}

// --- ARRAY/MAP/STRUCT AppendValue tests ---

func TestAppendValueToArrowField_Array(t *testing.T) {
	mem := memory.DefaultAllocator
	elemType := &util.DataType{Category: util.DataTypeCategory_INT32}
	arrType := &util.DataType{
		Category: util.DataTypeCategory_ARRAY,
		Info:     &util.DataType_ArrayTypeInfo{ArrayTypeInfo: &util.ArrayTypeInfo{ElementType: elemType}},
	}
	lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer lb.Release()

	err := AppendValueToArrowField(lb, []interface{}{int32(1), int32(2), int32(3)}, arrType)
	assert.NoError(t, err)

	err = AppendValueToArrowField(lb, []interface{}{int32(10)}, arrType)
	assert.NoError(t, err)

	assert.Equal(t, 2, lb.Len())

	arr := lb.NewListArray()
	defer arr.Release()
	assert.Equal(t, 2, arr.Len())
	// first element: [1,2,3]
	start0, end0 := arr.ValueOffsets(0)
	assert.Equal(t, 3, int(end0-start0))
	// second element: [10]
	start1, end1 := arr.ValueOffsets(1)
	assert.Equal(t, 1, int(end1-start1))
}

func TestAppendValueToArrowField_ArrayWithNull(t *testing.T) {
	mem := memory.DefaultAllocator
	elemType := &util.DataType{Category: util.DataTypeCategory_STRING}
	arrType := &util.DataType{
		Category: util.DataTypeCategory_ARRAY,
		Info:     &util.DataType_ArrayTypeInfo{ArrayTypeInfo: &util.ArrayTypeInfo{ElementType: elemType}},
	}
	lb := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer lb.Release()

	err := AppendValueToArrowField(lb, []interface{}{"a", nil, "c"}, arrType)
	assert.NoError(t, err)
	assert.Equal(t, 1, lb.Len())
}

func TestAppendValueToArrowField_Map(t *testing.T) {
	mem := memory.DefaultAllocator
	keyType := &util.DataType{Category: util.DataTypeCategory_STRING}
	valueType := &util.DataType{Category: util.DataTypeCategory_INT64}
	mapType := &util.DataType{
		Category: util.DataTypeCategory_MAP,
		Info: &util.DataType_MapTypeInfo{MapTypeInfo: &util.MapTypeInfo{
			KeyType: keyType, ValueType: valueType,
		}},
	}
	mb := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64, false)
	defer mb.Release()

	err := AppendValueToArrowField(mb, map[string]interface{}{"a": int64(1), "b": int64(2)}, mapType)
	assert.NoError(t, err)
	assert.Equal(t, 1, mb.Len())

	mapArr := mb.NewMapArray()
	defer mapArr.Release()
	assert.Equal(t, 1, mapArr.Len())
}

func TestAppendValueToArrowField_MapWithNullValue(t *testing.T) {
	mem := memory.DefaultAllocator
	keyType := &util.DataType{Category: util.DataTypeCategory_STRING}
	valueType := &util.DataType{Category: util.DataTypeCategory_INT32}
	mapType := &util.DataType{
		Category: util.DataTypeCategory_MAP,
		Info: &util.DataType_MapTypeInfo{MapTypeInfo: &util.MapTypeInfo{
			KeyType: keyType, ValueType: valueType,
		}},
	}
	mb := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, false)
	defer mb.Release()

	err := AppendValueToArrowField(mb, map[string]interface{}{"x": int32(10), "y": nil}, mapType)
	assert.NoError(t, err)
	assert.Equal(t, 1, mb.Len())
}

func TestAppendValueToArrowField_Struct(t *testing.T) {
	mem := memory.DefaultAllocator
	structDataType := &util.DataType{
		Category: util.DataTypeCategory_STRUCT,
		Info: &util.DataType_StructTypeInfo{StructTypeInfo: &util.StructTypeInfo{
			Fields: []*util.StructTypeInfo_Field{
				{Name: "name", Type: &util.DataType{Category: util.DataTypeCategory_STRING}},
				{Name: "score", Type: &util.DataType{Category: util.DataTypeCategory_FLOAT64}},
			},
		}},
	}
	arrowStructType := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	)
	sb := array.NewStructBuilder(mem, arrowStructType)
	defer sb.Release()

	err := AppendValueToArrowField(sb, map[string]interface{}{"name": "alice", "score": float64(95.5)}, structDataType)
	assert.NoError(t, err)
	assert.Equal(t, 1, sb.Len())

	err = AppendValueToArrowField(sb, map[string]interface{}{"name": "bob", "score": float64(88.0)}, structDataType)
	assert.NoError(t, err)
	assert.Equal(t, 2, sb.Len())

	structArr := sb.NewStructArray()
	defer structArr.Release()
	assert.Equal(t, 2, structArr.Len())
}

func TestAppendValueToArrowField_StructWithMissingField(t *testing.T) {
	mem := memory.DefaultAllocator
	structDataType := &util.DataType{
		Category: util.DataTypeCategory_STRUCT,
		Info: &util.DataType_StructTypeInfo{StructTypeInfo: &util.StructTypeInfo{
			Fields: []*util.StructTypeInfo_Field{
				{Name: "a", Type: &util.DataType{Category: util.DataTypeCategory_INT32}},
				{Name: "b", Type: &util.DataType{Category: util.DataTypeCategory_STRING}},
			},
		}},
	}
	arrowStructType := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	sb := array.NewStructBuilder(mem, arrowStructType)
	defer sb.Release()

	// only provide field "a", "b" should be null
	err := AppendValueToArrowField(sb, map[string]interface{}{"a": int32(42)}, structDataType)
	assert.NoError(t, err)
	assert.Equal(t, 1, sb.Len())
}

func TestAppendValueToArrowField_NestedArrayOfStruct(t *testing.T) {
	mem := memory.DefaultAllocator
	structType := &util.DataType{
		Category: util.DataTypeCategory_STRUCT,
		Info: &util.DataType_StructTypeInfo{StructTypeInfo: &util.StructTypeInfo{
			Fields: []*util.StructTypeInfo_Field{
				{Name: "id", Type: &util.DataType{Category: util.DataTypeCategory_INT32}},
				{Name: "val", Type: &util.DataType{Category: util.DataTypeCategory_STRING}},
			},
		}},
	}
	arrType := &util.DataType{
		Category: util.DataTypeCategory_ARRAY,
		Info:     &util.DataType_ArrayTypeInfo{ArrayTypeInfo: &util.ArrayTypeInfo{ElementType: structType}},
	}
	arrowStructType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: "val", Type: arrow.BinaryTypes.String},
	)
	lb := array.NewListBuilder(mem, arrowStructType)
	defer lb.Release()

	value := []interface{}{
		map[string]interface{}{"id": int32(1), "val": "one"},
		map[string]interface{}{"id": int32(2), "val": "two"},
	}
	err := AppendValueToArrowField(lb, value, arrType)
	assert.NoError(t, err)
	assert.Equal(t, 1, lb.Len())
}

// --- BulkloadWriter complex type write tests ---

// newComplexTypeWriter creates a BulkloadWriter with complex type columns for testing.
// It sets up schema with ARRAY, MAP, STRUCT, and nested types, then calls ConstructArrowSchema.
func newComplexTypeWriter(t *testing.T) *BulkloadWriter {
	t.Helper()

	int32Type := &util.DataType{Category: util.DataTypeCategory_INT32}
	stringType := &util.DataType{Category: util.DataTypeCategory_STRING}
	float64Type := &util.DataType{Category: util.DataTypeCategory_FLOAT64}

	arrayType := &util.DataType{
		Category: util.DataTypeCategory_ARRAY,
		Info:     &util.DataType_ArrayTypeInfo{ArrayTypeInfo: &util.ArrayTypeInfo{ElementType: int32Type}},
	}
	mapType := &util.DataType{
		Category: util.DataTypeCategory_MAP,
		Info: &util.DataType_MapTypeInfo{MapTypeInfo: &util.MapTypeInfo{
			KeyType: stringType, ValueType: float64Type,
		}},
	}
	structType := &util.DataType{
		Category: util.DataTypeCategory_STRUCT,
		Info: &util.DataType_StructTypeInfo{StructTypeInfo: &util.StructTypeInfo{
			Fields: []*util.StructTypeInfo_Field{
				{Name: "name", Type: stringType},
				{Name: "score", Type: int32Type},
			},
		}},
	}
	// nested: ARRAY<STRUCT<k:STRING, v:INT32>>
	nestedType := &util.DataType{
		Category: util.DataTypeCategory_ARRAY,
		Info: &util.DataType_ArrayTypeInfo{ArrayTypeInfo: &util.ArrayTypeInfo{
			ElementType: &util.DataType{
				Category: util.DataTypeCategory_STRUCT,
				Info: &util.DataType_StructTypeInfo{StructTypeInfo: &util.StructTypeInfo{
					Fields: []*util.StructTypeInfo_Field{
						{Name: "k", Type: stringType},
						{Name: "v", Type: int32Type},
					},
				}},
			},
		}},
	}

	schema := map[string]*util.DataType{
		"id":     int32Type,
		"tags":   arrayType,
		"attrs":  mapType,
		"info":   structType,
		"nested": nestedType,
	}

	dataFields := []*ingestion.DataField{
		{Name: "id", Type: int32Type},
		{Name: "tags", Type: arrayType},
		{Name: "attrs", Type: mapType},
		{Name: "info", Type: structType},
		{Name: "nested", Type: nestedType},
	}

	w := &BulkloadWriter{
		MetaData: &BulkloadMetadata{
			Table: CZTable{
				Schema:    schema,
				TableMeta: &ingestion.StreamSchema{DataFields: dataFields},
			},
			StreamInfo: &ingestion.BulkLoadStreamInfo{
				StreamId:      "test-complex",
				PartitionSpec: "",
			},
		},
		StreamOptions: &BulkloadOptions{Table: "test_table"},
	}
	return w
}

func TestBulkloadWriter_ConstructArrowSchema_ComplexTypes(t *testing.T) {
	w := newComplexTypeWriter(t)
	err := w.ConstructArrowSchema()
	assert.NoError(t, err)
	assert.Equal(t, 5, w.ArrowSchema.NumFields())

	// id: INT32
	assert.Equal(t, arrow.PrimitiveTypes.Int32, w.ArrowSchema.Field(0).Type)
	// tags: LIST<INT32>
	_, isList := w.ArrowSchema.Field(1).Type.(*arrow.ListType)
	assert.True(t, isList)
	// attrs: MAP<STRING, FLOAT64>
	_, isMap := w.ArrowSchema.Field(2).Type.(*arrow.MapType)
	assert.True(t, isMap)
	// info: STRUCT<name:STRING, score:INT32>
	st, isStruct := w.ArrowSchema.Field(3).Type.(*arrow.StructType)
	assert.True(t, isStruct)
	assert.Equal(t, 2, st.NumFields())
	// nested: LIST<STRUCT<k:STRING, v:INT32>>
	outerList, isList := w.ArrowSchema.Field(4).Type.(*arrow.ListType)
	assert.True(t, isList)
	_, isStruct = outerList.Elem().(*arrow.StructType)
	assert.True(t, isStruct)
}

func TestBulkloadWriter_FlushRecordBatch_ComplexTypes(t *testing.T) {
	w := newComplexTypeWriter(t)
	err := w.ConstructArrowSchema()
	assert.NoError(t, err)

	// Prepare a record batch manually (simulating WriteRow + FlushRecordBatch)
	w.CurrentRecordBatch = map[string][]interface{}{
		"id":   {int32(1), int32(2)},
		"tags": {[]interface{}{int32(10), int32(20)}, []interface{}{int32(30)}},
		"attrs": {
			map[string]interface{}{"x": float64(1.1), "y": float64(2.2)},
			map[string]interface{}{"z": float64(3.3)},
		},
		"info": {
			map[string]interface{}{"name": "alice", "score": int32(95)},
			map[string]interface{}{"name": "bob", "score": int32(88)},
		},
		"nested": {
			[]interface{}{
				map[string]interface{}{"k": "a", "v": int32(1)},
				map[string]interface{}{"k": "b", "v": int32(2)},
			},
			[]interface{}{
				map[string]interface{}{"k": "c", "v": int32(3)},
			},
		},
	}
	w.CurrentRecordBatchRows = 2

	// Build Arrow record directly (same logic as FlushRecordBatch but without file writer)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	rb := array.NewRecordBuilder(mem, w.ArrowSchema)
	defer rb.Release()

	for i, column := range w.ArrowSchema.Fields() {
		values := w.CurrentRecordBatch[column.Name]
		for _, value := range values {
			if value == "" {
				rb.Field(i).AppendNull()
			} else {
				err := AppendValueToArrowField(rb.Field(i), value, w.MetaData.Table.Schema[column.Name])
				assert.NoError(t, err)
			}
		}
	}

	record := rb.NewRecord()
	defer record.Release()

	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, 5, int(record.NumCols()))

	// Verify id column
	idCol := record.Column(0).(*array.Int32)
	assert.Equal(t, int32(1), idCol.Value(0))
	assert.Equal(t, int32(2), idCol.Value(1))

	// Verify tags column (list)
	tagsCol := record.Column(1).(*array.List)
	assert.Equal(t, 2, tagsCol.Len())
	s0, e0 := tagsCol.ValueOffsets(0)
	assert.Equal(t, 2, int(e0-s0)) // first row: [10, 20]
	s1, e1 := tagsCol.ValueOffsets(1)
	assert.Equal(t, 1, int(e1-s1)) // second row: [30]

	// Verify attrs column (map)
	attrsCol := record.Column(2).(*array.Map)
	assert.Equal(t, 2, attrsCol.Len())

	// Verify info column (struct)
	infoCol := record.Column(3).(*array.Struct)
	assert.Equal(t, 2, infoCol.Len())
	nameField := infoCol.Field(0).(*array.String)
	assert.Equal(t, "alice", nameField.Value(0))
	assert.Equal(t, "bob", nameField.Value(1))
	scoreField := infoCol.Field(1).(*array.Int32)
	assert.Equal(t, int32(95), scoreField.Value(0))
	assert.Equal(t, int32(88), scoreField.Value(1))

	// Verify nested column (list of struct)
	nestedCol := record.Column(4).(*array.List)
	assert.Equal(t, 2, nestedCol.Len())
	ns0, ne0 := nestedCol.ValueOffsets(0)
	assert.Equal(t, 2, int(ne0-ns0)) // first row: 2 structs
	ns1, ne1 := nestedCol.ValueOffsets(1)
	assert.Equal(t, 1, int(ne1-ns1)) // second row: 1 struct
}

func TestBulkloadWriter_FlushRecordBatch_ComplexTypesWithNulls(t *testing.T) {
	w := newComplexTypeWriter(t)
	err := w.ConstructArrowSchema()
	assert.NoError(t, err)

	w.CurrentRecordBatch = map[string][]interface{}{
		"id":   {int32(1)},
		"tags": {[]interface{}{int32(10), nil, int32(30)}}, // array with null element
		"attrs": {
			map[string]interface{}{"a": float64(1.0), "b": nil}, // map with null value
		},
		"info": {
			map[string]interface{}{"name": "charlie"}, // struct with missing "score" field
		},
		"nested": {
			[]interface{}{
				map[string]interface{}{"k": "x", "v": int32(99)},
				nil, // null struct element in array
			},
		},
	}
	w.CurrentRecordBatchRows = 1

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	rb := array.NewRecordBuilder(mem, w.ArrowSchema)
	defer rb.Release()

	for i, column := range w.ArrowSchema.Fields() {
		values := w.CurrentRecordBatch[column.Name]
		for _, value := range values {
			if value == "" {
				rb.Field(i).AppendNull()
			} else {
				err := AppendValueToArrowField(rb.Field(i), value, w.MetaData.Table.Schema[column.Name])
				assert.NoError(t, err)
			}
		}
	}

	record := rb.NewRecord()
	defer record.Release()

	assert.Equal(t, int64(1), record.NumRows())

	// tags: [10, null, 30]
	tagsCol := record.Column(1).(*array.List)
	s, e := tagsCol.ValueOffsets(0)
	assert.Equal(t, 3, int(e-s))

	// info: struct with missing score -> score should be null
	infoCol := record.Column(3).(*array.Struct)
	scoreField := infoCol.Field(1).(*array.Int32)
	assert.True(t, scoreField.IsNull(0))
}

// Row.SetValue is a generic setter for complex types
func TestRow_SetComplexValues(t *testing.T) {
	int32Type := &util.DataType{Category: util.DataTypeCategory_INT32}
	stringType := &util.DataType{Category: util.DataTypeCategory_STRING}

	row := &Row{
		Columns: map[string]*util.DataType{
			"id": int32Type,
			"tags": {
				Category: util.DataTypeCategory_ARRAY,
				Info:     &util.DataType_ArrayTypeInfo{ArrayTypeInfo: &util.ArrayTypeInfo{ElementType: int32Type}},
			},
			"info": {
				Category: util.DataTypeCategory_STRUCT,
				Info: &util.DataType_StructTypeInfo{StructTypeInfo: &util.StructTypeInfo{
					Fields: []*util.StructTypeInfo_Field{
						{Name: "name", Type: stringType},
					},
				}},
			},
		},
		TableName:        "test",
		ColumnNameValues: map[string]interface{}{},
	}

	// Set simple value
	assert.NoError(t, row.SetInt("id", int32(1)))
	assert.Equal(t, int32(1), row.ColumnNameValues["id"])

	// Set array value directly
	row.ColumnNameValues["tags"] = []interface{}{int32(10), int32(20)}
	assert.Equal(t, []interface{}{int32(10), int32(20)}, row.ColumnNameValues["tags"])

	// Set struct value directly
	row.ColumnNameValues["info"] = map[string]interface{}{"name": "test"}
	m, ok := row.ColumnNameValues["info"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "test", m["name"])
}
