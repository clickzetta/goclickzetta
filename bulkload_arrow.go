package goclickzetta

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/clickzetta/goclickzetta/protos/bulkload/util"
	"github.com/shopspring/decimal"
)

// ConvertToArrowDataType converts a protobuf DataType to an Arrow DataType.
func ConvertToArrowDataType(tpe *util.DataType) (arrow.DataType, error) {
	switch tpe.GetCategory() {
	case util.DataTypeCategory_BOOLEAN:
		return arrow.FixedWidthTypes.Boolean, nil
	case util.DataTypeCategory_INT8:
		return arrow.PrimitiveTypes.Int8, nil
	case util.DataTypeCategory_INT16:
		return arrow.PrimitiveTypes.Int16, nil
	case util.DataTypeCategory_INT32:
		return arrow.PrimitiveTypes.Int32, nil
	case util.DataTypeCategory_INT64:
		return arrow.PrimitiveTypes.Int64, nil
	case util.DataTypeCategory_FLOAT32:
		return arrow.PrimitiveTypes.Float32, nil
	case util.DataTypeCategory_FLOAT64:
		return arrow.PrimitiveTypes.Float64, nil
	case util.DataTypeCategory_DECIMAL:
		p := int32(tpe.GetDecimalTypeInfo().GetPrecision())
		s := int32(tpe.GetDecimalTypeInfo().GetScale())
		return &arrow.Decimal128Type{Precision: p, Scale: s}, nil
	case util.DataTypeCategory_STRING, util.DataTypeCategory_CHAR, util.DataTypeCategory_VARCHAR,
		util.DataTypeCategory_JSON:
		return arrow.BinaryTypes.String, nil
	case util.DataTypeCategory_BINARY, util.DataTypeCategory_BITMAP:
		return arrow.BinaryTypes.Binary, nil
	case util.DataTypeCategory_DATE:
		return arrow.FixedWidthTypes.Date32, nil
	case util.DataTypeCategory_TIMESTAMP_LTZ:
		tu := tpe.GetTimestampInfo().GetTsUnit()
		switch tu {
		case util.TimestampUnit_SECONDS:
			return arrow.FixedWidthTypes.Timestamp_s, nil
		case util.TimestampUnit_MILLISECONDS:
			return arrow.FixedWidthTypes.Timestamp_ms, nil
		case util.TimestampUnit_MICROSECONDS:
			return arrow.FixedWidthTypes.Timestamp_us, nil
		case util.TimestampUnit_NANOSECONDS:
			return arrow.FixedWidthTypes.Timestamp_ns, nil
		default:
			return nil, errors.New("unsupported timestamp unit: " + tu.String())
		}
	case util.DataTypeCategory_TIMESTAMP_NTZ:
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case util.DataTypeCategory_VECTOR_TYPE:
		vi := tpe.GetVectorInfo()
		dim := int(vi.GetDimension())
		elemSize := vectorNumberTypeWidth(vi.GetNumberType())
		return &arrow.FixedSizeBinaryType{ByteWidth: dim * elemSize}, nil
	case util.DataTypeCategory_ARRAY:
		elemType, err := ConvertToArrowDataType(tpe.GetArrayTypeInfo().GetElementType())
		if err != nil {
			return nil, fmt.Errorf("ARRAY element type: %w", err)
		}
		return arrow.ListOfField(arrow.Field{Name: "element", Type: elemType, Nullable: tpe.GetArrayTypeInfo().GetElementType().GetNullable()}), nil
	case util.DataTypeCategory_MAP:
		keyType, err := ConvertToArrowDataType(tpe.GetMapTypeInfo().GetKeyType())
		if err != nil {
			return nil, fmt.Errorf("MAP key type: %w", err)
		}
		valueType, err := ConvertToArrowDataType(tpe.GetMapTypeInfo().GetValueType())
		if err != nil {
			return nil, fmt.Errorf("MAP value type: %w", err)
		}
		return arrow.MapOf(keyType, valueType), nil
	case util.DataTypeCategory_STRUCT:
		fields := tpe.GetStructTypeInfo().GetFields()
		arrowFields := make([]arrow.Field, len(fields))
		for i, f := range fields {
			ft, err := ConvertToArrowDataType(f.GetType())
			if err != nil {
				return nil, fmt.Errorf("STRUCT field %s: %w", f.GetName(), err)
			}
			arrowFields[i] = arrow.Field{Name: f.GetName(), Type: ft, Nullable: f.GetType().GetNullable()}
		}
		return arrow.StructOf(arrowFields...), nil
	default:
		return nil, errors.New("unsupported data type: " + tpe.String())
	}
}

// vectorNumberTypeWidth returns the byte width for a VectorNumberType.
// Aligned with Java ArrowSchemaConvert.toArrowType: only I8, I32, F32 are supported.
func vectorNumberTypeWidth(nt util.VectorNumberType) int {
	switch nt {
	case util.VectorNumberType_I8:
		return 1
	case util.VectorNumberType_I32:
		return 4
	case util.VectorNumberType_F32:
		return 4
	default:
		// fallback to 4 bytes (float32)
		return 4
	}
}

// ConvertToArrowValue converts a Go value to its string representation for Arrow.
func ConvertToArrowValue(value interface{}, tpe *util.DataType) (string, error) {
	switch tpe.GetCategory() {
	case util.DataTypeCategory_BOOLEAN:
		v, ok := value.(bool)
		if !ok {
			return "", fmt.Errorf("cannot convert value to bool")
		}
		return strconv.FormatBool(v), nil
	case util.DataTypeCategory_INT8:
		v, ok := value.(int8)
		if !ok {
			return "", fmt.Errorf("cannot convert value to int8")
		}
		return strconv.Itoa(int(v)), nil
	case util.DataTypeCategory_INT16:
		v, ok := value.(int16)
		if !ok {
			return "", fmt.Errorf("cannot convert value to int16")
		}
		return strconv.Itoa(int(v)), nil
	case util.DataTypeCategory_INT32:
		v, ok := value.(int32)
		if !ok {
			return "", fmt.Errorf("cannot convert value to int32")
		}
		return strconv.Itoa(int(v)), nil
	case util.DataTypeCategory_INT64:
		v, ok := value.(int64)
		if !ok {
			return "", fmt.Errorf("cannot convert value to int64")
		}
		return strconv.Itoa(int(v)), nil
	case util.DataTypeCategory_FLOAT32:
		v, ok := value.(float32)
		if !ok {
			return "", fmt.Errorf("cannot convert value to float32")
		}
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case util.DataTypeCategory_FLOAT64:
		v, ok := value.(float64)
		if !ok {
			return "", fmt.Errorf("cannot convert value to float64")
		}
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case util.DataTypeCategory_DECIMAL:
		v, ok := value.(decimal.Decimal)
		if !ok {
			return "", fmt.Errorf("cannot convert value to decimal")
		}
		return v.String(), nil
	case util.DataTypeCategory_STRING, util.DataTypeCategory_CHAR, util.DataTypeCategory_VARCHAR,
		util.DataTypeCategory_JSON, util.DataTypeCategory_DATE, util.DataTypeCategory_TIMESTAMP_LTZ,
		util.DataTypeCategory_TIMESTAMP_NTZ:
		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("cannot convert value to string")
		}
		return v, nil
	default:
		return "", errors.New("unsupported data type: " + tpe.String())
	}
}

// AppendValueToArrowField appends a value to an Arrow array builder.
func AppendValueToArrowField(field array.Builder, value interface{}, tpe *util.DataType) error {
	// Handle complex types and binary types first (they don't go through ConvertToArrowValue)
	switch tpe.GetCategory() {
	case util.DataTypeCategory_ARRAY:
		return appendBulkloadArrayValue(field, value, tpe)
	case util.DataTypeCategory_MAP:
		return appendBulkloadMapValue(field, value, tpe)
	case util.DataTypeCategory_STRUCT:
		return appendBulkloadStructValue(field, value, tpe)
	case util.DataTypeCategory_VECTOR_TYPE:
		if b, ok := value.([]byte); ok {
			field.(*array.FixedSizeBinaryBuilder).Append(b)
			return nil
		}
		return fmt.Errorf("VECTOR_TYPE expects []byte value, got %T", value)
	case util.DataTypeCategory_BINARY, util.DataTypeCategory_BITMAP:
		if b, ok := value.([]byte); ok {
			field.(*array.BinaryBuilder).Append(b)
			return nil
		}
		return fmt.Errorf("BINARY expects []byte value, got %T", value)
	}

	v, err := ConvertToArrowValue(value, tpe)
	if err != nil {
		return err
	}
	switch tpe.GetCategory() {
	case util.DataTypeCategory_BOOLEAN:
		return field.(*array.BooleanBuilder).AppendValueFromString(v)
	case util.DataTypeCategory_INT8:
		return field.(*array.Int8Builder).AppendValueFromString(v)
	case util.DataTypeCategory_INT16:
		return field.(*array.Int16Builder).AppendValueFromString(v)
	case util.DataTypeCategory_INT32:
		return field.(*array.Int32Builder).AppendValueFromString(v)
	case util.DataTypeCategory_INT64:
		return field.(*array.Int64Builder).AppendValueFromString(v)
	case util.DataTypeCategory_FLOAT32:
		return field.(*array.Float32Builder).AppendValueFromString(v)
	case util.DataTypeCategory_FLOAT64:
		return field.(*array.Float64Builder).AppendValueFromString(v)
	case util.DataTypeCategory_DECIMAL:
		return field.(*array.Decimal128Builder).AppendValueFromString(v)
	case util.DataTypeCategory_STRING, util.DataTypeCategory_CHAR, util.DataTypeCategory_VARCHAR,
		util.DataTypeCategory_JSON:
		return field.(*array.StringBuilder).AppendValueFromString(v)
	case util.DataTypeCategory_DATE:
		return field.(*array.Date32Builder).AppendValueFromString(v)
	case util.DataTypeCategory_TIMESTAMP_LTZ, util.DataTypeCategory_TIMESTAMP_NTZ:
		return field.(*array.TimestampBuilder).AppendValueFromString(v)
	default:
		return errors.New("unsupported data type: " + tpe.String())
	}
}

// appendBulkloadArrayValue appends an array ([]interface{}) to a ListBuilder.
func appendBulkloadArrayValue(field array.Builder, value interface{}, tpe *util.DataType) error {
	lb, ok := field.(*array.ListBuilder)
	if !ok {
		return fmt.Errorf("expected ListBuilder for ARRAY, got %T", field)
	}
	arr, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("ARRAY expects []interface{} value, got %T", value)
	}
	elemType := tpe.GetArrayTypeInfo().GetElementType()
	lb.Append(true)
	vb := lb.ValueBuilder()
	for _, elem := range arr {
		if elem == nil {
			vb.AppendNull()
		} else {
			if err := AppendValueToArrowField(vb, elem, elemType); err != nil {
				return fmt.Errorf("ARRAY element: %w", err)
			}
		}
	}
	return nil
}

// appendBulkloadMapValue appends a map to a MapBuilder.
func appendBulkloadMapValue(field array.Builder, value interface{}, tpe *util.DataType) error {
	mb, ok := field.(*array.MapBuilder)
	if !ok {
		return fmt.Errorf("expected MapBuilder for MAP, got %T", field)
	}
	keyType := tpe.GetMapTypeInfo().GetKeyType()
	valueType := tpe.GetMapTypeInfo().GetValueType()
	kb := mb.KeyBuilder()
	ib := mb.ItemBuilder()

	switch m := value.(type) {
	case map[string]interface{}:
		mb.Append(true)
		for k, v := range m {
			if err := AppendValueToArrowField(kb, k, keyType); err != nil {
				return fmt.Errorf("MAP key: %w", err)
			}
			if v == nil {
				ib.AppendNull()
			} else {
				if err := AppendValueToArrowField(ib, v, valueType); err != nil {
					return fmt.Errorf("MAP value: %w", err)
				}
			}
		}
		return nil
	case map[interface{}]interface{}:
		mb.Append(true)
		for k, v := range m {
			if err := AppendValueToArrowField(kb, k, keyType); err != nil {
				return fmt.Errorf("MAP key: %w", err)
			}
			if v == nil {
				ib.AppendNull()
			} else {
				if err := AppendValueToArrowField(ib, v, valueType); err != nil {
					return fmt.Errorf("MAP value: %w", err)
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("MAP expects map[string]interface{} or map[interface{}]interface{}, got %T", value)
	}
}

// appendBulkloadStructValue appends a struct (map[string]interface{}) to a StructBuilder.
func appendBulkloadStructValue(field array.Builder, value interface{}, tpe *util.DataType) error {
	sb, ok := field.(*array.StructBuilder)
	if !ok {
		return fmt.Errorf("expected StructBuilder for STRUCT, got %T", field)
	}
	m, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("STRUCT expects map[string]interface{} value, got %T", value)
	}
	protoFields := tpe.GetStructTypeInfo().GetFields()
	sb.Append(true)
	for i, f := range protoFields {
		fb := sb.FieldBuilder(i)
		v, exists := m[f.GetName()]
		if !exists || v == nil {
			fb.AppendNull()
		} else {
			if err := AppendValueToArrowField(fb, v, f.GetType()); err != nil {
				return fmt.Errorf("STRUCT field %s: %w", f.GetName(), err)
			}
		}
	}
	return nil
}
