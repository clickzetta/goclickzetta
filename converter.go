package goclickzetta

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
)

type timezoneType int

const (
	// TimestampLTZType denotes a LTZ timezoneType for array binds
	TimestampLTZType timezoneType = iota
	// DateType denotes a date type for array binds
	DateType
)

// goTypeToClickzetta translates Go data type to Clickzetta data type.
func goTypeToClickzetta(v driver.Value, tsmode clickzettaType) clickzettaType {
	switch v.(type) {
	case int64, sql.NullInt64:
		return BIGINT
	case float64, sql.NullFloat64:
		return DOUBLE
	case bool, sql.NullBool:
		return BOOLEAN
	case float32:
		return FLOAT
	case int8:
		return TINYINT
	case int16, sql.NullInt16:
		return SMALLINT
	case int32, sql.NullInt32, int:
		return INT
	case string, sql.NullString:
		return STRING
	case time.Time, sql.NullTime:
		return tsmode
	}

	return NOT_SUPPORTED
}

var decimalShift = new(big.Int).Exp(big.NewInt(2), big.NewInt(64), nil)

func intToBigFloat(val int64, scale int64) *big.Float {
	f := new(big.Float).SetInt64(val)
	s := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
	return new(big.Float).Quo(f, s)
}

func decimalToBigInt(num decimal128.Num) *big.Int {
	high := new(big.Int).SetInt64(num.HighBits())
	low := new(big.Int).SetUint64(num.LowBits())
	return new(big.Int).Add(new(big.Int).Mul(high, decimalShift), low)
}

func decimalToBigFloat(num decimal128.Num, divisor *big.Float) *big.Float {
	f := new(big.Float).SetInt(decimalToBigInt(num))
	return new(big.Float).Quo(f, divisor)
}

// normalizeKeyValueJSON takes a JSON string potentially containing arrays of
// objects with shape {"key":..., "value":...} and recursively converts those
// arrays into JSON objects, so nested map-like data becomes object style.
func normalizeKeyValueJSON(raw string) string {
	var v interface{}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return raw
	}
	nv := transformKeyValueShape(v)
	b, err := json.Marshal(nv)
	if err != nil {
		return raw
	}
	return string(b)
}

func transformKeyValueShape(v interface{}) interface{} {
	switch x := v.(type) {
	case []interface{}:
		// Check if this is an array of {key,value} objects
		allKV := true
		obj := make(map[string]interface{})
		for _, el := range x {
			m, ok := el.(map[string]interface{})
			if !ok {
				allKV = false
				break
			}
			// strict mode: only when key and value are present
			if len(m) != 2 {
				allKV = false
				break
			}
			keyVal, hasKey := m["key"]
			val, hasVal := m["value"]
			if !hasKey || !hasVal {
				allKV = false
				break
			}
			keyStr, ok := keyVal.(string)
			if !ok {
				allKV = false
				break
			}
			obj[keyStr] = transformKeyValueShape(val)
		}
		if allKV {
			return obj
		}
		for i, el := range x {
			x[i] = transformKeyValueShape(el)
		}
		return x
	case map[string]interface{}:
		for k, vv := range x {
			x[k] = transformKeyValueShape(vv)
		}
		return x
	default:
		return v
	}
}

// arrowArrayToSlice 将 Arrow Array 转换为 Go slice
func arrowArrayToSlice(arr arrow.Array) []interface{} {
	if arr == nil {
		return nil
	}

	result := make([]interface{}, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			result[i] = nil
			continue
		}
		result[i] = arrowValueToInterface(arr, i)
	}
	return result
}

// arrowValueToInterface 将 Arrow Array 的单个元素转换为 Go interface{}
func arrowValueToInterface(arr arrow.Array, index int) interface{} {
	if arr.IsNull(index) {
		return nil
	}

	switch data := arr.(type) {
	case *array.Boolean:
		return data.Value(index)
	case *array.Int8:
		return data.Value(index)
	case *array.Int16:
		return data.Value(index)
	case *array.Int32:
		return data.Value(index)
	case *array.Int64:
		return data.Value(index)
	case *array.Uint8:
		return data.Value(index)
	case *array.Uint16:
		return data.Value(index)
	case *array.Uint32:
		return data.Value(index)
	case *array.Uint64:
		return data.Value(index)
	case *array.Float32:
		return data.Value(index)
	case *array.Float64:
		return data.Value(index)
	case *array.String:
		return data.Value(index)
	case *array.Binary:
		return data.Value(index)
	case *array.Timestamp:
		return data.Value(index).ToTime(arrow.Microsecond)
	case *array.Date32:
		return time.Unix(int64(data.Value(index))*86400, 0).UTC()
	case *array.Decimal128:
		num := data.Value(index)
		decType := data.DataType().(*arrow.Decimal128Type)
		if decType.Scale == 0 {
			if decType.Precision == 20 {
				return num.LowBits()
			}
			return num.BigInt()
		}
		divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decType.Scale)), nil))
		return decimalToBigFloat(num, divisor)
	case *array.List:
	case *array.LargeList:
		start, end := data.ValueOffsets(index)
		listValues := data.ListValues()
		result := make([]interface{}, end-start)
		for i := int(start); i < int(end); i++ {
			result[i-int(start)] = arrowValueToInterface(listValues, i)
		}
		return result
	case *array.FixedSizeList:
		listSize := data.DataType().(*arrow.FixedSizeListType).Len()
		start := index * int(listSize)
		end := start + int(listSize)
		listValues := data.ListValues()
		result := make([]interface{}, listSize)
		for i := start; i < end; i++ {
			result[i-start] = arrowValueToInterface(listValues, i)
		}
		return result
	case *array.Map:
		return arrowMapToGoMap(data, index)
	case *array.Struct:
		return arrowStructToGoMap(data, index)
	default:
		// 对于不支持的类型，返回字符串表示
		return arr.(fmt.Stringer).String()
	}
	return nil
}

// arrowMapToGoMap 将 Arrow Map 转换为 Go map
func arrowMapToGoMap(data *array.Map, index int) map[string]interface{} {
	if data.IsNull(index) {
		return nil
	}

	start, end := data.ValueOffsets(index)
	result := make(map[string]interface{}, end-start)

	keys := data.Keys()
	items := data.Items()

	for j := int(start); j < int(end); j++ {
		var keyStr string
		if strKeys, ok := keys.(*array.String); ok {
			keyStr = strKeys.Value(j)
		} else {
			// 非字符串 key，尝试转换
			keyStr = fmt.Sprintf("%v", arrowValueToInterface(keys, j))
		}

		result[keyStr] = arrowValueToInterface(items, j)
	}

	return result
}

// arrowMapToTypedGoMap converts Arrow Map to a typed Go map based on value type
// Returns map[string]string for string values, map[string]int64 for int values, etc.
func arrowMapToTypedGoMap(data *array.Map, index int) interface{} {
	if data.IsNull(index) {
		return nil
	}

	start, end := data.ValueOffsets(index)
	keys := data.Keys()
	items := data.Items()

	// determine the type based on items array type
	switch itemData := items.(type) {
	case *array.String:
		result := make(map[string]string, end-start)
		for j := int(start); j < int(end); j++ {
			keyStr := getMapKeyString(keys, j)
			if !itemData.IsNull(j) {
				result[keyStr] = itemData.Value(j)
			} else {
				result[keyStr] = ""
			}
		}
		return result
	case *array.Int64:
		result := make(map[string]int64, end-start)
		for j := int(start); j < int(end); j++ {
			keyStr := getMapKeyString(keys, j)
			if !itemData.IsNull(j) {
				result[keyStr] = itemData.Value(j)
			}
		}
		return result
	case *array.Int32:
		result := make(map[string]int32, end-start)
		for j := int(start); j < int(end); j++ {
			keyStr := getMapKeyString(keys, j)
			if !itemData.IsNull(j) {
				result[keyStr] = itemData.Value(j)
			}
		}
		return result
	case *array.Float64:
		result := make(map[string]float64, end-start)
		for j := int(start); j < int(end); j++ {
			keyStr := getMapKeyString(keys, j)
			if !itemData.IsNull(j) {
				result[keyStr] = itemData.Value(j)
			}
		}
		return result
	case *array.Boolean:
		result := make(map[string]bool, end-start)
		for j := int(start); j < int(end); j++ {
			keyStr := getMapKeyString(keys, j)
			if !itemData.IsNull(j) {
				result[keyStr] = itemData.Value(j)
			}
		}
		return result
	case *array.Decimal128:
		decType := itemData.DataType().(*arrow.Decimal128Type)
		// check if this is decimal(20, 0) which should be converted to map[...]uint64
		if decType.Precision == 20 && decType.Scale == 0 {
			// check key type for map[uint64]uint64
			if keyDecimal, ok := keys.(*array.Decimal128); ok {
				keyDecType := keyDecimal.DataType().(*arrow.Decimal128Type)
				if keyDecType.Precision == 20 && keyDecType.Scale == 0 {
					result := make(map[uint64]uint64, end-start)
					for j := int(start); j < int(end); j++ {
						if !keyDecimal.IsNull(j) && !itemData.IsNull(j) {
							result[keyDecimal.Value(j).LowBits()] = itemData.Value(j).LowBits()
						}
					}
					return result
				}
			}
			// fallback to map[string]uint64
			result := make(map[string]uint64, end-start)
			for j := int(start); j < int(end); j++ {
				keyStr := getMapKeyString(keys, j)
				if !itemData.IsNull(j) {
					result[keyStr] = itemData.Value(j).LowBits()
				}
			}
			return result
		}
		// fallback to map[string]interface{}
		return arrowMapToGoMap(data, index)
	default:
		// fallback to map[string]interface{}
		return arrowMapToGoMap(data, index)
	}
}

// getMapKeyString extracts string key from Arrow array
func getMapKeyString(keys arrow.Array, index int) string {
	if strKeys, ok := keys.(*array.String); ok {
		return strKeys.Value(index)
	}
	return fmt.Sprintf("%v", arrowValueToInterface(keys, index))
}

// arrowStructToGoMap 将 Arrow Struct 转换为 Go map
func arrowStructToGoMap(data *array.Struct, index int) map[string]interface{} {
	if data.IsNull(index) {
		return nil
	}

	structType := data.DataType().(*arrow.StructType)
	result := make(map[string]interface{}, data.NumField())

	for i := 0; i < data.NumField(); i++ {
		fieldName := structType.Field(i).Name
		fieldArray := data.Field(i)
		result[fieldName] = arrowValueToInterface(fieldArray, index)
	}

	return result
}

// convertListToTypedSlice converts Arrow list values to a typed Go slice
// Returns []string for string arrays, []int64 for int arrays, etc.
func convertListToTypedSlice(listValues arrow.Array, start, end int) interface{} {
	if listValues == nil {
		return []interface{}{}
	}

	// Handle empty arrays - return typed empty slice based on array type
	if start >= end {
		switch listValues.(type) {
		case *array.String:
			return []string{}
		case *array.Int64:
			return []int64{}
		case *array.Int32:
			return []int32{}
		case *array.Float64:
			return []float64{}
		case *array.Boolean:
			return []bool{}
		case *array.Decimal128:
			decType := listValues.DataType().(*arrow.Decimal128Type)
			if decType.Precision == 20 && decType.Scale == 0 {
				return []uint64{}
			}
			return []interface{}{}
		default:
			return []interface{}{}
		}
	}

	switch data := listValues.(type) {
	case *array.String:
		result := make([]string, end-start)
		for j := start; j < end; j++ {
			if data.IsNull(j) {
				result[j-start] = ""
			} else {
				result[j-start] = data.Value(j)
			}
		}
		return result
	case *array.Int64:
		result := make([]int64, end-start)
		for j := start; j < end; j++ {
			if !data.IsNull(j) {
				result[j-start] = data.Value(j)
			}
		}
		return result
	case *array.Int32:
		result := make([]int32, end-start)
		for j := start; j < end; j++ {
			if !data.IsNull(j) {
				result[j-start] = data.Value(j)
			}
		}
		return result
	case *array.Float64:
		result := make([]float64, end-start)
		for j := start; j < end; j++ {
			if !data.IsNull(j) {
				result[j-start] = data.Value(j)
			}
		}
		return result
	case *array.Boolean:
		result := make([]bool, end-start)
		for j := start; j < end; j++ {
			if !data.IsNull(j) {
				result[j-start] = data.Value(j)
			}
		}
		return result
	case *array.Decimal128:
		decType := data.DataType().(*arrow.Decimal128Type)
		// check if this is decimal(20, 0) which should be converted to []uint64
		if decType.Precision == 20 && decType.Scale == 0 {
			result := make([]uint64, end-start)
			for j := start; j < end; j++ {
				if !data.IsNull(j) {
					result[j-start] = data.Value(j).LowBits()
				}
			}
			return result
		}
		// fallback to []interface{} for other decimal types
		result := make([]interface{}, end-start)
		for j := start; j < end; j++ {
			result[j-start] = arrowValueToInterface(listValues, j)
		}
		return result
	default:
		// fallback to []interface{} for unsupported types
		result := make([]interface{}, end-start)
		for j := start; j < end; j++ {
			result[j-start] = arrowValueToInterface(listValues, j)
		}
		return result
	}
}

func processIntValues(srcValue arrow.Array, destcol []interface{}, scale int64, higherPrecision bool) {
	var divisor float64
	if scale != 0 && !higherPrecision {
		divisor = math.Pow10(int(scale))
	}

	switch data := srcValue.(type) {
	case *array.Int64:
		for i, val := range data.Int64Values() {
			if !srcValue.IsNull(i) {
				destcol[i] = formatIntValue(int64(val), scale, higherPrecision, divisor)
			}
		}
	case *array.Int32:
		for i, val := range data.Int32Values() {
			if !srcValue.IsNull(i) {
				destcol[i] = formatIntValue(int64(val), scale, higherPrecision, divisor)
			}
		}
	case *array.Int16:
		for i, val := range data.Int16Values() {
			if !srcValue.IsNull(i) {
				destcol[i] = formatIntValue(int64(val), scale, higherPrecision, divisor)
			}
		}
	case *array.Int8:
		for i, val := range data.Int8Values() {
			if !srcValue.IsNull(i) {
				destcol[i] = formatIntValue(int64(val), scale, higherPrecision, divisor)
			}
		}
	}
}

func formatIntValue(val int64, scale int64, higherPrecision bool, divisor float64) interface{} {
	if scale == 0 {
		if higherPrecision {
			return val
		}
		return fmt.Sprintf("%d", val)
	}

	if higherPrecision {
		return intToBigFloat(val, scale)
	}
	return fmt.Sprintf("%.*f", scale, float64(val)/divisor)
}

func arrowToValue(
	destcol []interface{},
	srcColumnMeta execResponseColumnType,
	srcValue arrow.Array,
	loc *time.Location,
	higherPrecision bool) error {

	var err error
	if len(destcol) != srcValue.Len() {
		err = fmt.Errorf("array interface length mismatch")
	}
	logger.Debugf("clickzetta data type: %v, arrow data type: %v", srcColumnMeta.Type, srcValue.DataType())

	switch getclickzettaType(strings.ToUpper(srcColumnMeta.Type)) {
	case DECIMAL, TINYINT, SMALLINT, INT, BIGINT:
		switch data := srcValue.(type) {
		case *array.Decimal128:
			var divisor *big.Float
			if srcColumnMeta.Scale != 0 {
				divisor = new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(srcColumnMeta.Scale), nil))
			}

			// check if this is decimal(20, 0) which should be converted to uint64
			isUint64Type := srcColumnMeta.Precision == 20 && srcColumnMeta.Scale == 0

			for i, num := range data.Values() {
				if !srcValue.IsNull(i) {
					if srcColumnMeta.Scale == 0 {
						if isUint64Type {
							// convert to uint64
							destcol[i] = num.LowBits()
						} else if higherPrecision {
							destcol[i] = num.BigInt()
						} else {
							destcol[i] = num.ToString(0)
						}
					} else {
						f := decimalToBigFloat(num, divisor)
						if higherPrecision {
							destcol[i] = f
						} else {
							destcol[i] = fmt.Sprintf("%.*f", srcColumnMeta.Scale, f)
						}
					}
				}
			}

		case *array.Int64, *array.Int32, *array.Int16, *array.Int8:
			processIntValues(data, destcol, srcColumnMeta.Scale, higherPrecision)
		}
		return err
	case ARRAY:
		switch data := srcValue.(type) {
		case *array.List:
			for i := range destcol {
				if !srcValue.IsNull(i) {
					start, end := data.ValueOffsets(i)
					listValues := data.ListValues()
					destcol[i] = convertListToTypedSlice(listValues, int(start), int(end))
				}
			}
		case *array.LargeList:
			for i := range destcol {
				if !srcValue.IsNull(i) {
					start, end := data.ValueOffsets(i)
					listValues := data.ListValues()
					destcol[i] = convertListToTypedSlice(listValues, int(start), int(end))
				}
			}
		case *array.FixedSizeList:
			for i := range destcol {
				if !srcValue.IsNull(i) {
					listSize := data.DataType().(*arrow.FixedSizeListType).Len()
					start := i * int(listSize)
					end := start + int(listSize)
					listValues := data.ListValues()
					destcol[i] = convertListToTypedSlice(listValues, start, end)
				}
			}
		default:
			return fmt.Errorf("unsupported ARRAY arrow type: %T", srcValue)
		}
		return err
	case MAP:
		// convert Arrow Map to typed Go map
		data := srcValue.(*array.Map)
		for i := range destcol {
			if !srcValue.IsNull(i) {
				destcol[i] = arrowMapToTypedGoMap(data, i)
			}
		}
		return err
	case STRUCT:
		if data, ok := srcValue.(*array.Struct); ok {
			for i := range destcol {
				if !srcValue.IsNull(i) {
					destcol[i] = arrowStructToGoMap(data, i)
				}
			}
			return err
		}
		return fmt.Errorf("unsupported STRUCT arrow type: %T", srcValue)
	case BOOLEAN:
		boolData := srcValue.(*array.Boolean)
		for i := range destcol {
			if !srcValue.IsNull(i) {
				destcol[i] = boolData.Value(i)
			}
		}
		return err
	case DOUBLE:
		for i, flt64 := range srcValue.(*array.Float64).Float64Values() {
			if !srcValue.IsNull(i) {
				destcol[i] = flt64
			}
		}
		return err
	case FLOAT:
		for i, flt32 := range srcValue.(*array.Float32).Float32Values() {
			if !srcValue.IsNull(i) {
				destcol[i] = flt32
			}
		}
		return err
	case STRING, VARCHAR, CHAR, JSON:
		str := srcValue.(*array.String)
		for i := range destcol {
			if !srcValue.IsNull(i) {
				destcol[i] = str.Value(i)
			}
		}
		return err
	case DATE:
		for i, date32 := range srcValue.(*array.Date32).Date32Values() {
			if !srcValue.IsNull(i) {
				t0 := time.Unix(int64(date32)*86400, 0).UTC()
				destcol[i] = t0
			}
		}
		return err
	case TIMESTAMP_LTZ:

		for i, t := range srcValue.(*array.Timestamp).TimestampValues() {
			if !srcValue.IsNull(i) {
				destcol[i] = t.ToTime(arrow.Microsecond).UTC()
			}
		}

		return err
	}

	return fmt.Errorf("unsupported data type")
}

type TypedNullTime struct {
	Time   sql.NullTime
	TzType timezoneType
}

func convertTzTypeToClickzettaType(tzType timezoneType) clickzettaType {
	switch tzType {
	case TimestampLTZType:
		return TIMESTAMP_LTZ
	case DateType:
		return DATE
	}
	return NOT_SUPPORTED
}
