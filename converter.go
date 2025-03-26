package goclickzetta

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
)

const format = "2006-01-02 15:04:05.999999999"

type timezoneType int

const (
	// TimestampLTZType denotes a LTZ timezoneType for array binds
	TimestampLTZType timezoneType = iota
	// DateType denotes a date type for array binds
	DateType
)

type interfaceArrayBinding struct {
	hasTimezone       bool
	tzType            timezoneType
	timezoneTypeArray interface{}
}

func isInterfaceArrayBinding(t interface{}) bool {
	switch t.(type) {
	case interfaceArrayBinding:
		return true
	case *interfaceArrayBinding:
		return true
	default:
		return false
	}
}

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

// clickzettaTypeToGo translates Clickzetta data type to Go data type.
func clickzettaTypeToGo(dbtype clickzettaType, scale int64) reflect.Type {
	switch dbtype {
	case TINYINT:
		return reflect.TypeOf(int8(0))
	case SMALLINT:
		return reflect.TypeOf(int16(0))
	case INT:
		return reflect.TypeOf(int32(0))
	case BIGINT:
		return reflect.TypeOf(int64(0))
	case FLOAT:
		return reflect.TypeOf(float32(0))
	case DOUBLE:
		return reflect.TypeOf(float64(0))
	case BOOLEAN:
		return reflect.TypeOf(true)
	case STRING, CHAR, VARCHAR:
		return reflect.TypeOf("")
	case DATE, TIMESTAMP_LTZ:
		return reflect.TypeOf(time.Now())
	}
	logger.Errorf("unsupported dbtype is specified. %v", dbtype)
	return reflect.TypeOf("")
}

// valueToString converts arbitrary golang type to a string. This is mainly used in binding data with placeholders
// in queries.
func valueToString(v driver.Value, tsmode clickzettaType) (*string, error) {
	logger.Debugf("TYPE: %v, %v", reflect.TypeOf(v), reflect.ValueOf(v))
	if v == nil {
		return nil, nil
	}
	v1 := reflect.ValueOf(v)
	switch v1.Kind() {
	case reflect.Bool:
		s := strconv.FormatBool(v1.Bool())
		return &s, nil
	case reflect.Int64:
		s := strconv.FormatInt(v1.Int(), 10)
		return &s, nil
	case reflect.Float64:
		s := strconv.FormatFloat(v1.Float(), 'g', -1, 32)
		return &s, nil
	case reflect.String:
		s := v1.String()
		return &s, nil
	case reflect.Slice, reflect.Map:
		if v1.IsNil() {
			return nil, nil
		}
		s := v1.String()
		return &s, nil
	case reflect.Struct:
		switch typedVal := v.(type) {
		case time.Time:
			return timeTypeValueToString(typedVal, tsmode)
		case sql.NullTime:
			if !typedVal.Valid {
				return nil, nil
			}
			return timeTypeValueToString(typedVal.Time, tsmode)
		case sql.NullBool:
			if !typedVal.Valid {
				return nil, nil
			}
			s := strconv.FormatBool(typedVal.Bool)
			return &s, nil
		case sql.NullInt64:
			if !typedVal.Valid {
				return nil, nil
			}
			s := strconv.FormatInt(typedVal.Int64, 10)
			return &s, nil
		case sql.NullFloat64:
			if !typedVal.Valid {
				return nil, nil
			}
			s := strconv.FormatFloat(typedVal.Float64, 'g', -1, 32)
			return &s, nil
		case sql.NullString:
			if !typedVal.Valid {
				return nil, nil
			}
			return &typedVal.String, nil
		}
	}
	return nil, fmt.Errorf("unsupported type: %v", v1.Kind())
}

func timeTypeValueToString(tm time.Time, tsmode clickzettaType) (*string, error) {
	switch tsmode {
	case DATE:
		_, offset := tm.Zone()
		tm = tm.Add(time.Second * time.Duration(offset))
		s := strconv.FormatInt(tm.Unix()*1000, 10)
		return &s, nil
	case TIMESTAMP_LTZ:
		unixTime, _ := new(big.Int).SetString(fmt.Sprintf("%d", tm.Unix()), 10)
		m, _ := new(big.Int).SetString(strconv.FormatInt(1e9, 10), 10)
		unixTime.Mul(unixTime, m)
		tmNanos, _ := new(big.Int).SetString(fmt.Sprintf("%d", tm.Nanosecond()), 10)
		s := unixTime.Add(unixTime, tmNanos).String()
		return &s, nil
	}
	return nil, fmt.Errorf("unsupported time type: %v", tsmode)
}

// extractTimestamp extracts the internal timestamp data to epoch time in seconds and milliseconds
func extractTimestamp(srcValue *string) (sec int64, nsec int64, err error) {
	logger.Debugf("SRC: %v", srcValue)
	var i int
	for i = 0; i < len(*srcValue); i++ {
		if (*srcValue)[i] == '.' {
			sec, err = strconv.ParseInt((*srcValue)[0:i], 10, 64)
			if err != nil {
				return 0, 0, err
			}
			break
		}
	}
	if i == len(*srcValue) {
		// no fraction
		sec, err = strconv.ParseInt(*srcValue, 10, 64)
		if err != nil {
			return 0, 0, err
		}
		nsec = 0
	} else {
		s := (*srcValue)[i+1:]
		nsec, err = strconv.ParseInt(s+strings.Repeat("0", 9-len(s)), 10, 64)
		if err != nil {
			return 0, 0, err
		}
	}
	logger.Infof("sec: %v, nsec: %v", sec, nsec)
	return sec, nsec, nil
}

func stringToValue(
	dest *driver.Value,
	srcColumnMeta execResponseColumnType,
	srcValue *string,
	loc *time.Location,
) error {
	if srcValue == nil {
		logger.Debugf("clickzetta data type: %v, raw value: nil", srcColumnMeta.Type)
		*dest = nil
		return nil
	}
	logger.Debugf("clickzetta data type: %v, raw value: %v", srcColumnMeta.Type, *srcValue)
	switch srcColumnMeta.Type {
	case "DATE":
		v, err := strconv.ParseInt(*srcValue, 10, 64)
		if err != nil {
			return err
		}
		*dest = time.Unix(v*86400, 0).UTC()
		return nil
	case "timestamp_ltz":
		sec, nsec, err := extractTimestamp(srcValue)
		if err != nil {
			return err
		}
		if loc == nil {
			loc = time.Now().Location()
		}
		*dest = time.Unix(sec, nsec).In(loc)
		return nil
	}
	*dest = *srcValue
	return nil
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

func decimalToBigFloat(num decimal128.Num, scale int64) *big.Float {
	f := new(big.Float).SetInt(decimalToBigInt(num))
	s := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
	return new(big.Float).Quo(f, s)
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
			for i, num := range data.Values() {
				if !srcValue.IsNull(i) {
					if srcColumnMeta.Scale == 0 {
						if higherPrecision {
							destcol[i] = num.BigInt()
						} else {
							destcol[i] = num.ToString(0)
						}
					} else {
						f := decimalToBigFloat(num, srcColumnMeta.Scale)
						if higherPrecision {
							destcol[i] = f
						} else {
							destcol[i] = fmt.Sprintf("%.*f", srcColumnMeta.Scale, f)
						}
					}
				}
			}
		case *array.Int64:
			for i, val := range data.Int64Values() {
				if !srcValue.IsNull(i) {
					if srcColumnMeta.Scale == 0 {
						if higherPrecision {
							destcol[i] = val
						} else {
							destcol[i] = fmt.Sprintf("%d", val)
						}
					} else {
						if higherPrecision {
							f := intToBigFloat(val, srcColumnMeta.Scale)
							destcol[i] = f
						} else {
							destcol[i] = fmt.Sprintf("%.*f", srcColumnMeta.Scale, float64(val)/math.Pow10(int(srcColumnMeta.Scale)))
						}
					}
				}
			}
		case *array.Int32:
			for i, val := range data.Int32Values() {
				if !srcValue.IsNull(i) {
					if srcColumnMeta.Scale == 0 {
						if higherPrecision {
							destcol[i] = int64(val)
						} else {
							destcol[i] = fmt.Sprintf("%d", val)
						}
					} else {
						if higherPrecision {
							f := intToBigFloat(int64(val), srcColumnMeta.Scale)
							destcol[i] = f
						} else {
							destcol[i] = fmt.Sprintf("%.*f", srcColumnMeta.Scale, float64(val)/math.Pow10(int(srcColumnMeta.Scale)))
						}
					}
				}
			}
		case *array.Int16:
			for i, val := range data.Int16Values() {
				if !srcValue.IsNull(i) {
					if srcColumnMeta.Scale == 0 {
						if higherPrecision {
							destcol[i] = int64(val)
						} else {
							destcol[i] = fmt.Sprintf("%d", val)
						}
					} else {
						if higherPrecision {
							f := intToBigFloat(int64(val), srcColumnMeta.Scale)
							destcol[i] = f
						} else {
							destcol[i] = fmt.Sprintf("%.*f", srcColumnMeta.Scale, float64(val)/math.Pow10(int(srcColumnMeta.Scale)))
						}
					}
				}
			}
		case *array.Int8:
			for i, val := range data.Int8Values() {
				if !srcValue.IsNull(i) {
					if srcColumnMeta.Scale == 0 {
						if higherPrecision {
							destcol[i] = int64(val)
						} else {
							destcol[i] = fmt.Sprintf("%d", val)
						}
					} else {
						if higherPrecision {
							f := intToBigFloat(int64(val), srcColumnMeta.Scale)
							destcol[i] = f
						} else {
							destcol[i] = fmt.Sprintf("%.*f", srcColumnMeta.Scale, float64(val)/math.Pow10(int(srcColumnMeta.Scale)))
						}
					}
				}
			}
		}
		return err
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
