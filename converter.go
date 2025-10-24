package goclickzetta

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
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
