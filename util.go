package goclickzetta

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

// toNamedValues converts a slice of driver.Value to a slice of driver.NamedValue for Go 1.8 SQL package
func toNamedValues(values []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for idx, value := range values {
		namedValues[idx] = driver.NamedValue{Name: "", Ordinal: idx + 1, Value: value}
	}
	return namedValues
}

func splitSQL(query string) []string {
	var ret []string
	var c rune // current char
	var p rune // previous char
	b := 0     // begin of current sql
	state := 1 // current state
	const (
		NORMAL              = 1
		IDENTIFIER          = 2
		SINGLE_QUOTATION    = 3
		DOUBLE_QUOTATION    = 4
		SINGLE_LINE_COMMENT = 5
		MULTI_LINE_COMMENT  = 6
	)

	for i, char := range query {
		c = char
		switch state {
		case NORMAL:
			if c == ';' {
				if i-b > 0 {
					ret = append(ret, query[b:i])
				}
				b = i + 1
				p = 0
			} else if p == '-' && c == '-' {
				state = SINGLE_LINE_COMMENT
				p = 0
			} else if p == '/' && c == '*' {
				state = MULTI_LINE_COMMENT
				p = 0
			} else if c == '`' {
				state = IDENTIFIER
				p = 0
			} else if c == '\'' {
				state = SINGLE_QUOTATION
				p = 0
			} else if c == '"' {
				state = DOUBLE_QUOTATION
				p = 0
			} else {
				p = c
			}
		case IDENTIFIER:
			if c == '`' && p != '\\' {
				state = NORMAL
				p = 0
			} else {
				p = c
			}
		case SINGLE_QUOTATION:
			if c == '\'' && p != '\\' {
				state = NORMAL
				p = 0
			} else if p == '\\' {
				p = 0
			} else {
				p = c
			}
		case DOUBLE_QUOTATION:
			if c == '"' && p != '\\' {
				state = NORMAL
				p = 0
			} else if p == '\\' {
				p = 0
			} else {
				p = c
			}
		case SINGLE_LINE_COMMENT:
			if c == '\n' {
				state = NORMAL
				p = 0
			} else {
				p = c
			}
		case MULTI_LINE_COMMENT:
			if p == '*' && c == '/' {
				state = NORMAL
				p = 0
			} else {
				p = c
			}
		}
	}

	if b < len(query) {
		ret = append(ret, query[b:])
	}

	if ret == nil {
		return []string{}
	}

	return ret
}

func replacePlaceholders(query string, bindings []driver.NamedValue) (string, error) {
	var result strings.Builder
	var bindingIndex int

	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			if bindingIndex >= len(bindings) {
				return "", fmt.Errorf("not enough bindings for placeholders")
			}

			// 获取当前绑定的值
			value := bindings[bindingIndex].Value

			// 根据值的类型进行格式化
			switch v := value.(type) {
			case string:
				cv, _ := handleComplexTypeParam(v)
				result.WriteString(cv)
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				result.WriteString(fmt.Sprintf("%d", v))
			case float32, float64:
				result.WriteString(fmt.Sprintf("%f", v))
			case bool:
				result.WriteString(fmt.Sprintf("%t", v))
			case nil:
				result.WriteString("NULL")
			case time.Time:
				result.WriteString(fmt.Sprintf("timestamp '%s'", v.Format("2006-01-02 15:04:05")))
			default:
				return "", fmt.Errorf("unsupported binding type: %T", value)
			}

			bindingIndex++
		} else {
			result.WriteByte(query[i])
		}
	}

	if bindingIndex < len(bindings) {
		return "", fmt.Errorf("too many bindings for placeholders")
	}

	return result.String(), nil
}

func handleComplexTypeParam(value string) (string, error) {
	// if value is array/map/struct, return it directly; otherwise, add quotes
	if (strings.HasPrefix(value, "array(") && strings.HasSuffix(value, ")")) ||
		(strings.HasPrefix(value, "map(") && strings.HasSuffix(value, ")")) ||
		(strings.HasPrefix(value, "struct(") && strings.HasSuffix(value, ")")) {
		return value, nil
	}
	return fmt.Sprintf("'%s'", value), nil
}
