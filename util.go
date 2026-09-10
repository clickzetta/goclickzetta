package goclickzetta

import (
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// stringLiteralEscapeModeHint tells the server how the driver escaped string
// literals it interpolated into a statement. The driver and the server MUST
// agree on this mode: if the server ignores the hint and parses literals under
// a different convention, escaped quotes can break out of the literal.
const stringLiteralEscapeModeHint = "cz.sql.string.literal.escape.mode"

// timestampFormat is the layout used when binding a time.Time into a statement.
// The server documents microseconds as the maximum TIMESTAMP precision, so the
// fraction stops at six digits; trailing zeros are dropped, leaving whole
// seconds without a decimal point. The zone offset is included: TIMESTAMP is
// LTZ by default and resolves the instant from it, while TIMESTAMP_NTZ ignores
// the offset and keeps the wall clock.
const timestampFormat = "2006-01-02 15:04:05.999999-07:00"

// sqlEscapeMode is a validated string-literal escaping convention. A value from
// configuration passes through resolveStringEscapeMode, which folds case and
// resolves the numeric aliases; from there on only the canonical spellings below
// travel through the driver, including the hint sent to the server.
type sqlEscapeMode string

const (
	// escapeBackslash escapes both ' and \ with a backslash.
	escapeBackslash sqlEscapeMode = "backslash"
	// escapeQuote doubles ' and treats \ as an ordinary character. Only pick it
	// against a server known to honour the hint: see resolveStringEscapeMode.
	escapeQuote sqlEscapeMode = "quote"
	// escapeQuoteBackslash escapes both ' and \ with a backslash.
	escapeQuoteBackslash sqlEscapeMode = "quote_backslash"

	defaultStringEscapeMode = string(escapeBackslash)
)

// RawSQLValue is a binding value interpolated verbatim, with no quoting or
// escaping. It exists for complex-type constructors that cannot be expressed
// as a string literal, for example:
//
//	db.Exec("INSERT INTO t VALUES (?)", goclickzetta.RawSQLValue("array(1,2,3)"))
//
// The caller owns the safety of the fragment. Never build a RawSQLValue out of
// untrusted input: its contents reach the server as executable SQL. The
// fragment must be valid UTF-8, because the request is JSON-encoded and
// invalid bytes would be replaced with U+FFFD on the way out.
type RawSQLValue string

// toNamedValues converts a slice of driver.Value to a slice of driver.NamedValue for Go 1.8 SQL package
func toNamedValues(values []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for idx, value := range values {
		namedValues[idx] = driver.NamedValue{Name: "", Ordinal: idx + 1, Value: value}
	}
	return namedValues
}

// isRawStringPrefix reports whether the quote at index quote opens a raw string
// literal, that is whether it is directly preceded by the r or R prefix.
//
// A raw string processes no escapes: r'C:\' is a complete literal holding one
// backslash, and length(r'a\\b') is 4. Without this the scanners would read the
// backslash in front of the closing quote as an escape and run past the end of
// the literal.
//
// The byte in front of the prefix has to be a non-identifier one, so that a name
// ending in r followed by a quote is left alone. Bytes above ASCII count as
// identifier bytes, because a multi-byte rune can be part of a name.
func isRawStringPrefix(query string, quote int) bool {
	if quote == 0 {
		return false
	}
	if prefix := query[quote-1]; prefix != 'r' && prefix != 'R' {
		return false
	}
	if quote == 1 {
		return true
	}
	return !isIdentifierByte(query[quote-2])
}

func isIdentifierByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	case b == '_', b == '$', b >= 0x80:
		return true
	default:
		return false
	}
}

// splitSQL splits query on statement separators, skipping semicolons that sit
// inside literals, quoted identifiers or comments. It takes the escape mode
// because whether a backslash escapes decides where a literal ends: under quote
// mode 'C:\' is complete, under the backslash convention it is not.
func splitSQL(query string, mode sqlEscapeMode) []string {
	// Callers validate the mode before scanning. Keep this helper free of
	// implicit trimming, case folding, or fallback behaviour.
	backslashEscapes := mode != escapeQuote
	// A raw string escapes nothing, so the backslash rule is suspended for the
	// literal that r or R opened.
	rawLiteral := false
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
				rawLiteral = isRawStringPrefix(query, i)
				p = 0
			} else if c == '"' {
				state = DOUBLE_QUOTATION
				rawLiteral = isRawStringPrefix(query, i)
				p = 0
			} else {
				p = c
			}
		case IDENTIFIER:
			if c == '`' && !(backslashEscapes && p == '\\') {
				state = NORMAL
				p = 0
			} else {
				p = c
			}
		case SINGLE_QUOTATION:
			escapes := backslashEscapes && !rawLiteral
			if c == '\'' && !(escapes && p == '\\') {
				state = NORMAL
				p = 0
			} else if escapes && p == '\\' {
				p = 0
			} else {
				p = c
			}
		case DOUBLE_QUOTATION:
			escapes := backslashEscapes && !rawLiteral
			if c == '"' && !(escapes && p == '\\') {
				state = NORMAL
				p = 0
			} else if escapes && p == '\\' {
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

func isInsertStatement(query string) bool {
	const keyword = "INSERT"
	query = strings.TrimSpace(query)
	for query != "" {
		switch {
		case strings.HasPrefix(query, "--"):
			newline := strings.IndexByte(query, '\n')
			if newline < 0 {
				return false
			}
			query = strings.TrimSpace(query[newline+1:])
		case strings.HasPrefix(query, "/*"):
			end := strings.Index(query[2:], "*/")
			if end < 0 {
				return false
			}
			query = strings.TrimSpace(query[end+4:])
		default:
			// EqualFold on the prefix alone: strings.ToUpper would copy the
			// whole statement, and this runs on every exec.
			if len(query) < len(keyword) || !strings.EqualFold(query[:len(keyword)], keyword) {
				return false
			}
			// Reject identifiers that merely start with INSERT, e.g. INSERTED.
			if len(query) == len(keyword) {
				return true
			}
			next := query[len(keyword)]
			return !(next == '_' || next == '$' ||
				(next >= '0' && next <= '9') ||
				(next >= 'a' && next <= 'z') ||
				(next >= 'A' && next <= 'Z'))
		}
	}
	return false
}

// resolveStringEscapeMode resolves a mode coming from configuration (DSN or
// DriverFlags).
//
// The accepted spellings match the Java driver's fromConfig so a configuration
// can be moved between the two: leading and trailing space is trimmed, case is
// folded, an empty value means "not configured" and selects the default, and the
// numeric aliases 0, 1, 2 and 3 are recognised alongside the names.
//
// escapeQuote (also spelled 2) is accepted for that compatibility, but it is
// only safe against a server that parses literals under the same convention. A
// server that ignores the hint drops a doubled single quote and still treats \
// as an escape, so a quote goes missing and a value ending in a backslash
// swallows the closing quote, which is the break-out this escaping exists to
// prevent. Callers own that decision; the default is escapeBackslash, which
// does not depend on the hint taking effect.
func resolveStringEscapeMode(value string) (sqlEscapeMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		// Not configured. defaultStringEscapeMode is what the Java driver
		// falls back to as well.
		return sqlEscapeMode(defaultStringEscapeMode), nil
	case string(escapeBackslash), "0", "1":
		return escapeBackslash, nil
	case string(escapeQuote), "2":
		return escapeQuote, nil
	case string(escapeQuoteBackslash), "3":
		return escapeQuoteBackslash, nil
	default:
		return "", fmt.Errorf("unsupported %s value %q; expected %q, %q, or %q", stringLiteralEscapeModeHint, value, escapeBackslash, escapeQuote, escapeQuoteBackslash)
	}
}

func validateStringEscapeMode(value string) (sqlEscapeMode, error) {
	mode := sqlEscapeMode(value)
	switch mode {
	case escapeBackslash, escapeQuote, escapeQuoteBackslash:
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported %s value %q; expected %q, %q, or %q", stringLiteralEscapeModeHint, value, escapeBackslash, escapeQuote, escapeQuoteBackslash)
	}
}

const (
	normalState = iota
	singleQuoteState
	doubleQuoteState
	backtickState
	lineCommentState
	blockCommentState
)

// replacePlaceholders interpolates bindings into the ? placeholders of query.
// Placeholders are only recognised outside string literals, quoted identifiers
// and comments, so a ? that is part of the statement text is left alone.
func replacePlaceholders(query string, bindings []driver.NamedValue, mode sqlEscapeMode) (string, error) {
	mode, err := validateStringEscapeMode(string(mode))
	if err != nil {
		return "", err
	}

	var result strings.Builder
	result.Grow(len(query))
	var bindingIndex int

	state := normalState
	// A raw string escapes nothing, so the backslash rule and the doubled-quote
	// rule are both suspended for the literal that r or R opened.
	rawLiteral := false
	for i := 0; i < len(query); {
		ch := query[i]
		switch state {
		case normalState:
			switch {
			case ch == '-' && i+1 < len(query) && query[i+1] == '-':
				result.WriteString("--")
				i += 2
				state = lineCommentState
			case ch == '/' && i+1 < len(query) && query[i+1] == '*':
				result.WriteString("/*")
				i += 2
				state = blockCommentState
			case ch == '\'':
				rawLiteral = isRawStringPrefix(query, i)
				result.WriteByte(ch)
				i++
				state = singleQuoteState
			case ch == '"':
				rawLiteral = isRawStringPrefix(query, i)
				result.WriteByte(ch)
				i++
				state = doubleQuoteState
			case ch == '`':
				// A quoted identifier has no raw form.
				rawLiteral = false
				result.WriteByte(ch)
				i++
				state = backtickState
			case ch == '?':
				if bindingIndex >= len(bindings) {
					return "", fmt.Errorf("not enough bindings for placeholders")
				}
				// "SELECT 1-?" bound to -5 would interpolate to "SELECT 1--5",
				// where -- opens a line comment and silently truncates the
				// statement. A space keeps the two signs apart. It is written
				// whatever the value is, because whitespace cannot change the
				// meaning of a statement here, while wrapping the literal in
				// parentheses can: INTERVAL (-5) DAY yields a timestamp_ltz
				// where INTERVAL -5 DAY yields a date.
				//
				// query[i-1] is the byte just written: this branch only runs in
				// normalState, where bytes are copied verbatim, and -- is
				// consumed as a pair that switches to lineCommentState, so a lone
				// - here really is the character in front of the placeholder.
				if i > 0 && query[i-1] == '-' {
					result.WriteByte(' ')
				}
				if err := appendSQLValue(&result, bindings[bindingIndex].Value, mode); err != nil {
					return "", fmt.Errorf("binding %d: %w", bindingIndex+1, err)
				}
				bindingIndex++
				i++
			default:
				result.WriteByte(ch)
				i++
			}
		case singleQuoteState, doubleQuoteState, backtickState:
			result.WriteByte(ch)
			i++
			if ch == '\\' && mode != escapeQuote && !rawLiteral && i < len(query) {
				result.WriteByte(query[i])
				i++
				continue
			}
			if (state == singleQuoteState && ch == '\'') ||
				(state == doubleQuoteState && ch == '"') ||
				(state == backtickState && ch == '`') {
				// Treat doubled delimiters as escaped for placeholder scanning. This
				// is compatible with quote mode and prevents ? inside a literal from
				// being mistaken for a binding in any mode. A raw string is exempt:
				// it ends at the first closing quote, so length(r'a''b') is 2.
				if !rawLiteral && i < len(query) && query[i] == ch {
					result.WriteByte(query[i])
					i++
				} else {
					state = normalState
				}
			}
		case lineCommentState:
			result.WriteByte(ch)
			i++
			if ch == '\n' {
				state = normalState
			}
		case blockCommentState:
			result.WriteByte(ch)
			i++
			if ch == '*' && i < len(query) && query[i] == '/' {
				result.WriteByte(query[i])
				i++
				state = normalState
			}
		}
	}

	// Report an unbalanced delimiter as such. Otherwise every ? after it is
	// swallowed as literal text and the caller gets a "too many bindings" error
	// pointing at the argument list instead of at the real problem.
	switch state {
	case singleQuoteState:
		return "", fmt.Errorf("unterminated string literal in query")
	case doubleQuoteState:
		return "", fmt.Errorf(`unterminated double-quoted section in query`)
	case backtickState:
		return "", fmt.Errorf("unterminated quoted identifier in query")
	case blockCommentState:
		return "", fmt.Errorf("unterminated block comment in query")
	}

	if bindingIndex < len(bindings) {
		return "", fmt.Errorf("too many bindings for placeholders")
	}

	return result.String(), nil
}

func appendSQLValue(result *strings.Builder, value driver.Value, mode sqlEscapeMode) error {
	switch v := value.(type) {
	case RawSQLValue:
		if !utf8.ValidString(string(v)) {
			return fmt.Errorf("raw SQL value is not valid UTF-8")
		}
		result.WriteString(string(v))
	case string:
		literal, err := encodeSQLStringLiteral(v, mode)
		if err != nil {
			return err
		}
		result.WriteString(literal)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		result.WriteString(fmt.Sprintf("%d", v))
	case float32:
		return appendFloatValue(result, float64(v), 32)
	case float64:
		return appendFloatValue(result, v, 64)
	case []byte:
		// X'<hex>' is the BINARY literal form; an empty slice becomes X''.
		result.WriteString("X'")
		for _, b := range v {
			result.WriteByte(hexUpper[b>>4])
			result.WriteByte(hexUpper[b&0x0f])
		}
		result.WriteByte('\'')
	case bool:
		result.WriteString(fmt.Sprintf("%t", v))
	case nil:
		result.WriteString("NULL")
	case time.Time:
		result.WriteString(fmt.Sprintf("timestamp '%s'", v.Format(timestampFormat)))
	default:
		// sql.NullString and friends, plus pointer types, only satisfy
		// driver.Valuer. CheckNamedValue accepts everything so database/sql
		// never converts them for us; do it here.
		converted, err := driver.DefaultParameterConverter.ConvertValue(value)
		if err != nil {
			return fmt.Errorf("unsupported binding type %T: %w", value, err)
		}
		if reflect.TypeOf(converted) == reflect.TypeOf(value) {
			// Conversion was a no-op, so recursing would not terminate.
			return fmt.Errorf("unsupported binding type: %T", value)
		}
		return appendSQLValue(result, converted, mode)
	}
	return nil
}

// hexUpper is the alphabet for BINARY literals; the server prints them
// uppercase, so bindings match what a round trip returns.
const hexUpper = "0123456789ABCDEF"

// appendFloatValue writes the shortest representation that round-trips back to
// the same value. %f would silently truncate to six decimals, turning 1.5e-10
// into 0.000000.
func appendFloatValue(result *strings.Builder, value float64, bitSize int) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("cannot encode non-finite float value %v", value)
	}
	result.WriteString(strconv.FormatFloat(value, 'g', -1, bitSize))
	return nil
}

// encodeSQLStringLiteral wraps a valid UTF-8 value in single quotes, escaping
// according to mode. It walks bytes rather than runes so valid multi-byte UTF-8
// sequences are copied byte-for-byte; invalid UTF-8 is rejected because the
// ClickZetta STRING type only accepts valid UTF-8 and JSON would otherwise
// replace invalid bytes with U+FFFD before the request reaches the server.
func encodeSQLStringLiteral(value string, mode sqlEscapeMode) (string, error) {
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("string binding is not valid UTF-8")
	}
	mode, err := validateStringEscapeMode(string(mode))
	if err != nil {
		return "", err
	}
	var result strings.Builder
	result.Grow(len(value) + 2)
	result.WriteByte('\'')
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if mode == escapeQuote {
			// Backslash carries no meaning in this mode, and a literal newline
			// or tab inside a quoted string is valid SQL, so both pass through.
			// NUL is the one byte with no safe representation here.
			switch ch {
			case '\x00':
				return "", fmt.Errorf("cannot encode NUL byte in %s mode", escapeQuote)
			case '\'':
				result.WriteString("''")
			default:
				result.WriteByte(ch)
			}
			continue
		}
		switch ch {
		case '\'':
			// Both remaining modes keep the backslash escape. quote_backslash
			// also accepts '' but doubling is riskier: the documented example
			// shows a doubled quote collapsing to nothing, so \' is used for
			// both.
			result.WriteString(`\'`)
		case '\\':
			result.WriteString(`\\`)
		case '\x00':
			result.WriteString(`\0`)
		case '\b':
			result.WriteString(`\b`)
		case '\n':
			result.WriteString(`\n`)
		case '\r':
			result.WriteString(`\r`)
		case '\t':
			result.WriteString(`\t`)
		case '\x1a':
			result.WriteString(`\Z`)
		default:
			result.WriteByte(ch)
		}
	}
	result.WriteByte('\'')
	return result.String(), nil
}

// shouldUseArrowBindings reports whether bindings should be sent as Arrow IPC
// binary data instead of being interpolated into the statement. The Arrow
// encoder expects the whole batch in bindings[0], so a single-row INSERT still
// has to go through interpolation even under separate_params.
func shouldUseArrowBindings(query string, bindings []driver.NamedValue, isSeparate bool) bool {
	if len(bindings) == 0 || !isSeparate || !isInsertStatement(query) {
		return false
	}
	rows, ok := bindings[0].Value.([][]interface{})
	// An empty batch has no Arrow payload to send, and routing it there would
	// leave the placeholders in the statement unbound.
	return ok && len(rows) > 0
}
