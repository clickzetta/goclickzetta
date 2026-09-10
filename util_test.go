package goclickzetta

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestTimeFormat(t *testing.T) {
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.999999999"))
}

func TestEncodeSQLStringLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"ordinary", "ordinary text", "'ordinary text'"},
		{"single quote", "O'Brien", `'O\'Brien'`},
		{"like pattern", "%'%", `'%\'%'`},
		{"backslash", `C:\Temp\new`, `'C:\\Temp\\new'`},
		{"controls", "line\n\treturn\r\x00\x1a", `'line\n\treturn\r\0\Z'`},
		{"unicode", "中文🙂", "'中文🙂'"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeSQLStringLiteral(tc.in, "backslash")
			if err != nil {
				t.Fatalf("encodeSQLStringLiteral() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("encodeSQLStringLiteral() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReplacePlaceholdersSkipsSQLLiteralsAndComments(t *testing.T) {
	query := `SELECT '?' AS literal, ? AS value, "?" AS identifier /* ? */ -- ?
? AS second`
	bindings := []driver.NamedValue{
		{Ordinal: 1, Value: "O'Brien"},
		{Ordinal: 2, Value: "%'%"},
	}
	want := `SELECT '?' AS literal, 'O\'Brien' AS value, "?" AS identifier /* ? */ -- ?
'%\'%' AS second`
	got, err := replacePlaceholders(query, bindings, escapeBackslash)
	if err != nil {
		t.Fatalf("replacePlaceholders() error = %v", err)
	}
	if got != want {
		t.Fatalf("replacePlaceholders() = %q, want %q", got, want)
	}
}

func TestReplacePlaceholdersQuoteModeBackslashIsNotEscape(t *testing.T) {
	query := `SELECT 'path\\' AS literal, ? AS value`
	want := `SELECT 'path\\' AS literal, 'value' AS value`
	got, err := replacePlaceholders(query, []driver.NamedValue{{Ordinal: 1, Value: "value"}}, "quote")
	if err != nil {
		t.Fatalf("replacePlaceholders() error = %v", err)
	}
	if got != want {
		t.Fatalf("replacePlaceholders() = %q, want %q", got, want)
	}
}

func TestReplacePlaceholdersBindingErrors(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		bindings []driver.NamedValue
	}{
		{"not enough", "SELECT ?", nil},
		{"too many", "SELECT 1", []driver.NamedValue{{Ordinal: 1, Value: "value"}}},
		{"unsupported type", "SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: map[string]string{"a": "b"}}}},
		{"unsupported slice", "SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: []string{"a"}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := replacePlaceholders(tc.query, tc.bindings, escapeBackslash); err == nil {
				t.Fatal("replacePlaceholders() error = nil, want error")
			}
		})
	}
}

func TestIsInsertStatement(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"INSERT INTO t VALUES (?)", true},
		{"  insert into t values (?)", true},
		{"-- comment\nINSERT INTO t VALUES (?)", true},
		{"/* comment */\ninsert into t values (?)", true},
		{"SELECT * FROM t", false},
		{"-- comment only", false},
	}
	for _, tc := range tests {
		if got := isInsertStatement(tc.query); got != tc.want {
			t.Errorf("isInsertStatement(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
}

func TestEncodeSQLStringLiteralModes(t *testing.T) {
	tests := []struct {
		mode sqlEscapeMode
		want string
	}{
		{escapeQuote, `'O''Brien\Temp'`},
		{escapeQuoteBackslash, `'O\'Brien\\Temp'`},
	}
	for _, tc := range tests {
		got, err := encodeSQLStringLiteral("O'Brien\\Temp", tc.mode)
		if err != nil {
			t.Fatalf("mode %s: encodeSQLStringLiteral() error = %v", tc.mode, err)
		}
		if got != tc.want {
			t.Errorf("mode %s: got %q, want %q", tc.mode, got, tc.want)
		}
	}
	if _, err := encodeSQLStringLiteral("a\x00b", escapeQuote); err == nil {
		t.Fatal("quote mode accepted a NUL byte without a safe encoding")
	}
}

func TestEncodeSQLStringLiteralRejectsInvalidUTF8(t *testing.T) {
	in := string([]byte{0x41, 0xff, 0xfe, 0x42})
	for _, mode := range []sqlEscapeMode{escapeBackslash, escapeQuote, escapeQuoteBackslash} {
		if _, err := encodeSQLStringLiteral(in, mode); err == nil {
			t.Errorf("mode %s: expected invalid UTF-8 error", mode)
		}
	}
}

func TestReplacePlaceholdersRejectsInvalidUTF8RawSQLValue(t *testing.T) {
	value := RawSQLValue(string([]byte{'S', 0xff, 'L'}))
	if _, err := replacePlaceholders("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: value}}, escapeBackslash); err == nil {
		t.Fatal("expected invalid UTF-8 error for RawSQLValue")
	}
}

func TestEncodeSQLStringLiteralQuoteModeKeepsControlBytes(t *testing.T) {
	// A literal newline or tab inside a single-quoted string is valid SQL and
	// needs no escape, so quote mode must pass it through.
	got, err := encodeSQLStringLiteral("line\n\treturn\r", "quote")
	if err != nil {
		t.Fatalf("encodeSQLStringLiteral() error = %v", err)
	}
	if want := "'line\n\treturn\r'"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// NUL has no safe representation in quote mode and must still be rejected.
	if _, err := encodeSQLStringLiteral("a\x00b", "quote"); err == nil {
		t.Error("quote mode accepted a NUL byte without a safe encoding")
	}
}

func TestAppendSQLValueFloatFormatting(t *testing.T) {
	tests := []struct {
		value driver.Value
		want  string
	}{
		{1.5e-10, "1.5e-10"},
		{1.234567891234, "1.234567891234"},
		{float32(0.1), "0.1"},
		{float64(0), "0"},
		{-2.5, "-2.5"},
		{1.5e-10 * -1, "-1.5e-10"},
	}
	for _, tc := range tests {
		got, err := replacePlaceholders("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: tc.value}}, "backslash")
		if err != nil {
			t.Fatalf("value %v: replacePlaceholders() error = %v", tc.value, err)
		}
		if want := "SELECT " + tc.want; got != want {
			t.Errorf("value %v: got %q, want %q", tc.value, got, want)
		}
	}
	for _, v := range []driver.Value{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := replacePlaceholders("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: v}}, "backslash"); err == nil {
			t.Errorf("value %v: error = nil, want error for non-finite float", v)
		}
	}
}

func TestReplacePlaceholdersUnterminatedSections(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"unterminated single quote", "SELECT 'unclosed, ?"},
		{"unterminated double quote", `SELECT "unclosed, ?`},
		{"unterminated backtick", "SELECT `unclosed, ?"},
		{"unterminated block comment", "SELECT 1 /* unclosed, ?"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := replacePlaceholders(tc.query, []driver.NamedValue{{Ordinal: 1, Value: "x"}}, "backslash")
			if err == nil {
				t.Fatal("error = nil, want an unterminated-section error")
			}
			if strings.Contains(err.Error(), "too many bindings") {
				t.Errorf("error blames the binding list instead of the unbalanced delimiter: %v", err)
			}
		})
	}
}

func TestIsInsertStatementPrefixBoundary(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"INSERTX INTO t VALUES (?)", false},
		{"INSERT", true},
		{"INSERTED", false},
		{"INSER", false},
		{"", false},
	}
	for _, tc := range tests {
		if got := isInsertStatement(tc.query); got != tc.want {
			t.Errorf("isInsertStatement(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
}

func TestReplacePlaceholdersRawSQLValue(t *testing.T) {
	// Complex-type constructors go through RawSQLValue now. A plain string that
	// happens to look like one stays a quoted literal: prefix sniffing on user
	// strings was the injection hole this replaces.
	got, err := replacePlaceholders("INSERT INTO t VALUES (?, ?)", []driver.NamedValue{
		{Ordinal: 1, Value: RawSQLValue("array(1,2,3)")},
		{Ordinal: 2, Value: "array(1,2,3)"},
	}, escapeBackslash)
	if err != nil {
		t.Fatalf("replacePlaceholders() error = %v", err)
	}
	want := "INSERT INTO t VALUES (array(1,2,3), 'array(1,2,3)')"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestShouldUseArrowBindings(t *testing.T) {
	batch := []driver.NamedValue{{Ordinal: 1, Value: [][]interface{}{{1, "a"}}}}
	single := []driver.NamedValue{{Ordinal: 1, Value: "x"}}
	tests := []struct {
		name       string
		query      string
		bindings   []driver.NamedValue
		isSeparate bool
		want       bool
	}{
		{"batch insert with separate_params", "INSERT INTO t VALUES (?)", batch, true, true},
		{"lowercase batch insert", "insert into t values (?)", batch, true, true},
		{"single-row insert falls back to interpolation", "insert into t values (?)", single, true, false},
		{"batch insert without separate_params", "INSERT INTO t VALUES (?)", batch, false, false},
		{"select is never an arrow batch", "SELECT ?", batch, true, false},
		{"no bindings", "INSERT INTO t VALUES (1)", nil, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldUseArrowBindings(tc.query, tc.bindings, tc.isSeparate); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReplacePlaceholdersRejectsNonExactMode(t *testing.T) {
	// Escape mode values are configuration contract values. Do not silently
	// trim, case-fold, or replace an invalid value with the default mode.
	query := `SELECT 'C:\' AS p, ? AS v`
	bindings := []driver.NamedValue{{Ordinal: 1, Value: "x"}}
	want, err := replacePlaceholders(query, bindings, escapeQuote)
	if err != nil {
		t.Fatalf("canonical mode: replacePlaceholders() error = %v", err)
	}
	for _, mode := range []sqlEscapeMode{"QUOTE", " quote ", "Quote", ""} {
		if _, err := replacePlaceholders(query, bindings, mode); err == nil {
			t.Errorf("mode %q: expected validation error", string(mode))
		}
	}
	if got, err := replacePlaceholders(query, bindings, escapeQuote); err != nil || got != want {
		t.Errorf("exact quote mode: got %q err=%v, want %q", got, err, want)
	}
}

func TestAppendSQLValueTimestampPrecision(t *testing.T) {
	shanghai := time.FixedZone("CST", 8*3600)
	tests := []struct {
		name  string
		value time.Time
		want  string
	}{
		{
			// The server documents microseconds as the maximum precision, so
			// nanosecond digits are truncated rather than sent.
			"precision is capped at microseconds",
			time.Date(2026, 9, 9, 18, 30, 45, 123456789, shanghai),
			"timestamp '2026-09-09 18:30:45.123456+08:00'",
		},
		{
			"whole seconds carry no fraction",
			time.Date(2026, 9, 9, 18, 30, 45, 0, shanghai),
			"timestamp '2026-09-09 18:30:45+08:00'",
		},
		{
			"milliseconds are not padded",
			time.Date(2026, 9, 9, 18, 30, 45, 123000000, shanghai),
			"timestamp '2026-09-09 18:30:45.123+08:00'",
		},
		{
			// The offset is what lets TIMESTAMP (LTZ by default) resolve the
			// instant; TIMESTAMP_NTZ ignores it and keeps the wall clock.
			"UTC renders a zero offset",
			time.Date(2026, 9, 9, 10, 30, 45, 0, time.UTC),
			"timestamp '2026-09-09 10:30:45+00:00'",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := replacePlaceholders("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: tc.value}}, escapeBackslash)
			if err != nil {
				t.Fatalf("replacePlaceholders() error = %v", err)
			}
			if want := "SELECT " + tc.want; got != want {
				t.Errorf("got %q, want %q", got, want)
			}
		})
	}
}

func TestSplitSQLHonorsEscapeMode(t *testing.T) {
	// In quote mode a backslash is an ordinary character, so 'C:\' is a complete
	// literal and the trailing semicolon is a statement separator. Under the
	// backslash convention the same text is an unterminated literal, so the
	// semicolon belongs to it.
	query := `SELECT 'C:\' AS p;`
	if got := splitSQL(query, escapeQuote); len(got) != 1 || got[0] != `SELECT 'C:\' AS p` {
		t.Errorf("quote mode: splitSQL() = %q, want [%q]", got, `SELECT 'C:\' AS p`)
	}
	if got := splitSQL(query, escapeBackslash); len(got) != 1 || got[0] != query {
		t.Errorf("backslash mode: splitSQL() = %q, want [%q]", got, query)
	}
	// splitSQL assumes its caller has already validated the mode; invalid
	// configuration is rejected before this helper is reached.
	// A semicolon inside a literal is never a separator, in any mode.
	for _, mode := range []sqlEscapeMode{escapeBackslash, escapeQuote, escapeQuoteBackslash} {
		if got := splitSQL(`SELECT 'a;b' FROM t;`, mode); len(got) != 1 || got[0] != `SELECT 'a;b' FROM t` {
			t.Errorf("mode %s: splitSQL() = %q", mode, got)
		}
	}
}

func TestAppendSQLValueUnwrapsValuer(t *testing.T) {
	// CheckNamedValue accepts every value, which bypasses the default converter
	// in database/sql, so driver.Valuer wrappers have to be unwrapped here.
	tests := []struct {
		name  string
		value driver.Value
		want  string
	}{
		{"NullString set", sql.NullString{String: "O'Brien", Valid: true}, `'O\'Brien'`},
		{"NullString null", sql.NullString{Valid: false}, "NULL"},
		{"NullInt64 set", sql.NullInt64{Int64: 7, Valid: true}, "7"},
		{"NullFloat64 set", sql.NullFloat64{Float64: 1.5e-10, Valid: true}, "1.5e-10"},
		{"NullBool set", sql.NullBool{Bool: true, Valid: true}, "true"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := replacePlaceholders("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: tc.value}}, escapeBackslash)
			if err != nil {
				t.Fatalf("replacePlaceholders() error = %v", err)
			}
			if want := "SELECT " + tc.want; got != want {
				t.Errorf("got %q, want %q", got, want)
			}
		})
	}
}

func TestAppendSQLValueBinary(t *testing.T) {
	// BINARY literals use the X'hex' form documented for ClickZetta.
	got, err := replacePlaceholders("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: []byte("Hello")}}, escapeBackslash)
	if err != nil {
		t.Fatalf("replacePlaceholders() error = %v", err)
	}
	if want := "SELECT X'48656C6C6F'"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// Binary is the one type where arbitrary bytes are fine: hex has no UTF-8
	// constraint and survives JSON encoding.
	got, err = replacePlaceholders("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: []byte{0x00, 0xff}}}, escapeBackslash)
	if err != nil {
		t.Fatalf("replacePlaceholders() error = %v", err)
	}
	if want := "SELECT X'00FF'"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, err := replacePlaceholders("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: []byte{}}}, escapeBackslash); err != nil || got != "SELECT X''" {
		t.Errorf("empty binary: got %q err=%v, want \"SELECT X''\"", got, err)
	}
}

func TestEncodeSQLStringLiteralQuoteBackslashUsesBackslashForQuote(t *testing.T) {
	// The documented quote_backslash example shows '' collapsing to nothing
	// rather than to one quote, so escape the quote with a backslash, which that
	// mode definitely honours.
	got, err := encodeSQLStringLiteral(`O'Brien\Temp`, escapeQuoteBackslash)
	if err != nil {
		t.Fatalf("encodeSQLStringLiteral() error = %v", err)
	}
	if want := `'O\'Brien\\Temp'`; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestResolveStringEscapeMode(t *testing.T) {
	// 接受的写法与 Java 驱动的 fromConfig 对齐:去空白、忽略大小写、空值取默认,
	// 并识别数字别名 0/1/2/3。
	accepted := map[string]sqlEscapeMode{
		"":                  sqlEscapeMode(defaultStringEscapeMode),
		"   ":               sqlEscapeMode(defaultStringEscapeMode),
		"backslash":         escapeBackslash,
		"BACKSLASH":         escapeBackslash,
		" Backslash ":       escapeBackslash,
		"0":                 escapeBackslash,
		"1":                 escapeBackslash,
		"quote":             escapeQuote,
		"QUOTE":             escapeQuote,
		" quote ":           escapeQuote,
		"2":                 escapeQuote,
		"quote_backslash":   escapeQuoteBackslash,
		"QUOTE_BACKSLASH":   escapeQuoteBackslash,
		"\tquote_backslash": escapeQuoteBackslash,
		"3":                 escapeQuoteBackslash,
	}
	for value, want := range accepted {
		got, err := resolveStringEscapeMode(value)
		if err != nil {
			t.Errorf("resolveStringEscapeMode(%q) error = %v", value, err)
			continue
		}
		if got != want {
			t.Errorf("resolveStringEscapeMode(%q) = %q, want %q", value, string(got), string(want))
		}
	}

	for _, value := range []string{"none", "4", "-1", "back slash", "quote-backslash"} {
		if _, err := resolveStringEscapeMode(value); err == nil {
			t.Errorf("resolveStringEscapeMode(%q) error = nil, want error", value)
		}
	}
}

func TestAppendSQLValueNegativeNumbersDoNotFormComment(t *testing.T) {
	// "SELECT 1-?" 绑定负数时,裸的负号会和前面的减号连成 --,把语句其余部分变成
	// 行注释。占位符紧跟 - 时插入一个空格把两个符号隔开。
	cases := []struct {
		value driver.Value
		want  string
	}{
		{-5, `SELECT 1- -5`},
		{int64(-5), `SELECT 1- -5`},
		{int32(-5), `SELECT 1- -5`},
		{-1.5, `SELECT 1- -1.5`},
		{float32(-1.5), `SELECT 1- -1.5`},
		{5, `SELECT 1- 5`},
		{1.5, `SELECT 1- 1.5`},
		{uint64(5), `SELECT 1- 5`},
	}
	for _, c := range cases {
		got, err := replacePlaceholders(`SELECT 1-?`, []driver.NamedValue{{Ordinal: 1, Value: c.value}}, escapeBackslash)
		if err != nil {
			t.Errorf("replacePlaceholders(%v) error = %v", c.value, err)
			continue
		}
		if got != c.want {
			t.Errorf("replacePlaceholders(%v) = %q, want %q", c.value, got, c.want)
		}
		if strings.Contains(got, "--") {
			t.Errorf("replacePlaceholders(%v) = %q, contains a line comment", c.value, got)
		}
	}
}

func TestShouldUseArrowBindingsRejectsEmptyBatch(t *testing.T) {
	// 空批次没有 Arrow 载荷可发,走 Arrow 路径会把 ? 原样留在语句里。
	empty := []driver.NamedValue{{Ordinal: 1, Value: [][]interface{}{}}}
	if shouldUseArrowBindings("INSERT INTO t VALUES (?)", empty, true) {
		t.Error("shouldUseArrowBindings(empty batch) = true, want false")
	}
	batch := []driver.NamedValue{{Ordinal: 1, Value: [][]interface{}{{1}, {2}}}}
	if !shouldUseArrowBindings("INSERT INTO t VALUES (?)", batch, true) {
		t.Error("shouldUseArrowBindings(batch) = false, want true")
	}
}

func TestSplitSQLSeparatorOnlyQueries(t *testing.T) {
	// 只有分隔符时返回空切片,调用方必须处理,否则取最后一条会越界。
	for _, query := range []string{";", ";;", ""} {
		if got := splitSQL(query, escapeBackslash); len(got) != 0 {
			t.Errorf("splitSQL(%q) = %#v, want empty", query, got)
		}
	}
}

// TestRawStringLiteralsAreNotEscaped covers the r/R prefix. A raw string
// processes no escapes, so the literal ends at the first closing quote even when
// a backslash sits in front of it; COPY INTO options such as 'escape'=r'\' have
// exactly that shape.
func TestRawStringLiteralsAreNotEscaped(t *testing.T) {
	interpolation := []struct {
		query string
		want  string
	}{
		{`SELECT r'C:\', ?`, `SELECT r'C:\', 1`},
		{`SELECT R'C:\', ?`, `SELECT R'C:\', 1`},
		{`SELECT r"C:\", ?`, `SELECT r"C:\", 1`},
		// A raw string ends at the first quote, so the ? here is statement text.
		{`SELECT ?, r'a?b'`, `SELECT 1, r'a?b'`},
		{`SELECT r'a''b', ?`, `SELECT r'a''b', 1`},
		{`SELECT regexp_extract(?, r'(\d+)', 1)`, `SELECT regexp_extract(1, r'(\d+)', 1)`},
		{`SELECT 'C:\\', ?`, `SELECT 'C:\\', 1`},
	}
	for _, tc := range interpolation {
		got, err := replacePlaceholders(tc.query, []driver.NamedValue{{Ordinal: 1, Value: 1}}, escapeBackslash)
		if err != nil {
			t.Errorf("replacePlaceholders(%q) error = %v", tc.query, err)
			continue
		}
		if got != tc.want {
			t.Errorf("replacePlaceholders(%q) = %q, want %q", tc.query, got, tc.want)
		}
	}

	// A name ending in r followed by a quote is left alone, so the backslash
	// still escapes and the literal really is unterminated.
	if _, err := replacePlaceholders(`SELECT var'a\', ?`, []driver.NamedValue{{Ordinal: 1, Value: 1}}, escapeBackslash); err == nil {
		t.Error("replacePlaceholders(identifier ending in r) error = nil, want unterminated literal")
	}

	splits := []struct {
		query string
		want  int
	}{
		{`SELECT r'C:\'; SELECT 1`, 2},
		{`SELECT R"C:\"; SELECT 1`, 2},
		{`SELECT 'C:\\'; SELECT 1`, 2},
		// The separator is inside the literal in both spellings.
		{`SELECT r'a;b'`, 1},
		{`SELECT 'a;b'`, 1},
	}
	for _, tc := range splits {
		if got := splitSQL(tc.query, escapeBackslash); len(got) != tc.want {
			t.Errorf("splitSQL(%q) = %d statements %q, want %d", tc.query, len(got), got, tc.want)
		}
	}
}

func TestIsRawStringPrefix(t *testing.T) {
	tests := []struct {
		query string
		quote int
		want  bool
	}{
		{`r'a'`, 1, true},
		{`R'a'`, 1, true},
		{`(r'a')`, 2, true},
		{`concat(x, r'a')`, 11, true},
		{`'a'`, 0, false},
		{`var'a'`, 3, false},
		{`x_r'a'`, 3, false},
		{`r2'a'`, 2, false},
	}
	for _, tc := range tests {
		if got := isRawStringPrefix(tc.query, tc.quote); got != tc.want {
			t.Errorf("isRawStringPrefix(%q, %d) = %v, want %v", tc.query, tc.quote, got, tc.want)
		}
	}
}
