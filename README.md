# Go Clickzetta Driver

This topic provides instructions for installing, running, and modifying the Go Clickzetta Driver. The driver supports Go's [database/sql](https://golang.org/pkg/database/sql/) package.

# Prerequisites

The following software packages are required to use the Go Clickzetta Driver.

## Go

The latest driver requires the [Go language](https://golang.org/) 1.19 or higher. The supported operating systems are Linux, Mac OS, and Windows, but you may run the driver on other platforms if the Go language works correctly on those platforms.



# Installation

Get goclickzetta source code, if not installed.

```sh
go get -u github.com/clickzetta/goclickzetta
```
<!-- 
# Docs

For detailed documentation and basic usage examples, please see the documentation at
[goclickzetta-doc](xxxx).
-->

# Development

The developer notes are hosted with the source code on [GitHub](https://github.com/clickzetta/goclickzetta/tree/v0.0.5).

## Example code

* The following example code demonstrates how to use the Go Clickzetta Driver to connect to a Clickzetta account and run a simple query.

```go

import (
"database/sql"
"fmt"
_ "github.com/clickzetta/goclickzetta"
)

type CountResult struct {
    Count int64
}

db, err := sql.Open("clickzetta", "${username}:${pwd}@${protocol}(${service}/${schema}?virtualCluster=${vc}&workspace=${workspace}&instance=${instanceName}")
if err != nil {
    t.Error(err)
}

res, err := db.Query("select count(1) from table;")
if err != nil {
    t.Error(err)
}

for res.Next() {
    var result CountResult
    err := res.Scan(&result.Count)
    if err != nil {
        t.Error(err)
    }
    fmt.Printf("result is: %v", result)
}



```
* The following example code demonstrates how to use the Go Clickzetta connection to write batch data to a Clickzetta table.

```go
dsn := "${username}:${pwd}@${protocol}(${service}/${schema}?virtualCluster=${vc}&workspace=${workspace}&instance=${instanceName}"
conn, err := connect(dsn)
if err != nil {
t.Error(err)
}
options := BulkloadOptions{
Table:     "table",
Operation: APPEND,
}
stream, err := conn.CreateBulkloadStream(options)
writer, err := stream.OpenWriter(0)
row := writer.CreateRow()
row.SetBigint("id", int64(1))

row.SetString("month", "January")

row.SetBigint("amount", int64(2))

row.SetDecimal("cost", decimal.NewFromFloat(1.1))

writer.WriteRow(row)

writer.Close()
stream.Close()
```

More examples can be found in the [examples](https://github.com/clickzetta/goclickzetta/blob/main/statement_test.go).

## DSN (Data Source Name)
The Data Source Name has a common format, like the following:

```
${username}:${pwd}@${protocol}(${service}/${schema}?virtualCluster=${vc}&workspace=${workspace}&instance=${instanceName}
```
* **username**: The username of the Clickzetta account.
* **pwd**: The password of the Clickzetta account.
* **protocol**: The protocol of the Clickzetta service. The default value is https.(http,tcp ...)
* **service**: The Clickzetta service name.
* **schema**: The Clickzetta schema name.
* **vc**: The Clickzetta virtual cluster name.
* **workspace**: The Clickzetta workspace name.
* **instanceName**: The Clickzetta instance name.

Optional query parameters:

* **magic_token**: Uses an existing JWT token and skips username/password login.
* **sdk.query.max.retries**: Sets the maximum number of submit and polling retries. The default is `10`.
* **trace_timing**: Enables phase-level query timing logs. Timing records are emitted at debug level and do not include SQL text or response payloads.

Timing diagnostics can also be enabled for the whole process before startup:

```sh
CLICKZETTA_TRACE_TIMING=1 ./your-application
```

The SDK logger must be configured at debug level for timing records to be emitted.

* **cz.sql.string.literal.escape.mode**: How the driver escapes string literals it interpolates into a statement. The resolved mode is also sent to the server as a query hint of the same name, so both sides agree on how to read the literal. Accepted values:
  * `backslash` (default), or the aliases `0` and `1`: escape `'` as `\'` and `\` as `\\`.
  * `quote_backslash`, or the alias `3`: escape `'` as `\'` and `\` as `\\`. This mode accepts `''` as well, but the backslash form is used because a doubled quote is ambiguous in it, so the encoding is identical to `backslash`.
  * `quote`, or the alias `2`: double `'` as `''` and treat `\` as an ordinary character.

  Values are parsed the same way as in the Java driver: surrounding whitespace is trimmed, case is ignored, and an empty value means the option is not configured and selects the default. An unsupported value is rejected before submission rather than replaced with the default. The hint sent to the server always carries the canonical name, not the alias or the original spelling.

  **Only select `quote` against a server known to honour this hint.** It is the one mode whose safety depends on the server: a server that ignores the hint drops a doubled single quote and still treats `\` as an escape, so `O'Brien` silently arrives as `OBrien`, and a value ending in a backslash swallows the closing quote and lets the remainder of the value be parsed as SQL. The `backslash` default does not depend on the hint taking effect. If you are unsure whether your server honours it, check with `select 'O''Brien'` — a server that honours `quote` returns `O'Brien`, one that ignores it returns `OBrien`.

  If this option is supplied through `WithDriverFlags`, it takes precedence over the DSN value. The mode can be overridden per statement that way. The driver and the server **must** agree on this mode: if the server ignores the hint and parses literals under a different convention, an escaped quote can terminate the literal early.

The mode also decides where a string literal ends while the driver scans the statement, which is why it is resolved before the statement is split.

The `r`/`R` prefix is recognised while scanning. A raw string processes no escapes, so `r'C:\'` is a complete literal holding one backslash and `length(r'a\\b')` is 4 — the mode does not apply inside one. This matters for statements such as `COPY INTO ... ('escape'=r'\')`, whose literal ends with a backslash. A quote directly behind a name that happens to end in `r` is not treated as a prefix.

### String Bindings

A `string` binding must be valid UTF-8. Invalid bytes are rejected with an error rather than being sent, because the request is JSON-encoded on the way out and `encoding/json` would silently replace them with U+FFFD. Pass such data as `[]byte` instead.

The statement itself is checked the same way, so a query carrying invalid UTF-8 is rejected instead of reaching the server altered.

### Numeric Bindings

Integers and finite floats are interpolated as bare literals. A placeholder that sits directly behind a `-` gets a space in front of it, because otherwise a negative value's sign would merge with that operator: `SELECT 1-?` bound to `-5` would become `SELECT 1--5`, where `--` opens a line comment and truncates the statement. It becomes `SELECT 1- -5` instead. A space is used rather than parentheses because whitespace cannot change what a statement means, while grouping can: `INTERVAL (-5) DAY` yields a `timestamp_ltz` where `INTERVAL -5 DAY` yields a `date`. `NaN` and the infinities are rejected, since neither has a literal form.

### Binary Bindings

A `[]byte` binding is rendered as the hexadecimal `BINARY` literal `X'...'`, so an empty slice becomes `X''`.

### Nullable and Pointer Bindings

Types that only implement `driver.Valuer` — `sql.NullString`, `sql.NullInt64` and friends — plus pointers to supported types are unwrapped before interpolation, so a null value becomes `NULL`.

### Time Bindings

A `time.Time` binding is rendered as `timestamp 'YYYY-MM-DD HH:MM:SS[.ffffff]±HH:MM'`. The fraction is capped at six digits because microseconds are the maximum `TIMESTAMP` precision the server supports, and trailing zeros are dropped, so a whole-second value renders without a decimal point.

The zone offset is part of the literal. `TIMESTAMP` is `TIMESTAMP_LTZ` by default, so the server resolves the instant from that offset; a `TIMESTAMP_NTZ` column ignores it and keeps the wall-clock time as written. Convert with `In()` first if the wall-clock rendering matters:

```go
db.Exec("INSERT INTO t VALUES (?)", ts.In(time.UTC))
```

### Complex-Type Bindings

Values for `ARRAY`, `MAP` and `STRUCT` columns are passed with `RawSQLValue`, which is interpolated verbatim:

```go
db.Exec("INSERT INTO t VALUES (?)", goclickzetta.RawSQLValue("array(1,2,3)"))
```

The caller owns the safety of that fragment; never build a `RawSQLValue` from untrusted input, because its contents reach the server as executable SQL.

> **Breaking change**: earlier versions detected complex types by sniffing the binding string for an `array(`/`map(`/`struct(` prefix and passing it through unquoted, which let any user-supplied string reach the server as SQL. Plain strings are now always emitted as quoted literals. Bindings that relied on the old prefix behaviour must be wrapped in `RawSQLValue`.
>
> Two related behaviour changes come with the safe interpolation: a `?` inside a string literal, quoted identifier or comment is no longer treated as a placeholder (`... LIKE '%?%'` now reports `too many bindings for placeholders` instead of silently substituting), and interpolation failures are returned as errors instead of being ignored.
>
> A `time.Time` binding now carries its zone offset into the literal, where earlier versions emitted a bare wall clock. This one changes data rather than raising an error: writing a value whose zone differs from the server session zone into a `TIMESTAMP` (`TIMESTAMP_LTZ`) column now stores the instant the value denotes, not the same wall clock reinterpreted in the server's zone. Call `In()` if the previous rendering is what you want — see [Time Bindings](#time-bindings).

When User use the Clickzetta driver to execute SQL and write batch data , must construct the DSN.

## BulkLoad
Users can use BulkLoad to write data to Clickzetta.BulkLoad has three modes: APPEND, OVERWRITE, and UPSERT. The default mode is APPEND.
* **APPEND**: The APPEND mode appends data to the table. 
* **OVERWRITE**: The OVERWRITE mode overwrites the table. If the table has data, the data is deleted. 
* **UPSERT**: The UPSERT mode updates the table. Users must specify the primary key when using this mode. If the primary key exists, the data is updated. If the primary key does not exist, the data is inserted.



## Row
Users can use Row to write data to Clickzetta. Row has the following methods:
* **SetBigint**: Sets the value of a int64 column.
* **SetBoolean**: Sets the value of a boolean column.
* **SetDate**: Sets the value of a date column. (value should be string. eg: "2023-01-01")
* **SetDecimal**: Sets the value of a decimal.Decimal column.
* **SetDouble**: Sets the value of a float64 column.
* **SetFloat**: Sets the value of a float32 column.
* **SetInt**: Sets the value of an int32 column.
* **SetSmallint**: Sets the value of a int16 column.
* **SetString**: Sets the value of a string column.
* **SetTimestamp**: Sets the value of a timestamp column. (value should be string. eg: "2023-01-01 00:00:00")
* **SetTinyInt**: Sets the value of an int8 column.

## Support

For official support, contact Clickzetta support at:
[https://www.yunqi.tech](https://www.yunqi.tech).
