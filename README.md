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

