package goclickzetta

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/shopspring/decimal"
)

type CountResult struct {
	Count int64
}

type SchemaResult struct {
	Schema string
}

func TestBulkLoad(t *testing.T) {
	t.Run("TestBulkLoadMinorData", TestBulkLoadMinorData)
	t.Run("CheckBulkLoadResult", CheckBulkLoadResult)
	t.Run("CheckBulkLoadShow", CheckBulkLoadShow)
}

func CheckBulkLoadResult(t *testing.T) {
	db, err := sql.Open("clickzetta", integrationDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Error("db is nil")
	}
	defer db.Close()
	res, err := db.Query("select count(1) from upsert_cluster_pt_python;")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	for res.Next() {
		var result CountResult
		err := res.Scan(&result.Count)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("result is: %v", result)
	}

}

func CheckBulkLoadShow(t *testing.T) {
	db, err := sql.Open("clickzetta", integrationDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Error("db is nil")
	}
	defer db.Close()
	res, err := db.Query("show create table upsert_cluster_pt_python;")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	for res.Next() {
		var result SchemaResult
		err := res.Scan(&result.Schema)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("result is: %v", result)
	}

}

func TestBulkLoadMinorData(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := integrationDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := BulkloadOptions{
		Table:     "append_cluster_python",
		Operation: APPEND,
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestBulkLoadMajorData(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := integrationDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := BulkloadOptions{
		Table:     "append_cluster_python",
		Operation: APPEND,
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		row := writer.CreateRow()
		err = row.SetBigint("id", int64(1))
		if err != nil {
			t.Fatal(err)
		}
		err = row.SetString("month", "January")
		if err != nil {
			t.Fatal(err)
		}
		err = row.SetBigint("amount", int64(2))
		if err != nil {
			t.Fatal(err)
		}
		err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
		if err != nil {
			t.Fatal(err)
		}
		err = writer.WriteRow(row)
		if err != nil {
			t.Fatal(err)
		}
		count++
		if count == 100000000 {
			break
		}
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestBulkLoadDistributedWriter(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := integrationDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := BulkloadOptions{
		Table:     "append_cluster_python",
		Operation: APPEND,
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Fatal(err)
	}
	streamId := stream.GetStreamId()
	executorStream, err := conn.GetDistributeBulkloadStream(streamId, options)
	if err != nil {
		t.Fatal(err)
	}
	var writerList []*BulkloadWriter
	writerIndex := 0
	for writerIndex < 5 {
		writer, err := executorStream.OpenWriter(int64(writerIndex))
		if err != nil {
			t.Fatal(err)
		}
		writerList = append(writerList, writer)
		writerIndex++
	}
	for _, writer := range writerList {
		row := writer.CreateRow()
		err = row.SetBigint("id", int64(1))
		if err != nil {
			t.Fatal(err)
		}
		err = row.SetString("month", "January")
		if err != nil {
			t.Fatal(err)
		}
		err = row.SetBigint("amount", int64(2))
		if err != nil {
			t.Fatal(err)
		}
		err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
		if err != nil {
			t.Fatal(err)
		}
		err = writer.WriteRow(row)
		if err != nil {
			t.Fatal(err)
		}
		err = writer.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	err = executorStream.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestBulkLoadOverwrite(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := integrationDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := BulkloadOptions{
		Table:     "append_cluster_python",
		Operation: OVERWRITE,
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestBulkLoadUpsert(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := integrationDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := BulkloadOptions{
		Table:      "append_cluster_python",
		Operation:  UPSERT,
		RecordKeys: []string{"id"},
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Close()
	if err != nil {
		t.Fatal(err)
	}

}
func TestBulkLoadAppendPt(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := integrationDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := BulkloadOptions{
		Table:         "upsert_cluster_pt_python",
		Operation:     APPEND,
		PartitionSpec: "pt=python_bulkload",
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestBulkLoadOverwritePt(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := integrationDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := BulkloadOptions{
		Table:         "upsert_cluster_pt_python",
		Operation:     OVERWRITE,
		PartitionSpec: "pt=python_bulkload",
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestBulkLoadUpsertPt(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := integrationDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := BulkloadOptions{
		Table:         "upsert_cluster_pt_python",
		Operation:     UPSERT,
		PartitionSpec: "pt=python_bulkload",
		RecordKeys:    []string{"id"},
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Close()
	if err != nil {
		t.Fatal(err)
	}

}
