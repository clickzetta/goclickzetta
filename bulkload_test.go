package goclickzetta

import (
	"database/sql"
	"fmt"
	"github.com/shopspring/decimal"
	"testing"
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
	db, err := sql.Open("clickzetta", "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta")
	if err != nil {
		t.Error(err)
	}
	if db == nil {
		t.Error("db is nil")
	}
	defer db.Close()
	res, err := db.Query("select count(1) from upsert_cluster_pt_python;")
	if err != nil {
		t.Error(err)
	}
	defer res.Close()
	for res.Next() {
		var result CountResult
		err := res.Scan(&result.Count)
		if err != nil {
			t.Error(err)
		}
		fmt.Printf("result is: %v", result)
	}

}

func CheckBulkLoadShow(t *testing.T) {
	db, err := sql.Open("clickzetta", "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta")
	if err != nil {
		t.Error(err)
	}
	if db == nil {
		t.Error("db is nil")
	}
	defer db.Close()
	res, err := db.Query("show create table upsert_cluster_pt_python;")
	if err != nil {
		t.Error(err)
	}
	defer res.Close()
	for res.Next() {
		var result SchemaResult
		err := res.Scan(&result.Schema)
		if err != nil {
			t.Error(err)
		}
		fmt.Printf("result is: %v", result)
	}

}

func TestBulkLoadMinorData(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta"
	conn, err := connect(dsn)
	if err != nil {
		t.Error(err)
	}
	options := BulkloadOptions{
		Table:     "append_cluster_python",
		Operation: APPEND,
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Error(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Error(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Error(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Error(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Error(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Error(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Error(err)
	}
	err = writer.Close()
	if err != nil {
		t.Error(err)
	}
	err = stream.Close()
	if err != nil {
		t.Error(err)
	}

}

func TestBulkLoadMajorData(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta"
	conn, err := connect(dsn)
	if err != nil {
		t.Error(err)
	}
	options := BulkloadOptions{
		Table:     "append_cluster_python",
		Operation: APPEND,
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Error(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Error(err)
	}
	count := 0
	for {
		row := writer.CreateRow()
		err = row.SetBigint("id", int64(1))
		if err != nil {
			t.Error(err)
		}
		err = row.SetString("month", "January")
		if err != nil {
			t.Error(err)
		}
		err = row.SetBigint("amount", int64(2))
		if err != nil {
			t.Error(err)
		}
		err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
		if err != nil {
			t.Error(err)
		}
		err = writer.WriteRow(row)
		if err != nil {
			t.Error(err)
		}
		count++
		if count == 100000000 {
			break
		}
	}
	err = writer.Close()
	if err != nil {
		t.Error(err)
	}
	err = stream.Close()
	if err != nil {
		t.Error(err)
	}

}

func TestBulkLoadDistributedWriter(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta"
	conn, err := connect(dsn)
	if err != nil {
		t.Error(err)
	}
	options := BulkloadOptions{
		Table:     "append_cluster_python",
		Operation: APPEND,
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Error(err)
	}
	streamId := stream.GetStreamId()
	executorStream, err := conn.GetDistributeBulkloadStream(streamId, options)
	if err != nil {
		t.Error(err)
	}
	var writerList []*BulkloadWriter
	writerIndex := 0
	for writerIndex < 5 {
		writer, err := executorStream.OpenWriter(int64(writerIndex))
		if err != nil {
			t.Error(err)
		}
		writerList = append(writerList, writer)
		writerIndex++
	}
	for _, writer := range writerList {
		row := writer.CreateRow()
		err = row.SetBigint("id", int64(1))
		if err != nil {
			t.Error(err)
		}
		err = row.SetString("month", "January")
		if err != nil {
			t.Error(err)
		}
		err = row.SetBigint("amount", int64(2))
		if err != nil {
			t.Error(err)
		}
		err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
		if err != nil {
			t.Error(err)
		}
		err = writer.WriteRow(row)
		if err != nil {
			t.Error(err)
		}
		err = writer.Close()
		if err != nil {
			t.Error(err)
		}
	}

	err = executorStream.Close()
	if err != nil {
		t.Error(err)
	}

}

func TestBulkLoadOverwrite(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta"
	conn, err := connect(dsn)
	if err != nil {
		t.Error(err)
	}
	options := BulkloadOptions{
		Table:     "append_cluster_python",
		Operation: OVERWRITE,
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Error(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Error(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Error(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Error(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Error(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Error(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Error(err)
	}
	err = writer.Close()
	if err != nil {
		t.Error(err)
	}
	err = stream.Close()
	if err != nil {
		t.Error(err)
	}

}

func TestBulkLoadUpsert(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta"
	conn, err := connect(dsn)
	if err != nil {
		t.Error(err)
	}
	options := BulkloadOptions{
		Table:      "append_cluster_python",
		Operation:  UPSERT,
		RecordKeys: []string{"id"},
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Error(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Error(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Error(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Error(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Error(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Error(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Error(err)
	}
	err = writer.Close()
	if err != nil {
		t.Error(err)
	}
	err = stream.Close()
	if err != nil {
		t.Error(err)
	}

}
func TestBulkLoadAppendPt(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta"
	conn, err := connect(dsn)
	if err != nil {
		t.Error(err)
	}
	options := BulkloadOptions{
		Table:         "upsert_cluster_pt_python",
		Operation:     APPEND,
		PartitionSpec: "pt=python_bulkload",
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Error(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Error(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Error(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Error(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Error(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Error(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Error(err)
	}
	err = writer.Close()
	if err != nil {
		t.Error(err)
	}
	err = stream.Close()
	if err != nil {
		t.Error(err)
	}

}

func TestBulkLoadOverwritePt(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta"
	conn, err := connect(dsn)
	if err != nil {
		t.Error(err)
	}
	options := BulkloadOptions{
		Table:         "upsert_cluster_pt_python",
		Operation:     OVERWRITE,
		PartitionSpec: "pt=python_bulkload",
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Error(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Error(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Error(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Error(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Error(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Error(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Error(err)
	}
	err = writer.Close()
	if err != nil {
		t.Error(err)
	}
	err = stream.Close()
	if err != nil {
		t.Error(err)
	}

}

func TestBulkLoadUpsertPt(t *testing.T) {
	t.Log("TestBulkloadMinorData")
	dsn := "cz_lh_smoke_test:Abc123456@https(dev-api.zettadecision.com)/lakehouse_daily?virtualCluster=cz_gp_daily&workspace=system_smoke&instance=clickzetta"
	conn, err := connect(dsn)
	if err != nil {
		t.Error(err)
	}
	options := BulkloadOptions{
		Table:         "upsert_cluster_pt_python",
		Operation:     UPSERT,
		PartitionSpec: "pt=python_bulkload",
		RecordKeys:    []string{"id"},
	}
	stream, err := conn.CreateBulkloadStream(options)
	if err != nil {
		t.Error(err)
	}
	writer, err := stream.OpenWriter(0)
	if err != nil {
		t.Error(err)
	}
	row := writer.CreateRow()
	err = row.SetBigint("id", int64(1))
	if err != nil {
		t.Error(err)
	}
	err = row.SetString("month", "January")
	if err != nil {
		t.Error(err)
	}
	err = row.SetBigint("amount", int64(2))
	if err != nil {
		t.Error(err)
	}
	err = row.SetDecimal("cost", decimal.NewFromFloat(1.1))
	if err != nil {
		t.Error(err)
	}
	err = writer.WriteRow(row)
	if err != nil {
		t.Error(err)
	}
	err = writer.Close()
	if err != nil {
		t.Error(err)
	}
	err = stream.Close()
	if err != nil {
		t.Error(err)
	}

}
