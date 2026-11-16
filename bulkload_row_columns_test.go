package goclickzetta

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/clickzetta/goclickzetta/protos/bulkload/ingestion"
	"github.com/clickzetta/goclickzetta/protos/bulkload/util"
)

// newOrdersWriter returns a writer for orders(id BIGINT NOT NULL, name STRING,
// cost DOUBLE) that writes parquet files to a temp dir. Nothing is uploaded.
func newOrdersWriter(t *testing.T) *BulkloadWriter {
	t.Helper()
	idType := &util.DataType{Category: util.DataTypeCategory_INT64, Nullable: false}
	nameType := &util.DataType{Category: util.DataTypeCategory_STRING, Nullable: true}
	costType := &util.DataType{Category: util.DataTypeCategory_FLOAT64, Nullable: true}
	w := &BulkloadWriter{
		MetaData: &BulkloadMetadata{
			Table: CZTable{
				Schema: map[string]*util.DataType{"id": idType, "name": nameType, "cost": costType},
				TableMeta: &ingestion.StreamSchema{DataFields: []*ingestion.DataField{
					{Name: "id", Type: idType},
					{Name: "name", Type: nameType},
					{Name: "cost", Type: costType},
				}},
			},
			StreamInfo: &ingestion.BulkLoadStreamInfo{StreamId: "test-orders", Identifier: &ingestion.TableIdentifier{TableName: "orders"}},
		},
		StreamOptions: &BulkloadOptions{Table: "orders"},
		LocalBaseDir:  t.TempDir(),
		FileNameUUID:  "test",
	}
	if err := w.ConstructArrowSchema(); err != nil {
		t.Fatalf("ConstructArrowSchema: %v", err)
	}
	return w
}

// readBack flushes the writer's current batch, closes its parquet file and
// returns the file contents as one record.
func readBack(t *testing.T, w *BulkloadWriter) arrow.Table {
	t.Helper()
	if _, err := w.FlushRecordBatch(); err != nil {
		t.Fatalf("FlushRecordBatch: %v", err)
	}
	name := w.CurrentFileName()
	if err := w.Writer.Close(); err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}
	f, err := os.Open(name)
	if err != nil {
		t.Fatalf("open parquet file: %v", err)
	}
	defer f.Close()
	pf, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatalf("parquet reader: %v", err)
	}
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("arrow reader: %v", err)
	}
	table, err := reader.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("ReadTable: %v", err)
	}
	t.Cleanup(table.Release)
	return table
}

// column renders a table column as strings, with NULL for null values.
func column(t *testing.T, table arrow.Table, name string) []string {
	t.Helper()
	idx := table.Schema().FieldIndices(name)
	if len(idx) != 1 {
		t.Fatalf("column %q not found", name)
	}
	var out []string
	for _, chunk := range table.Column(idx[0]).Data().Chunks() {
		for i := 0; i < chunk.Len(); i++ {
			if chunk.IsNull(i) {
				out = append(out, "NULL")
			} else {
				out = append(out, chunk.ValueStr(i))
			}
		}
	}
	return out
}

func writeRow(t *testing.T, w *BulkloadWriter, set func(r *Row) error) error {
	t.Helper()
	r := w.CreateRow()
	if err := set(r); err != nil {
		t.Fatalf("set row: %v", err)
	}
	return w.WriteRow(r)
}

// A nullable column left unset is written as NULL. Before the fix the unset
// column had fewer values than the others and building the Arrow record
// panicked with "field N has M rows".
func TestWriteRowUnsetNullableColumnWritesNull(t *testing.T) {
	w := newOrdersWriter(t)
	rows := []func(r *Row) error{
		func(r *Row) error {
			r.SetBigint("id", int64(1))
			r.SetString("name", "a")
			return r.SetDouble("cost", 1.5)
		},
		func(r *Row) error { r.SetBigint("id", int64(2)); return r.SetDouble("cost", 2.5) },
		func(r *Row) error { r.SetBigint("id", int64(3)); return r.SetString("name", "c") },
	}
	for i, set := range rows {
		if err := writeRow(t, w, set); err != nil {
			t.Fatalf("WriteRow %d: %v", i, err)
		}
	}
	table := readBack(t, w)
	if got := strings.Join(column(t, table, "name"), ","); got != "a,NULL,c" {
		t.Errorf("name = %s, want a,NULL,c", got)
	}
	if got := strings.Join(column(t, table, "cost"), ","); got != "1.5,2.5,NULL" {
		t.Errorf("cost = %s, want 1.5,2.5,NULL", got)
	}
}

// A NOT NULL column left unset is rejected by WriteRow, naming the column,
// and the rejected row leaves no partial values behind in the batch.
func TestWriteRowUnsetNotNullColumnFails(t *testing.T) {
	w := newOrdersWriter(t)
	err := writeRow(t, w, func(r *Row) error { return r.SetString("name", "no id") })
	if err == nil || !strings.Contains(err.Error(), `"id"`) {
		t.Fatalf("WriteRow error = %v, want an error naming column \"id\"", err)
	}
	if err := writeRow(t, w, func(r *Row) error { r.SetBigint("id", int64(7)); return r.SetString("name", "ok") }); err != nil {
		t.Fatalf("WriteRow after rejected row: %v", err)
	}
	table := readBack(t, w)
	if got := strings.Join(column(t, table, "id"), ","); got != "7" {
		t.Errorf("id = %s, want 7 (only the accepted row)", got)
	}
	if got := strings.Join(column(t, table, "name"), ","); got != "ok" {
		t.Errorf("name = %s, want ok (only the accepted row)", got)
	}
}

// Passing nil to a setter writes NULL; a NOT NULL column still rejects it.
func TestRowSetNilWritesNull(t *testing.T) {
	w := newOrdersWriter(t)
	if err := writeRow(t, w, func(r *Row) error {
		r.SetBigint("id", int64(1))
		r.SetString("name", nil)
		return r.SetDouble("cost", nil)
	}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	err := writeRow(t, w, func(r *Row) error { r.SetString("name", "x"); return r.SetBigint("id", nil) })
	if err == nil || !strings.Contains(err.Error(), `"id"`) {
		t.Fatalf("WriteRow with NULL id error = %v, want an error naming column \"id\"", err)
	}
	table := readBack(t, w)
	if got := strings.Join(column(t, table, "name"), ",") + "|" + strings.Join(column(t, table, "cost"), ","); got != "NULL|NULL" {
		t.Errorf("name|cost = %s, want NULL|NULL", got)
	}
}

// A setter for a column the table does not have fails instead of silently
// dropping the value.
func TestRowSetUnknownColumnFails(t *testing.T) {
	w := newOrdersWriter(t)
	r := w.CreateRow()
	if err := r.SetString("nmae", "typo"); err == nil || !strings.Contains(err.Error(), `"nmae"`) {
		t.Fatalf("SetString(nmae) error = %v, want an error naming the column", err)
	}
}

// A value put straight into ColumnNameValues for an unknown column is
// rejected by WriteRow.
func TestWriteRowUnknownColumnFails(t *testing.T) {
	w := newOrdersWriter(t)
	r := w.CreateRow()
	r.SetBigint("id", int64(1))
	r.ColumnNameValues["nmae"] = "typo"
	if err := w.WriteRow(r); err == nil || !strings.Contains(err.Error(), `"nmae"`) {
		t.Fatalf("WriteRow error = %v, want an error naming column \"nmae\"", err)
	}
}

// An empty string is written as an empty string, not NULL. This is what
// earlier versions stored: their "" -> NULL mapping never took effect because
// the parquet columns were REQUIRED. Kept on purpose for compatibility.
func TestWriteRowEmptyStringStaysEmpty(t *testing.T) {
	w := newOrdersWriter(t)
	if err := writeRow(t, w, func(r *Row) error { r.SetBigint("id", int64(1)); return r.SetString("name", "") }); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	table := readBack(t, w)
	if got := column(t, table, "name"); len(got) != 1 || got[0] != "" {
		t.Errorf("name = %q, want [\"\"]", got)
	}
}
