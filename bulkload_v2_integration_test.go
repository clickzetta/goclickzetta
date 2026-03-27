package goclickzetta

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

// Integration tests — require CZ_TEST_DSN env var.
// Run: CZ_TEST_DSN='...' go test -v -run TestBulkloadV2 -timeout 300s

func skipIfNoDSN(t *testing.T) string {
	dsn := os.Getenv("CZ_TEST_DSN")
	if dsn == "" {
		t.Skip("CZ_TEST_DSN not set, skipping integration test")
	}
	return dsn
}

func TestBulkloadV2_Append(t *testing.T) {
	dsn := skipIfNoDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := conn.CreateBulkloadStream(BulkloadOptions{
		Table:     "go_sdk_bulkload_test",
		Operation: APPEND,
	})
	if err != nil {
		t.Fatal(err)
	}

	writer, err := stream.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		row := writer.CreateRow()
		row.SetBigint("id", int64(i+100))
		row.SetString("name", fmt.Sprintf("go_v2_test_%d", i))
		row.SetBigint("amount", int64(i*10))
		row.SetDecimal("cost", decimal.NewFromFloat(float64(i)*1.5))
		if err := writer.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify committables are produced
	comms := writer.GetCommittables()
	if len(comms) == 0 {
		t.Fatal("expected committables after writer close")
	}
	t.Logf("Writer produced %d committable(s) with %d file(s)", len(comms), len(comms[0].Files))

	// Commit via stream.Close()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("Stream committed successfully")
}

func TestBulkloadV2_CommitterSeparate(t *testing.T) {
	dsn := skipIfNoDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := conn.CreateBulkloadStream(BulkloadOptions{
		Table:     "go_sdk_bulkload_test",
		Operation: APPEND,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write with explicit writer/committer separation
	writer, err := stream.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}

	row := writer.CreateRow()
	row.SetBigint("id", int64(999))
	row.SetString("name", "go_v2_committer_test")
	row.SetBigint("amount", int64(99))
	row.SetDecimal("cost", decimal.NewFromFloat(9.99))
	if err := writer.WriteRow(row); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	// Use committer explicitly
	committer := stream.CreateCommitter()
	committables := writer.GetCommittables()
	if err := committer.Commit(committables); err != nil {
		t.Fatal(err)
	}
	stream.Closed = true
	t.Log("Explicit committer committed successfully")
}

func TestBulkloadV2_VerifyData(t *testing.T) {
	dsn := skipIfNoDSN(t)
	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("select count(1) from go_sdk_bulkload_test where name like 'go_v2_%'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		var count int64
		rows.Scan(&count)
		t.Logf("Rows with go_v2_ prefix: %d", count)
		if count == 0 {
			t.Error("expected some rows written by V2 tests")
		}
	}
}

func TestBulkloadV2_WriterReuse(t *testing.T) {
	dsn := skipIfNoDSN(t)
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := conn.CreateBulkloadStream(BulkloadOptions{
		Table:     "go_sdk_bulkload_test",
		Operation: APPEND,
	})
	if err != nil {
		t.Fatal(err)
	}

	w1, err := stream.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := stream.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	if w1 != w2 {
		t.Fatal("CreateWriter(0) should return same writer instance")
	}

	row := w1.CreateRow()
	row.SetBigint("id", int64(888))
	row.SetString("name", "go_v2_reuse_test")
	row.SetBigint("amount", int64(88))
	row.SetDecimal("cost", decimal.NewFromFloat(8.88))
	if err := w1.WriteRow(row); err != nil {
		t.Fatal(err)
	}

	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("Writer reuse test passed")
}

// TestBulkloadV2_ComplexTypes is an e2e test for ARRAY, MAP, STRUCT column writes.
// Run: CZ_TEST_DSN='...' go test -v -run TestBulkloadV2_ComplexTypes -timeout 300s
func TestBulkloadV2_ComplexTypes(t *testing.T) {
	dsn := skipIfNoDSN(t)

	tableName := "go_sdk_bulkload_complex_test"

	// --- Step 1: create table with complex type columns via SQL ---
	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Fatal("drop table:", err)
	}

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		id        INT,
		tags      ARRAY<INT>,
		attrs     MAP<STRING, STRING>,
		info      STRUCT<name: STRING, score: INT>,
		PRIMARY KEY (id)
	)`, tableName)
	_, err = db.Exec(createSQL)
	if err != nil {
		t.Fatal("create table:", err)
	}
	t.Log("Table created")

	// --- Step 2: write data via bulkload v2 ---
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := conn.CreateBulkloadStream(BulkloadOptions{
		Table:     tableName,
		Operation: APPEND,
	})
	if err != nil {
		t.Fatal("create stream:", err)
	}

	writer, err := stream.CreateWriter(0)
	if err != nil {
		t.Fatal("create writer:", err)
	}

	testData := []struct {
		id    int32
		tags  []interface{}
		attrs map[string]interface{}
		info  map[string]interface{}
	}{
		{1, []interface{}{int32(10), int32(20), int32(30)}, map[string]interface{}{"color": "red", "size": "L"}, map[string]interface{}{"name": "alice", "score": int32(95)}},
		{2, []interface{}{int32(40)}, map[string]interface{}{"color": "blue"}, map[string]interface{}{"name": "bob", "score": int32(88)}},
		{3, []interface{}{int32(50), int32(60)}, map[string]interface{}{"env": "prod", "region": "us"}, map[string]interface{}{"name": "charlie", "score": int32(72)}},
	}

	for _, d := range testData {
		row := writer.CreateRow()
		row.SetInt("id", d.id)
		row.ColumnNameValues["tags"] = d.tags
		row.ColumnNameValues["attrs"] = d.attrs
		row.ColumnNameValues["info"] = d.info
		if err := writer.WriteRow(row); err != nil {
			t.Fatal("write row:", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal("close writer:", err)
	}

	comms := writer.GetCommittables()
	if len(comms) == 0 {
		t.Fatal("expected committables")
	}
	t.Logf("Writer produced %d committable(s)", len(comms))

	if err := stream.Close(); err != nil {
		t.Fatal("close stream:", err)
	}
	t.Log("Stream committed")

	// --- Step 3: verify data via SQL ---
	var count int64
	err = db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatal("count query:", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
	t.Logf("Verified %d rows in table", count)

	// Verify a specific row
	rows, err := db.Query(fmt.Sprintf("SELECT id FROM %s WHERE id = 1", tableName))
	if err != nil {
		t.Fatal("select query:", err)
	}
	defer rows.Close()
	if rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal("scan:", err)
		}
		t.Logf("Row id=1 verified successfully")
	} else {
		t.Fatal("expected row with id=1")
	}

	// --- Step 4: cleanup ---
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Log("cleanup drop table:", err)
	}
	t.Log("Cleanup done")
}

// TestBulkloadV2_AsyncUploadPipeline writes enough data to trigger multiple file rolls,
// verifying the async upload pipeline works correctly end-to-end.
// Uses small thresholds to trigger file rolls with minimal data.
func TestBulkloadV2_AsyncUploadPipeline(t *testing.T) {
	dsn := skipIfNoDSN(t)

	tableName := "go_sdk_bulkload_async_test"

	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Fatal("drop table:", err)
	}

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		id     INT,
		name   STRING,
		value  BIGINT,
		PRIMARY KEY (id)
	)`, tableName)
	_, err = db.Exec(createSQL)
	if err != nil {
		t.Fatal("create table:", err)
	}
	t.Log("Table created")

	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := conn.CreateBulkloadStream(BulkloadOptions{
		Table:     tableName,
		Operation: APPEND,
	})
	if err != nil {
		t.Fatal("create stream:", err)
	}

	writer, err := stream.CreateWriter(0)
	if err != nil {
		t.Fatal("create writer:", err)
	}

	// Set small thresholds to trigger multiple file rolls with minimal data
	writer.MaxRowsPerFile = 500
	writer.BatchFlushRows = 200

	totalRows := 2000
	for i := 0; i < totalRows; i++ {
		row := writer.CreateRow()
		row.SetInt("id", int32(i))
		row.SetString("name", fmt.Sprintf("async_test_%d", i))
		row.SetBigint("value", int64(i*100))
		if err := writer.WriteRow(row); err != nil {
			t.Fatal("write row:", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal("close writer:", err)
	}

	comms := writer.GetCommittables()
	if len(comms) == 0 {
		t.Fatal("expected committables")
	}
	numVolFiles := len(comms[0].DstFiles)
	t.Logf("Writer produced %d volume file(s) (expected >= 4 from %d rows with MaxRowsPerFile=500)", numVolFiles, totalRows)
	if numVolFiles < 4 {
		t.Fatalf("expected at least 4 files from %d rows with MaxRowsPerFile=500, got %d", totalRows, numVolFiles)
	}

	if err := stream.Close(); err != nil {
		t.Fatal("close stream:", err)
	}
	t.Log("Stream committed")

	// Verify row count
	var count int64
	err = db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatal("count query:", err)
	}
	if count != int64(totalRows) {
		t.Fatalf("expected %d rows, got %d", totalRows, count)
	}
	t.Logf("Verified %d rows in table", count)

	// Verify data integrity: check first, last, and a middle row
	for _, checkId := range []int{0, totalRows / 2, totalRows - 1} {
		var id int
		var name string
		var value int64
		err = db.QueryRow(fmt.Sprintf("SELECT id, name, value FROM %s WHERE id = %d", tableName, checkId)).Scan(&id, &name, &value)
		if err != nil {
			t.Fatalf("query id=%d: %v", checkId, err)
		}
		expectedName := fmt.Sprintf("async_test_%d", checkId)
		expectedValue := int64(checkId * 100)
		if name != expectedName || value != expectedValue {
			t.Fatalf("id=%d: expected name=%s value=%d, got name=%s value=%d", checkId, expectedName, expectedValue, name, value)
		}
	}
	t.Log("Data integrity verified")

	// Verify local files are cleaned up
	localDir := writer.LocalBaseDir
	entries, err := os.ReadDir(localDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read local dir: %v", err)
	}
	if len(entries) > 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected local dir to be empty after upload, found %d files: %v", len(entries), names)
	}
	t.Log("Local files cleaned up verified")

	// Cleanup
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Log("cleanup drop table:", err)
	}
	t.Log("Cleanup done")
}

// TestBulkloadV2_MergeInto tests the UPSERT (MERGE INTO) path end-to-end:
// 1. Create table with PK
// 2. APPEND initial data
// 3. UPSERT with overlapping keys (update existing + insert new)
// 4. Verify final row count and data correctness
func TestBulkloadV2_MergeInto(t *testing.T) {
	dsn := skipIfNoDSN(t)

	tableName := "go_sdk_bulkload_upsert_test"

	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Fatal("drop table:", err)
	}

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		id    INT,
		name  STRING,
		value BIGINT,
		PRIMARY KEY (id)
	)`, tableName)
	_, err = db.Exec(createSQL)
	if err != nil {
		t.Fatal("create table:", err)
	}
	t.Log("Table created")

	// --- Step 1: APPEND initial data (id 1~5) ---
	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}

	appendStream, err := conn.CreateBulkloadStream(BulkloadOptions{
		Table:     tableName,
		Operation: APPEND,
	})
	if err != nil {
		t.Fatal("create append stream:", err)
	}

	writer, err := appendStream.CreateWriter(0)
	if err != nil {
		t.Fatal("create writer:", err)
	}

	for i := 1; i <= 5; i++ {
		row := writer.CreateRow()
		row.SetInt("id", int32(i))
		row.SetString("name", fmt.Sprintf("original_%d", i))
		row.SetBigint("value", int64(i*10))
		if err := writer.WriteRow(row); err != nil {
			t.Fatal("write row:", err)
		}
	}

	if err := appendStream.Close(); err != nil {
		t.Fatal("close append stream:", err)
	}
	t.Log("APPEND committed: 5 rows (id 1~5)")

	// Verify initial data
	var count int64
	err = db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatal("count query:", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 rows after APPEND, got %d", count)
	}

	// --- Step 2: UPSERT with overlapping keys ---
	// id 3~5: update existing rows
	// id 6~8: insert new rows
	conn2, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}

	upsertStream, err := conn2.CreateBulkloadStream(BulkloadOptions{
		Table:      tableName,
		Operation:  UPSERT,
		RecordKeys: []string{"id"},
	})
	if err != nil {
		t.Fatal("create upsert stream:", err)
	}

	upsertWriter, err := upsertStream.CreateWriter(0)
	if err != nil {
		t.Fatal("create upsert writer:", err)
	}

	for i := 3; i <= 8; i++ {
		row := upsertWriter.CreateRow()
		row.SetInt("id", int32(i))
		row.SetString("name", fmt.Sprintf("upserted_%d", i))
		row.SetBigint("value", int64(i*100))
		if err := upsertWriter.WriteRow(row); err != nil {
			t.Fatal("write upsert row:", err)
		}
	}

	if err := upsertStream.Close(); err != nil {
		t.Fatal("close upsert stream:", err)
	}
	t.Log("UPSERT committed: 6 rows (id 3~8)")

	// --- Step 3: Verify final state ---
	// Should have 8 rows total (id 1~8)
	err = db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatal("count query:", err)
	}
	if count != 8 {
		t.Fatalf("expected 8 rows after UPSERT, got %d", count)
	}
	t.Logf("Verified %d total rows", count)

	// Verify untouched rows (id 1,2) still have original values
	for _, checkId := range []int{1, 2} {
		var name string
		var value int64
		err = db.QueryRow(fmt.Sprintf("SELECT name, value FROM %s WHERE id = %d", tableName, checkId)).Scan(&name, &value)
		if err != nil {
			t.Fatalf("query id=%d: %v", checkId, err)
		}
		expectedName := fmt.Sprintf("original_%d", checkId)
		expectedValue := int64(checkId * 10)
		if name != expectedName || value != expectedValue {
			t.Fatalf("id=%d: expected name=%s value=%d, got name=%s value=%d", checkId, expectedName, expectedValue, name, value)
		}
	}
	t.Log("Untouched rows (id 1,2) verified")

	// Verify upserted rows (id 3~8) have new values
	for checkId := 3; checkId <= 8; checkId++ {
		var name string
		var value int64
		err = db.QueryRow(fmt.Sprintf("SELECT name, value FROM %s WHERE id = %d", tableName, checkId)).Scan(&name, &value)
		if err != nil {
			t.Fatalf("query id=%d: %v", checkId, err)
		}
		expectedName := fmt.Sprintf("upserted_%d", checkId)
		expectedValue := int64(checkId * 100)
		if name != expectedName || value != expectedValue {
			t.Fatalf("id=%d: expected name=%s value=%d, got name=%s value=%d", checkId, expectedName, expectedValue, name, value)
		}
	}
	t.Log("Upserted rows (id 3~8) verified")

	// Verify local files cleaned up
	localDir := upsertWriter.LocalBaseDir
	entries, _ := os.ReadDir(localDir)
	if len(entries) > 0 {
		t.Fatalf("expected local dir empty, found %d files", len(entries))
	}
	t.Log("Local files cleaned up verified")

	// Cleanup
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Log("cleanup drop table:", err)
	}
	t.Log("Cleanup done")
}

// TestBulkloadV2_PartialUpdate tests UPSERT with PartialUpdateColumns:
// only specified columns are updated, other non-PK columns keep original values.
func TestBulkloadV2_PartialUpdate(t *testing.T) {
	dsn := skipIfNoDSN(t)

	tableName := "go_sdk_bulkload_partial_test"

	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id    INT,
		name  STRING,
		score BIGINT,
		tag   STRING,
		PRIMARY KEY (id)
	)`, tableName))
	if err != nil {
		t.Fatal("create table:", err)
	}
	t.Log("Table created")

	// Step 1: APPEND initial data
	conn1, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := conn1.CreateBulkloadStream(BulkloadOptions{Table: tableName, Operation: APPEND})
	if err != nil {
		t.Fatal(err)
	}
	w1, err := s1.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		row := w1.CreateRow()
		row.SetInt("id", int32(i))
		row.SetString("name", fmt.Sprintf("name_%d", i))
		row.SetBigint("score", int64(i*10))
		row.SetString("tag", fmt.Sprintf("tag_%d", i))
		if err := w1.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal("append close:", err)
	}
	t.Log("APPEND committed: 3 rows")

	// Step 2: UPSERT with PartialUpdateColumns = ["score"]
	// Only "score" should be updated; "name" and "tag" should keep original values
	conn2, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := conn2.CreateBulkloadStream(BulkloadOptions{
		Table:                tableName,
		Operation:            UPSERT,
		RecordKeys:           []string{"id"},
		PartialUpdateColumns: []string{"score"},
	})
	if err != nil {
		t.Fatal("create upsert stream:", err)
	}
	w2, err := s2.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	// Update id 1,2 with new score; id 4 is new
	for _, d := range []struct {
		id    int32
		name  string
		score int64
		tag   string
	}{
		{1, "ignored_name", 999, "ignored_tag"},
		{2, "ignored_name", 888, "ignored_tag"},
		{4, "new_name_4", 444, "new_tag_4"},
	} {
		row := w2.CreateRow()
		row.SetInt("id", d.id)
		row.SetString("name", d.name)
		row.SetBigint("score", d.score)
		row.SetString("tag", d.tag)
		if err := w2.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := s2.Close(); err != nil {
		t.Fatal("upsert close:", err)
	}
	t.Log("UPSERT with PartialUpdateColumns committed")

	// Step 3: Verify
	var count int64
	db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s", tableName)).Scan(&count)
	if count != 4 {
		t.Fatalf("expected 4 rows, got %d", count)
	}

	// id=1: score updated to 999, name and tag should remain original
	var name, tag string
	var score int64
	db.QueryRow(fmt.Sprintf("SELECT name, score, tag FROM %s WHERE id = 1", tableName)).Scan(&name, &score, &tag)
	if score != 999 {
		t.Fatalf("id=1: expected score=999, got %d", score)
	}
	if name != "name_1" {
		t.Fatalf("id=1: expected name=name_1 (unchanged), got %s", name)
	}
	if tag != "tag_1" {
		t.Fatalf("id=1: expected tag=tag_1 (unchanged), got %s", tag)
	}
	t.Log("id=1 partial update verified: score updated, name/tag unchanged")

	// id=3: untouched
	db.QueryRow(fmt.Sprintf("SELECT name, score, tag FROM %s WHERE id = 3", tableName)).Scan(&name, &score, &tag)
	if score != 30 || name != "name_3" || tag != "tag_3" {
		t.Fatalf("id=3: expected original values, got name=%s score=%d tag=%s", name, score, tag)
	}
	t.Log("id=3 untouched verified")

	// id=4: new row
	db.QueryRow(fmt.Sprintf("SELECT name, score, tag FROM %s WHERE id = 4", tableName)).Scan(&name, &score, &tag)
	if score != 444 {
		t.Fatalf("id=4: expected score=444, got %d", score)
	}
	t.Log("id=4 new row verified")

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Log("Cleanup done")
}

// TestBulkloadV2_Properties tests that custom Properties are injected as SET statements.
func TestBulkloadV2_Properties(t *testing.T) {
	dsn := skipIfNoDSN(t)

	tableName := "go_sdk_bulkload_props_test"

	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id   INT,
		name STRING,
		PRIMARY KEY (id)
	)`, tableName))
	if err != nil {
		t.Fatal("create table:", err)
	}
	t.Log("Table created")

	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}

	// Use Properties to set a custom SQL property
	s, err := conn.CreateBulkloadStream(BulkloadOptions{
		Table:     tableName,
		Operation: APPEND,
		Properties: map[string]string{
			"cz.sql.type.conversion": "tolerant",
		},
	})
	if err != nil {
		t.Fatal("create stream:", err)
	}

	w, err := s.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		row := w.CreateRow()
		row.SetInt("id", int32(i))
		row.SetString("name", fmt.Sprintf("props_test_%d", i))
		if err := w.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatal("close stream:", err)
	}
	t.Log("Stream with Properties committed")

	var count int64
	db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s", tableName)).Scan(&count)
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
	t.Logf("Verified %d rows", count)

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Log("Cleanup done")
}

// TestBulkloadV2_Overwrite tests Operation=OVERWRITE: replaces all existing data.
func TestBulkloadV2_Overwrite(t *testing.T) {
	dsn := skipIfNoDSN(t)
	tableName := "go_sdk_bulkload_overwrite_test"

	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id   INT,
		name STRING,
		PRIMARY KEY (id)
	)`, tableName))
	if err != nil {
		t.Fatal("create table:", err)
	}
	t.Log("Table created")

	// Step 1: APPEND initial 5 rows
	conn1, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := conn1.CreateBulkloadStream(BulkloadOptions{Table: tableName, Operation: APPEND})
	if err != nil {
		t.Fatal(err)
	}
	w1, err := s1.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		row := w1.CreateRow()
		row.SetInt("id", int32(i))
		row.SetString("name", fmt.Sprintf("old_%d", i))
		if err := w1.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	var count int64
	db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s", tableName)).Scan(&count)
	if count != 5 {
		t.Fatalf("expected 5 rows after APPEND, got %d", count)
	}
	t.Log("APPEND committed: 5 rows")

	// Step 2: OVERWRITE with 2 rows — should replace all data
	conn2, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := conn2.CreateBulkloadStream(BulkloadOptions{Table: tableName, Operation: OVERWRITE})
	if err != nil {
		t.Fatal(err)
	}
	w2, err := s2.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 10; i <= 11; i++ {
		row := w2.CreateRow()
		row.SetInt("id", int32(i))
		row.SetString("name", fmt.Sprintf("new_%d", i))
		if err := w2.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("OVERWRITE committed: 2 rows")

	// Verify: should have exactly 2 rows, old data gone
	db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s", tableName)).Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2 rows after OVERWRITE, got %d", count)
	}

	var name string
	db.QueryRow(fmt.Sprintf("SELECT name FROM %s WHERE id = 10", tableName)).Scan(&name)
	if name != "new_10" {
		t.Fatalf("expected name=new_10, got %s", name)
	}

	// Old rows should be gone
	err = db.QueryRow(fmt.Sprintf("SELECT name FROM %s WHERE id = 1", tableName)).Scan(&name)
	if err == nil {
		t.Fatal("expected id=1 to be gone after OVERWRITE")
	}
	t.Log("OVERWRITE verified: old data replaced")

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Log("Cleanup done")
}

// TestBulkloadV2_PartitionSpec tests writing to a partitioned table with static partition spec.
func TestBulkloadV2_PartitionSpec(t *testing.T) {
	dsn := skipIfNoDSN(t)
	tableName := "go_sdk_bulkload_partition_test"

	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id   INT,
		name STRING,
		pt   STRING,
		PRIMARY KEY (id, pt)
	) PARTITIONED BY (pt)`, tableName))
	if err != nil {
		t.Fatal("create table:", err)
	}
	t.Log("Partitioned table created")

	// Write with static partition spec pt=2026
	conn1, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := conn1.CreateBulkloadStream(BulkloadOptions{
		Table:         tableName,
		Operation:     APPEND,
		PartitionSpec: "pt=2026",
	})
	if err != nil {
		t.Fatal(err)
	}
	w1, err := s1.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		row := w1.CreateRow()
		row.SetInt("id", int32(i))
		row.SetString("name", fmt.Sprintf("part_test_%d", i))
		// pt is set automatically by PartitionSpec
		if err := w1.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("APPEND with PartitionSpec=pt=2026 committed")

	// Verify partition value
	var count int64
	db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s WHERE pt = '2026'", tableName)).Scan(&count)
	if count != 3 {
		t.Fatalf("expected 3 rows in partition pt=2026, got %d", count)
	}
	t.Logf("Verified %d rows in partition pt=2026", count)

	// Verify no rows in other partitions
	db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s WHERE pt != '2026'", tableName)).Scan(&count)
	if count != 0 {
		t.Fatalf("expected 0 rows outside partition pt=2026, got %d", count)
	}
	t.Log("Partition isolation verified")

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Log("Cleanup done")
}

// TestBulkloadV2_LoadUri tests writing with a custom LoadUri (temp directory).
func TestBulkloadV2_LoadUri(t *testing.T) {
	dsn := skipIfNoDSN(t)
	tableName := "go_sdk_bulkload_loaduri_test"

	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id   INT,
		name STRING,
		PRIMARY KEY (id)
	)`, tableName))
	if err != nil {
		t.Fatal("create table:", err)
	}
	t.Log("Table created")

	// Create a custom temp dir
	customDir, err := os.MkdirTemp("", "go_sdk_loaduri_test_*")
	if err != nil {
		t.Fatal("create temp dir:", err)
	}
	defer os.RemoveAll(customDir)

	conn, err := connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	s, err := conn.CreateBulkloadStream(BulkloadOptions{
		Table:     tableName,
		Operation: APPEND,
		LoadUri:   customDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	w, err := s.CreateWriter(0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify writer is using our custom dir
	if !strings.HasPrefix(w.LocalBaseDir, customDir) {
		t.Fatalf("expected LocalBaseDir to start with %s, got %s", customDir, w.LocalBaseDir)
	}
	t.Logf("Writer using custom LoadUri: %s", w.LocalBaseDir)

	for i := 1; i <= 3; i++ {
		row := w.CreateRow()
		row.SetInt("id", int32(i))
		row.SetString("name", fmt.Sprintf("loaduri_test_%d", i))
		if err := w.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("Stream with custom LoadUri committed")

	var count int64
	db.QueryRow(fmt.Sprintf("SELECT count(1) FROM %s", tableName)).Scan(&count)
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
	t.Logf("Verified %d rows", count)

	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Log("Cleanup done")
}
