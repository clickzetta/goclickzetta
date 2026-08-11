package goclickzetta

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

func getTestDSN(t *testing.T) string {
	dsn := os.Getenv("CZ_TEST_DSN")
	if dsn == "" {
		t.Skip("CZ_TEST_DSN not set, skipping integration test")
	}
	return dsn
}

func TestDriver(t *testing.T) {
	t.Run("TestOpen", TestOpen)
	t.Run("TestOpenWithConfig", TestOpenWithConfig)
	t.Run("TestOpenWithString", TestOpenWithString)
}

func TestOpenWithString(t *testing.T) {
	dsn := getTestDSN(t)
	driver := ClickzettaDriver{}
	conn, err := driver.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("conn is nil")
	}
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestOpen(t *testing.T) {
	dsn := getTestDSN(t)
	driver := ClickzettaDriver{}
	conn, err := driver.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("conn is nil")
	}
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestOpenWithConfig(t *testing.T) {
	ctx := context.TODO()
	dsn := getTestDSN(t)
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}

	driver := ClickzettaDriver{}
	conn, err := driver.OpenWithConfig(ctx, *cfg)
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("conn is nil")
	}
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}

}

func TestSqlOpen(t *testing.T) {
	dsn := getTestDSN(t)
	db, err := sql.Open("clickzetta", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})

	tableName := fmt.Sprintf("goclickzetta_sql_it_%d", time.Now().UnixNano())
	testCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if _, err := db.ExecContext(testCtx, fmt.Sprintf("CREATE TABLE %s (id BIGINT, name STRING)", tableName)); err != nil {
		t.Fatalf("create integration test table: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if _, err := db.ExecContext(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			t.Errorf("drop integration test table %s: %v", tableName, err)
		}
	})

	if _, err := db.ExecContext(testCtx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob')", tableName)); err != nil {
		t.Fatalf("insert integration test rows: %v", err)
	}

	rows, err := db.QueryContext(testCtx, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", tableName))
	if err != nil {
		t.Fatalf("query integration test table: %v", err)
	}
	defer rows.Close()

	type testRow struct {
		id   int64
		name string
	}
	expected := []testRow{{id: 1, name: "alice"}, {id: 2, name: "bob"}}
	actual := make([]testRow, 0, len(expected))
	for rows.Next() {
		var result testRow
		if err := rows.Scan(&result.id, &result.name); err != nil {
			t.Fatalf("scan integration test row: %v", err)
		}
		actual = append(actual, result)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate integration test rows: %v", err)
	}
	if len(actual) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(actual))
	}
	for index := range expected {
		if actual[index] != expected[index] {
			t.Errorf("row %d: expected %+v, got %+v", index, expected[index], actual[index])
		}
	}
}
