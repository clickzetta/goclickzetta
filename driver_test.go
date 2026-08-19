package goclickzetta

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestDriver(t *testing.T) {
	t.Run("TestOpen", TestOpen)
	t.Run("TestOpenWithConfig", TestOpenWithConfig)
	t.Run("TestOpenWithString", TestOpenWithString)
}

func TestOpenWithString(t *testing.T) {
	dsn := integrationDSN(t)
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
	driver := ClickzettaDriver{}
	conn, err := driver.Open(integrationDSN(t))
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
	cfg := integrationConfig(t)

	driver := ClickzettaDriver{}
	conn, err := driver.OpenWithConfig(ctx, cfg)
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
	db, err := sql.Open("clickzetta", integrationDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}
	t.Cleanup(func() { _ = db.Close() })
	tableName := fmt.Sprintf("goclickzetta_sql_it_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT, name STRING)", tableName)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if _, err := db.ExecContext(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			t.Errorf("drop integration table: %v", err)
		}
	})
	if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob')", tableName)); err != nil {
		t.Fatal(err)
	}
	res, err := db.QueryContext(ctx, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	type resultRow struct {
		ID   int64
		Name string
	}
	count := 0
	for res.Next() {
		var result resultRow
		err := res.Scan(&result.ID, &result.Name)
		if err != nil {
			t.Fatal(err)
		}
		count++
		fmt.Printf("result is: %v, count is %v\n", result, count)
	}
	if err := res.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}
