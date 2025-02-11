package goclickzetta

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

func TestDriver(t *testing.T) {
	t.Run("TestOpen", TestOpen)
	t.Run("TestOpenWithConfig", TestOpenWithConfig)
	t.Run("TestOpenWithString", TestOpenWithString)
}

func TestOpenWithString(t *testing.T) {
	dsn := "username:passwprd@https(mock.clickzetta.com)/schema?virtualCluster=default&workspace=mock&instance=mock"
	driver := ClickzettaDriver{}
	conn, err := driver.Open(dsn)
	if err != nil {
		t.Error(err)
	}
	if conn == nil {
		t.Error("conn is nil")
	}
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestOpen(t *testing.T) {
	cfg := Config{
		UserName:       "username",
		Password:       "password!",
		Protocol:       "https",
		Service:        "https://mock.clickzetta.com",
		Instance:       "mock",
		Workspace:      "mock",
		VirtualCluster: "default",
		Schema:         "default",
	}
	dsnStr := DSN(&cfg)
	driver := ClickzettaDriver{}
	conn, err := driver.Open(dsnStr)
	if err != nil {
		t.Error(err)
	}
	if conn == nil {
		t.Error("conn is nil")
	}
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestOpenWithConfig(t *testing.T) {
	ctx := context.TODO()
	cfg := Config{
		UserName:       "username",
		Password:       "password!",
		Protocol:       "https",
		Service:        "https://mock.clickzetta.com",
		Instance:       "mock",
		Workspace:      "mock",
		VirtualCluster: "default",
		Schema:         "default",
	}

	driver := ClickzettaDriver{}
	conn, err := driver.OpenWithConfig(ctx, cfg)
	if err != nil {
		t.Error(err)
	}
	if conn == nil {
		t.Error("conn is nil")
	}
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}

}

func TestSqlOpen(t *testing.T) {
	db, err := sql.Open("clickzetta", "username:passwprd@https(mock.clickzetta.com)/schema?virtualCluster=default&workspace=mock&instance=mock")
	if err != nil {
		t.Error(err)
	}
	if db == nil {
		t.Error("db is nil")
	}
	defer db.Close()
	res, err := db.Query("select * from clickzetta_sample_data.ecommerce_events_history.ecommerce_events_multicategorystore_live limit 100000;")
	if err != nil {
		t.Error(err)
	}
	defer res.Close()
	type ResultHuge struct {
		EventTime    string
		EventType    string
		ProductId    string
		CategoryId   string
		CategoryCode string
		Brand        string
		Price        float64
		UserId       string
		UserSession  string
	}
	count := 0
	for res.Next() {
		var result ResultHuge
		err := res.Scan(&result.EventTime, &result.EventType, &result.ProductId, &result.CategoryId, &result.CategoryCode, &result.Brand, &result.Price, &result.UserId, &result.UserSession)
		if err != nil {
			t.Error(err)
		}
		count++
		fmt.Printf("result is: %v, count is %v\n", result, count)
	}
}
