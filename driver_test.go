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
	dsn := "SD_demo:Asddemo123!@https(api.clickzetta.com)/ecommerce_events_history?virtualCluster=default&workspace=quickStart_WS&instance=6861c888"
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
		UserName:       "SD_demo",
		Password:       "Asddemo123!",
		Protocol:       "https",
		Service:        "api.clickzetta.com",
		Instance:       "6861c888",
		Workspace:      "quickStart_WS",
		VirtualCluster: "default",
		Schema:         "ecommerce_events_history",
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
		UserName:       "SD_demo",
		Password:       "Asddemo123!",
		Protocol:       "https",
		Service:        "https://api.clickzetta.com",
		Instance:       "6861c888",
		Workspace:      "quickStart_WS",
		VirtualCluster: "default",
		Schema:         "ecommerce_events_history",
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
	db, err := sql.Open("clickzetta", "SD_demo:Asddemo123!@https(api.clickzetta.com)/ecommerce_events_history?virtualCluster=default&workspace=quickStart_WS&instance=6861c888")
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
