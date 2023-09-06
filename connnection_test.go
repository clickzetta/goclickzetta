package goclickzetta

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/zeebo/assert"
	"io"
	"testing"
)

func TestBuildConnection(t *testing.T) {
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
	conn, err := buildClickzettaConn(ctx, cfg)
	if err != nil {
		t.Error(err)
	}
	if conn.cfg.Token == "" {
		t.Error("token is empty")
	}
	fmt.Println(conn.cfg.Token)
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestConnectionQuery(t *testing.T) {
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
	conn, err := buildClickzettaConn(ctx, cfg)
	if err != nil {
		t.Error(err)
	}
	if conn.cfg.Token == "" {
		t.Error("token is empty")
	}
	data, err := conn.Query("select * from clickzetta_sample_data.ecommerce_events_history.ecommerce_events_multicategorystore_live limit 10;", nil)
	if err != nil {
		t.Error(err)
	}
	result := make([]driver.Value, 9)
	for data.Next(result) != io.EOF {
		fmt.Println(result)
	}
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestConnectionExec(t *testing.T) {
	ctx := context.TODO()
	cfg := Config{
		UserName:       "SD_demo",
		Password:       "Asddemo123!",
		Protocol:       "https",
		Service:        "https://api.clickzetta.com",
		Instance:       "6861c888",
		Workspace:      "quickStart_WS",
		VirtualCluster: "default",
		Schema:         "public",
	}
	conn, err := buildClickzettaConn(ctx, cfg)
	if err != nil {
		t.Error(err)
	}
	if conn.cfg.Token == "" {
		t.Error("token is empty")
	}
	ddl := "CREATE TABLE `customers_go_test` (\n  `id` int(11) NULL,\n  `first_name` varchar(50) NULL,\n  `last_name` varchar(50) NULL,\n  `email` varchar(100) NULL,\n  `phone_number` varchar(20) NULL,\n  `address` varchar(200) NULL\n)"
	result, err := conn.Exec(ddl, nil)
	if err != nil {
		t.Error(err)
	}
	res, ok := result.(ClickzettaResult)
	if !ok {
		t.Error("result is not ClickzettaResult")
	}
	if res.GetStatus() != queryStatus(QueryStatusComplete) {
		fmt.Println(res.GetError().Error())
		t.Error("query status is failed")
	}
	assert.Equal(t, res.GetStatus(), queryStatus(QueryStatusComplete))
	del := "DROP TABLE `customers_go_test`"
	result, err = conn.Exec(del, nil)
	if err != nil {
		t.Error(err)
	}
	res, ok = result.(ClickzettaResult)
	if !ok {
		t.Error("result is not ClickzettaResult")
	}
	if res.GetStatus() != queryStatus(QueryStatusComplete) {
		fmt.Println(res.GetError().Error())
		t.Error("query status is failed")
	}
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestConnectionPing(t *testing.T) {
	ctx := context.TODO()
	cfg := Config{
		UserName:       "SD_demo",
		Password:       "Asddemo123!",
		Protocol:       "https",
		Service:        "https://api.clickzetta.com",
		Instance:       "6861c888",
		Workspace:      "quickStart_WS",
		VirtualCluster: "default",
		Schema:         "public",
	}
	conn, err := buildClickzettaConn(ctx, cfg)
	if err != nil {
		t.Error(err)
	}
	err = conn.Ping(ctx)
	if err != nil {
		t.Error(err)
	}
	err = conn.Close()
	if err != nil {
		t.Error(err)
	}
}
