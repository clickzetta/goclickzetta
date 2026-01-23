package goclickzetta

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/zeebo/assert"
)

func newTestConfig() Config {
	return Config{
		UserName:       "weiliu",
		Password:       "Abc123456",
		Protocol:       "http",
		Service:        "http://uat-api.clickzetta.com",
		Instance:       "jnsxwfyr",
		Workspace:      "weiliu",
		VirtualCluster: "default_ap",
		Schema:         "public",
	}
}

func TestBuildConnection(t *testing.T) {
	ctx := context.TODO()
	conn, err := buildClickzettaConn(ctx, newTestConfig())
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
	conn, err := buildClickzettaConn(ctx, newTestConfig())
	if err != nil {
		t.Error(err)
	}
	if conn.cfg.Token == "" {
		t.Error("token is empty")
	}
	data, err := conn.Query("select 1+1;", nil)
	if err != nil {
		t.Error(err)
	}
	result := make([]driver.Value, 1)
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
	conn, err := buildClickzettaConn(ctx, newTestConfig())
	if err != nil {
		t.Error(err)
	}
	if conn.cfg.Token == "" {
		t.Error("token is empty")
	}
	ddl := "CREATE TABLE `customers_go_test` (\n  `id` int NULL,\n  `first_name` string NULL,\n  `last_name` string NULL,\n  `email` string NULL,\n  `phone_number` string NULL,\n  `address` string NULL\n)"
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
	conn, err := buildClickzettaConn(ctx, newTestConfig())
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

func TestConnectionSetCatalog(t *testing.T) {
	ctx := context.TODO()
	conn, err := buildClickzettaConn(ctx, newTestConfig())
	if err != nil {
		t.Error(err)
	}

	workspace := conn.cfg.Workspace
	// Test initial catalog is empty
	assert.Equal(t, conn.GetCatalog(), workspace)

	// Test SetCatalog
	conn.SetCatalog("test_catalog")
	assert.Equal(t, conn.GetCatalog(), "test_catalog")

	// Test SetCatalog overwrites previous value
	conn.SetCatalog("another_catalog")
	assert.Equal(t, conn.GetCatalog(), "another_catalog")

	// Verify it's stored in Params
	assert.NotNil(t, conn.cfg.Params["catalog"])
	assert.Equal(t, *conn.cfg.Params["catalog"], "another_catalog")

	// Verify workspace
	assert.NotNil(t, conn.cfg.Workspace)
	assert.Equal(t, *&conn.cfg.Workspace, workspace)

	err = conn.Close()
	if err != nil {
		t.Error(err)
	}
}
