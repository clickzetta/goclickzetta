package main

import (
	"log"
	"time"

	"github.com/clickzetta/goclickzetta"
	"gorm.io/gorm"
)

func main() {
	// 创建一个新的驱动
	dsn := "username:password@https(example.clickzetta.com)/schema?virtualCluster=default&workspace=workspace&instance=instance"
	driver := goclickzetta.Open(dsn)
	db, err := gorm.Open(driver, &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// 测试查询
	// define result, with 3 columns (id int, msg, string, create_time timestamp)

	type ResultInfo struct {
		ID         int       `gorm:"column:id"`
		Msg        string    `gorm:"column:msg"`
		CreateTime time.Time `gorm:"column:create_time"`
	}

	var results []ResultInfo

	db1 := db.Raw("SELECT * from test_kettle")

	if err := db1.Scan(&results).Error; err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}

	// if result != 1 {
	// 	log.Fatalf("Expected result to be 1, got %d", result)
	// }

	log.Print("Successfully connected to database with result ", results)
	for _, result := range results {
		log.Printf("ID: %d, Msg: %s, CreateTime: %s", result.ID, result.Msg, result.CreateTime)
	}

	table := db.Table("test_kettle")

	line := ResultInfo{ID: 3, Msg: "Hello_3", CreateTime: time.Now()}
	result := table.Create(&line)
	// Ensure the operation was successful
	if result.Error != nil {
		log.Fatalf("Failed to create record: %v", result.Error)
	}
	if line.ID == 0 {
		log.Fatalf("Expected ID to be non-zero, got %d", line.ID)
	}

	var found ResultInfo
	if err := table.First(&found, line.ID).Error; err != nil {
		log.Fatalf("Failed to find record: %v", err)
	}
	log.Printf("Found record: %v", found)

	// 测试删除
	result = table.Delete(&line)
	if result.Error != nil {
		log.Fatalf("Failed to delete record: %v", result.Error)
	}

	found = ResultInfo{}
	table = db.Table("test_kettle")
	if err := table.First(&found, line.ID).Error; err != nil {
		log.Printf("Failed to find deleted record: %v", err)
	}
}
