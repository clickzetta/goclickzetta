package goclickzetta

import (
	"testing"

	"github.com/zeebo/assert"
	"gorm.io/gorm"
)

func TestClickZettaDialector(t *testing.T) {
	// 创建一个新的驱动
	dsn := integrationDSN(t)
	driver := Open(dsn)
	db, err := gorm.Open(driver, &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	var result int

	db1 := db.Raw("SELECT 1")

	if err := db1.Scan(&result).Error; err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}

	assert.Equal(t, result, 1)
}
