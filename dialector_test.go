package goclickzetta

import (
	"log"
	"os"
	"testing"

	"github.com/zeebo/assert"
	"gorm.io/gorm"
)

func TestClickZettaDialector(t *testing.T) {
	dsn := os.Getenv("CZ_TEST_DSN")
	if dsn == "" {
		t.Skip("CZ_TEST_DSN not set, skipping integration test")
	}
	driver := Open(dsn)
	db, err := gorm.Open(driver, &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	var result int

	db1 := db.Raw("SELECT 1")

	if err := db1.Scan(&result).Error; err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}

	assert.Equal(t, result, 1)
}
