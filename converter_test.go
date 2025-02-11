package goclickzetta

import (
	"github.com/zeebo/assert"
	"testing"
	"time"
)

func TestGoTypeToClickzetta(t *testing.T) {
	assert.Equal(t, clickzettaType(BIGINT), goTypeToClickzetta(int64(1), clickzettaType(DATE)))
	assert.Equal(t, clickzettaType(BOOLEAN), goTypeToClickzetta(true, clickzettaType(DATE)))
	assert.Equal(t, clickzettaType(DATE), goTypeToClickzetta(time.Now(), clickzettaType(DATE)))
	assert.Equal(t, clickzettaType(DOUBLE), goTypeToClickzetta(float64(1.1), clickzettaType(DATE)))
	assert.Equal(t, clickzettaType(FLOAT), goTypeToClickzetta(float32(1.1), clickzettaType(DATE)))
	assert.Equal(t, clickzettaType(INT), goTypeToClickzetta(int(1), clickzettaType(DATE)))
	assert.Equal(t, clickzettaType(SMALLINT), goTypeToClickzetta(int16(1), clickzettaType(DATE)))
	assert.Equal(t, clickzettaType(STRING), goTypeToClickzetta("test", clickzettaType(DATE)))
	assert.Equal(t, clickzettaType(TIMESTAMP_LTZ), goTypeToClickzetta(time.Now(), clickzettaType(TIMESTAMP_LTZ)))
	assert.Equal(t, clickzettaType(TINYINT), goTypeToClickzetta(int8(1), clickzettaType(DATE)))

}
