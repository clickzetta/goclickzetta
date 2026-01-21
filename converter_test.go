package goclickzetta

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/zeebo/assert"
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

// ---------- normalizeKeyValueJSON coverage ----------

func TestNormalize_ArrayOfKV_StrictFold(t *testing.T) {
	in := `[{"key":"trace_id","value":"t-001"},{"key":"version","value":"1"}]`
	out := normalizeKeyValueJSON(in)
	var got map[string]string
	assert.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, "t-001", got["trace_id"])
	assert.Equal(t, "1", got["version"])
}

func TestNormalize_ArrayOfKV_ExtraField_NoFold(t *testing.T) {
	in := `[{"key":"a","value":"1","extra":true}]`
	out := normalizeKeyValueJSON(in)
	// not fold, should keep as array
	var got []map[string]interface{}
	assert.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, 1, len(got))
	assert.True(t, got[0]["extra"].(bool))
}

func TestNormalize_ArrayOfKV_MissingValue_NoFold(t *testing.T) {
	in := `[{"key":"only"}]`
	out := normalizeKeyValueJSON(in)
	// not fold, should keep as array
	var got []map[string]interface{}
	assert.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, 1, len(got))
	_, has := got[0]["value"]
	assert.False(t, has)
}

func TestNormalize_Nested_StructWithMap(t *testing.T) {
	in := `{"m":[{"key":"name","value":"liu"}],"n":1}`
	out := normalizeKeyValueJSON(in)
	var got map[string]interface{}
	assert.NoError(t, json.Unmarshal([]byte(out), &got))
	m := got["m"].(map[string]interface{})
	assert.Equal(t, "liu", m["name"])
	assert.Equal(t, float64(1), got["n"]) // json number is float64
}

func TestNormalize_ArrayNested_ArrayOfMaps(t *testing.T) {
	in := `[[{"key":"k1","value":"v1"}], [{"key":"k2","value":"v2"}]]`
	out := normalizeKeyValueJSON(in)
	var got []map[string]string
	assert.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, "v1", got[0]["k1"])
	assert.Equal(t, "v2", got[1]["k2"])
}

func TestNormalize_ValuesNonString(t *testing.T) {
	in := `[{"key":"n","value":123},{"key":"b","value":true}]`
	out := normalizeKeyValueJSON(in)
	var got map[string]interface{}
	assert.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, float64(123), got["n"]) // json number is float64
	assert.Equal(t, true, got["b"])
}

// ---------- arrowToValue coverage (MAP string/string and fallback) ----------

func buildMapArrayStringString(keys, values []string) *array.Map {
	mb := array.NewMapBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	vb := mb.ItemBuilder().(*array.StringBuilder)
	mb.Append(true)
	for i := range keys {
		kb.Append(keys[i])
		vb.Append(values[i])
	}
	arr := mb.NewMapArray()
	mb.Release()
	return arr
}

func buildMapArrayStringInt(keys []string, values []int32) *array.Map {
	mb := array.NewMapBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, false)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	vb := mb.ItemBuilder().(*array.Int32Builder)
	mb.Append(true)
	for i := range keys {
		kb.Append(keys[i])
		vb.Append(values[i])
	}
	arr := mb.NewMapArray()
	mb.Release()
	return arr
}

func TestArrowToValue_Map_StringString_ObjectJSON(t *testing.T) {
	arr := buildMapArrayStringString([]string{"trace_id", "version"}, []string{"t-001", "1"})
	defer arr.Release()

	meta := execResponseColumnType{Type: "MAP"}
	dest := make([]interface{}, arr.Len())
	err := arrowToValue(dest, meta, arr, time.Local, false)
	assert.NoError(t, err)
	// should be object JSON
	var got map[string]string
	assert.NoError(t, json.Unmarshal([]byte(dest[0].(string)), &got))
	assert.Equal(t, "t-001", got["trace_id"])
	assert.Equal(t, "1", got["version"])
}

func TestArrowToValue_Map_StringInt_FallbackNormalize(t *testing.T) {
	arr := buildMapArrayStringInt([]string{"n", "m"}, []int32{10, 20})
	defer arr.Release()

	meta := execResponseColumnType{Type: "MAP"}
	dest := make([]interface{}, arr.Len())
	err := arrowToValue(dest, meta, arr, time.Local, false)
	assert.NoError(t, err)
	// should be object JSON, number should be number
	var got map[string]interface{}
	assert.NoError(t, json.Unmarshal([]byte(dest[0].(string)), &got))
	assert.Equal(t, float64(10), got["n"]) // json number is float64
	assert.Equal(t, float64(20), got["m"]) // json number is float64
}
