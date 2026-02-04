package goclickzetta

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/shopspring/decimal"
)

// convertBindingsToArrowBinary convert bindings to Arrow IPC binary data
// bindings is a list of rows, each row contains multiple column values
func convertBindingsToArrowBinary(bindings []driver.NamedValue) ([][]byte, error) {
	if len(bindings) == 0 {
		return nil, nil
	}

	mem := memory.DefaultAllocator

	// bindings[0].Value should be [][]interface{} type, containing all rows data
	rows, ok := bindings[0].Value.([][]interface{})
	if !ok {
		return nil, fmt.Errorf("bindings[0].Value is not [][]interface{}")
	}

	if len(rows) == 0 {
		return nil, nil
	}

	// parse first row to determine number of columns
	firstRow := rows[0]
	if len(firstRow) == 0 {
		return nil, fmt.Errorf("first row has no columns")
	}

	numCols := len(firstRow)
	totalRows := len(rows)

	// global type inference: traverse all rows, find the first non-nil value for each column
	// this can ensure that all chunks use the same Schema
	columnSamples := make([]interface{}, numCols)
	for rowIdx, row := range rows {
		if len(row) != numCols {
			return nil, fmt.Errorf("row %d has %d columns, expected %d", rowIdx, len(row), numCols)
		}
		for colIdx, value := range row {
			if columnSamples[colIdx] == nil && value != nil {
				columnSamples[colIdx] = value
			}
		}
	}

	// Pre-calculate Schema and Fields to avoid repeated inference in each chunk
	fields := make([]arrow.Field, numCols)
	fieldIDGen := &fieldIDGenerator{nextID: 1}
	for i, sample := range columnSamples {
		// Use a temporary builder just to get the Field type
		field, builder := getArrowTypeAndBuilder(mem, sample, fmt.Sprintf("col_%d", i), fieldIDGen)
		fields[i] = field
		builder.Release()
	}
	schema := arrow.NewSchema(fields, nil)

	batchSize := 5000
	numChunks := (totalRows + batchSize - 1) / batchSize
	results := make([][]byte, numChunks)

	var wg sync.WaitGroup
	// use channel to limit concurrency, avoid memory increasing too fast, default using CPU cores
	sem := make(chan struct{}, runtime.NumCPU())
	// use channel to capture the first error
	errChan := make(chan error, 1)

	for i := 0; i < numChunks; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > totalRows {
			end = totalRows
		}

		// if there is an error, stop starting new task
		select {
		case <-errChan:
			goto Wait
		default:
		}

		wg.Add(1)
		go func(chunkIdx int, start, end int) {
			defer wg.Done()

			// get token
			sem <- struct{}{}
			defer func() { <-sem }()

			// check if there is an error
			select {
			case <-errChan:
				return
			default:
			}

			chunkRows := rows[start:end]
			buf, err := processChunk(chunkRows, schema, mem)
			if err != nil {
				// try to write error, if there is an error, ignore it
				select {
				case errChan <- err:
				default:
				}
				return
			}
			results[chunkIdx] = buf
		}(i, start, end)
	}

Wait:
	wg.Wait()

	// check if there is an error
	select {
	case err := <-errChan:
		return nil, err
	default:
	}

	return results, nil
}

func processChunk(rows [][]interface{}, schema *arrow.Schema, mem memory.Allocator) ([]byte, error) {
	numCols := len(schema.Fields())
	numRows := len(rows)

	// create builder for current chunk based on Schema
	builders := make([]array.Builder, numCols)
	for i, field := range schema.Fields() {
		builders[i] = createBuilderFromType(mem, field.Type)
	}

	// fill data
	for _, row := range rows {
		for colIdx, value := range row {
			appendValue(builders[colIdx], value)
		}
	}

	// build arrays
	arrays := make([]arrow.Array, numCols)
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		builder.Release() // after array is created, builder can be released
	}

	// create and encode RecordBatch
	record := array.NewRecord(schema, arrays, int64(numRows))
	defer record.Release()
	for _, arr := range arrays {
		defer arr.Release()
	}

	var buf bytes.Buffer
	// use ZSTD compression
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithZstd())
	defer writer.Close()

	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("failed to write arrow record: %w", err)
	}

	return buf.Bytes(), nil
}

// createBuilderFromType creates a Builder based on arrow.DataType
func createBuilderFromType(mem memory.Allocator, dt arrow.DataType) array.Builder {
	switch t := dt.(type) {
	case *arrow.BooleanType:
		return array.NewBooleanBuilder(mem)
	case *arrow.Int8Type:
		return array.NewInt8Builder(mem)
	case *arrow.Int16Type:
		return array.NewInt16Builder(mem)
	case *arrow.Int32Type:
		return array.NewInt32Builder(mem)
	case *arrow.Int64Type:
		return array.NewInt64Builder(mem)
	case *arrow.Uint8Type:
		return array.NewUint8Builder(mem)
	case *arrow.Uint16Type:
		return array.NewUint16Builder(mem)
	case *arrow.Uint32Type:
		return array.NewUint32Builder(mem)
	case *arrow.Uint64Type:
		return array.NewUint64Builder(mem)
	case *arrow.Float32Type:
		return array.NewFloat32Builder(mem)
	case *arrow.Float64Type:
		return array.NewFloat64Builder(mem)
	case *arrow.StringType:
		return array.NewStringBuilder(mem)
	case *arrow.BinaryType:
		return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	case *arrow.TimestampType:
		return array.NewTimestampBuilder(mem, t)
	case *arrow.ListType:
		return array.NewListBuilder(mem, t.Elem())
	case *arrow.MapType:
		return array.NewMapBuilder(mem, t.KeyType(), t.ItemType(), false)
	case *arrow.StructType:
		return array.NewStructBuilder(mem, t)
	case *arrow.NullType:
		return array.NewNullBuilder(mem)
	case *arrow.Decimal128Type:
		return array.NewDecimal128Builder(mem, t)
	default:
		// Fallback for unknown types (should not happen with correct inference)
		return array.NewStringBuilder(mem)
	}
}

// toInterfaceSlice convert row to []interface{}
func toInterfaceSlice(row interface{}) []interface{} {
	switch v := row.(type) {
	case []interface{}:
		return v
	case []driver.Value:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result
	default:
		return nil
	}
}

// fieldIDGenerator generate unique field ID for all fields (including nested fields)
type fieldIDGenerator struct {
	nextID int
}

// next return the next available field ID
func (g *fieldIDGenerator) next() int {
	id := g.nextID
	g.nextID++
	return id
}

// makeFieldWithID create Arrow Field with field ID
func makeFieldWithID(name string, dataType arrow.DataType, nullable bool, fieldID int) arrow.Field {
	metadata := arrow.NewMetadata(
		[]string{"PARQUET:field_id"},
		[]string{fmt.Sprintf("%d", fieldID)},
	)
	return arrow.Field{
		Name:     name,
		Type:     dataType,
		Nullable: nullable,
		Metadata: metadata,
	}
}

// getArrowTypeAndBuilder return Arrow type and corresponding builder based on Go value
func getArrowTypeAndBuilder(mem memory.Allocator, value interface{}, fieldName string, fieldIDGen *fieldIDGenerator) (arrow.Field, array.Builder) {
	// explicitly handle nil value: return Null type
	// note: since convertBindingsToArrowBinary will scan all rows to find the first non-nil value,
	// only when all values in a column are nil will it reach here
	if value == nil {
		return makeFieldWithID(fieldName, arrow.Null, true, fieldIDGen.next()), array.NewNullBuilder(mem)
	}

	switch v := value.(type) {
	case bool:
		return makeFieldWithID(fieldName, arrow.FixedWidthTypes.Boolean, true, fieldIDGen.next()), array.NewBooleanBuilder(mem)
	case int8:
		return makeFieldWithID(fieldName, arrow.PrimitiveTypes.Int8, true, fieldIDGen.next()), array.NewInt8Builder(mem)
	case int16:
		return makeFieldWithID(fieldName, arrow.PrimitiveTypes.Int16, true, fieldIDGen.next()), array.NewInt16Builder(mem)
	case int32:
		return makeFieldWithID(fieldName, arrow.PrimitiveTypes.Int32, true, fieldIDGen.next()), array.NewInt32Builder(mem)
	case int, int64:
		return makeFieldWithID(fieldName, arrow.PrimitiveTypes.Int64, true, fieldIDGen.next()), array.NewInt64Builder(mem)
	case uint8:
		return makeFieldWithID(fieldName, arrow.PrimitiveTypes.Uint8, true, fieldIDGen.next()), array.NewUint8Builder(mem)
	case uint16:
		return makeFieldWithID(fieldName, arrow.PrimitiveTypes.Uint16, true, fieldIDGen.next()), array.NewUint16Builder(mem)
	case uint32:
		return makeFieldWithID(fieldName, arrow.PrimitiveTypes.Uint32, true, fieldIDGen.next()), array.NewUint32Builder(mem)
	case uint64:
		// uint64 max value is 18446744073709551615 (20 digits), use decimal(20, 0)
		decType := &arrow.Decimal128Type{Precision: 20, Scale: 0}
		return makeFieldWithID(fieldName, decType, true, fieldIDGen.next()), array.NewDecimal128Builder(mem, decType)
	case float32:
		return makeFieldWithID(fieldName, arrow.PrimitiveTypes.Float32, true, fieldIDGen.next()), array.NewFloat32Builder(mem)
	case float64:
		return makeFieldWithID(fieldName, arrow.PrimitiveTypes.Float64, true, fieldIDGen.next()), array.NewFloat64Builder(mem)
	case string:
		return makeFieldWithID(fieldName, arrow.BinaryTypes.String, true, fieldIDGen.next()), array.NewStringBuilder(mem)
	case []byte:
		return makeFieldWithID(fieldName, arrow.BinaryTypes.Binary, true, fieldIDGen.next()), array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	case time.Time:
		return makeFieldWithID(fieldName, arrow.FixedWidthTypes.Timestamp_us, true, fieldIDGen.next()), array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond})
	case sql.NullTime:
		return makeFieldWithID(fieldName, arrow.FixedWidthTypes.Timestamp_us, true, fieldIDGen.next()), array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond})
	case decimal.Decimal:
		// infer precision and scale from the decimal value
		precision, scale := inferDecimalPrecisionScale(v)
		decType := &arrow.Decimal128Type{Precision: precision, Scale: scale}
		return makeFieldWithID(fieldName, decType, true, fieldIDGen.next()), array.NewDecimal128Builder(mem, decType)
	case []int, []int64:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.PrimitiveTypes.Int64, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	case []int8:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.PrimitiveTypes.Int8, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.PrimitiveTypes.Int8)
	case []int16:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.PrimitiveTypes.Int16, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.PrimitiveTypes.Int16)
	case []int32:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.PrimitiveTypes.Int32, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	case []uint16:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.PrimitiveTypes.Uint16, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.PrimitiveTypes.Uint16)
	case []uint32:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.PrimitiveTypes.Uint32, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.PrimitiveTypes.Uint32)
	case []uint64:
		listFieldID := fieldIDGen.next()
		decType := &arrow.Decimal128Type{Precision: 20, Scale: 0}
		itemField := makeFieldWithID("item", decType, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, decType)
	case []float32:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.PrimitiveTypes.Float32, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.PrimitiveTypes.Float32)
	case []float64:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.PrimitiveTypes.Float64, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.PrimitiveTypes.Float64)
	case []string:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.BinaryTypes.String, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.BinaryTypes.String)
	case []bool:
		listFieldID := fieldIDGen.next()
		itemField := makeFieldWithID("item", arrow.FixedWidthTypes.Boolean, true, fieldIDGen.next())
		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, arrow.FixedWidthTypes.Boolean)
	case []interface{}:
		// check the type of the first non-nil element
		// assign ID to list field
		listFieldID := fieldIDGen.next()

		// recursively process array element types, element types also need to be assigned field ID
		var itemField arrow.Field
		for _, elem := range v {
			if elem != nil {
				itemField, _ = getArrowTypeAndBuilder(mem, elem, "item", fieldIDGen)
				break
			}
		}
		// if all elements are nil, use Null type
		if itemField.Name == "" {
			itemField = makeFieldWithID("item", arrow.Null, true, fieldIDGen.next())
		}

		listType := arrow.ListOfField(itemField)
		return makeFieldWithID(fieldName, listType, true, listFieldID), array.NewListBuilder(mem, itemField.Type)
	case map[string]interface{}:
		// convert to Arrow Map type
		mapFieldID := fieldIDGen.next()

		// infer value type from the first non-nil value
		var valueType arrow.DataType = arrow.BinaryTypes.String // default to string
		for _, val := range v {
			if val != nil {
				field, builder := getArrowTypeAndBuilder(mem, val, "value", fieldIDGen)
				valueType = field.Type
				builder.Release()
				break
			}
		}

		keyField := arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false}
		valueField := arrow.Field{Name: "value", Type: valueType, Nullable: true}
		mapType := arrow.MapOf(keyField.Type, valueField.Type)
		mapBuilder := array.NewMapBuilder(mem, keyField.Type, valueField.Type, false)
		return makeFieldWithID(fieldName, mapType, true, mapFieldID), mapBuilder
	case map[string]string:
		// convert to Arrow Map<String, String> type
		mapFieldID := fieldIDGen.next()

		mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
		mapBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
		return makeFieldWithID(fieldName, mapType, true, mapFieldID), mapBuilder
	case map[uint64]uint64:
		// convert to Arrow Map<Decimal(20,0), Decimal(20,0)> type
		mapFieldID := fieldIDGen.next()
		decType := &arrow.Decimal128Type{Precision: 20, Scale: 0}

		mapType := arrow.MapOf(decType, decType)
		mapBuilder := array.NewMapBuilder(mem, decType, decType, false)
		return makeFieldWithID(fieldName, mapType, true, mapFieldID), mapBuilder
	default:
		// for unknown type, use String type as fallback (can accept any value, including null)
		// String type is generic, can convert any type using fmt.Sprintf
		return makeFieldWithID(fieldName, arrow.BinaryTypes.String, true, fieldIDGen.next()), array.NewStringBuilder(mem)
	}
}

// appendValue add value to builder (recursively process complex types)
func appendValue(builder array.Builder, value interface{}) {
	if value == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Int8Builder:
		if v, ok := value.(int8); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Int16Builder:
		if v, ok := value.(int16); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Int32Builder:
		if v, ok := value.(int32); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Int64Builder:
		switch v := value.(type) {
		case int64:
			b.Append(v)
		case int:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Uint8Builder:
		if v, ok := value.(uint8); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Uint16Builder:
		if v, ok := value.(uint16); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Uint32Builder:
		if v, ok := value.(uint32); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Uint64Builder:
		if v, ok := value.(uint64); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Float32Builder:
		if v, ok := value.(float32); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Float64Builder:
		if v, ok := value.(float64); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.StringBuilder:
		if v, ok := value.(string); ok {
			b.Append(v)
		} else {
			b.Append(fmt.Sprintf("%v", value))
		}
	case *array.BinaryBuilder:
		if v, ok := value.([]byte); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		if v, ok := value.(time.Time); ok {
			b.Append(arrow.Timestamp(v.UnixMicro()))
		} else if v, ok := value.(sql.NullTime); ok {
			if v.Valid {
				b.Append(arrow.Timestamp(v.Time.UnixMicro()))
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.ListBuilder:
		appendListValue(b, value)
	case *array.MapBuilder:
		appendMapValue(b, value)
	case *array.StructBuilder:
		appendStructValue(b, value)
	case *array.NullBuilder:
		// NullBuilder can only store null values
		// if the value is not nil, only append null (because this column is inferred as all null)
		b.AppendNull()
	case *array.Decimal128Builder:
		appendDecimal128Value(b, value)
	}
}

// appendListValue add array value to ListBuilder (recursively process)
func appendListValue(lb *array.ListBuilder, value interface{}) {
	if value == nil {
		lb.AppendNull()
		return
	}

	lb.Append(true)
	vb := lb.ValueBuilder()

	// use reflection to recursively process any array type
	switch arr := value.(type) {
	case []int:
		for _, v := range arr {
			appendValue(vb, int64(v)) // convert to int64 uniformly
		}
	case []interface{}:
		for _, v := range arr {
			appendValue(vb, v) // recursively process each element
		}
	default:
		// use reflection to process other all array types
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Slice {
			for i := 0; i < rv.Len(); i++ {
				appendValue(vb, rv.Index(i).Interface()) // recursively process each element
			}
		}
	}
}

// appendStructValue add map value to StructBuilder (recursively process fields)
func appendStructValue(sb *array.StructBuilder, value interface{}) {
	if value == nil {
		sb.AppendNull()
		return
	}

	sb.Append(true)
	structType := sb.Type().(*arrow.StructType)

	if m, ok := value.(map[string]interface{}); ok {
		// add values in the order of struct fields
		for i := 0; i < sb.NumField(); i++ {
			fieldBuilder := sb.FieldBuilder(i)
			fieldName := structType.Field(i).Name

			if val, exists := m[fieldName]; exists {
				appendValue(fieldBuilder, val) // recursively process each field
			} else {
				fieldBuilder.AppendNull()
			}
		}
	}
}

// appendMapValue add map value to MapBuilder
func appendMapValue(mb *array.MapBuilder, value interface{}) {
	if value == nil {
		mb.AppendNull()
		return
	}

	mb.Append(true)
	keyBuilder := mb.KeyBuilder()
	itemBuilder := mb.ItemBuilder()

	switch m := value.(type) {
	case map[string]interface{}:
		kb := keyBuilder.(*array.StringBuilder)
		// sort keys for consistent order
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			kb.Append(k)
			appendValue(itemBuilder, m[k])
		}
	case map[string]string:
		kb := keyBuilder.(*array.StringBuilder)
		// sort keys for consistent order
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			kb.Append(k)
			appendValue(itemBuilder, m[k])
		}
	case map[uint64]uint64:
		kb := keyBuilder.(*array.Decimal128Builder)
		ib := itemBuilder.(*array.Decimal128Builder)
		// sort keys for consistent order
		keys := make([]uint64, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

		for _, k := range keys {
			kb.Append(uint64ToDecimal128(k))
			ib.Append(uint64ToDecimal128(m[k]))
		}
	}
}

// appendDecimal128Value appends a decimal or uint64 value to Decimal128Builder
func appendDecimal128Value(b *array.Decimal128Builder, value interface{}) {
	if value == nil {
		b.AppendNull()
		return
	}

	switch v := value.(type) {
	case decimal.Decimal:
		decType := b.Type().(*arrow.Decimal128Type)
		targetScale := decType.Scale

		// scale the decimal to match the target scale
		scaled := v.Shift(targetScale)
		coef := scaled.Coefficient()

		// convert big.Int to decimal128
		dec128 := bigIntToDecimal128(coef)
		b.Append(dec128)
	case uint64:
		// convert uint64 to decimal128
		dec128 := uint64ToDecimal128(v)
		b.Append(dec128)
	default:
		b.AppendNull()
	}
}

// uint64ToDecimal128 converts uint64 to arrow decimal128
func uint64ToDecimal128(v uint64) decimal128.Num {
	return decimal128.New(0, v)
}

// bigIntToDecimal128 converts a big.Int to arrow decimal128
func bigIntToDecimal128(bi *big.Int) decimal128.Num {
	if bi == nil {
		return decimal128.New(0, 0)
	}

	// check if negative
	negative := bi.Sign() < 0
	if negative {
		bi = new(big.Int).Abs(bi)
	}

	// get bytes in big-endian format
	bytes := bi.Bytes()

	// convert to low and high uint64
	var lo, hi uint64

	// process bytes from right to left (big-endian to little-endian conversion)
	for i := len(bytes) - 1; i >= 0; i-- {
		bytePos := len(bytes) - 1 - i
		if bytePos < 8 {
			lo |= uint64(bytes[i]) << (bytePos * 8)
		} else if bytePos < 16 {
			hi |= uint64(bytes[i]) << ((bytePos - 8) * 8)
		}
	}

	result := decimal128.New(int64(hi), lo)
	if negative {
		result = result.Negate()
	}

	return result
}

// inferDecimalPrecisionScale infers precision and scale from a decimal.Decimal value
func inferDecimalPrecisionScale(d decimal.Decimal) (int32, int32) {
	// get the scale (number of decimal places)
	scale := int32(-d.Exponent())
	if scale < 0 {
		scale = 0
	}

	// get the coefficient string to count total digits
	coef := d.Coefficient()
	if coef == nil {
		return 38, scale // default precision
	}

	// count digits in coefficient
	coefStr := coef.String()
	if len(coefStr) > 0 && coefStr[0] == '-' {
		coefStr = coefStr[1:]
	}
	precision := int32(len(coefStr))

	// ensure precision is at least scale + 1
	if precision < scale+1 {
		precision = scale + 1
	}

	// cap at max decimal128 precision (38)
	if precision > 38 {
		precision = 38
	}

	return precision, scale
}
