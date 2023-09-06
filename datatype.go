package goclickzetta

type clickzettaType int

const (
	BIGINT clickzettaType = iota
	BOOLEAN
	CHAR
	DATE
	DECIMAL
	DOUBLE
	FLOAT
	INT
	INTERVAL
	SMALLINT
	STRING
	TIMESTAMP_LTZ
	TINYINT
	ARRAY
	MAP
	STRUCT
	VARCHAR
	NOT_SUPPORTED
)

var clickzettaTypes = [...]string{"INT64", "BOOLEAN", "CHAR", "DATE", "DECIMAL",
	"FLOAT64", "FLOAT32", "INT32", "INTERVAL", "INT16",
	"STRING", "TIMESTAMP_LTZ", "INT8", "ARRAY", "MAP", "STRUCT", "VARCHAR", "NOT_SUPPORTED"}

func (st clickzettaType) String() string {
	return clickzettaTypes[st]
}

func (st clickzettaType) Byte() byte {
	return byte(st)
}

func getclickzettaType(typ string) clickzettaType {
	for i, sft := range clickzettaTypes {
		if sft == typ {
			return clickzettaType(i)
		}
	}
	return NOT_SUPPORTED
}
