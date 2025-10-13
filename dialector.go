package goclickzetta

import (
	"database/sql"
	"fmt"
	"math"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

const (
	DefaultDriverName = "clickzetta"
)

type ClickZettaConfig struct {
	DriverName                    string
	DSN                           string
	Conn                          gorm.ConnPool
	SkipInitializeWithVersion     bool
	DefaultStringSize             uint
	DefaultDatetimePrecision      *int
	DisableWithReturning          bool
	DisableDatetimePrecision      bool
	DontSupportRenameIndex        bool
	DontSupportRenameColumn       bool
	DontSupportForShareClause     bool
	DontSupportNullAsDefaultValue bool
	DontSupportRenameColumnUnique bool
	DontSupportDropConstraint     bool
}

type Dialector struct {
	*ClickZettaConfig
}

var (
	// CreateClauses create clauses
	CreateClauses = []string{"INSERT", "VALUES"}
	// QueryClauses query clauses
	QueryClauses = []string{}
	// UpdateClauses update clauses
	UpdateClauses = []string{"UPDATE", "SET", "WHERE"}
	// DeleteClauses delete clauses
	DeleteClauses = []string{"DELETE", "FROM", "WHERE"}

	defaultDatetimePrecision = 3
)

func Open(dsn string) gorm.Dialector {
	return &Dialector{ClickZettaConfig: &ClickZettaConfig{DSN: dsn}}
}

func (dialector Dialector) Name() string {
	return DefaultDriverName
}

func (dialector Dialector) NowFunc(n int) func() time.Time {
	return func() time.Time {
		round := time.Second / time.Duration(math.Pow10(n))
		return time.Now().Round(round)
	}
}

func (dialector Dialector) Apply(config *gorm.Config) error {
	if config.NowFunc != nil {
		return nil
	}

	if dialector.DefaultDatetimePrecision == nil {
		dialector.DefaultDatetimePrecision = &defaultDatetimePrecision
	}
	// while maintaining the readability of the code, separate the business logic from
	// the general part and leave it to the function to do it here.
	config.NowFunc = dialector.NowFunc(*dialector.DefaultDatetimePrecision)
	return nil
}

// DataTypeOf implements gorm.Dialector.
func (dialector Dialector) DataTypeOf(*schema.Field) string {
	panic("unimplemented")
}

// DefaultValueOf implements gorm.Dialector.
func (dialector Dialector) DefaultValueOf(*schema.Field) clause.Expression {
	panic("unimplemented")
}

// Migrator implements gorm.Dialector.
func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	panic("unimplemented")
}

func (dialector Dialector) Initialize(db *gorm.DB) error {
	// register callbacks
	callbackConfig := &callbacks.Config{
		QueryClauses:  QueryClauses,
		UpdateClauses: UpdateClauses,
		DeleteClauses: DeleteClauses,
	}

	callbacks.RegisterDefaultCallbacks(db, callbackConfig)

	db.Callback().Create().Replace("gorm:create", dialector.Create)

	if dialector.DriverName == "" {
		dialector.DriverName = DefaultDriverName
	}

	if dialector.DefaultDatetimePrecision == nil {
		dialector.DefaultDatetimePrecision = &defaultDatetimePrecision
	}

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		var err error
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.DSN)
		if err != nil {
			return err
		}
	}

	// for k, v := range dialector.ClauseBuilders() {
	// 	db.ClauseBuilders[k] = v
	// }
	return nil
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	writer.WriteString(str)
	writer.WriteByte('`')
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return fmt.Sprintf("SQL: %s, Vars: %v", sql, vars)
}
