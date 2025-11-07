package goclickzetta

import (
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
)

func (dialector *Dialector) Create(db *gorm.DB) {
	if db.Error == nil {
		if db.Statement.Schema != nil && !db.Statement.Unscoped {
			for _, c := range db.Statement.Schema.CreateClauses {
				db.Statement.AddClause(c)
			}
		}

		if db.Statement.SQL.String() == "" {
			db.Statement.SQL.Grow(180)
			db.Statement.AddClauseIfNotExists(clause.Insert{})

			if values := callbacks.ConvertToCreateValues(db.Statement); len(values.Values) >= 1 {
				prepareValues := clause.Values{
					Columns: values.Columns,
					Values:  [][]interface{}{},
				}
				db.Statement.AddClause(prepareValues)
				db.Statement.Build("INSERT", "VALUES", "ON CONFLICT")

				sql := db.Statement.SQL.String()
				i := strings.LastIndex(sql, "VALUES")
				if i == -1 {
					i = len(sql)
				}
				sql = sql[:i] + "using arrow" + sql[i+len("VALUES"):]
				_, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, sql, values.Values)
				if db.AddError(err) != nil {
					return
				}

				return
			}
		}

		if !db.DryRun && db.Error == nil {
			result, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)

			if db.Statement.Result != nil {
				db.Statement.Result.Result = result
			}
			db.AddError(err)
		}
	}
}
