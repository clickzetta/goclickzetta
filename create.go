package goclickzetta

import (
	"encoding/json"
	"fmt"
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
				db.Statement.Build("INSERT", "VALUES")

				allParts := make([]string, 0, len(values.Values))
				for _, value := range values.Values {
					// value is typically []interface{} (one row). If it's a slice, stringify it
					parts := make([]string, 0, len(value))
					for _, v := range value {
						b, err := json.Marshal(v)
						if err != nil {
							return
						}
						parts = append(parts, string(b))
					}
					row := fmt.Sprintf("(%s)", strings.Join(parts, ","))
					allParts = append(allParts, row)
				}
				allRows := strings.Join(allParts, ",")

				sql := db.Statement.SQL.String() + " " + allRows

				result, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, sql)

				if db.Statement.Result != nil {
					db.Statement.Result.Result = result
				}
				db.AddError(err)
				return
			}
		}
	}
}
