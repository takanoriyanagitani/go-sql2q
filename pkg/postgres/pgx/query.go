package pgx2q

import (
	"strings"
	"text/template"
)

func tmplNewMust(name string) func(tmpl string) *template.Template {
	return func(tmpl string) *template.Template {
		var t *template.Template = template.New(name)
		return template.Must(t.Parse(tmpl))
	}
}

var createTable *template.Template = tmplNewMust("create")(`
    CREATE TABLE IF NOT EXISTS {{.tableName}} (
        id BIGSERIAL,
        dt BYTEA,
        CONSTRAINT {{.tableName}}_pkc PRIMARY KEY(id)
    )
`)

var addQueue *template.Template = tmplNewMust("add")(`
    INSERT INTO {{.tableName}}(dt)
    VALUES($1)
`)

var getQueue *template.Template = tmplNewMust("get")(`
    SELECT
        id,
        dt
    FROM {{.tableName}}
    ORDER BY id
    LIMIT 1
`)

var delQueue *template.Template = tmplNewMust("del")(`
    DELETE FROM {{.tableName}}
    WHERE id=$1
`)

var cntQueue *template.Template = tmplNewMust("cnt")(`
    SELECT COUNT(*) FROM {{.tableName}}
`)

type builtQuery struct {
	txt string
	err error
}

type queryBuilder func(tableName validTableName) builtQuery

func queryBuilderNew(tmpl *template.Template) queryBuilder {
	return func(tableName validTableName) builtQuery {
		var buf strings.Builder
		e := tmpl.Execute(&buf, map[string]string{"tableName": tableName.valid})
		return builtQuery{
			txt: buf.String(),
			err: e,
		}
	}
}

var createTab queryBuilder = queryBuilderNew(createTable)

var createAdd queryBuilder = queryBuilderNew(addQueue)
var createGet queryBuilder = queryBuilderNew(getQueue)
var createDel queryBuilder = queryBuilderNew(delQueue)
var createCnt queryBuilder = queryBuilderNew(cntQueue)
