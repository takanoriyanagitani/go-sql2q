package pgx2q

import (
	"fmt"
	"regexp"
	"strings"
)

type validTableName struct {
	valid string
}

func validTableNameBuilderNew(validator tableValidator) func(uncheckedTableName string) (validTableName, error) {
	return func(uncheckedTableName string) (validTableName, error) {
		e := validator(uncheckedTableName)
		if nil != e {
			return validTableName{}, e
		}
		valid := uncheckedTableName
		return validTableName{valid}, nil
	}
}

type validTablenameBuilder func(uncheckedTableName string) (validTableName, error)

var validTableNameBuilderPostgres validTablenameBuilder = validTableNameBuilderNew(tableValidatorRegexpPostgres)

type tableValidator func(tableName string) error

func regexpTableValidatorNew(r *regexp.Regexp) tableValidator {
	return func(tableName string) error {
		var valid bool = r.MatchString(tableName)
		if valid {
			return nil
		}
		return fmt.Errorf("Invalid table name: %s", tableName)
	}
}

var regexpTablePatternPostgres *regexp.Regexp = regexp.MustCompile(strings.Join([]string{
	`^[a-z]`,           // 1st byte
	`[a-z0-9_]{0,62}$`, // 2nd ~ 63rd byte
}, ""))

var tableValidatorRegexpPostgres tableValidator = regexpTableValidatorNew(regexpTablePatternPostgres)
