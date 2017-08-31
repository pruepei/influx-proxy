package backend

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/influxdata/influxdb/influxql"
)

// InfluxQLGenerator is aim to generate a series new QL
// the new QL organize with ;
func InfluxQLGenerator(oldQL string) (newQL string, err error) {
	// firstly, to check SHOW query
	if isSHOW, _ := regexp.MatchString("^SHOW|^show", oldQL); isSHOW {
		return oldQL, nil
	}

	pg, err := influxql.NewParser(strings.NewReader(oldQL)).ParseQuery()
	if err != nil {
		return "", err
	}

	// secondly, to re-generator the SELECT statement
	var selelctStringSlice []string
	for _, stmt := range pg.Statements {
		if selectStatement, ok := stmt.(*influxql.SelectStatement); ok {
			a := fmt.Sprintf("%s", influxQLSelectGenerator(selectStatement))
			selelctStringSlice = append(selelctStringSlice, a)
		}
	}
	newQL = fmt.Sprint(strings.Join(selelctStringSlice[:], "; "))
	return newQL, nil
}

func influxQLSelectGenerator(oldOneQL *influxql.SelectStatement) (newOneQL *influxql.SelectStatement) {
	newOneQL = oldOneQL.Clone()
	for _, filed := range newOneQL.Fields {
		switch expr := filed.Expr.(type) {
		case *influxql.Call:
			if expr.Name == "mean" {
				log.Printf("oldQL: %s\n", oldOneQL)
				expr.Name = "sum"
				var newField influxql.Field
				newField.Expr = &influxql.Call{Name: "count", Args: expr.Args}
				newOneQL.Fields = append(newOneQL.Fields, &newField)
				log.Printf("newQL: %s\n", newOneQL)
			}
		}
	}
	return newOneQL
}
