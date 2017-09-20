// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: ping.liu

package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
)

var (
	ForbidCmds         = "(?i:select\\s+\\*|^\\s*delete|^\\s*drop|^\\s*grant|^\\s*revoke|\\(\\)\\$)"
	SupportCmds        = "(?i:where.*time|show.*from)"
	ExecutorCmds       = "(?i:show.*measurements)"
	ErrNotClusterQuery = errors.New("not a cluster query")
)

type influxQL string

type InfluxQueryContext struct {
	SourceQL        string
	TargetQL        string
	SourceResults   [][]byte // chan?
	TargetResult    []byte
	aggregatePolicy influxql.Fields
	errors          error
}

// NewQueryContext aim to create a new InfluxQueryContext struct
func NewQueryContext(source string) *InfluxQueryContext {
	return &InfluxQueryContext{
		SourceQL: source,
		TargetQL: "",
	}
}

// re-generate SQL
func (qc *InfluxQueryContext) QLGenerator() {

	// show query
	if isSHOW, _ := regexp.MatchString("^SHOW|^show", qc.SourceQL); isSHOW {
		qc.TargetQL = qc.SourceQL
		return
	}

	// to parse
	pg, err := influxql.NewParser(strings.NewReader(qc.SourceQL)).ParseQuery()
	if err != nil {
		qc.errors = err
	}

	// select query
	for _, stmt := range pg.Statements {
		if selectStatement, ok := stmt.(*influxql.SelectStatement); ok {
			qc.TargetQL, qc.aggregatePolicy = helperQLSelectGenerator(selectStatement)
		}
	}
}

func helperQLSelectGenerator(sourceQL *influxql.SelectStatement) (targetQL string, aggregatePolicy influxql.Fields) {
	newQL := sourceQL.Clone()
	for _, filed := range newQL.Fields {
		switch expr := filed.Expr.(type) {

		// convert mean(f1) -> sum(f1), count(f1)
		case *influxql.Call:
			if expr.Name == "mean" {
				expr.Name = "sum"
				var newField influxql.Field
				newField.Expr = &influxql.Call{Name: "count", Args: expr.Args}
				newQL.Fields = append(newQL.Fields, &newField)
			}
		}
	}
	return fmt.Sprintf("%s", newQL), sourceQL.Fields
}

// Aggregate aim to re-process result from backend
func (qc *InfluxQueryContext) Aggregate() {
	for _, resultFromBackend := range qc.SourceResults {
		result, _ := unmarshalJSON(resultFromBackend)
		for _, r := range result {
			helperCacheMiddleResult(r)
		}
	}
}

func helperCacheMiddleResult(r *query.Result) {
	//fieldvalue := make(map[string]string)
	for _, row := range r.Series {
		for _, value := range row.Values[0] {
			fmt.Printf("%s\n", reflect.TypeOf(value))
		}
	}
	//log.Printf("%s", fieldvalue)
	//return fieldvalue
}

// UnmarshalJSON decode http response -> influxql.Result.
func unmarshalJSON(b []byte) ([]*query.Result, error) {
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}
	err := json.Unmarshal(b, &o)
	if err != nil {
		return nil, err
	}
	return o.Results, nil
}

type InfluxQLExecutor struct {
}

func (iqe *InfluxQLExecutor) Query(w http.ResponseWriter, req *http.Request) (res []byte, err error) {
	q := strings.TrimSpace(req.FormValue("q"))
	// better way??
	matched, err := regexp.MatchString(ExecutorCmds, q)
	if err != nil || !matched {
		return nil, ErrNotClusterQuery
	}

	w.WriteHeader(200)
	w.Write([]byte(""))

	return
}
