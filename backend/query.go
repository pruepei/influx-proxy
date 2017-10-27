// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: ping.liu

package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
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

type mapFieldValue map[string]interface{}
type mapFieldVaules map[string][]interface{}

// InfluxQueryContext field name
//
//			       / backend
// 			      /
//  client --> inflxdb-proxy -- backend
//    			      \
//    			       \ backend
//
//  client -- sourceQL --> inflxdb-proxy -- targetQL --> backend
//  backend --  sourceResults --> influxdb-proxy -- targetResult --> client
//
type InfluxQueryContext struct {
	SourceQL      string
	TargetQL      string
	SourceResults [][]byte
	TargetResult  []byte
	sourceFields  influxql.Fields
	targetFields  influxql.Fields
	sourceDatas   []mapFieldVaules
	targetDatas   []mapFieldValue
	errors        error
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
			qc.TargetQL, qc.targetFields, qc.sourceFields = helperQLSelectGenerator(selectStatement)
		}
	}
}

func helperQLSelectGenerator(sourceQL *influxql.SelectStatement) (targetQL string, targetFields influxql.Fields, sourceFields influxql.Fields) {
	newQL := sourceQL.Clone()
	for _, field := range newQL.Fields {
		switch expr := field.Expr.(type) {

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
	return fmt.Sprintf("%s", newQL), newQL.Fields, sourceQL.Fields
}

// Aggregate aim to re-process result from backend
func (qc *InfluxQueryContext) Aggregate() {
	// copy a sourceResult for targetResult
	qc.TargetResult = qc.SourceResults[0]
	targetResult, _ := unmarshalHttpResponse(qc.TargetResult)

	// serialization of per-backend result
	// as: [map[field(column):[value1, value2, value3]], map[field(column):[value1, value2, value3]]...]
	for fieldIndex, field := range qc.targetFields {
		var values []interface{}
		for _, sourceResult := range qc.SourceResults {
			result, _ := unmarshalHttpResponse(sourceResult)
			for _, r := range result {
				values = append(values, r.Series[0].Values[0][fieldIndex+1])
			}
		}
		m := make(mapFieldVaules)
		m[fmt.Sprint(field)] = values
		qc.sourceDatas = append(qc.sourceDatas, m)
	}

	// do max, min, count, sum, mean
	// and keep the result to qc.targetDatas
	for fieldOffset, field := range qc.sourceFields {
		switch expr := field.Expr.(type) {
		case *influxql.Call:
			var mapKVS []mapFieldValue
			mapKV := make(mapFieldValue)
			if expr.Name == "max" {
				for _, fieldvalues := range qc.sourceDatas {
					if values, ok := fieldvalues[fmt.Sprint(field)]; ok {
						mapKV[fmt.Sprint(field)] = getmax(values)
						mapKVS = append(mapKVS, mapKV)
					}
				}

			}
			if expr.Name == "min" {
				for _, fieldvalues := range qc.sourceDatas {
					if values, ok := fieldvalues[fmt.Sprint(field)]; ok {
						mapKV[fmt.Sprint(field)] = getmin(values)
						mapKVS = append(mapKVS, mapKV)
					}
				}
			}

			if expr.Name == "count" || expr.Name == "sum" {
				for _, fieldvalues := range qc.sourceDatas {
					if values, ok := fieldvalues[fmt.Sprint(field)]; ok {
						mapKV[fmt.Sprint(field)] = getsum(values)
						mapKVS = append(mapKVS, mapKV)
					}
				}
			}
			if expr.Name == "mean" {
				countExpr := &influxql.Call{Name: "count", Args: expr.Args}
				sumExpr := &influxql.Call{Name: "sum", Args: expr.Args}
				var valueCount []interface{}
				var valueSum []interface{}
				for i, fieldvalues := range qc.sourceDatas {

					// delete the column, value of SUM()
					if values, ok := fieldvalues[fmt.Sprint(sumExpr)]; ok {
						valueSum = values
						qc.sourceDatas = append(qc.sourceDatas[:i], qc.sourceDatas[i+1:]...)
						targetResult[0].Series[0].Columns = append(targetResult[0].Series[0].Columns[:i+1], targetResult[0].Series[0].Columns[i+2:]...)
						targetResult[0].Series[0].Values[0] = append(targetResult[0].Series[0].Values[0][:i+1], targetResult[0].Series[0].Values[0][i+2:]...)
					}

					// didn't delete the value of COUNT(), but replace the name with `mean`
					if values, ok := fieldvalues[fmt.Sprint(countExpr)]; ok {
						valueCount = values
						qc.sourceDatas[fieldOffset] = mapFieldVaules{fmt.Sprint(field): values}
						// a bug if more mean aggregate with one SQL
						targetResult[0].Series[0].Columns[fieldOffset+1] = "mean"
					}

				}
				mapKV[fmt.Sprint(field)] = getmean(valueSum, valueCount)
				mapKVS = append(mapKVS, mapKV)
			}
			qc.targetDatas = mapKVS
		}
	}

	// create a new result by qc.targetDatas and sourceResult
	for i, f := range qc.sourceFields {
		for k, v := range qc.targetDatas[i] {
			if fmt.Sprint(f) == fmt.Sprint(k) {
				log.Println(v)
				log.Println(targetResult[0].Series[0].Values[0][i+1])
				targetResult[0].Series[0].Values[0][i+1] = v
			}
		}
	}
	qc.TargetResult, _ = marshalHttpResponse(targetResult)
}

// unmarshalHttpResponse aim decode http response -> influxql.Results.
func unmarshalHttpResponse(response []byte) ([]*query.Result, error) {
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}
	err := json.Unmarshal(response, &o)
	if err != nil {
		return nil, err
	}
	return o.Results, nil
}

// unmarshalHttpResponse aim decode http response -> influxql.Results.
func marshalHttpResponse(r []*query.Result) (response []byte, e error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r

	return json.Marshal(&o)
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
