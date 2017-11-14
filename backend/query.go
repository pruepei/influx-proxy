// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
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
//
type InfluxQueryContext struct {
	ic            *InfluxCluster
	nodes         [][]BackendAPI
	sourceQL      *influxql.Query
	TargetQL      string
	sourceResults chan *Result
	TargetResult  []byte
	sourceFields  influxql.Fields
	targetFields  influxql.Fields
	errors        error
}

// NewQueryContext aim to create a new InfluxQueryContext struct
func NewQueryContext(source string, nodes [][]BackendAPI, ic *InfluxCluster) (*InfluxQueryContext, error) {
	pg, err := influxql.NewParser(strings.NewReader(source)).ParseQuery()
	if err != nil {
		return nil, err
	}
	return &InfluxQueryContext{
		ic:       ic,
		nodes:    nodes,
		sourceQL: pg,
		TargetQL: "",
	}, nil
}

func (qc *InfluxQueryContext) Commit(w http.ResponseWriter, req *http.Request) error {
	result := make(chan *Result)
	defer close(result)

	// no shard
	if len(qc.nodes) == 1 {
		go qc.executeQuery(w, req, qc.nodes[0], result)
		res := <-result
		if res.Err == nil {
			w.WriteHeader(200)
			w.Write(res.Res)
			return nil
		}
		w.WriteHeader(400)
		w.Write([]byte("query error"))
		return res.Err
	}

	// with shard
	qc.qlGenerator()
	for _, node := range qc.nodes {
		go qc.executeQuery(w, req, node, result)
	}
	finalizedResult, err := qc.aggregate(result)
	if err == nil {
		w.WriteHeader(200)
		w.Write(finalizedResult)
		return nil
	}
	w.WriteHeader(400)
	w.Write([]byte("query error"))
	return err
}

func (qc *InfluxQueryContext) executeQuery(w http.ResponseWriter, req *http.Request, apis []BackendAPI, result chan *Result) {
	var res []byte
	var err error
	// same zone
	for _, api := range apis {
		if api.GetZone() != qc.ic.Zone {
			continue
		}
		if !api.IsActive() || api.IsWriteOnly() {
			continue
		}
		res, err = api.Query(w, req)
		if err == nil {
			result <- &Result{Res: res}
			return
		}
	}

	// difference zone
	for _, api := range apis {
		if api.GetZone() == qc.ic.Zone {
			continue
		}
		if !api.IsActive() || api.IsWriteOnly() {
			continue
		}
		res, err = api.Query(w, req)
		if err == nil {
			result <- &Result{Res: res}
			return
		}
	}

	result <- &Result{Err: errors.New("query error")}
	atomic.AddInt64(&qc.ic.stats.QueryRequestsFail, 1)
}

// re-generate SQL
func (qc *InfluxQueryContext) qlGenerator() {

	for _, stmt := range qc.sourceQL.Statements {
		// select query
		if selectStatement, ok := stmt.(*influxql.SelectStatement); ok {
			qc.TargetQL = selectStatement.String()
			qc.targetFields = selectStatement.Fields
			qc.sourceFields = selectStatement.Fields
		}
		// show field keys
		if showStatement, ok := stmt.(*influxql.ShowFieldKeysStatement); ok {
			qc.TargetQL = showStatement.String()
		}
		// show tag keys
		if showStatement, ok := stmt.(*influxql.ShowTagKeysStatement); ok {
			qc.TargetQL = showStatement.String()
		}
		// show tag value
		if showStatement, ok := stmt.(*influxql.ShowTagValuesStatement); ok {
			qc.TargetQL = showStatement.String()
		}
	}
}

// Aggregate aim to re-process result from backend
func (qc *InfluxQueryContext) aggregate(sourceResults chan *Result) (result []byte, err error) {

	var sourceresults []*Result
	for sourceResult := range sourceResults {
		sourceresults = append(sourceresults, sourceResult)
	}

	TargetResult := sourceresults[0].Res
	targetResult, _ := unmarshalHTTPResponse(TargetResult)

	// serialization of per-backend result
	// as: [map[field(column):[value1, value2, value3]], map[field(column):[value1, value2, value3]]...]
	var sourceDatas []mapFieldVaules
	for fieldIndex, field := range qc.targetFields {
		var values []interface{}
		for _, sourceResult := range sourceresults {
			result, _ := unmarshalHTTPResponse(sourceResult.Res)
			for _, r := range result {
				values = append(values, r.Series[0].Values[0][fieldIndex+1])
			}
		}
		m := make(mapFieldVaules)
		m[fmt.Sprint(field)] = values
		sourceDatas = append(sourceDatas, m)
	}

	var targetDatas []mapFieldValue
	// do max, min, count, sum, mean
	// and keep the result to qc.targetDatas
	for fieldOffset, field := range qc.sourceFields {
		switch expr := field.Expr.(type) {
		case *influxql.Call:
			var mapKVS []mapFieldValue
			mapKV := make(mapFieldValue)
			if expr.Name == "max" {
				for _, fieldvalues := range sourceDatas {
					if values, ok := fieldvalues[fmt.Sprint(field)]; ok {
						mapKV[fmt.Sprint(field)] = getmax(values)
						mapKVS = append(mapKVS, mapKV)
					}
				}

			}
			if expr.Name == "min" {
				for _, fieldvalues := range sourceDatas {
					if values, ok := fieldvalues[fmt.Sprint(field)]; ok {
						mapKV[fmt.Sprint(field)] = getmin(values)
						mapKVS = append(mapKVS, mapKV)
					}
				}
			}

			if expr.Name == "count" || expr.Name == "sum" {
				for _, fieldvalues := range sourceDatas {
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
				for i, fieldvalues := range sourceDatas {

					// delete the column, value of SUM()
					if values, ok := fieldvalues[fmt.Sprint(sumExpr)]; ok {
						valueSum = values
						sourceDatas = append(sourceDatas[:i], sourceDatas[i+1:]...)
						targetResult[0].Series[0].Columns = append(targetResult[0].Series[0].Columns[:i+1], targetResult[0].Series[0].Columns[i+2:]...)
						targetResult[0].Series[0].Values[0] = append(targetResult[0].Series[0].Values[0][:i+1], targetResult[0].Series[0].Values[0][i+2:]...)
					}

					// didn't delete the value of COUNT(), but replace the name with `mean`
					if values, ok := fieldvalues[fmt.Sprint(countExpr)]; ok {
						valueCount = values
						sourceDatas[fieldOffset] = mapFieldVaules{fmt.Sprint(field): values}
						// a bug if more mean aggregate with one SQL
						targetResult[0].Series[0].Columns[fieldOffset+1] = "mean"
					}

				}
				mapKV[fmt.Sprint(field)] = getmean(valueSum, valueCount)
				mapKVS = append(mapKVS, mapKV)
			}
			targetDatas = mapKVS
		}
	}

	// create a new result by qc.targetDatas and sourceResult
	for i, f := range qc.sourceFields {
		for k, v := range targetDatas[i] {
			if fmt.Sprint(f) == fmt.Sprint(k) {
				log.Println(v)
				log.Println(targetResult[0].Series[0].Values[0][i+1])
				targetResult[0].Series[0].Values[0][i+1] = v
			}
		}
	}
	TargetResult, _ = marshalHTTPResponse(targetResult)
	return TargetResult, nil
}

// unmarshalHTTPResponse aim to decode http response -> influxql.Results.
func unmarshalHTTPResponse(response []byte) ([]*query.Result, error) {
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

// marshalHTTPResponse aim to encode influxql.Results -> http response .
func marshalHTTPResponse(r []*query.Result) (response []byte, e error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r

	return json.Marshal(&o)
}
