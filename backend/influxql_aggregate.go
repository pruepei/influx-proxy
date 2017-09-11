package backend

import (
	"encoding/json"

	"log"

	"github.com/influxdata/influxdb/influxql"
)

// InfluxQLAggregate is aim to re-organize the result of basic query statement
func InfluxQLAggregate(resultFromBackends [][]byte) (resultAggr []byte, err error) {

	// []byte -> influxql.Result -> models.Row
	for _, resultFromBackend := range resultFromBackends {
		result, err := unmarshalJSON(resultFromBackend)
		if err != nil {
			log.Println(err)
			break
		}
		for _, r := range result {
			for _, row := range r.Series {
				log.Println(row.Columns)
				log.Println(row.Name)
				log.Println(row.Tags)
				log.Println(row.Values)
			}
		}
	}
	//
	// 2: 聚合策略 regenerator 来获取

	return resultAggr, nil
}

// UnmarshalJSON decodes the data from http response to influxql.Result.
func unmarshalJSON(b []byte) ([]*influxql.Result, error) {
	var o struct {
		Results []*influxql.Result `json:"results,omitempty"`
		Err     string             `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return nil, err
	}
	return o.Results, nil
}
