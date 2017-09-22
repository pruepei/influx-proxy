package backend

import (
	"bytes"
	"testing"
)

func TestInfluxQLGenerator(t *testing.T) {
	tests := []struct {
		source string
		target string
	}{
		// SELECT:
		{
			source: `SELECT * FROM NOAA_water_database..s1`,
			target: `SELECT * FROM NOAA_water_database..s1`,
		},
		// sum, count, max, min, OPERATION:
		{
			source: `SELECT sum(f0) FROM s1`,
			target: `SELECT sum(f0) FROM s1`,
		},
		{
			source: `SELECT count(f0) FROM s1`,
			target: `SELECT count(f0) FROM s1`,
		},
		{
			source: `SELECT min(f0) FROM s1`,
			target: `SELECT min(f0) FROM s1`,
		},
		{
			source: `SELECT max(f0) FROM s1`,
			target: `SELECT max(f0) FROM s1`,
		},
		// mean:
		{
			source: `SELECT mean(f0) FROM s1`,
			target: `SELECT sum(f0), count(f0) FROM s1`,
		},
		{
			source: `SELECT mean(f0), mean(f1), mean(f2), max(f3) FROM s1`,
			target: `SELECT sum(f0), sum(f1), sum(f2), max(f3), count(f0), count(f1), count(f2) FROM s1`,
		},
		// GROUP BY can't work with mean
		// {
		// 	source: `SELECT mean(f0) FROM s1 GROUP BY location`,
		// 	target:   `Select sum(f0) FROM s1 GROUP BY location;SELECT count(f0) FROM s1 GROUP BY location`,
		// },
		// {
		// 	source: `SELECT mean(index) FROM h2o_quality GROUP BY *`,
		// 	target:   `SELECT sum(index) FROM h2o_quality GROUP BY *; SELECT count(index) FROM h2o_quality GROUP BY *`,
		// },
		// {
		// 	source: `SELECT count(f0) FROM s1 WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),location`,
		// 	target:   `SELECT count(f0) FROM s1 WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),location`,
		// },
		// MATHS EXPRESSION:
		{
			source: `SELECT (f0 * 2) + 4 FROM s1`,
			target: `SELECT (f0 * 2) + 4 FROM s1`,
		},
		//LIMIT:
		{
			source: `SELECT f0, location FROM s1 LIMIT 3`,
			target: `SELECT f0, location FROM s1 LIMIT 3`,
		},
		// Regular Expressions
		{
			source: `SELECT mean(f1) FROM /s2/`,
			target: `SELECT sum(f1), count(f1) FROM /s2/`,
		},
		{
			source: `SELECT mean(f0) FROM s1 WHERE location =~ /[m]/ AND f0 > 3`,
			target: `SELECT sum(f0), count(f0) FROM s1 WHERE location =~ /[m]/ AND f0 > 3`,
		},
		// Subqueries
		// {
		// 	source: `SELECT sum("max") FROM (SELECT max("f0") FROM "s1" GROUP BY "location")`,
		// 	target:   `SELECT max("f0") FROM "s1" GROUP BY "location"`,
		// },
		// show
		{
			source: `SHOW MEASUREMENTS`,
			target: `SHOW MEASUREMENTS`,
		},
	}

	for _, test := range tests {
		qc1 := NewQueryContext(test.source)
		qc1.QLGenerator()
		if test.target != qc1.TargetQL {
			t.Error(qc1.SourceQL)
			t.Error(qc1.TargetQL)
		}
	}
}

func TestInfluxQLAggregate(t *testing.T) {
	tests := []struct {
		sourceql      string
		sourceResults [][]byte
		targetResult  []byte
	}{
		{
			sourceql:      "SELECT mean(water_level) FROM h2o_feet",
			sourceResults: [][]byte{[]byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","sum","count"],"values":[[0,67777.66900000004,15258]]}]}]}`), []byte(``)},
			targetResult:  []byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","mean"],"values":[[0,4.442107025822522]]}]}]}`),
		},
		{
			sourceql:      "SELECT max(water_level) FROM h2o_feet",
			sourceResults: [][]byte{[]byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","max"],"values":[["2015-08-29T07:24:00Z",9.954]]}]}]}`), []byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","max"],"values":[["2015-08-29T07:24:00Z",9.964]]}]}]}`)},
			targetResult:  []byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","max"],"values":[["2015-08-29T07:24:00Z",9.964]]}]}]}`),
		},
		{
			sourceql:      "SELECT min(water_level) FROM h2o_feet",
			sourceResults: [][]byte{[]byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","min"],"values":[["2015-08-29T14:30:00Z",-0.61]]}]}]}`), []byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","min"],"values":[["2015-08-29T14:30:00Z",-0.62]]}]}]}`)},
			targetResult:  []byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","min"],"values":[["2015-08-29T14:30:00Z",-0.62]]}]}]}`),
		},
		{
			sourceql:      "SELECT sum(water_level) FROM h2o_feet",
			sourceResults: [][]byte{[]byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",67777.66900000004]]}]}]}`), []byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",67777.66900000004]]}]}]}`)},
			targetResult:  []byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",135555.33800000008]]}]}]}`),
		},
		{
			sourceql:      "SELECT count(water_level) FROM h2o_feet",
			sourceResults: [][]byte{[]byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",15258]]}]}]}`), []byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",15258]]}]}]}`)},
			targetResult:  []byte(`{"results":[{"statement_id":0,"series":[{"name":"h2o_feet","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",30516]]}]}]}`),
		},
	}
	for _, test := range tests {
		qc := &InfluxQueryContext{
			SourceQL:      test.sourceql,
			SourceResults: test.sourceResults,
		}
		qc.QLGenerator()
		qc.Aggregate()
		if !bytes.Equal(qc.TargetResult, test.targetResult) {
			t.Errorf("===")
			t.Errorf("%s", qc.SourceQL)
			t.Errorf("%s", qc.TargetQL)
			t.Errorf("%s", qc.SourceResults)
			t.Errorf("%s", qc.TargetResult)
			t.Errorf("%s", test.targetResult)
		}
	}
}
