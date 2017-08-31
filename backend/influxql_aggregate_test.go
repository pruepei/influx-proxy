package backend

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/influxql"
)

func TestInfluxQLAggregate(t *testing.T) {

	// curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=NOAA_water_database" \
	// --data-urlencode 'q=SELECT MEAN("water_level") FROM "h2o_feet"'
	var mean = []byte(`{
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "name": "h2o_feet",
                    "columns": [
                        "time",
                        "mean"
                    ],
                    "values": [
                        [
                            "1970-01-01T00:00:00Z",
                            4.442107025822522
                        ]
                    ]
                }
            ]
        }
    ]
}`)
	// curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=NOAA_water_database" \
	// --data-urlencode 'q=SELECT SUM("water_level") FROM "h2o_feet"'
	var sum = []byte(`{
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "name": "h2o_feet",
                    "columns": [
                        "time",
                        "sum"
                    ],
                    "values": [
                        [
                            "1970-01-01T00:00:00Z",
                            67777.66900000004
                        ]
                    ]
                }
            ]
        }
    ]
}`)

	// curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=NOAA_water_database" \
	// --data-urlencode 'q=SELECT COUNT("water_level") FROM "h2o_feet"'
	var count = []byte(`{
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "name": "h2o_feet",
                    "columns": [
                        "time",
                        "count"
                    ],
                    "values": [
                        [
                            "1970-01-01T00:00:00Z",
                            15258
                        ]
                    ]
                }
            ]
        }
    ]
}`)
	results := make([]influxql.Result, 2)
	results[0].UnmarshalJSON(count)
	results[1].UnmarshalJSON(sum)

	answerMid, _ := InfluxQLAggregate(results, 21)
	answ, _ := answerMid.MarshalJSON()
	if bytes.Equal(answ, mean) {
		t.Error("error in Aggregate")
	}
}
