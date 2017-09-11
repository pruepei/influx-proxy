package backend

import (
	"bytes"
	"testing"
)

func TestInfluxQLAggregate(t *testing.T) {
	tests := []struct {
		source [][]byte
		dest   []byte
	}{
		{
			source: [][]byte{[]byte(`{
  "results": [
    {
      "statement_id": 0,
      "series": [
        {
          "name": "h2o_feet",
          "columns": [
            "time",
            "sum",
            "count"
          ],
          "values": [
            [
              "1970-01-01T00:00:00Z",
              67777.66900000004,
              15258
            ]
          ]
        }
      ]
    }
  ]
}`), []byte("")},
			dest: []byte(`{
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
}`),
		},
	}
	for _, test := range tests {
		resultAggr, err := InfluxQLAggregate(test.source)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(resultAggr, test.dest) {
			t.Errorf("resultAggr: %s", resultAggr)
			t.Errorf("dest: %s", test.dest)
		}
	}
}
