package backend

import (
	"testing"
)

func TestInfluxQLGenerator(t *testing.T) {
	tests := []struct {
		source string
		dest   string
	}{
		// SELECT:
		{
			source: `SELECT * FROM NOAA_water_database..s1`,
			dest:   `SELECT * FROM NOAA_water_database..s1`,
		},
		// sum, count, max, min, OPERATION:
		{
			source: `SELECT sum(f0) FROM s1`,
			dest:   `SELECT sum(f0) FROM s1`,
		},
		{
			source: `SELECT count(f0) FROM s1`,
			dest:   `SELECT count(f0) FROM s1`,
		},
		{
			source: `SELECT min(f0) FROM s1`,
			dest:   `SELECT min(f0) FROM s1`,
		},
		{
			source: `SELECT max(f0) FROM s1`,
			dest:   `SELECT max(f0) FROM s1`,
		},
		// mean:
		{
			source: `SELECT mean(f0) FROM s1`,
			dest:   `SELECT sum(f0), count(f0) FROM s1`,
		},
		{
			source: `SELECT mean(f0), mean(f1), mean(f2), max(f3) FROM s1`,
			dest:   `SELECT sum(f0), sum(f1), sum(f2), max(f3), count(f0), count(f1), count(f2) FROM s1`,
		},
		// GROUP BY can't work with mean
		// {
		// 	source: `SELECT mean(f0) FROM s1 GROUP BY location`,
		// 	dest:   `Select sum(f0) FROM s1 GROUP BY location;SELECT count(f0) FROM s1 GROUP BY location`,
		// },
		// {
		// 	source: `SELECT mean(index) FROM h2o_quality GROUP BY *`,
		// 	dest:   `SELECT sum(index) FROM h2o_quality GROUP BY *; SELECT count(index) FROM h2o_quality GROUP BY *`,
		// },
		// {
		// 	source: `SELECT count(f0) FROM s1 WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),location`,
		// 	dest:   `SELECT count(f0) FROM s1 WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),location`,
		// },
		// MATHS EXPRESSION:
		{
			source: `SELECT (f0 * 2) + 4 FROM s1`,
			dest:   `SELECT (f0 * 2) + 4 FROM s1`,
		},
		//LIMIT:
		{
			source: `SELECT f0, location FROM s1 LIMIT 3`,
			dest:   `SELECT f0, location FROM s1 LIMIT 3`,
		},
		// Regular Expressions
		{
			source: `SELECT mean(f1) FROM /s2/`,
			dest:   `SELECT sum(f1), count(f1) FROM /s2/`,
		},
		{
			source: `SELECT mean(f0) FROM s1 WHERE location =~ /[m]/ AND f0 > 3`,
			dest:   `SELECT sum(f0), count(f0) FROM s1 WHERE location =~ /[m]/ AND f0 > 3`,
		},
		// Subqueries
		// {
		// 	source: `SELECT sum("max") FROM (SELECT max("f0") FROM "s1" GROUP BY "location")`,
		// 	dest:   `SELECT max("f0") FROM "s1" GROUP BY "location"`,
		// },
		// show
		{
			source: `SHOW MEASUREMENTS`,
			dest:   `SHOW MEASUREMENTS`,
		},
	}

	for _, test := range tests {
		newQL, err := InfluxQLGenerator(test.source)
		if err != nil {
			t.Errorf("Test: error with Paser %s \n", err)
		}
		if newQL != test.dest {
			t.Errorf("Test: new    QL is: %s\n", newQL)
			t.Errorf("Test: should QL is: %s\n\n", test.dest)
		}
	}
}
