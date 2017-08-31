package backend

import (
	"github.com/influxdata/influxdb/influxql"
)

// InfluxQLAggregate is aim to re-organize the result of basic query statement
func InfluxQLAggregate(results []influxql.Result, operator influxql.Token) (resultFinal influxql.Result, err error) {

	if operator.Precedence() == 0 {
		return
	}

	// todo
	//
	// 从 backend 拉来来的是 json 过后的结果
	// 需要从 json 逆向回 influx/query 中的 Result，不足的字段丢弃
	// 进一步逆回 influx/models.Rows 这样 row 应该对应着二次生成的 QL
	// 重新组合 row 放回 influx 原来的处理路径。

	return resultFinal, nil
}
