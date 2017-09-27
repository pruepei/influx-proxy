package backend

import (
	"log"
	"reflect"
)

func getmax(s []interface{}) (v interface{}) {
	var intT *int
	var int32T *int32
	var int64T *int64
	var uintT *uint
	var uint32T *uint32
	var uint64T *uint64
	var float32T *float32
	var float64T *float64
	for _, st := range s {
		switch sv := st.(type) {
		case float32:
			if float32T == nil {
				float32T = &sv
				v = *float32T
			}
			if *float32T < sv {
				float32T = &sv
				v = *float32T
			}
		case float64:
			if float64T == nil {
				float64T = &sv
				v = *float64T
			}
			if *float64T < sv {
				float64T = &sv
				v = *float64T
			}
		case int:
			if intT == nil {
				intT = &sv
				v = *intT
			}
			if *intT < sv {
				intT = &sv
				v = *intT
			}
		case int32:
			if int32T == nil {
				int32T = &sv
				v = *intT
			}
			if *int32T < sv {
				int32T = &sv
				v = *int32T
			}
		case int64:
			if int64T == nil {
				int64T = &sv
				v = *int64T
			}
			if *int64T < sv {
				int64T = &sv
				v = *int64T
			}
		case uint:
			if uintT == nil {
				uintT = &sv
				v = *uintT
			}
			if *uintT < sv {
				uintT = &sv
				v = *uintT
			}
		case uint32:
			if uint32T == nil {
				uint32T = &sv
				v = *uint32T
			}
			if *uint32T < sv {
				uint32T = &sv
				v = *uint32T
			}
		case uint64:
			if uint64T == nil {
				v = *uint64T
				uint64T = &sv
			}
			if *uint64T < sv {
				uint64T = &sv
				v = *uint64T
			}
		default:
			log.Println(reflect.TypeOf(sv))
			log.Panic("function.go doesn't match")
		}
	}
	return
}

func getmin(s []interface{}) (v interface{}) {
	var intT *int
	var int32T *int32
	var int64T *int64
	var uintT *uint
	var uint32T *uint32
	var uint64T *uint64
	var float32T *float32
	var float64T *float64
	for _, st := range s {
		switch sv := st.(type) {
		case float32:
			if float32T == nil {
				float32T = &sv
				v = *float32T
			}
			if *float32T > sv {
				float32T = &sv
				v = *float32T
			}
		case float64:
			if float64T == nil {
				float64T = &sv
				v = *float64T
			}
			if *float64T > sv {
				float64T = &sv
				v = *float64T
			}
		case int:
			if intT == nil {
				intT = &sv
				v = *intT
			}
			if *intT > sv {
				intT = &sv
				v = *intT
			}
		case int32:
			if int32T == nil {
				int32T = &sv
				v = *int32T
			}
			if *int32T > sv {
				int32T = &sv
				v = *int32T
			}
		case int64:
			if int64T == nil {
				v = *int64T
				int64T = &sv
			}
			if *int64T > sv {
				int64T = &sv
				v = *int64T
			}
		case uint:
			if uintT == nil {
				v = *uintT
				uintT = &sv
			}
			if *uintT > sv {
				uintT = &sv
				v = *uintT
			}
		case uint32:
			if uint32T == nil {
				v = *uint32T
				uint32T = &sv
			}
			if *uint32T > sv {
				uint32T = &sv
				v = *uint32T
			}
		case uint64:
			if uint64T == nil {
				v = *uint64T
				uint64T = &sv
			}
			if *uint64T > sv {
				uint64T = &sv
				v = *uint64T
			}
		default:
			log.Println(reflect.TypeOf(sv))
			log.Panic("function.go doesn't match")
		}
	}
	return

}

func getsum(s []interface{}) (v interface{}) {
	var intT int
	var int32T int32
	var int64T int64
	var uintT uint
	var uint32T uint32
	var uint64T uint64
	var float32T float32
	var float64T float64
	for _, st := range s {
		switch sv := st.(type) {
		case float32:
			float32T += sv
			v = float32T
		case float64:
			float64T += sv
			v = float64T
		case int:
			intT += sv
			v = intT
		case int32:
			int32T += sv
			v = int32T
		case int64:
			int64T += sv
			v = int64T
		case uint:
			uintT += sv
			v = uintT
		case uint32:
			uint32T += sv
			v = uint32T
		case uint64:
			uint64T += sv
			v = uint64T
		}
	}
	return
}

func getmean(sum []interface{}, count []interface{}) interface{} {
	return div(getsum(sum), getsum(count))
}

func div(s interface{}, c interface{}) (v interface{}) {
	// this types used by influxdb
	switch ss := s.(type) {
	case float32:
		if cc, cok := c.(float32); cok {
			v = ss / cc
		}
		if cc, cok := c.(float64); cok {
			v = float64(ss) / cc
		}
		if cc, cok := c.(int); cok {
			v = ss / float32(cc)
		}
		if cc, cok := c.(int32); cok {
			v = ss / float32(cc)
		}
		if cc, cok := c.(int64); cok {
			v = float64(ss) / float64(cc)
		}
		if cc, cok := c.(uint); cok {
			v = ss / float32(cc)
		}
		if cc, cok := c.(uint32); cok {
			v = ss / float32(cc)
		}
		if cc, cok := c.(uint64); cok {
			v = float64(ss) / float64(cc)
		}
	case float64:
		if cc, cok := c.(float32); cok {
			v = ss / float64(cc)
		}
		if cc, cok := c.(float64); cok {
			v = ss / cc
		}
		if cc, cok := c.(int); cok {
			v = ss / float64(cc)
		}
		if cc, cok := c.(int32); cok {
			v = ss / float64(cc)
		}
		if cc, cok := c.(int64); cok {
			v = ss / float64(cc)
		}
		if cc, cok := c.(uint); cok {
			v = ss / float64(cc)
		}
		if cc, cok := c.(uint32); cok {
			v = ss / float64(cc)
		}
		if cc, cok := c.(uint64); cok {
			v = ss / float64(cc)
		}
	case int:
		if cc, cok := c.(float32); cok {
			v = float32(ss) / cc
		}
		if cc, cok := c.(float64); cok {
			v = float64(ss) / cc
		}
		if cc, cok := c.(int); cok {
			v = ss / cc
		}
		if cc, cok := c.(int32); cok {
			v = int32(ss) / cc
		}
		if cc, cok := c.(int64); cok {
			v = int64(ss) / cc
		}
		if cc, cok := c.(uint); cok {
			v = uint(ss) / cc
		}
		if cc, cok := c.(uint32); cok {
			v = int32(ss) / int32(cc)
		}
		if cc, cok := c.(uint64); cok {
			v = int64(ss) / int64(cc)
		}
	case int32:
		if cc, cok := c.(float32); cok {
			v = float32(ss) / cc
		}
		if cc, cok := c.(float64); cok {
			v = float64(ss) / cc
		}
		if cc, cok := c.(int); cok {
			v = ss / int32(cc)
		}
		if cc, cok := c.(int32); cok {
			v = ss / cc
		}
		if cc, cok := c.(int64); cok {
			v = int64(ss) / cc
		}
		if cc, cok := c.(uint); cok {
			v = ss / int32(cc)
		}
		if cc, cok := c.(uint32); cok {
			v = ss / int32(cc)
		}
		if cc, cok := c.(uint64); cok {
			v = int64(ss) / int64(cc)
		}
	case int64:
		if cc, cok := c.(float32); cok {
			v = ss / int64(cc)
		}
		if cc, cok := c.(float64); cok {
			v = float64(ss) / cc
		}
		if cc, cok := c.(int); cok {
			v = ss / int64(cc)
		}
		if cc, cok := c.(int32); cok {
			v = ss / int64(cc)
		}
		if cc, cok := c.(int64); cok {
			v = ss / cc
		}
		if cc, cok := c.(uint); cok {
			v = ss / int64(cc)
		}
		if cc, cok := c.(uint32); cok {
			v = ss / int64(cc)
		}
		if cc, cok := c.(uint64); cok {
			v = ss / int64(cc)
		}
	case uint:
		if cc, cok := c.(float32); cok {
			v = float32(ss) / cc
		}
		if cc, cok := c.(float64); cok {
			v = float64(ss) / cc
		}
		if cc, cok := c.(int); cok {
			v = int(ss) / cc
		}
		if cc, cok := c.(int32); cok {
			v = int32(ss) / cc
		}
		if cc, cok := c.(int64); cok {
			v = int64(ss) / cc
		}
		if cc, cok := c.(uint); cok {
			v = ss / cc
		}
		if cc, cok := c.(uint32); cok {
			v = uint32(ss) / cc
		}
		if cc, cok := c.(uint64); cok {
			v = uint64(ss) / cc
		}
	case uint32:
		if cc, cok := c.(float32); cok {
			v = float32(ss) / cc
		}
		if cc, cok := c.(float64); cok {
			v = float64(ss) / cc
		}
		if cc, cok := c.(int); cok {
			v = int32(ss) / int32(cc)
		}
		if cc, cok := c.(int32); cok {
			v = int32(ss) / cc
		}
		if cc, cok := c.(int64); cok {
			v = int64(ss) / cc
		}
		if cc, cok := c.(uint); cok {
			v = ss / uint32(cc)
		}
		if cc, cok := c.(uint32); cok {
			v = ss / cc
		}
		if cc, cok := c.(uint64); cok {
			v = uint64(ss) / cc
		}
	case uint64:
		if cc, cok := c.(float32); cok {
			v = float64(ss) / float64(cc)
		}
		if cc, cok := c.(float64); cok {
			v = float64(ss) / cc
		}
		if cc, cok := c.(int); cok {
			v = int64(ss) / int64(cc)
		}
		if cc, cok := c.(int32); cok {
			v = int64(ss) / int64(cc)
		}
		if cc, cok := c.(int64); cok {
			v = int64(ss) / cc
		}
		if cc, cok := c.(uint); cok {
			v = ss / uint64(cc)
		}
		if cc, cok := c.(uint32); cok {
			v = ss / uint64(cc)
		}
		if cc, cok := c.(uint64); cok {
			v = ss / cc
		}

	}
	return v
}
