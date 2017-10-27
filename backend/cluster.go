// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"errors"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/eleme/influx-proxy/monitor"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/toolkits/consistent/rings"
)

var (
	ErrClosed          = errors.New("write in a closed file")
	ErrBackendNotExist = errors.New("use a backend not exists")
	ErrQueryForbidden  = errors.New("query forbidden")
)

const DefaultReplicas = 128

type Result struct {
	Res []byte
	Err error
}

func getMeasurementFromPoint(point string) (measurement string) {
	pointlen := len(point)
	for i := 0; i < pointlen; i++ {
		if point[i] == ',' || point[i] == ' ' {
			return point[0:i]
		}
		if point[i] == '\\' {
			i++
		}
	}
	return ""
}

func getHostnameFromPoint(point string) (hostname string) {
	if !strings.Contains(point, "host=") {
		return ""
	}
	pointlen := len(point)
	for i := 0; i < pointlen; i++ {
		if point[i:i+5] == "host=" {
			for j := i + 5; j < pointlen; j++ {
				if point[j] == ',' {
					return point[i+5 : j]
				}
			}
		}
	}
	return ""
}

func ScanKey(point string) (key string) {
	return getMeasurementFromPoint(point) + getHostnameFromPoint(point)
}

// faster then bytes.TrimRight, not sure why.
func TrimRight(p []byte, s []byte) (r []byte) {
	r = p
	if len(r) == 0 {
		return
	}

	i := len(r) - 1
	for ; bytes.IndexByte(s, r[i]) != -1; i-- {
	}
	return r[0 : i+1]
}

// every measurement has a ring, the ring has N virtual nodes and M members.
// N = M * Replicas
//
// write logic
// It use measurement+sortedTag to get node, then use the node key to get the backends,
// and write data to backends .
//
// query logic
// If ring has only one member, query result and return it.
// If the statement is 'show tag keys' or 'show field keys',
// query with one node and return result.
// If the statement is 'show tag values' or 'select',
// relay the query to all members, merge the result and return it.

type Ring struct {
	ring *rings.ConsistentHashNodeRing // consistent ring instance
	n2bs [][]BackendAPI                // ring node index to backends
}

type InfluxCluster struct {
	lock           sync.RWMutex
	Zone           string
	nexts          string
	query_executor Querier
	ForbiddenQuery []*regexp.Regexp
	ObligatedQuery []*regexp.Regexp
	cfgsrc         *RedisConfigSource
	bas            []BackendAPI
	backends       map[string]BackendAPI
	m2ring         map[string]*Ring // measurements to ring
	stats          *Statistics
	counter        *Statistics
	ticker         *time.Ticker
	defaultTags    map[string]string
	WriteTracing   int
	QueryTracing   int
	Replicas       int32
}

type Statistics struct {
	QueryRequests        int64
	QueryRequestsFail    int64
	WriteRequests        int64
	WriteRequestsFail    int64
	PingRequests         int64
	PingRequestsFail     int64
	PointsWritten        int64
	PointsWrittenFail    int64
	WriteRequestDuration int64
	QueryRequestDuration int64
}

func NewInfluxCluster(cfgsrc *RedisConfigSource, nodecfg *NodeConfig) (ic *InfluxCluster) {
	ic = &InfluxCluster{
		Zone:           nodecfg.Zone,
		nexts:          nodecfg.Nexts,
		query_executor: &InfluxQLExecutor{},
		cfgsrc:         cfgsrc,
		bas:            make([]BackendAPI, 0),
		stats:          &Statistics{},
		counter:        &Statistics{},
		ticker:         time.NewTicker(10 * time.Second),
		defaultTags:    map[string]string{"addr": nodecfg.ListenAddr},
		WriteTracing:   nodecfg.WriteTracing,
		QueryTracing:   nodecfg.QueryTracing,
		Replicas:       nodecfg.Replicas,
	}
	if nodecfg.Replicas == 0 {
		ic.Replicas = DefaultReplicas
	}
	host, err := os.Hostname()
	if err != nil {
		log.Println(err)
	}
	ic.defaultTags["host"] = host
	if nodecfg.Interval > 0 {
		ic.ticker = time.NewTicker(time.Second * time.Duration(nodecfg.Interval))
	}

	err = ic.ForbidQuery(ForbidCmds)
	if err != nil {
		panic(err)
		return
	}
	err = ic.EnsureQuery(SupportCmds)
	if err != nil {
		panic(err)
		return
	}

	// feature
	go ic.statistics()
	return
}

func (ic *InfluxCluster) statistics() {
	// how to quit
	for {
		<-ic.ticker.C
		ic.Flush()
		ic.counter = (*Statistics)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&ic.stats)),
			unsafe.Pointer(ic.counter)))
		err := ic.WriteStatistics()
		if err != nil {
			log.Println(err)
		}
	}
}

func (ic *InfluxCluster) Flush() {
	ic.counter.QueryRequests = 0
	ic.counter.QueryRequestsFail = 0
	ic.counter.WriteRequests = 0
	ic.counter.WriteRequestsFail = 0
	ic.counter.PingRequests = 0
	ic.counter.PingRequestsFail = 0
	ic.counter.PointsWritten = 0
	ic.counter.PointsWrittenFail = 0
	ic.counter.WriteRequestDuration = 0
	ic.counter.QueryRequestDuration = 0
}

func (ic *InfluxCluster) WriteStatistics() (err error) {
	metric := &monitor.Metric{
		Name: "influxdb.cluster",
		Tags: ic.defaultTags,
		Fields: map[string]interface{}{
			"statQueryRequest":         ic.counter.QueryRequests,
			"statQueryRequestFail":     ic.counter.QueryRequestsFail,
			"statWriteRequest":         ic.counter.WriteRequests,
			"statWriteRequestFail":     ic.counter.WriteRequestsFail,
			"statPingRequest":          ic.counter.PingRequests,
			"statPingRequestFail":      ic.counter.PingRequestsFail,
			"statPointsWritten":        ic.counter.PointsWritten,
			"statPointsWrittenFail":    ic.counter.PointsWrittenFail,
			"statQueryRequestDuration": ic.counter.QueryRequestDuration,
			"statWriteRequestDuration": ic.counter.WriteRequestDuration,
		},
		Time: time.Now(),
	}
	line, err := metric.ParseToLine()
	if err != nil {
		return
	}
	return ic.Write([]byte(line+"\n"), "ns")
}

func (ic *InfluxCluster) ForbidQuery(s string) (err error) {
	r, err := regexp.Compile(s)
	if err != nil {
		return
	}

	ic.lock.Lock()
	defer ic.lock.Unlock()
	ic.ForbiddenQuery = append(ic.ForbiddenQuery, r)
	return
}

func (ic *InfluxCluster) EnsureQuery(s string) (err error) {
	r, err := regexp.Compile(s)
	if err != nil {
		return
	}

	ic.lock.Lock()
	defer ic.lock.Unlock()
	ic.ObligatedQuery = append(ic.ObligatedQuery, r)
	return
}

func (ic *InfluxCluster) AddNext(ba BackendAPI) {
	ic.lock.Lock()
	defer ic.lock.Unlock()
	ic.bas = append(ic.bas, ba)
	return
}

func (ic *InfluxCluster) loadBackends() (backends map[string]BackendAPI, bas []BackendAPI, err error) {
	backends = make(map[string]BackendAPI)

	bkcfgs, err := ic.cfgsrc.LoadBackends()
	if err != nil {
		return
	}

	for name, cfg := range bkcfgs {
		backends[name], err = NewBackends(cfg, name)
		if err != nil {
			log.Printf("create backend error: %s", err)
			return
		}
	}

	if ic.nexts != "" {
		for _, nextname := range strings.Split(ic.nexts, ",") {
			ba, ok := backends[nextname]
			if !ok {
				err = ErrBackendNotExist
				log.Println(nextname, err)
				continue
			}
			bas = append(bas, ba)
		}
	}

	return
}

func (ic *InfluxCluster) loadMeasurements(backends map[string]BackendAPI) (m2ring map[string]*Ring, err error) {
	m2ring = make(map[string]*Ring)

	m_map, err := ic.cfgsrc.LoadMeasurements()
	if err != nil {
		return
	}

	for name, bs := range m_map {
		r := &Ring{n2bs: make([][]BackendAPI, len(bs))}
		nodes := make([]string, len(bs))
		for key, bs_names := range bs {
			var bss []BackendAPI
			for _, bs_name := range bs_names {
				bs, ok := backends[bs_name]
				if !ok {
					err = ErrBackendNotExist
					log.Println(bs_name, err)
					continue
				}
				bss = append(bss, bs)
			}
			r.n2bs[key] = bss
			nodes[key] = strconv.Itoa(key)
		}
		r.ring = rings.NewConsistentHashNodesRing(ic.Replicas, nodes)
		m2ring[name] = r
	}
	return
}

func (ic *InfluxCluster) LoadConfig() (err error) {
	backends, bas, err := ic.loadBackends()
	if err != nil {
		return
	}

	m2ring, err := ic.loadMeasurements(backends)
	if err != nil {
		return
	}

	ic.lock.Lock()
	orig_backends := ic.backends
	ic.backends = backends
	ic.bas = bas
	ic.m2ring = m2ring
	ic.lock.Unlock()

	for name, bs := range orig_backends {
		err = bs.Close()
		if err != nil {
			log.Printf("fail in close backend %s", name)
		}
	}
	return
}

func (ic *InfluxCluster) Ping() (version string, err error) {
	atomic.AddInt64(&ic.stats.PingRequests, 1)
	version = VERSION
	return
}

func (ic *InfluxCluster) CheckQuery(q string) (err error) {
	ic.lock.RLock()
	defer ic.lock.RUnlock()

	if len(ic.ForbiddenQuery) != 0 {
		for _, fq := range ic.ForbiddenQuery {
			if fq.MatchString(q) {
				return ErrQueryForbidden
			}
		}
	}

	if len(ic.ObligatedQuery) != 0 {
		for _, pq := range ic.ObligatedQuery {
			if pq.MatchString(q) {
				return
			}
		}
		return ErrQueryForbidden
	}

	return
}

func (ic *InfluxCluster) GetRing(key string) (ring *Ring, ok bool) {
	ic.lock.RLock()
	defer ic.lock.RUnlock()

	// exact match
	if ring, ok = ic.m2ring[key]; ok {
		return
	}

	// match use prefix
	for k, v := range ic.m2ring {
		if strings.HasPrefix(key, k) {
			ring = v
			ok = true
			break
		}
	}
	return
}

func (ic *InfluxCluster) Query(w http.ResponseWriter, req *http.Request) (err error) {
	atomic.AddInt64(&ic.stats.QueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ic.stats.QueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	switch req.Method {
	case "GET", "POST":
	default:
		w.WriteHeader(400)
		w.Write([]byte("illegal method"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	// TODO: all query in q?
	q := strings.TrimSpace(req.FormValue("q"))
	if q == "" {
		w.WriteHeader(400)
		w.Write([]byte("empty query"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	_, err = ic.query_executor.Query(w, req)
	if err == nil {
		return
	}

	err = ic.CheckQuery(q)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("query forbidden"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	key, err := GetMeasurementFromInfluxQL(q)
	if err != nil {
		log.Printf("can't get measurement: %s\n", q)
		w.WriteHeader(400)
		w.Write([]byte("can't get measurement"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	r, ok := ic.GetRing(key)
	if !ok {
		log.Printf("unknown measurement: %s,the query is %s\n", key, q)
		w.WriteHeader(400)
		w.Write([]byte("unknown measurement"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	if len(r.n2bs) == 1 {
		return ic.executeWithNoShard(w, req, r.n2bs[0])
	}

	p := influxql.NewParser(strings.NewReader(q))
	query, err := p.ParseQuery()
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("parse query error"))
		return
	}
	if len(query.Statements) > 1 {
		log.Printf("not support query: %s\n", q)
		w.WriteHeader(400)
		w.Write([]byte("not support multiple query"))
		return
	}
	switch (query.Statements[0]).(type) {
	case *influxql.ShowFieldKeysStatement:
		return ic.executeShowFieldKeysStatement(w, req, r.n2bs[0])
	case *influxql.ShowTagKeysStatement:
		return ic.executeShowTagKeysStatement(w, req, r.n2bs[0])
	case *influxql.ShowTagValuesStatement:
		return ic.executeShowTagValuesStatement(w, req, r.n2bs)
	case *influxql.SelectStatement:
		return ic.executeSelectStatement(w, req, r.n2bs)
	default:
		log.Printf("not support query: %s\n", q)
		w.WriteHeader(400)
		w.Write([]byte("not support"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
	}

	return
}

func (ic *InfluxCluster) executeShowFieldKeysStatement(w http.ResponseWriter, req *http.Request, apis []BackendAPI) error {
	return ic.executeWithNoShard(w, req, apis)
}

func (ic *InfluxCluster) executeShowTagKeysStatement(w http.ResponseWriter, req *http.Request, apis []BackendAPI) error {
	return ic.executeWithNoShard(w, req, apis)
}

func (ic *InfluxCluster) executeShowTagValuesStatement(w http.ResponseWriter, req *http.Request, n2bs [][]BackendAPI) error {
	// TODO
	return errors.New("not support shard query")
}

func (ic *InfluxCluster) executeSelectStatement(w http.ResponseWriter, req *http.Request, n2bs [][]BackendAPI) error {
	// TODO
	return errors.New("not support shard query")
}

func (ic *InfluxCluster) executeWithNoShard(w http.ResponseWriter, req *http.Request, apis []BackendAPI) error {
	result := make(chan *Result)
	defer close(result)
	go ic.executeQuery(w, req, apis, result)
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

func (ic *InfluxCluster) executeQuery(w http.ResponseWriter, req *http.Request, apis []BackendAPI, result chan *Result) {
	var res []byte
	var err error
	// same zone
	for _, api := range apis {
		if api.GetZone() != ic.Zone {
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
		if api.GetZone() == ic.Zone {
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
	atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
}

// Wrong in one row will not stop others.
// So don't try to return error, just print it.
func (ic *InfluxCluster) WriteRow(point models.Point) {
	atomic.AddInt64(&ic.stats.PointsWritten, 1)

	// empty point, ignore it.
	if point.StringSize() == 0 {
		return
	}

	key := string(point.Name())
	r, ok := ic.GetRing(key)
	if !ok {
		log.Printf("new measurement: %s\n", key)
		atomic.AddInt64(&ic.stats.PointsWrittenFail, 1)
		return
	}

	node, err := r.ring.GetNode(string(point.Key()))
	if err != nil {
		log.Println("E:", err)
		return
	}

	i, err := strconv.Atoi(node)
	if err != nil {
		log.Println(err)
		return
	}
	bs := r.n2bs[i]
	// don't block here for a long time, we just have one worker.
	for _, b := range bs {
		err = b.Write([]byte(point.String()))
		if err != nil {
			log.Printf("cluster write fail: %s\n", key)
			atomic.AddInt64(&ic.stats.PointsWrittenFail, 1)
			return
		}
	}
	return
}

func (ic *InfluxCluster) Write(p []byte, precision string) (err error) {
	atomic.AddInt64(&ic.stats.WriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ic.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	buf := bytes.NewBuffer(p)

	points, err := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), precision)
	if err != nil && len(points) == 0 {
		if err.Error() == "EOF" {
			return nil
		}
		log.Printf("error: %s\n", err)
		atomic.AddInt64(&ic.stats.WriteRequestsFail, 1)
		return
	}
	for _, point := range points {
		ic.WriteRow(point)
	}

	ic.lock.RLock()
	defer ic.lock.RUnlock()
	if len(ic.bas) > 0 {
		for _, n := range ic.bas {
			err = n.Write(p)
			if err != nil {
				log.Printf("error: %s\n", err)
				atomic.AddInt64(&ic.stats.WriteRequestsFail, 1)
			}
		}
	}

	return
}

func (ic *InfluxCluster) Close() (err error) {
	ic.lock.RLock()
	defer ic.lock.RUnlock()
	for name, bs := range ic.backends {
		err = bs.Close()
		if err != nil {
			log.Printf("fail in close backend %s", name)
		}
	}
	return
}
