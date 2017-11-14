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
	"github.com/influxdata/influxdb/models"
	"github.com/toolkits/consistent/rings"
)

var (
	ForbidCmds   = "(?i:select\\s\\*|^\\s*delete|^\\s*drop|^\\s*grant|^\\s*revoke|\\(\\)\\$)"
	SupportCmds  = "(?i:where.*time|show.*from)"
	ExecutorCmds = "(?i:show.*measurements)"
)

var (
	ErrClosed              = errors.New("write in a closed file")
	ErrBackendNotExist     = errors.New("no backend")
	ErrMeasurementNotExist = errors.New("no measurement")
	ErrQueryForbidden      = errors.New("query forbidden")
	ErrQueryEmpty          = errors.New("query empty")
	ErrQueryIllegal        = errors.New("query illegal method")
	ErrQueryNotCluster     = errors.New("query no cluster")
)

const DefaultReplicas = 128

type Result struct {
	Res []byte
	Err error
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

type Ring struct {
	ring          *rings.ConsistentHashNodeRing // consistent ring instance
	node2backends [][]BackendAPI                // ring node index to backends
}

type InfluxCluster struct {
	lock             sync.RWMutex
	Zone             string
	nexts            string
	ForbiddenQuery   []*regexp.Regexp
	ObligatedQuery   []*regexp.Regexp
	cfgsrc           *RedisConfigSource
	bas              []BackendAPI
	backends         map[string]BackendAPI
	measurement2ring map[string]*Ring
	stats            *Statistics
	counter          *Statistics
	ticker           *time.Ticker
	defaultTags      map[string]string
	WriteTracing     int
	QueryTracing     int
	Replicas         int32
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
		Zone:         nodecfg.Zone,
		nexts:        nodecfg.Nexts,
		cfgsrc:       cfgsrc,
		bas:          make([]BackendAPI, 0),
		stats:        &Statistics{},
		counter:      &Statistics{},
		ticker:       time.NewTicker(10 * time.Second),
		defaultTags:  map[string]string{"addr": nodecfg.ListenAddr},
		WriteTracing: nodecfg.WriteTracing,
		QueryTracing: nodecfg.QueryTracing,
		Replicas:     nodecfg.Replicas,
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
	}
	err = ic.EnsureQuery(SupportCmds)
	if err != nil {
		panic(err)
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

func (ic *InfluxCluster) loadMeasurements(backends map[string]BackendAPI) (measurement2ring map[string]*Ring, err error) {
	measurement2ring = make(map[string]*Ring)
	m_map, err := ic.cfgsrc.LoadMeasurements()
	if err != nil {
		return
	}

	for name, bs := range m_map {
		r := &Ring{node2backends: make([][]BackendAPI, len(bs))}
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
			r.node2backends[key] = bss
			nodes[key] = strconv.Itoa(key)
		}
		r.ring = rings.NewConsistentHashNodesRing(ic.Replicas, nodes)
		measurement2ring[name] = r
	}
	return
}

func (ic *InfluxCluster) LoadConfig() (err error) {
	backends, bas, err := ic.loadBackends()
	if err != nil {
		return
	}

	measurement2ring, err := ic.loadMeasurements(backends)
	if err != nil {
		return
	}

	ic.lock.Lock()
	orig_backends := ic.backends
	ic.backends = backends
	ic.bas = bas
	ic.measurement2ring = measurement2ring
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
	if ring, ok = ic.measurement2ring[key]; ok {
		return
	}

	// match use prefix
	for k, v := range ic.measurement2ring {
		if strings.HasPrefix(key, k) {
			ring = v
			ok = true
			break
		}
	}
	return
}

// Query logic:
// firstly, to get the all backends by the measurement of query statement, secondly,
// to forward query statement to `all` backends, then to aggregate the result together,
// finally, forward the result to client.
func (ic *InfluxCluster) Query(w http.ResponseWriter, req *http.Request) (err error) {
	atomic.AddInt64(&ic.stats.QueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ic.stats.QueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	switch req.Method {
	case "GET", "POST":
		break
	default:
		w.WriteHeader(400)
		w.Write([]byte("illegal method"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return ErrQueryIllegal
	}

	// TODO: all query in q?
	// 1: no empty
	// 2: no forbidden
	// 3: cluster friendly
	query := strings.TrimSpace(req.FormValue("q"))

	if query == "" {
		w.WriteHeader(400)
		w.Write([]byte("empty query"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return ErrQueryEmpty
	}

	matched, err := regexp.MatchString(ExecutorCmds, query)
	if err == nil && matched {
		w.WriteHeader(400)
		w.Write([]byte("query not supported"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return ErrQueryNotCluster
	}

	err = ic.CheckQuery(query)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("query forbidden"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return ErrQueryForbidden
	}

	measurement, err := GetMeasurementFromInfluxQL(query)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("can't get measurement"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		log.Printf("can't get measurement: %s\n", query)
		return ErrMeasurementNotExist
	}

	r, ok := ic.GetRing(measurement)
	if !ok {
		log.Printf("unknown measurement: %s,the query is %s\n", measurement, query)
		w.WriteHeader(400)
		w.Write([]byte("unknown measurement"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return ErrBackendNotExist
	}
	queryinstance, _ := NewQueryContext(query, r.node2backends, ic)
	return queryinstance.Commit(w, req)
}

// Write logic
// pre measurment pre hashring
// get the node from hashring with hostname
// finally, to get the backend with node id.

func (ic *InfluxCluster) Write(p []byte, precision string) (err error) {
	atomic.AddInt64(&ic.stats.WriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ic.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	buf := bytes.NewBuffer(p)
	points, err := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), precision)
	if err != nil || len(points) == 0 {
		if err.Error() == "EOF" {
			return nil
		}
		log.Printf("error: %s\n", err)
		atomic.AddInt64(&ic.stats.WriteRequestsFail, 1)
		return
	}

	writeCxt := NewWriteContext(points, ic)
	go writeCxt.Commit()

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

	return nil
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
