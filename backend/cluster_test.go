// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/toolkits/consistent/rings"
)

func TestScanKey(t *testing.T) {
	tests := []struct {
		point string
		key   string
	}{
		{
			point: "cpu,host=server01,region=uswest value=1 1434055562000000000",
			key:   "cpuserver01",
		},
		{
			point: "cpu value=3,value2=4 1434055562000010000",
			key:   "cpu",
		},
		{
			point: "temper\\ ature,machine=unit42,type=assembly internal=32,external=100 1434055562000000035",
			key:   "temper\\ ature",
		},
		{
			point: "temper\\,ature,machine=unit143,type=assembly internal=22,external=130 1434055562005000035",
			key:   "temper\\,ature",
		},
	}
	for _, v := range tests {
		target := ScanKey(v.point)
		if v.key != ScanKey(v.point) {
			t.Errorf("point is: %s and key is: %s", v.point, target)
		}
	}
}

func CreateTestInfluxCluster() (ic *InfluxCluster, err error) {
	redisConfig := &RedisConfigSource{}
	nodeConfig := &NodeConfig{}
	ic = NewInfluxCluster(redisConfig, nodeConfig)
	backends := make(map[string]BackendAPI)
	bkcfgs := make(map[string]*BackendConfig)
	cfg, _ := CreateTestBackendConfig("test1")
	bkcfgs["test1"] = cfg
	cfg, _ = CreateTestBackendConfig("test2")
	bkcfgs["test2"] = cfg
	cfg, _ = CreateTestBackendConfig("write_only")
	cfg.WriteOnly = 1
	bkcfgs["write_only"] = cfg
	for name, cfg := range bkcfgs {
		backends[name], err = NewBackends(cfg, name)
		if err != nil {
			return
		}
	}
	ic.backends = backends
	ic.nexts = "test2"
	ic.bas = append(ic.bas, backends["test2"])
	m2ring := make(map[string]*Ring)
	r1 := &Ring{n2bs: make([][]BackendAPI, 1)}
	r1.n2bs[0] = []BackendAPI{backends["write_only"], backends["test1"]}
	r1.ring = rings.NewConsistentHashNodesRing(DefaultReplicas, []string{"0"})
	m2ring["cpu"] = r1
	r2 := &Ring{n2bs: make([][]BackendAPI, 1)}
	r2.n2bs[0] = []BackendAPI{backends["write_only"]}
	r2.ring = rings.NewConsistentHashNodesRing(DefaultReplicas, []string{"0"})
	m2ring["write_only"] = r2
	ic.m2ring = m2ring

	return
}

func TestInfluxdbClusterWrite(t *testing.T) {
	ic, err := CreateTestInfluxCluster()
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		name string
		args []byte
		want error
	}{
		{
			name: "cpu",
			args: []byte("cpu value=3,value2=4 1434055562000010000"),
			want: nil,
		},
		{
			name: "cpu.load",
			args: []byte("cpu.load value=3,value2=4 1434055562000010000"),
			want: nil,
		},
		{
			name: "load.cpu",
			args: []byte("load.cpu value=3,value2=4 1434055562000010000"),
			want: nil,
		},
		{
			name: "test",
			args: []byte("test value=3,value2=4 1434055562000010000"),
		},
	}
	for _, tt := range tests {
		err := ic.Write(tt.args, "ns")
		if err != nil {
			t.Error(tt.name, err)
			continue
		}
	}
	time.Sleep(time.Second)
}
func TestInfluxdbClusterPing(t *testing.T) {
	ic, err := CreateTestInfluxCluster()
	if err != nil {
		t.Error(err)
		return
	}
	version, err := ic.Ping()
	if err != nil {
		t.Error(err)
		return
	}
	if version == "" {
		t.Error("empty version")
		return
	}
	time.Sleep(time.Second)
}

func TestInfluxdbClusterQuery(t *testing.T) {
	ic, err := CreateTestInfluxCluster()
	if err != nil {
		t.Error(err)
		return
	}
	w := NewDummyResponseWriter()
	w.Header().Add("X-Influxdb-Version", VERSION)
	q := url.Values{}
	q.Set("db", "test")

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{
			name:  "cpu",
			query: "SELECT * from cpu where time > now() - 1m",
			want:  400,
		},
		{
			name:  "test",
			query: "SELECT cpu_load from test WHERE time > now() - 1m",
			want:  400,
		},
		{
			name:  "cpu_load",
			query: " select cpu_load from cpu WHERE time > now() - 1m",
			want:  200,
		},
		{
			name:  "cpu.load",
			query: " select cpu_load from \"cpu.load\" WHERE time > now() - 1m",
			want:  200,
		},
		{
			name:  "load.cpu",
			query: " select cpu_load from \"load.cpu\" WHERE time > now() - 1m",
			want:  400,
		},
		{
			name:  "show_cpu",
			query: "SHOW tag keys from \"cpu\" ",
			want:  200,
		},
		{
			name:  "delete_cpu",
			query: " DELETE FROM \"cpu\" WHERE time < '2000-01-01T00:00:00Z'",
			want:  400,
		},
		{
			name:  "show_measurements",
			query: "SHOW measurements ",
			want:  200,
		},
		{
			name:  "cpu.load",
			query: " select cpu_load from \"cpu.load\" WHERE time > now() - 1m and host =~ /()$/",
			want:  400,
		},
		{
			name:  "cpu.load",
			query: " select cpu_load from \"cpu.load\" WHERE time > now() - 1m and host =~ /^()$/",
			want:  400,
		},
		{
			name:  "write.only",
			query: " select cpu_load from write_only WHERE time > now() - 1m",
			want:  400,
		},
	}

	for _, tt := range tests {
		q.Set("q", tt.query)
		req, _ := http.NewRequest("GET", "http://localhost:8086/query?"+q.Encode(), nil)
		req.URL.Query()
		ic.Query(w, req)
		if w.status != tt.want {
			t.Error(tt.name, err, w.status)
		}
	}
}
