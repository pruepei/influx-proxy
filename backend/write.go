package backend

import (
	"log"
	"strconv"
	"sync/atomic"

	"github.com/influxdata/influxdb/models"
)

type WriteContext struct {
	ic     *InfluxCluster
	points models.Points
}

func NewWriteContext(points models.Points, ic *InfluxCluster) (wctx *WriteContext) {
	return &WriteContext{
		ic:     ic,
		points: points,
	}
}

func (wc *WriteContext) Commit() {
	for _, point := range wc.points {
		wc.writeRow(point)
	}
}

func (wc *WriteContext) writeRow(point models.Point) {
	atomic.AddInt64(&wc.ic.stats.PointsWritten, 1)

	// empty point, ignore it.
	if point.StringSize() == 0 {
		return
	}

	measurment := string(point.Name())

	r, ok := wc.ic.GetRing(measurment)
	if !ok {
		log.Printf("new measurement: %s\n", measurment)
		atomic.AddInt64(&wc.ic.stats.PointsWrittenFail, 1)
		return
	}

	node, err := r.ring.GetNode(point.Tags().GetString("hostname"))

	if err != nil {
		log.Println("E:", err)
		return
	}

	index, err := strconv.Atoi(node)

	if err != nil {
		log.Println(err)
		return
	}

	// node 2 backendservers
	backends := r.node2backends[index]

	// don't block here for a long time, we just have one worker.
	for _, backend := range backends {
		err = backend.Write([]byte(point.String()))
		if err != nil {
			log.Printf("cluster write fail: %s\n", measurment)
			atomic.AddInt64(&wc.ic.stats.PointsWrittenFail, 1)
			return
		}
	}
	return
}
