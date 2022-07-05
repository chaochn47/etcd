// Copyright 2022 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v3rpc

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	adt "github.com/shenwei356/countminsketch"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/etcdserver"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	DefaultTotalMemoryBudget       = 1 * 1024 * 1024 * 1024
	DefaultThrottleEnableAtPercent = 10
	MegaByte                       = 1 * 1024 * 1024
	DefaultRespSize                = 4 * 1024 * 1024
	DefaultResetTimer              = 15 * time.Second
	DefaultAuditThresholdPercent   = 50
	SmallReqThreshold              = 8 * 1024
	LargeReqThreshold              = 16 * 1024 * 1024
	DefaultEstInterval             = 10 * time.Minute
)

type QueryType int64

const (
	QueryTypeUnknown QueryType = iota
	QueryTypeRange
)

type Query struct {
	qid   string
	qsize uint64
	qtype QueryType
}

type QueryMonitor interface {
	// Start the monitoring
	Start()

	// UpdateUsage : update counters
	UpdateUsage(req interface{}, resp interface{}, err error)

	// AdmitReq : Decide if we can admit the request.
	AdmitReq(req interface{}) bool

	// Stop the monitoring
	Stop()
}

// externally tunable parameters of bandwidth monitor
type BandwidthMonitorConfig struct {
	totalMemoryBudget   uint64
	enableAtPercent     uint64
	alwaysOnForLargeReq bool
}

// BandwidthMonitor implements memory pressure aware token bucket based rate limiter
type BandwidthMonitor struct {
	cfg                          BandwidthMonitorConfig
	defaultRespSize              uint64
	enableAtBytes                uint64
	budgetExhausted              bool
	resetTimer                   time.Duration
	estRespSize                  *adt.CountMinSketch
	qcount                       *adt.CountMinSketch
	respSizeUpdateTime           time.Time
	estimateUpdateInterval       time.Duration
	updateEstimate               bool
	auditThresholdPercent        uint64
	auditOn                      bool
	throttle                     *rate.Limiter
	mu                           sync.Mutex
	server                       *etcdserver.EtcdServer
}

func BuildQueryMonitorCfg(s *etcdserver.EtcdServer) BandwidthMonitorConfig {

	var cfg BandwidthMonitorConfig

	cfg.totalMemoryBudget = DefaultTotalMemoryBudget
	if s.Cfg.ExperimentalQmonMemoryBudgetMegabytes != 0 {
		cfg.totalMemoryBudget = uint64(s.Cfg.ExperimentalQmonMemoryBudgetMegabytes) * MegaByte
	}

	cfg.enableAtPercent = DefaultThrottleEnableAtPercent
	if s.Cfg.ExperimentalQmonThrottleEnableAtPercent != 0 {
		cfg.enableAtPercent = uint64(s.Cfg.ExperimentalQmonThrottleEnableAtPercent)
	}

	if s.Cfg.ExperimentalQmonAlwaysOnForLargeReq {
		cfg.alwaysOnForLargeReq = true
	}

	return cfg
}

func NewQueryMonitor(s *etcdserver.EtcdServer, cfg BandwidthMonitorConfig) QueryMonitor {
	var qm BandwidthMonitor
	qm.cfg = cfg

	qm.enableAtBytes = qm.cfg.totalMemoryBudget * qm.cfg.enableAtPercent / 100
	qm.defaultRespSize = DefaultRespSize
	qm.resetTimer = DefaultResetTimer

	//Even when alwaysOnForLargeReq is enabled, do not initialize remaining to totalMemoryBudget
	//Use the conservative bandwidth instead which is used when mem pressure is high
	remaining := qm.cfg.totalMemoryBudget - qm.enableAtBytes
	timeToGC := uint64(qm.resetTimer / time.Second)
	bw := remaining / timeToGC
	procs := uint64(runtime.GOMAXPROCS(0))

	qm.estRespSize, _ = adt.NewWithEstimates(0.0001, 0.9999)
	qm.qcount, _ = adt.NewWithEstimates(0.0001, 0.9999)
	qm.respSizeUpdateTime = time.Now()
	qm.estimateUpdateInterval = DefaultEstInterval
	qm.auditOn = false
	qm.auditThresholdPercent = DefaultAuditThresholdPercent
	qm.throttle = rate.NewLimiter(rate.Every(time.Second/time.Duration(bw)), int(bw))
	qm.budgetExhausted = false
	qm.server = s
	qm.server.Cfg.Logger.Warn("qmon - created query monitor.",
		zap.Uint64("memoryBudget", qm.cfg.totalMemoryBudget),
		zap.Uint64("gomaxprocs", procs),
		zap.Bool("Always on for large req", qm.cfg.alwaysOnForLargeReq),
		zap.Uint64("throttle enabled at percent", qm.cfg.enableAtPercent),
		zap.Uint64("throttle enabled at bytes", qm.enableAtBytes),
		zap.Uint64("throttle bandwidth bytes per sec per proc", bw))

	return &qm
}

func (ctrl *BandwidthMonitor) Start() {
	go ctrl.start()
}

func (ctrl *BandwidthMonitor) start() {
	ctrl.update()
	ctrl.periodicReset()
}

func (ctrl *BandwidthMonitor) periodicReset() {
	ticker := time.NewTicker(ctrl.resetTimer)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctrl.update()
		case <-ctrl.server.StopNotify():
			return
		}
	}
}

func (ctrl *BandwidthMonitor) update() {
	rss := getCurrentRssBytes(ctrl.server.Cfg.Logger)
	if rss == 0 {
		ctrl.server.Cfg.Logger.Error("qmon: unexpected condition rss is zero.")
		return
	}
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	ctrl.resetRespSizeUnsafe()
	ctrl.updateBudgetUnsafe(uint64(rss))
	ctrl.updateAuditFlagUnsafe(uint64(rss))
}

func (ctrl *BandwidthMonitor) resetRespSizeUnsafe() {
	//reset estimates early if we have detected a size difference
	if ctrl.updateEstimate || time.Since(ctrl.respSizeUpdateTime) > ctrl.estimateUpdateInterval {
		ctrl.server.Cfg.Logger.Info("qmon: clearing estimates.")
		ctrl.estRespSize, _ = adt.NewWithEstimates(0.0001, 0.9999)
		ctrl.respSizeUpdateTime = time.Now()
		ctrl.updateEstimate = false
	}
}

func (ctrl *BandwidthMonitor) updateAuditFlagUnsafe(rss uint64) {
	if (ctrl.cfg.totalMemoryBudget*ctrl.auditThresholdPercent)/100 <= uint64(rss) {
		ctrl.auditOn = true
	} else {
		ctrl.auditOn = false
	}
}

func (ctrl *BandwidthMonitor) updateBudgetUnsafe(rss uint64) {
	if ctrl.enableAtBytes <= uint64(rss) {
		ctrl.budgetExhausted = true
		debug.FreeOSMemory()
		ctrl.server.Cfg.Logger.Warn("qmon: Running FreeOSMemory.")
	} else {
		ctrl.budgetExhausted = false
	}
	ctrl.qcount, _ = adt.NewWithEstimates(0.0001, 0.9999)
}

func (ctrl *BandwidthMonitor) isDeclinedUnsafe(q Query) (bool, uint64, uint64) {
	respSize := q.qsize
	qcount := ctrl.qcount.EstimateString(q.qid)
	ctrl.qcount.UpdateString(q.qid, 1)
	if q.qtype == QueryTypeRange {
		respSize = ctrl.estRespSize.EstimateString(q.qid)
		if respSize == 0 {
			//We have not seen a response for this type of query.
			//Estimate based on default
			respSize = ctrl.defaultRespSize
		}
		q.qsize = respSize
	}
	if ctrl.budgetExhausted {
		//decline
		return true, respSize, qcount
	}

	//admit
	return false, respSize, qcount
}

func (ctrl *BandwidthMonitor) newQueryFromReqResp(req interface{}, resp interface{}) Query {
	var reqSize, respSize int
	var reqContent string
	qtype := QueryTypeUnknown
	switch _resp := resp.(type) {
	case *pb.RangeResponse:
		_req, ok := req.(*pb.RangeRequest)
		if ok && !_req.CountOnly {
			reqSize = _req.Size()
			reqContent = _req.String()
			qtype = QueryTypeRange
		}
		if _resp != nil && _resp.Count != 0 {
			respSize = _resp.Size()
		}
	default:
		reqSize = 0
		respSize = 0
	}

	var q Query
	q.qid = reqContent
	q.qsize = uint64(reqSize + respSize)
	q.qtype = qtype
	return q
}

func (ctrl *BandwidthMonitor) newQueryFromReq(req interface{}) Query {
	var q Query
	switch req.(type) {
	case *pb.RangeRequest:
		_req, ok := req.(*pb.RangeRequest)
		if ok {
			q = ctrl.newQRange(_req)
		}
	default:
	}
	return q
}

func (ctrl *BandwidthMonitor) newQRange(r *pb.RangeRequest) Query {
	var q Query
	q.qid = r.String()
	q.qsize = 0
	q.qtype = QueryTypeRange
	return q
}

func (ctrl *BandwidthMonitor) UpdateUsage(req interface{}, resp interface{}, err error) {

	//do not update estimate with failure response
	if err != nil {
		return
	}

	q := ctrl.newQueryFromReqResp(req, resp)
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	if q.qtype == QueryTypeUnknown {
		return
	}

	current := ctrl.estRespSize.EstimateString(q.qid)
	if current == 0 {
		ctrl.estRespSize.UpdateString(q.qid, q.qsize)
		current = q.qsize
	}
	if current != q.qsize && q.qsize > SmallReqThreshold {
		ctrl.server.Cfg.Logger.Warn("qmon qsize changed. update estimates.", zap.String("qid", q.qid), zap.Uint64("qsize", q.qsize), zap.Uint64("current", current))
		ctrl.updateEstimate = true
	}
	if ctrl.auditOn {
		ctrl.server.Cfg.Logger.Warn("qmon audit.", zap.String("qid", q.qid), zap.Uint64("qsize", q.qsize))
	}
}

func (ctrl *BandwidthMonitor) AdmitReq(req interface{}) bool {
	q := ctrl.newQueryFromReq(req)
	declined, qsize, qcount := ctrl.isDeclined(q)

	//dont rely on default resp size too much
	if (ctrl.cfg.alwaysOnForLargeReq || declined) && qcount > 5 && qsize == ctrl.defaultRespSize {
		ctrl.server.Cfg.Logger.Warn("qmon reject. Response size unknown.", zap.String("qid", q.qid), zap.Uint64("qsize", qsize), zap.Uint64("qcount", qcount))
		return false
	}

	if (qsize > SmallReqThreshold && declined) || (qsize > LargeReqThreshold && ctrl.cfg.alwaysOnForLargeReq) {
		throttledRequests.WithLabelValues("unary", "range").Inc()
		err := ctrl.throttle.WaitN(context.TODO(), int(qsize))
		if err != nil {
			ctrl.server.Cfg.Logger.Warn("qmon throttle failed.", zap.String("qid", q.qid), zap.Uint64("qsize", qsize), zap.Error(err))
			return false
		}
	}
	return true
}

func (ctrl *BandwidthMonitor) isDeclined(q Query) (bool, uint64, uint64) {
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	return ctrl.isDeclinedUnsafe(q)
}

func (ctrl *BandwidthMonitor) Stop() {
}

//TODO windows
func getCurrentRssBytes(logger *zap.Logger) uint64 {
	pid := os.Getpid()
	statm := fmt.Sprintf("/proc/%d/statm", pid)
	buf, err := os.ReadFile(statm)
	if err != nil {
		logger.Error("qmon failed to read statm file", zap.String("statm file", statm), zap.Error(err))
		return 0
	}

	fields := strings.Split(string(buf), " ")
	if len(fields) < 2 {
		logger.Error("qmon failed to parse statm file", zap.String("statm file", statm), zap.String("buff", string(buf)))
		return 0
	}

	rss, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		logger.Error("qmon cannot convert rss to int", zap.String("statm file", statm), zap.Error(err))
		return 0
	}

	return rss * uint64(os.Getpagesize())
}
