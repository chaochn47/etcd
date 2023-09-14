// Copyright 2023 The etcd Authors
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
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.etcd.io/raft/v3"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

type healthNotifier interface {
	StartServe()
	StopServe(reason string)
}

type healthChecker struct {
	hs *health.Server

	mu     sync.RWMutex
	served bool

	// implements readyz prober
	s *etcdserver.EtcdServer
	// prober configuration TODO 2023-09-13 externalize as etcd server flag
	successThreshold    int
	failureThreshold    int
	healthCheckInterval time.Duration
	// prober state
	successCount int
	failureCount int
}

func NewHealthChecker(hs *health.Server, s *etcdserver.EtcdServer) healthNotifier {
	hc := &healthChecker{hs: hs, s: s, successThreshold: 2, failureThreshold: 3, healthCheckInterval: 100 * time.Millisecond}
	// set grpc health server as serving status blindly since
	// the grpc server will serve iff s.ReadyNotify() is closed.
	hc.StartServe()

	go hc.startProbe()
	return hc
}

func (hc *healthChecker) StartServe() {
	if hc.hs != nil {
		if hc.s.Logger() != nil {
			hc.s.Logger().Info("start serving gRPC requests from client")
		}
		hc.hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		hc.changeServedTo(true)
	}
}

func (hc *healthChecker) StopServe(reason string) {
	if hc.hs != nil {
		if hc.s.Logger() != nil {
			hc.s.Logger().Warn("stop serving gRPC requests from client", zap.String("reason", reason))
		}
		hc.hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		hc.changeServedTo(false)
	}
}

func (hc *healthChecker) changeServedTo(served bool) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.served = served
}

func (hc *healthChecker) IsServed() bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.served
}

func (hc *healthChecker) startProbe() {
	// stop probing if there is no consumer.
	if hc.hs == nil {
		return
	}

	ticker := time.NewTicker(hc.healthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-hc.s.StoppingNotify():
			return
		case <-ticker.C:
			if ready, notReadyReason := checkReadyz(hc.s, 2*time.Second); ready {
				hc.successCount += 1
				hc.failureCount = 0
				if hc.successCount >= hc.successThreshold && !hc.IsServed() {
					hc.StartServe()
				}
			} else {
				hc.failureCount += 1
				hc.successCount = 0
				hc.s.Logger().Warn("readyz check failed", zap.Int("failure-count", hc.failureCount))
				if hc.failureCount >= hc.failureThreshold && hc.IsServed() {
					hc.StopServe(notReadyReason)
				}
			}
		}
	}
}

// checkReadyz prober implementation should be in sync with readyz design in the future.
// https://docs.google.com/document/d/1109lUxD326yRwmMVX-tkJMpm8pTbOw7MD1XsSIo37MU
// it is now in sync with etcdhttp.HandleHealth implementation
// 1. checkLeader
// 2. checkAlarms
// 3. checkQuorumRead
func checkReadyz(s *etcdserver.EtcdServer, timeout time.Duration) (ready bool, notReadyReason string) {
	if s.Leader() == types.ID(raft.None) {
		return false, "local member does not have leader"
	}
	for _, alarm := range s.Alarms() {
		if types.ID(alarm.MemberID) == s.MemberId() && alarm.Alarm == etcdserverpb.AlarmType_CORRUPT {
			return false, "local member has corrupt alarm"
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	// range requests will only be timed-out in the LinearizableReadNotify phase, and
	// blocking in the mvcc phase in case of defrag.
	// However, use solely LinearizableReadNotify to indicate readyz will produce false positive serving status when defrag is active.
	_, err := s.Range(ctx, &etcdserverpb.RangeRequest{KeysOnly: true, Limit: 1})
	cancel()
	if err != nil && err != auth.ErrUserEmpty && err != auth.ErrPermissionDenied {
		return false, fmt.Sprintf("local member quorum read with %s timeout failed due to %v", timeout.String(), err)
	}
	return true, ""
}
