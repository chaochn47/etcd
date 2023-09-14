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

//go:build !cluster_proxy

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/config"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
)

const (
	// in sync with how kubernetes uses etcd
	// https://github.com/kubernetes/kubernetes/blob/release-1.28/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L59-L71
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second
	dialTimeout      = 20 * time.Second

	clientRuntime           = 10 * time.Second
	failureDetectionLatency = 6 * time.Second
	// expect no more than 5 failed requests
	failedRequests = 5
)

func TestFailover(t *testing.T) {
	tcs := []struct {
		name                    string
		clusterOptions          []e2e.EPClusterOption
		failureInjector         func(t *testing.T, clus *e2e.EtcdProcessCluster)
		failureDetectionLatency time.Duration
	}{
		{
			name:                    "network_partition",
			clusterOptions:          []e2e.EPClusterOption{e2e.WithClusterSize(3), e2e.WithIsPeerTLS(true), e2e.WithPeerProxy(true)},
			failureInjector:         blackhole,
			failureDetectionLatency: failureDetectionLatency,
		},
		{
			name:                    "stalled_disk_write",
			clusterOptions:          []e2e.EPClusterOption{e2e.WithClusterSize(3), e2e.WithGoFailEnabled(true)},
			failureInjector:         blockDiskWrite,
			failureDetectionLatency: failureDetectionLatency,
		},
		{
			name:            "defrag",
			clusterOptions:  []e2e.EPClusterOption{e2e.WithClusterSize(3), e2e.WithGoFailEnabled(true)},
			failureInjector: triggerDefrag,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			e2e.BeforeTest(t)
			clus, err := e2e.NewEtcdProcessCluster(context.TODO(), t, tc.clusterOptions...)
			t.Cleanup(func() { clus.Stop() })
			endpoints := clus.EndpointsGRPC()

			cnt, success := 0, 0
			donec := make(chan struct{})
			errc := make(chan error, 1)

			go func() {
				var lastErr error
				var cc *clientv3.Client
				defer func() {
					if cc != nil {
						cc.Close()
					}
					errc <- lastErr
					close(donec)
					close(errc)
				}()
				cc, cerr := clientv3.New(clientv3.Config{
					DialTimeout:          dialTimeout,
					DialKeepAliveTime:    keepaliveTime,
					DialKeepAliveTimeout: keepaliveTimeout,
					Endpoints:            endpoints,
					DialOptions: []grpc.DialOption{
						grpc.WithDisableServiceConfig(),
						grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy": "round_robin", "healthCheckConfig": {"serviceName": ""}}`),
						// the following service config will disable grpc client health check
						//grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy": "round_robin"}`),
					},
				})
				require.NoError(t, cerr)
				timeout := time.After(clientRuntime)

				time.Sleep(tc.failureDetectionLatency)
				for {
					select {
					case <-timeout:
						return
					default:
					}
					cctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					//start := time.Now()
					_, err = cc.Get(cctx, "health")
					//t.Logf("TS (%s): number #%d health check took %v", time.Now().UTC().Format(time.RFC3339), cnt, time.Since(start))
					cancel()
					cnt++
					if err != nil {
						lastErr = err
						continue
					}
					success++
				}
			}()

			tc.failureInjector(t, clus)

			<-donec
			err, ok := <-errc
			if ok && err != nil {
				t.Logf("etcd client failed to fail over, error (%v)", err)
			}
			t.Logf("request failure rate is %.2f%%, traffic volume success %d requests, total %d requests", (1-float64(success)/float64(cnt))*100, success, cnt)
			// expect no more than 5 failed requests
			require.InDelta(t, cnt, success, failedRequests)
		})
	}
}

func blackhole(t *testing.T, clus *e2e.EtcdProcessCluster) {
	member := clus.Procs[0]
	proxy := member.PeerProxy()
	t.Logf("Blackholing traffic from and to member %q", member.Config().Name)
	proxy.BlackholeTx()
	proxy.BlackholeRx()
}

func blockDiskWrite(t *testing.T, clus *e2e.EtcdProcessCluster) {
	err := clus.Procs[0].Failpoints().Setup(context.Background(), "raftBeforeSave", `sleep(10000)`)
	require.NoError(t, err)
	clus.Procs[0].Etcdctl().Put(context.Background(), "foo", "bar", config.PutOptions{})
}

func triggerDefrag(t *testing.T, clus *e2e.EtcdProcessCluster) {
	err := clus.Procs[0].Failpoints().Setup(context.Background(), "defragBeforeCopy", `sleep(8000)`)
	require.NoError(t, err)
	err = clus.Procs[0].Etcdctl().Defragment(context.Background(), config.DefragOption{Timeout: time.Minute})
	require.NoError(t, err)
}
