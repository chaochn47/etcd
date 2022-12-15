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

package linearizability

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/config"
	"go.etcd.io/etcd/tests/v3/framework/interfaces"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	DefaultTraffic         Traffic = readWriteSingleKey{key: "key", writes: []opChance{{operation: Put, chance: 90}, {operation: Delete, chance: 5}, {operation: Txn, chance: 5}}}
	DefaultTrafficWithAuth Traffic = readWriteSingleKey{key: "key", writes: []opChance{{operation: Put, chance: 90}, {operation: Delete, chance: 5}, {operation: Txn, chance: 5}}, authEnabled: true}
)

type Traffic interface {
	PreRun(ctx context.Context, c interfaces.Client, lg *zap.Logger) error
	Run(ctx context.Context, c *recordingClient, limiter *rate.Limiter, ids idProvider)

	AuthEnabled() bool
}

type readWriteSingleKey struct {
	key    string
	writes []opChance

	authEnabled bool
}

type opChance struct {
	operation Operation
	chance    int
}

func (t readWriteSingleKey) PreRun(ctx context.Context, c interfaces.Client, lg *zap.Logger) error {
	if t.AuthEnabled() {
		lg.Info("set up auth")
		return setupAuth(ctx, c)
	}
	return nil
}

func (t readWriteSingleKey) Run(ctx context.Context, c *recordingClient, limiter *rate.Limiter, ids idProvider) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Execute one read per one write to avoid operation history include too many failed writes when etcd is down.
		resp, err := t.Read(ctx, c, limiter)
		if err != nil && !strings.Contains(err.Error(), "etcdserver: permission denied") {
			continue
		}
		// Provide each write with unique id to make it easier to validate operation history.
		t.Write(ctx, c, limiter, ids.RequestId(), resp)
	}
}

func (t readWriteSingleKey) AuthEnabled() bool {
	return t.authEnabled
}

func (t readWriteSingleKey) Read(ctx context.Context, c *recordingClient, limiter *rate.Limiter) ([]*mvccpb.KeyValue, error) {
	getCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	resp, err := c.Get(getCtx, t.key)
	cancel()
	if err == nil {
		limiter.Wait(ctx)
	}
	return resp, err
}

func (t readWriteSingleKey) Write(ctx context.Context, c *recordingClient, limiter *rate.Limiter, id int, kvs []*mvccpb.KeyValue) error {
	putCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)

	var err error
	switch t.pickWriteOperation() {
	case Put:
		err = c.Put(putCtx, t.key, fmt.Sprintf("%d", id))
	case Delete:
		err = c.Delete(putCtx, t.key)
	case Txn:
		var value string
		if len(kvs) != 0 {
			value = string(kvs[0].Value)
		}
		err = c.Txn(putCtx, t.key, value, fmt.Sprintf("%d", id))
	default:
		panic("invalid operation")
	}
	cancel()
	if err == nil {
		limiter.Wait(ctx)
	}
	return err
}

func (t readWriteSingleKey) pickWriteOperation() Operation {
	sum := 0
	for _, op := range t.writes {
		sum += op.chance
	}
	roll := rand.Int() % sum
	for _, op := range t.writes {
		if roll < op.chance {
			return op.operation
		}
		roll -= op.chance
	}
	panic("unexpected")
}

const (
	rootUserName     = "root"
	rootRoleName     = "root"
	rootUserPassword = "123"
	testUserName     = "test-user"
	testRoleName     = "test-role"
	testUserPassword = "abc"
)

func setupAuth(ctx context.Context, c interfaces.Client) error {
	if _, err := c.UserAdd(ctx, rootUserName, rootUserPassword, config.UserAddOptions{}); err != nil {
		return err
	}
	if _, err := c.RoleAdd(ctx, rootRoleName); err != nil {
		return err
	}
	if _, err := c.UserGrantRole(ctx, rootUserName, rootRoleName); err != nil {
		return err
	}
	if _, err := c.UserAdd(ctx, testUserName, testUserPassword, config.UserAddOptions{}); err != nil {
		return err
	}
	if _, err := c.RoleAdd(ctx, testRoleName); err != nil {
		return err
	}
	if _, err := c.UserGrantRole(ctx, testUserName, testRoleName); err != nil {
		return err
	}
	if _, err := c.RoleGrantPermission(ctx, testRoleName, "key", "key0", clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
		return err
	}
	if err := c.AuthEnable(ctx); err != nil {
		return err
	}
	return nil
}
