// Copyright 2026 The etcd Authors
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

package etcdserver

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/pkg/v3/featuregate"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/features"
	"go.etcd.io/etcd/server/v3/mock/mockstorage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// TestAsyncStorageWritesAppliesHandedOutEntriesBeforeSnapshot runs the scenario
// of https://github.com/etcd-io/raft/issues/543 through etcd's raftNode and a
// real Raft node. Member 2 follows leader 1, with voters 1, 2 and 3. Raft hands
// entry 1 ("a") to the apply thread, then accepts a snapshot at index 2 ("ab")
// before entry 1 is applied, and hands the snapshot to the append thread at
// once. Raft lets the two threads run in any order. etcd must still apply
// entry 1 before installing the snapshot: the Ready loop hands entry 1 to the
// applier queue, applyc, before it takes the next Ready, and the append worker
// hands the snapshot to the same queue.
func TestAsyncStorageWritesAppliesHandedOutEntriesBeforeSnapshot(t *testing.T) {
	lg := zaptest.NewLogger(t)
	fg := features.NewDefaultServerFeatureGate("test", lg)
	require.NoError(t, fg.(featuregate.MutableFeatureGate).Set("RaftAsyncStorageWrites=true"))
	ms := raft.NewMemoryStorage()
	cfg := raftConfig(config.ServerConfig{Logger: lg, ElectionTicks: 10, ServerFeatureGate: fg}, 2, ms)
	require.True(t, cfg.AsyncStorageWrites)
	cfg.Storage = votersStorage{MemoryStorage: ms, voters: []uint64{1, 2, 3}}
	n := newReadyTap(t, raft.RestartNode(cfg))
	r := newRaftNode(raftNodeConfig{
		lg:                 lg,
		isIDRemoved:        func(uint64) bool { return false },
		Node:               n,
		raftStorage:        ms,
		storage:            mockstorage.NewStorageRecorder(""),
		transport:          newNopTransporter(),
		asyncStorageWrites: true,
		localID:            2,
	})
	r.start(newTestRaftReadyHandler())
	t.Cleanup(func() { requireStops(t, r) })

	// Leader 1 replicates and commits entry 1 ("a"). Once member 2 has persisted
	// it, Raft hands it to the apply thread, and the Ready loop sends it to
	// applyc. Nothing receives from applyc yet, so entry 1 is not applied.
	require.NoError(t, r.Step(t.Context(), msgFromLeader(&raftpb.Message{
		Type:    raftpb.MsgApp.Enum(),
		LogTerm: new(uint64(0)),
		Index:   new(uint64(0)),
		Commit:  new(uint64(1)),
		Entries: []*raftpb.Entry{{Index: new(uint64(1)), Term: new(uint64(1)), Data: []byte("a")}},
	})))
	n.waitFor(t, "apply [1]")

	// Leader 1 sends a snapshot at index 2 ("ab"). Raft accepts it although
	// entry 1 is not applied, and hands it to the append thread at once.
	require.NoError(t, r.Step(t.Context(), msgFromLeader(&raftpb.Message{
		Type: raftpb.MsgSnap.Enum(),
		Snapshot: &raftpb.Snapshot{
			Data: []byte("ab"),
			Metadata: &raftpb.SnapshotMetadata{
				Index:     new(uint64(2)),
				Term:      new(uint64(1)),
				ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
			},
		},
	})))
	n.waitFor(t, "append snapshot 2")
	st := r.Status()
	require.Equal(t, uint64(2), st.GetCommit())
	require.Zero(t, st.Applied)

	// Give the snapshot time to overtake entry 1, if it can.
	time.Sleep(100 * time.Millisecond)

	// Apply in applyc order, as applyAll does, to a state machine that holds the
	// data of the applied entries and, unlike applyEntries, skips none.
	var order []string
	var state string
	for range 2 {
		ap := receive(t, r.applyc)
		if !raft.IsEmptySnap(ap.snapshot) {
			receive(t, ap.notifyc)
			order = append(order, fmt.Sprintf("snapshot %d", ap.snapshot.GetMetadata().GetIndex()))
			state = string(ap.snapshot.GetData())
		}
		for _, e := range ap.entries {
			order = append(order, fmt.Sprintf("entry %d", e.GetIndex()))
			state += string(e.GetData())
		}
		receive(t, ap.notifyc)
		ap.onApplyCompleted()
	}
	assert.Equal(t, []string{"entry 1", "snapshot 2"}, order)
	require.Equal(t, "ab", state)
	require.Eventually(t, func() bool { return r.Status().Applied == 2 }, time.Second, time.Millisecond)
}

// msgFromLeader addresses m from leader 1 at term 1 to member 2.
func msgFromLeader(m *raftpb.Message) *raftpb.Message {
	m.From, m.To, m.Term = new(uint64(1)), new(uint64(2)), new(uint64(1))
	return m
}

// votersStorage is a MemoryStorage whose initial configuration has the given
// voters, like that of a member restarting with an empty log.
type votersStorage struct {
	*raft.MemoryStorage
	voters []uint64
}

func (s votersStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	hs, _, err := s.MemoryStorage.InitialState()
	return hs, &raftpb.ConfState{Voters: s.voters}, err
}

// readyTap passes the Readys of a Raft node on to etcd's Ready loop, and
// reports what each one hands to the local apply thread or, for a snapshot, to
// the append thread. Raft hands out a Ready once the tap has taken it.
type readyTap struct {
	raft.Node
	readyc chan raft.Ready
	eventc chan string
}

func newReadyTap(t *testing.T, n raft.Node) *readyTap {
	tap := &readyTap{Node: n, readyc: make(chan raft.Ready), eventc: make(chan string, 16)}
	donec := make(chan struct{})
	t.Cleanup(func() { close(donec) })
	emit := func(event string) bool {
		select {
		case tap.eventc <- event:
			return true
		case <-donec:
			return false
		}
	}
	go func() {
		for {
			var rd raft.Ready
			select {
			case rd = <-n.Ready():
			case <-donec:
				return
			}
			for _, m := range rd.Messages {
				switch {
				case m.GetTo() == raft.LocalApplyThread:
					if !emit(fmt.Sprintf("apply %v", entryIndexes(m.GetEntries()))) {
						return
					}
				case m.GetTo() == raft.LocalAppendThread && !raft.IsEmptySnap(m.GetSnapshot()):
					if !emit(fmt.Sprintf("append snapshot %d", m.GetSnapshot().GetMetadata().GetIndex())) {
						return
					}
				}
			}
			select {
			case tap.readyc <- rd:
			case <-donec:
				return
			}
		}
	}()
	return tap
}

func (tap *readyTap) Ready() <-chan raft.Ready { return tap.readyc }

// waitFor waits until Raft hands out a Ready with the given event.
func (tap *readyTap) waitFor(t *testing.T, event string) {
	t.Helper()
	for {
		if receive(t, tap.eventc) == event {
			return
		}
	}
}
