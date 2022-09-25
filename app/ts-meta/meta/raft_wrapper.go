/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package meta

import (
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	raftboltdb "github.com/openGemini/openGemini/open_src/github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

const (
	raftSnapshotsRetained = 2
)

var ErrRaftNotOpen = fmt.Errorf("raft instance is not ready")
var ErrRaftTransOpenFailed = fmt.Errorf("raft trans open failed")

type raftWrapper struct {
	ln          net.Listener
	raft        *raft.Raft
	logStore    raft.LogStore
	snapStore   raft.SnapshotStore
	stableStore raft.StableStore
	notifyCh    chan bool
}

func newRaftWrapper(s *Store, ln net.Listener, peers []string) (*raftWrapper, error) {
	rw := &raftWrapper{ln: ln, notifyCh: s.notifyCh}
	raftConf := rw.raftConfig(s.config)
	trans := newRaftTrans(ln)
	if trans == nil {
		return nil, ErrRaftTransOpenFailed
	}
	var err error
	err = rw.raftStore(s.config)
	if err != nil {
		return nil, err
	}

	configuration := raft.Configuration{}
	for i := range peers {
		configuration.Servers = append(configuration.Servers,
			raft.Server{ID: raft.ServerID(peers[i]), Address: raft.ServerAddress(peers[i])})
	}

	hasExistState, _ := raft.HasExistingState(rw.logStore, rw.stableStore, rw.snapStore)
	if hasExistState {
		err = raft.RecoverCluster(raftConf, (*storeFSM)(s), rw.logStore, rw.stableStore, rw.snapStore, trans, configuration)
		if err != nil {
			return nil, err
		}
	} else if s.config.RPCBindAddress == s.config.JoinPeers[0] { // bootstrap with all peers will make choose leader for longer time
		logger.GetLogger().Info("bootstrap from first peer!!!")
		err = raft.BootstrapCluster(raftConf, rw.logStore, rw.stableStore, rw.snapStore, trans, configuration)
		if err != nil {
			return nil, err
		}
	}
	rw.raft, err = raft.NewRaft(raftConf, (*storeFSM)(s), rw.logStore, rw.stableStore, rw.snapStore, trans)

	if err != nil {
		return nil, err
	}

	return rw, nil
}

func (r *raftWrapper) raftConfig(c *config.Meta) *raft.Config {
	conf := c.BuildRaft()
	conf.LocalID = raft.ServerID(r.ln.Addr().String())
	conf.NotifyCh = r.notifyCh
	return conf
}

func (r *raftWrapper) raftStore(c *config.Meta) error {
	var err error
	r.snapStore, err = raft.NewFileSnapshotStore(c.Dir, raftSnapshotsRetained, nil)
	if err != nil {
		return err
	}
	switch c.RaftStore {
	case "boltdb":
		store, err := raftboltdb.NewBoltStore(filepath.Join(c.Dir, "raft.db"))
		if err != nil {
			return fmt.Errorf("new boltdb store: %s", err)
		}
		r.logStore = store
		r.stableStore = store
	default:
		return fmt.Errorf("invalid input store %s", c.RaftStore)
	}
	return nil
}

func (r *raftWrapper) peers() ([]string, error) {
	future := r.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	peerNodes := make([]string, len(future.Configuration().Servers))
	for i := range future.Configuration().Servers {
		peerNodes[i] = string(future.Configuration().Servers[i].Address)
	}
	return peerNodes, nil
}

func (r *raftWrapper) isLeader() bool {
	return r != nil && r.raft.State() == raft.Leader
}

func (r *raftWrapper) isCandidate() bool {
	return r != nil && r.raft.State() == raft.Candidate
}

func (r *raftWrapper) leader() string {
	if r == nil || r.raft == nil {
		return ""
	}
	return string(r.raft.Leader())
}

func (r *raftWrapper) UserSnapshot() error {
	future := r.raft.Snapshot()
	return future.Error()
}

func (r *raftWrapper) close() error {
	if r == nil {
		return nil
	}

	if r.raft != nil {
		if err := r.raft.Shutdown().Error(); err != nil {
			return err
		}
	}

	if r.logStore != nil {
		switch logStore := r.logStore.(type) {
		case *raftboltdb.BoltStore:
			return logStore.Close()
		default:

		}
	}

	return nil
}

func (r *raftWrapper) apply(b []byte) error {
	if r == nil || r.raft == nil {
		return errno.NewError(errno.RaftIsNotOpen)
	}
	f := r.raft.Apply(b, 0)
	if err := f.Error(); err != nil {
		if err == raft.ErrNotLeader {
			return errno.NewError(errno.MetaIsNotLeader)
		}
		return err
	}

	resp := f.Response()
	if resp == nil {
		return nil
	}
	if err, ok := resp.(error); ok {
		return err
	}
	panic(fmt.Sprintf("unexpected response: %#v", resp))
}

func (r *raftWrapper) addServer(addr string) error {
	if r == nil || r.raft == nil {
		return ErrRaftNotOpen
	}
	future := r.raft.AddVoter(raft.ServerID(addr), raft.ServerAddress(addr), 0, 0)
	return future.Error()
}

func (r *raftWrapper) showDebugInfo(witch string) ([]byte, error) {
	if r == nil || r.raft == nil {
		return nil, ErrRaftNotOpen
	}
	switch witch {
	case "raft-stat":
		stat := r.raft.Stats()
		b, err := json.Marshal(stat)
		if err != nil {
			logger.GetLogger().Error(fmt.Sprintf("marshal stat fial: %s", err))
			return nil, fmt.Errorf("marshal stat fial: %s", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unkown witch info to get")
	}
}

func newRaftTrans(ln net.Listener) *raft.NetworkTransport {
	layer := newRaftLayer(nil, ln)
	addr, ok := layer.Addr().(*net.TCPAddr)
	if !ok {
		if err := ln.Close(); err != nil {
			logger.GetLogger().Error("raft addr is not tcp addr, try to listen failed", zap.Error(err))
		}
		return nil
	}
	if addr.IP == nil || addr.IP.IsUnspecified() {
		if err := ln.Close(); err != nil {
			logger.GetLogger().Error("raft addr is not valid, try to listen failed", zap.Error(err))
		}
		return nil
	}
	return raft.NewNetworkTransport(layer, 3, 10*time.Second, nil)
}

type raftLayer struct {
	addr net.Addr
	ln   net.Listener
}

func newRaftLayer(addr net.Addr, ln net.Listener) *raftLayer {
	return &raftLayer{
		addr: addr,
		ln:   ln,
	}
}

func (l *raftLayer) Addr() net.Addr {
	if l.addr != nil {
		return l.addr
	}
	return l.ln.Addr()
}

func (l *raftLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, err
	}
	_, err = conn.Write([]byte{MuxHeader})
	if err != nil {
		return nil, conn.Close()
	}
	return conn, err
}

func (l *raftLayer) Accept() (net.Conn, error) { return l.ln.Accept() }

func (l *raftLayer) Close() error { return l.ln.Close() }
