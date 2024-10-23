// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package raftconn

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/raftlog"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	maxSizePerMsg   = 4096
	maxInflightMsgs = 256
	tickInterval    = 400 * time.Millisecond
	maxProposeId    = math.MaxUint64 - 10000
	identitySep     = "_"
)

type Commit struct {
	Database       string
	PtId           uint32
	MasterShId     uint64
	Data           [][]byte // []entry data
	ApplyDoneC     chan<- struct{}
	CommittedIndex uint64
	fromReplay     bool
}

type RaftNode struct {
	once        sync.Once
	proposeC    chan []byte            // proposed messages
	confChangeC chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan *Commit           // entries committed to log (influx.Rows)
	ReplayC     chan *Commit           // entries committed to log (influx.Rows)
	errorC      chan<- error           // errors from raft session

	nodeId   uint64 // real data node id
	database string
	ptId     uint32
	id       uint64            // the raft node id, value is ptId + 1
	peers    map[uint32]uint64 // key is ptId, value is nodeId

	tick *time.Ticker

	ctx      context.Context
	cancelFn context.CancelFunc

	appliedIndex uint64

	// lock(RWMutex) is for fields which can be changed after init.
	lock      sync.RWMutex
	confState *raftpb.ConfState
	node      raft.Node

	// Fields which are never changed after init.
	startTime time.Time
	Cfg       *raft.Config
	Store     *raftlog.RaftDiskStorage
	RaftPeers []raft.Peer

	ISend netstorage.SendRaftMessageToStorage

	MetaClient metaclient.MetaClient

	SnapShotter *raftlog.SnapShotter

	logger *logger.Logger

	proposeId atomic.Uint64

	dataCommittedMu sync.RWMutex
	DataCommittedC  map[uint64]chan error // key is proposeId

	Identity string // db_ptId
}

func StartNode(store *raftlog.RaftDiskStorage, nodeId uint64, database string, id uint64,
	peers []raft.Peer,
	client metaclient.MetaClient,
	transPeers map[uint32]uint64) *RaftNode {

	c := raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         store,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		Logger:          logger.GetSrLogger(),
	}

	ctx := context.Background()
	cctx, cancelFn := context.WithCancel(ctx)

	snapShotter := &raftlog.SnapShotter{
		RaftFlag:   1, // The initial value is 1, indicating that the index can be advanced.
		RaftFlushC: make(chan bool),
	}
	n := &RaftNode{
		startTime: time.Now(),
		Cfg:       &c,
		Store:     store,
		RaftPeers: peers,

		proposeC:    make(chan []byte),
		confChangeC: make(chan raftpb.ConfChange),
		commitC:     make(chan *Commit),
		errorC:      make(chan error),
		nodeId:      nodeId,
		database:    database,
		ptId:        GetPtId(id),
		id:          id,
		peers:       transPeers,
		tick:        time.NewTicker(tickInterval),
		ctx:         cctx,
		cancelFn:    cancelFn,

		SnapShotter:    snapShotter,
		MetaClient:     client,
		ISend:          netstorage.NewNetStorage(client),
		DataCommittedC: make(map[uint64]chan error),
	}
	n.initIdentity()
	return n
}

func (n *RaftNode) initIdentity() {
	n.Identity = n.database + identitySep + strconv.Itoa(int(n.ptId))
}

func (n *RaftNode) GenerateProposeId() uint64 {
	n.proposeId.CompareAndSwap(maxProposeId, 0)
	return n.proposeId.Add(1)
}

func (n *RaftNode) GetIdentity() string {
	return n.Identity
}

// make sure dw.identity == n.identify before call
func (n *RaftNode) AddCommittedDataC(dw *raftlog.DataWrapper) (chan error, error) {
	n.dataCommittedMu.Lock()
	defer n.dataCommittedMu.Unlock()
	_, ok := n.DataCommittedC[dw.ProposeId]
	if ok {
		return nil, errno.NewError(errno.UsedProposeId, dw.Identity, dw.ProposeId)
	}
	c := make(chan error)
	n.DataCommittedC[dw.ProposeId] = c
	return c, nil
}

func (n *RaftNode) RemoveCommittedDataC(dw *raftlog.DataWrapper) {
	n.dataCommittedMu.Lock()
	defer n.dataCommittedMu.Unlock()
	_, ok := n.DataCommittedC[dw.ProposeId]
	if !ok {
		logger.GetLogger().Error("RemoveCommittedDataC proposeId not exist", zap.String("identity", dw.Identity), zap.Uint64("proposeId", dw.ProposeId))
		return
	}
	close(n.DataCommittedC[dw.ProposeId])
	delete(n.DataCommittedC, dw.ProposeId)
}

// make sure dw.identity == n.identify before call
func (n *RaftNode) RetCommittedDataC(dw *raftlog.DataWrapper, committedErr error) {
	n.dataCommittedMu.RLock()
	defer n.dataCommittedMu.RUnlock()
	_, ok := n.DataCommittedC[dw.ProposeId]
	if !ok {
		// maybe wait committed return timeout or node restarted
		logger.GetLogger().Error("PushCommittedDataC proposeId not exist", zap.String("identity", dw.Identity), zap.Uint64("proposeId", dw.ProposeId))
		return
	}
	n.DataCommittedC[dw.ProposeId] <- committedErr
}

// WithLogger sets logger to the RaftNode
func (n *RaftNode) WithLogger(log *logger.Logger) {
	n.logger = log.With(zap.String("database", n.database), zap.Uint32("partition", GetPtId(n.id)))
}

func (n *RaftNode) InitAndStartNode() error {
	_, restart, err := n.PastLife()
	if err != nil {
		return err
	}

	if restart {
		state, err := n.Store.HardState()
		if err != nil {
			panic("Unable to get existing hardState")
		}
		n.appliedIndex = state.Commit
		sp, err := n.Store.Snapshot()
		n.SnapShotter.CommittedIndex = sp.Metadata.Index
		if err != nil {
			panic("Unable to get existing snapshot")
		}
		if raftlog.IsValidSnapshot(sp) {
			// It is important that we pick up the conf state here.
			n.SetConfState(&sp.Metadata.ConfState)
			// replay wal
			err := n.replay(sp)
			if err != nil {
				panic("replay wal error")
			}
		}
		n.node = raft.RestartNode(n.Cfg)
	} else {
		n.node = raft.StartNode(n.Cfg, n.RaftPeers)
	}

	go n.proposals()
	go n.serveChannels()
	go n.snapshotAfterFlush()
	go n.deleteEntryLogPeriodically()
	return nil
}

func (n *RaftNode) TransferLeadership(newLeader uint64) {
	// wait for electing a leader
	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}
		if n.node.Status().Lead == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
	// try to transfer to a new leader
	oldLeader := n.node.Status().Lead
	if oldLeader != newLeader {
		n.node.TransferLeadership(n.ctx, oldLeader, newLeader)
	}
	n.logger.Info("raft try to elect a new leader", zap.Uint32("old leader partition", GetPtId(oldLeader)), zap.Uint32("new leader partition", GetPtId(newLeader)))

	var timer = time.NewTimer(10 * time.Second)
	for loop := true; loop && n.node.Status().Lead != newLeader; {
		select {
		case <-timer.C:
			loop = false
			n.logger.Error("try to transfer leadership timeout")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	n.logger.Info("try to transfer leadership finish")
}

// serveChannels handles the raft messages
func (n *RaftNode) serveChannels() {
	var leader bool

	for {
		select {
		case <-n.ctx.Done():
			n.node.Stop()
			return
		case <-n.tick.C:
			n.node.Tick()

		case rd := <-n.node.Ready():
			start := time.Now()
			if rd.SoftState != nil {
				leader = rd.RaftState == raft.StateLeader
			}

			if leader {
				// Leader can send messages in parallel with writing to disk.
				for i := range n.processMessages(rd.Messages) {
					n.send(rd.Messages[i])
				}
				n.logger.Debug("leader send message successful", zap.Duration("time used", time.Since(start)))
				start = time.Now()
			}

			// wal write
			n.SaveToStorage(&rd.HardState, rd.Entries, &rd.Snapshot)
			n.logger.Debug("save to storage successful", zap.Duration("time used", time.Since(start)))
			start = time.Now()

			for rd.MustSync {
				if err := n.Store.TrySync(); err != nil {
					n.logger.Error("Error while calling Store.Sync", zap.Error(err))
					time.Sleep(10 * time.Millisecond)
					continue
				}
				n.logger.Debug("sync store data to disk successful", zap.Duration("time used", time.Since(start)))
				start = time.Now()
				break
			}

			applyDoneC, ok := n.PublishEntries(n.entriesToApply(rd.CommittedEntries))
			if !ok {
				n.Stop()
				return
			}
			n.waitWriteOK(applyDoneC)
			n.logger.Debug("publish entries successful", zap.Duration("time used", time.Since(start)))
			start = time.Now()

			if !leader {
				// Followers should send messages later.
				for i := range n.processMessages(rd.Messages) {
					n.send(rd.Messages[i])
				}
				n.logger.Debug("follower send message successful", zap.Duration("time used", time.Since(start)))
			}

			n.node.Advance()
		}
	}
}

func (n *RaftNode) isLeader() bool {
	r := n.node
	return r.Status().Lead == r.Status().ID
}

// proposals sends proposals over raft
func (n *RaftNode) proposals() {
	confChangeCount := uint64(0)

	for n.proposeC != nil && n.confChangeC != nil {
		select {
		case prop, ok := <-n.proposeC:
			if !ok {
				n.proposeC = nil
			} else {
				// blocks until accepted by raft state machine
				err := n.node.Propose(n.ctx, prop)
				if err != nil {
					n.logger.Error("propose data error", zap.Error(err))
				}
			}
		case cc, ok := <-n.confChangeC:
			if !ok {
				n.confChangeC = nil
			} else {
				confChangeCount++
				cc.ID = confChangeCount
				err := n.node.ProposeConfChange(n.ctx, cc)
				if err != nil {
					n.logger.Error("propose conf change error", zap.Error(err))
				}
			}
		}
	}
	// client closed channel; shutdown raft if not already
	if n.cancelFn != nil {
		n.cancelFn()
	}
}

// SetConfState would store the latest ConfState generated by ApplyConfChange.
func (n *RaftNode) SetConfState(cs *raftpb.ConfState) {
	n.logger.Info(fmt.Sprintf("Setting conf state to %+v\n", cs))
	n.lock.Lock()
	defer n.lock.Unlock()
	n.confState = cs
}

// When there is a `raftpb.EntryConfChange` after creating the snapshot,
// then the confState included in the snapshot is out of date. so We need
// to update the confState before sending a snapshot to a follower.
func (n *RaftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	confState := *n.ConfState()
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = confState
		}
	}
	return ms
}

func (n *RaftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}

	firstIdx := ents[0].Index
	if firstIdx > n.appliedIndex+1 {
		n.logger.Warn(fmt.Sprintf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, n.appliedIndex))
	}

	if n.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[n.appliedIndex-firstIdx+1:]
	}
	return nents
}

// PublishEntries writes committed log entries to Commit channel and returns
// whether all entries could be published.
func (n *RaftNode) PublishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([][]byte, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				continue
			}
			data = append(data, ents[i].Data)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			err := cc.Unmarshal(ents[i].Data)
			if err != nil {
				n.logger.Error("unmarshal conf changed failed", zap.Error(err))
				continue
			}
			n.confState = n.node.ApplyConfChange(cc)
			n.saveConfStateToMeta()
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
			case raftpb.ConfChangeRemoveNode:
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case n.commitC <- &Commit{
			Database:       n.database,
			PtId:           GetPtId(n.id),
			Data:           data,
			ApplyDoneC:     applyDoneC,
			CommittedIndex: ents[len(ents)-1].Index,
		}:
		case <-n.ctx.Done():
			return nil, false
		}
	}

	// after Commit, update appliedIndex
	n.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (n *RaftNode) saveConfStateToMeta() {
	// this is a special snapshot, which is used for store confState. This is to prevent the service from being suspended before the snapshot is taken.
	snapshot := &raftpb.Snapshot{
		Data: nil,
		Metadata: raftpb.SnapshotMetadata{
			ConfState: *n.confState,
			Index:     0,
			Term:      0,
		},
	}
	err := n.Store.Save(nil, nil, snapshot)
	if err != nil {
		n.logger.Error("store confstate error", zap.Error(err))
	}
}

func (n *RaftNode) waitWriteOK(applyDoneC <-chan struct{}) {
	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-n.ctx.Done():
			return
		}
	}
}

// Stop stops this raft node
func (n *RaftNode) Stop() {
	if n.cancelFn != nil {
		n.cancelFn()
	}
	n.once.Do(func() {
		close(n.proposeC)
		close(n.confChangeC)
		close(n.commitC)
		close(n.errorC)
	})
}

func (n *RaftNode) GetProposeC() chan []byte {
	return n.proposeC
}

func (n *RaftNode) GetCommitC() <-chan *Commit {
	return n.commitC
}

// StepRaftMessage sends raft message to the raft state machine
func (n *RaftNode) StepRaftMessage(msg []raftpb.Message) {
	for _, m := range msg {
		err := n.node.Step(n.ctx, m)
		if err != nil || n.ctx.Err() != nil {
			n.logger.Error("step raft message error", zap.Error(err), zap.Any("ctx error", n.ctx.Err()))
		}
	}
}

// ConfState would return the latest ConfState stored in node.
func (n *RaftNode) ConfState() *raftpb.ConfState {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.confState
}

// send sends the given RAFT message from this node.
// TODO: Consider sending msg to channel and then bulk consume
func (n *RaftNode) send(msg raftpb.Message) {
	nodeId := n.peers[GetPtId(msg.To)]
	if nodeId == n.nodeId {
		n.logger.Error("sending message to itself")
		return
	}
	err := n.ISend.SendRaftMessages(nodeId, n.database, GetPtId(msg.To), msg)
	if err != nil {
		n.logger.Error("send raft message error", zap.Error(err))
	}
}

// SaveToStorage saves the hard state, entries, and snapshot to persistent storage, in that order.
func (n *RaftNode) SaveToStorage(h *raftpb.HardState, es []raftpb.Entry, s *raftpb.Snapshot) {
	for {
		if err := n.Store.SaveEntries(h, es, s); err != nil {
			n.logger.Error("While trying to save Raft update. Retrying...", zap.Error(err))
		} else {
			return
		}
	}
}

// PastLife returns the index of the snapshot before the restart (if any) and whether there was
// a previous state that should be recovered after a restart.
func (n *RaftNode) PastLife() (uint64, bool, error) {
	var sp raftpb.Snapshot
	var idx uint64
	var restart bool
	var err error

	sp, err = n.Store.Snapshot()
	if err != nil {
		return 0, false, err
	}
	if !raft.IsEmptySnap(sp) {
		n.logger.Info(fmt.Sprintf("Found Snapshot.Metadata: %+v\n", sp.Metadata))
		restart = true
		idx = sp.Metadata.Index
	}

	var hd raftpb.HardState
	hd, err = n.Store.HardState()
	if err != nil {
		return 0, false, err
	}
	if !raft.IsEmptyHardState(hd) {
		n.logger.Info(fmt.Sprintf("Found hardstate: %+v\n", hd))
		restart = true
	}

	num := n.Store.NumEntries()
	n.logger.Info(fmt.Sprintf("ptId %d found %d entries\n", n.ptId, num))
	// We'll always have at least one entry.
	if num > 1 {
		restart = true
	}
	return idx, restart, nil
}

func (n *RaftNode) snapshotAfterFlush() {
	for {
		select {
		case <-n.SnapShotter.RaftFlushC:
			if err := n.snapShot(); err != nil {
				n.logger.Error("do snapshot error", zap.Error(err))
			}
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *RaftNode) snapShot() error {
	index := n.SnapShotter.CommittedIndex
	for {
		// We should never let CreateSnapshot have an error.
		err := n.Store.CreateSnapshot(index, n.ConfState(), []byte("snapshot"))
		if err == nil {
			break
		}
		if errors.Is(err, raft.ErrSnapOutOfDate) {
			logger.GetLogger().Error("Error while calling CreateSnapshot. ", zap.Error(err))
			break
		}
		logger.GetLogger().Error("Error while calling CreateSnapshot. Retrying...", zap.Error(err))
	}
	return nil
}

func (n *RaftNode) replay(sp raftpb.Snapshot) error {
	hardState, err2 := n.Store.HardState()
	if err2 != nil {
		return err2
	}
	committedIndex := hardState.Commit
	fromIndex := sp.Metadata.Index
	if fromIndex == 0 {
		fromIndex = 1
	}

	first, last := n.Store.GetFirstLast()
	n.logger.Info("raftNode replay range", zap.String("db", n.database), zap.Uint32("ptid", n.ptId), zap.Uint64("lo", fromIndex), zap.Uint64("hi", committedIndex+1), zap.Uint64("first", first), zap.Uint64("last", last))

	entries, err1 := n.Store.Entries(fromIndex, committedIndex+1, math.MaxUint64)
	if err1 != nil {
		return err1
	}
	if len(entries) == 0 {
		return nil
	}

	data := make([][]byte, 0, len(entries))
	for i := range entries {
		switch entries[i].Type {
		case raftpb.EntryNormal:
			if len(entries[i].Data) == 0 {
				// ignore empty messages
				continue
			}
			data = append(data, entries[i].Data)
		}
	}

	if len(data) > 0 {
		select {
		case n.ReplayC <- &Commit{
			Database:   n.database,
			PtId:       GetPtId(n.id),
			Data:       data,
			fromReplay: true,
		}:
		case <-n.ctx.Done():
			return nil
		}
	}
	return nil
}

func (n *RaftNode) CheckAllRgMembers() bool {
	peers := n.peers
	nodeIdSet := make(map[uint64]struct{})
	for _, v := range peers {
		nodeIdSet[v] = struct{}{}
	}
	for k := range nodeIdSet {
		node, _ := n.MetaClient.DataNode(k)
		if node.Status != serf.StatusAlive {
			return false
		}
	}
	return true
}

func (n *RaftNode) deleteEntryLogPeriodically() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := n.deleteEntryLog(); err != nil {
				logger.GetLogger().Error("Error while deleting EntryLog.", zap.Error(err))
			}
		case <-n.ctx.Done():
			logger.GetLogger().Error("ctx done...")
		}
	}
}

func (n *RaftNode) deleteEntryLog() error {
	sp, err := n.Store.Snapshot()
	if err != nil {
		return err
	}
	index := sp.Metadata.Index
	if !n.CheckAllRgMembers() {
		return errors.New("replica group status is unhealthy")
	}
	if n.isLeader() {
		data, err2 := n.prepareDeleteEntryLogProposeData(index)
		if err2 != nil {
			return err2
		}
		n.proposeC <- data
	}
	return nil
}

func (n *RaftNode) prepareDeleteEntryLogProposeData(index uint64) ([]byte, error) {
	filId, _ := n.Store.SlotGe(index)
	progress := n.node.Status().Progress
	var minMatch uint64 = math.MaxUint64
	for _, v := range progress {
		match := v.Match
		if match < minMatch {
			minMatch = match
		}
	}
	var minIndex uint64
	memberFilId, _ := n.Store.SlotGe(minMatch)
	err := n.comparePeerFileIdWithLeaderFileId(memberFilId, filId)
	if err != nil {
		minIndex = minMatch
	} else {
		minIndex = index
	}
	// propose clean entry log
	var dst []byte
	dst = encoding.MarshalUint64(dst, minIndex)
	wrapper := &raftlog.DataWrapper{
		Data:     dst,
		DataType: raftlog.ClearEntryLog,
	}
	marshal := wrapper.Marshal()
	return marshal, nil
}

func (n *RaftNode) comparePeerFileIdWithLeaderFileId(memberFilId int, filId int) error {
	if memberFilId != filId {
		return errors.New("member file id is not equal leader file id ")
	}
	return nil
}

// GetRaftNodeId returns raft node id. Greater 1 than the ptId.
func GetRaftNodeId(ptId uint32) uint64 {
	return uint64(ptId) + 1
}

// GetPtId returns pt view id. Less 1 than the raft node.
func GetPtId(raftNode uint64) uint32 {
	return uint32(raftNode) - 1
}
