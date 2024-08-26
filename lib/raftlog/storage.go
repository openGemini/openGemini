/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package raftlog

import (
	"fmt"
	"math"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	raftEntriesDir    = "__raft_entries__"
	allEntriesMaxSize = 64 << 20
)

// RaftDiskStorage handles disk access and writing for the RAFT write-ahead log.
// Dir contains wal.meta file and <start idx zero padded>.entry files.
type RaftDiskStorage struct {
	dir string // wal/db0/pt0/__raft_entries__

	logger   *logger.Logger
	meta     *metaFile
	entryLog *entryLog
	lock     sync.Mutex
}

// Init initializes an instance of DiskStorage.
func Init(dir string) (*RaftDiskStorage, error) {
	dir = filepath.Join(dir, raftEntriesDir)
	rds := &RaftDiskStorage{
		dir:    dir,
		logger: logger.NewLogger(errno.ModuleStorageEngine),
	}

	lockFile := fileops.FileLockOption("")
	if err := fileops.MkdirAll(dir, 0750, lockFile); err != nil {
		return nil, errors.Wrapf(err, "mkdir failed:%s", dir)
	}

	var err error
	if rds.meta, err = newMetaFile(dir); err != nil {
		return nil, err
	}

	if rds.entryLog, err = openEntryLogs(dir); err != nil {
		return nil, err
	}

	snap, err := rds.meta.snapshot()
	if err != nil {
		return nil, err
	}

	first, err := rds.FirstIndexWithSnap()
	if err != nil {
		return nil, err
	}
	if !raft.IsEmptySnap(snap) {
		if snap.Metadata.Index+1 != first {
			return nil, errors.Newf("snap index: %d + 1 should be equal to first: %d\n", snap.Metadata.Index, first)
		}
	}

	// If db is not closed properly, there might be index ranges for which delete entries are not
	// inserted. So insert delete entries for those ranges starting from 0 to (first-1).
	rds.entryLog.deleteBefore(first - 1)

	last := rds.entryLog.lastIndex()
	rds.logger.Info("Init Raft Storage with snap", zap.Uint64("index", snap.Metadata.Index), zap.Uint64("first", first), zap.Uint64("last", last))
	return rds, nil
}

func (rds *RaftDiskStorage) SaveEntries(h *raftpb.HardState, entries []raftpb.Entry, snap *raftpb.Snapshot) error {
	if err := rds.Save(h, entries, snap); err != nil {
		return err
	}
	return nil
}

func (rds *RaftDiskStorage) SetUint(info MetaInfo, id uint64) { rds.meta.SetUint(info, id) }
func (rds *RaftDiskStorage) Uint(info MetaInfo) uint64        { return rds.meta.Uint(info) }

func (rds *RaftDiskStorage) Exist() bool {
	fis, err := fileops.ReadDir(rds.dir)
	if err != nil {
		return false
	}
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) == logSuffix {
			return true
		}
	}
	return false
}

func (rds *RaftDiskStorage) HardState() (raftpb.HardState, error) {
	if rds.meta == nil {
		return raftpb.HardState{}, errors.Errorf("uninitialized meta file")
	}
	return rds.meta.HardState()
}

// Implement the Raft.Storage interface.
// -------------------------------------

// InitialState returns the saved HardState and ConfState information.
func (rds *RaftDiskStorage) InitialState() (hs raftpb.HardState, cs raftpb.ConfState, err error) {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	rds.logger.Info("InitialState")
	defer rds.logger.Info("Done")
	hs, err = rds.meta.HardState()
	if err != nil {
		return
	}
	var snap raftpb.Snapshot
	snap, err = rds.meta.snapshot()
	if err != nil {
		return
	}
	return hs, snap.Metadata.ConfState, nil
}

func (rds *RaftDiskStorage) NumEntries() int {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	start := rds.entryLog.firstIndex()

	var count int
	for {
		ents := rds.entryLog.allEntries(start, math.MaxUint64, allEntriesMaxSize)
		if len(ents) == 0 {
			return count
		}
		count += len(ents)
		start = ents[len(ents)-1].Index + 1
	}
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (rds *RaftDiskStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	first := rds.entryLog.firstIndex()
	if lo < first {
		rds.logger.Error(fmt.Sprintf("lo: %d < first: %d\n", lo, first))
		return nil, raft.ErrCompacted
	}

	last := rds.entryLog.lastIndex()
	if hi > last+1 {
		rds.logger.Error(fmt.Sprintf("hi: %d > last+1: %d\n", hi, last+1))
		return nil, raft.ErrUnavailable
	}

	ents := rds.entryLog.allEntries(lo, hi, maxSize)
	return ents, nil
}

func (rds *RaftDiskStorage) Term(idx uint64) (uint64, error) {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	term, err := rds.entryLog.Term(idx)
	if err != nil {
		rds.logger.Error(fmt.Sprintf("TERM for %d = %v\n", idx, err))
	}
	return term, err
}

func (rds *RaftDiskStorage) LastIndex() (uint64, error) {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	li := rds.entryLog.lastIndex()
	si := rds.meta.Uint(SnapshotIndex)
	if li < si {
		return si, nil
	}
	return li, nil
}

// FirstIndex implements the Storage interface.
func (rds *RaftDiskStorage) FirstIndex() (uint64, error) {
	rds.lock.Lock()
	defer rds.lock.Unlock()
	return rds.firstIndex(), nil
}

// FirstIndex implements the Storage interface.
func (rds *RaftDiskStorage) FirstIndexWithSnap() (uint64, error) {
	rds.lock.Lock()
	defer rds.lock.Unlock()
	if si := rds.Uint(SnapshotIndex); si > 0 {
		return si + 1, nil
	}
	return rds.entryLog.firstIndex(), nil
}

func (rds *RaftDiskStorage) firstIndex() uint64 {
	return rds.entryLog.firstIndex()
}

// Snapshot returns the most recent snapshot.  If snapshot is temporarily
// unavailable, it should return ErrSnapshotTemporarilyUnavailable, so raft
// state machine could know that Storage needs some time to prepare snapshot
// and call Snapshot later.
func (rds *RaftDiskStorage) Snapshot() (raftpb.Snapshot, error) {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	return rds.meta.snapshot()
}

// ---------------- Raft.Storage interface complete.

// CreateSnapshot generates a snapshot with the given ConfState and data and writes it to disk.
func (rds *RaftDiskStorage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) error {
	rds.logger.Info(fmt.Sprintf("CreateSnapshot i=%d, cs=%+v", i, cs))

	rds.lock.Lock()
	defer rds.lock.Unlock()

	first := rds.firstIndex()
	if i < first {
		rds.logger.Error(fmt.Sprintf("i=%d<first=%d, ErrSnapOutOfDate", i, first))
		return raft.ErrSnapOutOfDate
	}

	e, err := rds.entryLog.seekEntry(i)
	if err != nil {
		return err
	}

	var snap raftpb.Snapshot
	snap.Metadata.Index = i
	snap.Metadata.Term = e.Term()
	if cs == nil {
		panic("raft ConfState is nil")
	}
	snap.Metadata.ConfState = *cs
	snap.Data = data

	if err = rds.meta.StoreSnapshot(&snap); err != nil {
		return err
	}
	return nil
}

func (rds *RaftDiskStorage) DeleteBefore(index uint64) {
	// Now we delete all the files which are below the snapshot index.
	rds.entryLog.deleteBefore(index)
}

func (rds *RaftDiskStorage) SlotGe(index uint64) (int, int) {
	return rds.entryLog.slotGe(index)
}

// Save would write Entries, HardState and Snapshot to persistent storage in order, i.e. Entries
// first, then HardState and Snapshot if they are not empty. If persistent storage supports atomic
// writes then all of them can be written together. Note that when writing an Entry with Index i,
// any previously-persisted entries with Index >= i must be discarded.
func (rds *RaftDiskStorage) Save(h *raftpb.HardState, entries []raftpb.Entry, snap *raftpb.Snapshot) error {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	if err := rds.entryLog.AddEntries(entries); err != nil {
		return err
	}

	if err := rds.meta.StoreHardState(h); err != nil {
		return err
	}
	if err := rds.meta.StoreSnapshot(snap); err != nil {
		return err
	}
	return nil
}

// TrySync trys to write all the contents to disk.
func (rds *RaftDiskStorage) TrySync() error {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	if err := rds.meta.meta.TrySync(); err != nil {
		return errors.Wrapf(err, "while syncing meta")
	}
	if err := rds.entryLog.current.entry.TrySync(); err != nil {
		return errors.Wrapf(err, "while syncing current file")
	}
	return nil
}

// Close closes the DiskStorage.
func (rds *RaftDiskStorage) Close() error {
	if err := rds.meta.Close(); err != nil {
		return errors.Wrap(err, "close meta file")
	}
	if err := rds.entryLog.Close(); err != nil {
		return errors.Wrap(err, "close entryLog file")
	}
	return nil
}
