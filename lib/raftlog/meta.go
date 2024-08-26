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

/*
Copyright 2020 Dgraph Labs, Inc. and Contributors
This code is originally from: https://github.com/dgraph-io/dgraph/tree/20.11.0-rc2/raftwal/meta.go
*/

package raftlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type MetaInfo int

const (
	RaftId MetaInfo = iota
	GroupId
	CheckpointIndex
	SnapshotIndex
	SnapshotTerm
)

const (
	RaftIdOffset          = 0
	GroupIdOffset         = 8
	CheckpointIndexOffset = 16
)

// getOffset returns offsets in wal.meta file.
func getOffset(info MetaInfo) int {
	switch info {
	case RaftId:
		return RaftIdOffset
	case GroupId:
		return GroupIdOffset
	case CheckpointIndex:
		return CheckpointIndexOffset
	case SnapshotIndex:
		return snapshotIndex
	case SnapshotTerm:
		return snapshotIndex + unit64Size
	default:
		panic("Invalid info: " + fmt.Sprint(info))
	}
}

// Constants to use when writing to meta and entry files.
const (
	// metaName is the name of the file used to store metadata (e.g raft ID, group ID).
	metaName = "raft.meta"
	// metaFileSize is the size of the raft.meta file.
	metaFileSize = 1 << 20 // 1MB
	// hardStateOffset is the offset of the hard state within the wal.meta file.
	hardStateOffset = 512
	// snapshotIndex stores the index and term corresponding to the snapshot.
	snapshotIndex = 1024
	// snapshotOffset is the offset of the snapshot within the wal.meta file.
	snapshotOffset = snapshotIndex + 16
)

// metaFile stores the RAFT metadata (e.g RAFT ID, snapshot, hard state).
type metaFile struct {
	dir string

	meta   FileWrapper
	logger *logger.Logger
}

// newMetaFile opens the meta file in the given directory.
func newMetaFile(dir string) (*metaFile, error) {
	mf := &metaFile{
		dir:    dir,
		logger: logger.NewLogger(errno.ModuleStorageEngine),
	}
	if err := mf.lazyOpen(); err != nil {
		return nil, err
	}
	return mf, nil
}

func (m *metaFile) lazyOpen() error {
	if m.meta != nil {
		return nil
	}
	var err error
	fname := filepath.Join(m.dir, metaName)
	// Open the file in read-write mode and creates it if it doesn't exist.
	m.meta, err = OpenFile(fname, os.O_RDWR|os.O_CREATE, metaFileSize)
	if errors.Is(err, NewFile) {
		err = nil
	}
	return errors.Wrapf(err, "unable to open meta file")
}

func (m *metaFile) bufAt(info MetaInfo) []byte {
	var data []byte
	if m.meta != nil {
		pos := getOffset(info)
		data = m.meta.GetEntryData(pos, pos+8)
	}
	return data
}

func (m *metaFile) Uint(info MetaInfo) uint64 {
	return binary.BigEndian.Uint64(m.bufAt(info))
}

func (m *metaFile) SetUint(info MetaInfo, id uint64) {
	if m.meta == nil {
		return
	}

	binary.BigEndian.PutUint64(m.bufAt(info), id)
	offset := getOffset(info)
	_, err := m.meta.WriteAt(int64(offset), m.bufAt(info))
	if err != nil {
		panic(fmt.Sprintf("unable to write meta file: %s, err: %+v", filepath.Join(m.dir, m.meta.Name()), err))
	}
}

func (m *metaFile) StoreHardState(hs *raftpb.HardState) error {
	if hs == nil || raft.IsEmptyHardState(*hs) {
		return nil
	}
	buf, err := hs.Marshal()
	if err != nil {
		return errors.Wrapf(err, "cannot marshal hard state")
	}
	if len(buf) >= snapshotIndex-hardStateOffset {
		return fmt.Errorf("invalid HardState")
	}
	if err = m.meta.WriteSlice(hardStateOffset, buf); err != nil {
		return err
	}
	return nil
}

func (m *metaFile) HardState() (raftpb.HardState, error) {
	m.meta.Name()
	val := m.meta.ReadSlice(hardStateOffset)
	var hs raftpb.HardState

	if len(val) == 0 {
		return hs, nil
	}
	if err := hs.Unmarshal(val); err != nil {
		return hs, errors.Wrapf(err, "cannot parse hardState")
	}
	return hs, nil
}

func (m *metaFile) StoreSnapshot(snap *raftpb.Snapshot) error {
	if snap == nil {
		return nil
	}
	if !IsValidSnapshot(*snap) {
		return nil
	}
	m.SetUint(SnapshotIndex, snap.Metadata.Index)
	m.SetUint(SnapshotTerm, snap.Metadata.Term)

	buf, err := snap.Marshal()
	if err != nil {
		return errors.Wrapf(err, "cannot marshal snapshot")
	}

	if err = m.meta.WriteSlice(snapshotOffset, buf); err != nil {
		return err
	}
	return nil
}

func IsValidSnapshot(snapshot raftpb.Snapshot) bool {
	if !raft.IsEmptySnap(snapshot) {
		return true
	}
	if snapshot.Metadata.ConfState.Voters != nil {
		return true
	}
	return false
}

func (m *metaFile) snapshot() (raftpb.Snapshot, error) {
	val := m.meta.ReadSlice(snapshotOffset)

	var snap raftpb.Snapshot
	if len(val) == 0 {
		return snap, nil
	}

	if err := snap.Unmarshal(val); err != nil {
		return snap, errors.Wrapf(err, "cannot parse snapshot")
	}
	return snap, nil
}

func (m *metaFile) Close() error {
	return m.meta.Close()
}
