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

package raftlog

import (
	"encoding/binary"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestMetaFile_StoreHardState_HardState(t *testing.T) {
	temp := t.TempDir()

	meta, err := newMetaFile(temp)
	require.NoError(t, err)
	defer meta.Close()

	var hs = raftpb.HardState{Term: 1, Vote: 10, Commit: 100}
	err = meta.StoreHardState(&hs)
	require.NoError(t, err)

	got, err := meta.HardState()
	require.NoError(t, err)
	require.EqualValues(t, hs, got)
}

func TestMetaFile_StoreSnapshot_snapshot(t *testing.T) {
	temp := t.TempDir()
	meta, err := newMetaFile(temp)
	require.NoError(t, err)
	defer meta.Close()

	var snap = raftpb.Snapshot{Data: []byte("hello"), Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 100}}
	err = meta.StoreSnapshot(&snap)
	require.NoError(t, err)

	got, err := meta.snapshot()
	require.NoError(t, err)
	require.EqualValues(t, snap, got)

	idx := meta.Uint(SnapshotIndex)
	require.Equal(t, 10, int(idx))
}

func TestIsValidSnapshot(t *testing.T) {
	var snap = raftpb.Snapshot{Data: []byte("hello"), Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 100}}
	isValid := IsValidSnapshot(snap)
	require.True(t, isValid)
	state := raftpb.ConfState{
		Voters: make([]uint64, 0),
	}
	snapShot2 := raftpb.Snapshot{Data: []byte("hello"),
		Metadata: raftpb.SnapshotMetadata{ConfState: state},
	}
	isValid = IsValidSnapshot(snapShot2)
	require.True(t, isValid)
}

func TestGetRaftEntryLog(t *testing.T) {
	filewrap := &memoryFileWrap{}

	logFile := &logFile{
		fid:   -1,
		entry: filewrap,
	}
	// write data
	fixedSizeSlice := make([]byte, 2048)
	filewrap.WriteSlice(100, fixedSizeSlice)
	// write slot
	entry := entry(make([]byte, entrySize))
	marshalEntry(entry, 1, 1, 1, 100)
	filewrap.WriteAt(0, entry)

	raftEntry, _ := logFile.getRaftEntry(0)
	require.NotNil(t, raftEntry)

	// Simulate the file damage scenario.
	filewrap.WriteSlice(3048, fixedSizeSlice)
	entry2 := make([]byte, entrySize)
	marshalEntry(entry2, 1, 1, 1, 6000)
	filewrap.WriteAt(0, entry2)

	raftEntry, err := logFile.getRaftEntry(0)
	require.NotNil(t, raftEntry)
	require.Error(t, err, "valid offset error")
}

type memoryFileWrap struct {
	data []byte
}

func (e *memoryFileWrap) Name() string {
	return "test"
}

func (e *memoryFileWrap) Size() int {
	return len(e.data)
}

func (e *memoryFileWrap) GetEntryData(start, end int) []byte {
	return e.data[start:end]
}
func (e *memoryFileWrap) Write(dat []byte) (int, error) {
	e.data = append(e.data, dat...)
	return len(e.data), nil
}
func (e *memoryFileWrap) WriteAt(offset int64, dat []byte) (int, error) {
	copy(e.data[offset:], dat)
	return len(e.data), nil
}
func (e *memoryFileWrap) WriteSlice(offset int64, dat []byte) error {
	if int(offset)+unit32Size+len(dat) >= len(e.data) {
		e.data = append(e.data, make([]byte, int(offset)-len(e.data)+unit32Size+len(dat))...)
	}
	dst := e.data[offset:]
	binary.BigEndian.PutUint32(dst[:unit32Size], uint32(len(dat))) // size
	copy(dst[unit32Size:], dat)                                    // data
	return nil
}
func (e *memoryFileWrap) ReadSlice(offset int64) []byte {
	sz := encoding.UnmarshalUint32(e.data[offset:])
	start := offset + unit32Size
	next := int(start) + int(sz)
	if next > len(e.data) {
		return []byte{}
	}
	return e.data[start:next]
}
func (e *memoryFileWrap) SliceSize(offset int) int {
	sz := encoding.UnmarshalUint32(e.data[offset:])
	return unit32Size + int(sz)
}
func (e *memoryFileWrap) Truncate(size int64) error {
	return nil
}
func (e *memoryFileWrap) TrySync() error {
	return nil
}
func (e *memoryFileWrap) Delete() error {
	e.data = nil
	return nil
}
func (e *memoryFileWrap) Close() error {
	return nil
}
func (e *memoryFileWrap) setCurrent() {

}
func (e *memoryFileWrap) rotateCurrent() {

}
