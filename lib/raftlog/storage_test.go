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
	"crypto/rand"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestStorageTerm(t *testing.T) {
	tmpDir := t.TempDir()
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		name string
		i    uint64

		werr   error
		wterm  uint64
		wpanic bool
	}{
		{"ErrCompacted", 2, raft.ErrCompacted, 0, false},
		{"Normal3", 3, nil, 3, false},
		{"Normal4", 4, nil, 4, false},
		{"Normal5", 5, nil, 5, false},
		{"ErrUnavailable", 6, raft.ErrUnavailable, 0, false},
	}

	rds, err := Init(tmpDir, 0)
	require.NoError(t, err)
	defer rds.Close()
	err = rds.Save(nil, ents, nil)
	require.NoError(t, err)

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()

			term, err := rds.Term(tt.i)
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
			if term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
			}
		})
	}
}

func TestStorageTerm2(t *testing.T) {
	tmpDir := t.TempDir()
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}

	sp := &raftpb.Snapshot{
		Data: []byte("foo"),
		Metadata: raftpb.SnapshotMetadata{
			Term:  7,
			Index: 12,
		},
	}
	rds, err := Init(tmpDir, 0)
	require.NoError(t, err)
	defer rds.Close()
	err = rds.Save(nil, ents, sp)
	require.NoError(t, err)

	tests := []struct {
		name string
		i    uint64

		werr   error
		wterm  uint64
		wpanic bool
	}{
		{"ErrCompacted", 11, raft.ErrCompacted, 0, false},
		{"Normal3", 12, nil, 7, false},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()

			term, err := rds.Term(tt.i)
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
			if term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
			}
		})
	}
}

func TestStorageEntries(t *testing.T) {
	tmpDir := t.TempDir()
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}
	tests := []struct {
		name            string
		lo, hi, maxsize uint64

		werr     error
		wentries []raftpb.Entry
	}{
		{"ErrCompacted", 2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		//{"ErrCompacted#2", 3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{"Normal4", 4, 5, math.MaxUint64, nil, []raftpb.Entry{{Index: 4, Term: 4}}},
		{"Normal4-5", 4, 6, math.MaxUint64, nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{"Normal4-6", 4, 7, math.MaxUint64, nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
		// even if maxsize is zero, the first entry should be returned
		{"Normal4_maxsize_is_zero", 4, 7, 0, nil, []raftpb.Entry{{Index: 4, Term: 4}}},
		// limit to 2
		{"Normal4-5_limit_to_2_maxsize#1", 4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// limit to 2
		{"Normal4-5_limit_to_2_maxsize#1", 4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{"Normal4-5_limit_to_2_maxsize#1", 4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// all
		{"Normal_all", 4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
	}

	rds, err := Init(tmpDir, 0)
	require.NoError(t, err)
	defer rds.Close()
	err = rds.Save(nil, ents, nil)
	require.NoError(t, err)

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entries, err := rds.Entries(tt.lo, tt.hi, tt.maxsize)
			assert.Equal(t, tt.werr, err, "#%d: err = %v, want %v", i, err, tt.werr)
			assert.EqualValues(t, tt.wentries, entries, "#%d: entries = %v, want %v", i, entries, tt.wentries)
		})
	}
}

func TestStorageLastIndex(t *testing.T) {
	tmpDir := t.TempDir()
	rds, err := Init(tmpDir, 0)
	require.NoError(t, err)
	defer rds.Close()

	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	err = rds.Save(nil, ents, nil)
	require.NoError(t, err)

	last, err := rds.LastIndex()
	require.NoError(t, err)
	require.EqualValues(t, 5, last)

	err = rds.Save(nil, []raftpb.Entry{{Index: 6, Term: 5}}, nil)
	require.NoError(t, err)
	last, err = rds.LastIndex()
	require.NoError(t, err)
	require.EqualValues(t, 6, last)
}

func TestStorageFirstIndex(t *testing.T) {
	tmpDir := t.TempDir()
	rds, err := Init(tmpDir, 0)
	require.NoError(t, err)
	defer rds.Close()

	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	err = rds.Save(nil, ents, nil)
	require.NoError(t, err)

	first, err := rds.FirstIndex()
	require.NoError(t, err)
	require.EqualValues(t, 3, first)
}

func TestStorageAppend(t *testing.T) {
	config.EntryFileRWType = 1
	defer func() { config.EntryFileRWType = 2 }()
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		name    string
		entries []raftpb.Entry

		werr     error
		wentries []raftpb.Entry
	}{
		{
			"case1", []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
		},
		{
			"case2", []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
		},
		{
			"case3", []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
		// truncate incoming entries, truncate the existing entries and append
		//{
		//	"case4", []raftpb.Entry{{Index: 2, Term: 3}, {Index: 3, Term: 3}, {Index: 4, Term: 5}},
		//	nil,
		//	[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		//},
		// truncate the existing entries and append
		{
			"case5", []raftpb.Entry{{Index: 4, Term: 5}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// direct append
		{
			"case6", []raftpb.Entry{{Index: 6, Term: 5}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			rds, err := Init(tmpDir, 0)
			require.NoError(t, err)
			defer rds.Close()
			err = rds.Save(nil, ents, nil)
			require.NoError(t, err)

			err = rds.entryLog.AddEntries(tt.entries)
			require.Equal(t, tt.werr, err, "#%d: err = %v, want %v", i, err, tt.werr)

			all := rds.entryLog.allEntries(0, math.MaxUint64, math.MaxUint64)
			require.EqualValues(t, tt.wentries, all, "#%d: entries = %v, want %v", i, all, tt.wentries)
		})

	}
}

func TestMetaFile(t *testing.T) {
	dir := t.TempDir()

	mf, err := newMetaFile(dir)
	require.NoError(t, err)
	defer mf.Close()
	id := mf.Uint(RaftId)
	require.Zero(t, id)

	mf.SetUint(RaftId, 10)
	id = mf.Uint(RaftId)
	require.NoError(t, err)
	require.Equal(t, uint64(10), id)

	hs, err := mf.HardState()
	require.NoError(t, err)
	require.Zero(t, hs)

	hs = raftpb.HardState{
		Term:   10,
		Vote:   20,
		Commit: 30,
	}
	require.NoError(t, mf.StoreHardState(&hs))

	hs1, err := mf.HardState()
	require.NoError(t, err)
	require.Equal(t, hs1, hs)

	sp, err := mf.snapshot()
	require.NoError(t, err)
	require.Zero(t, sp)

	sp = raftpb.Snapshot{
		Data: []byte("foo"),
		Metadata: raftpb.SnapshotMetadata{
			Term:  200,
			Index: 12,
		},
	}
	require.NoError(t, mf.StoreSnapshot(&sp))

	sp1, err := mf.snapshot()
	require.NoError(t, err)
	require.Equal(t, sp, sp1)
}

func TestEntryFile(t *testing.T) {
	dir := t.TempDir()
	el, err := openEntryLogs(dir)
	require.NoError(t, err)
	defer el.Close()
	require.Equal(t, uint64(1), el.firstIndex())
	require.Zero(t, el.lastIndex())

	e, err := el.seekEntry(2)
	require.Error(t, err)
	require.NotNil(t, e)

	require.NoError(t, el.AddEntries([]raftpb.Entry{{Index: 1, Term: 1, Data: []byte("abc")}}))
	entries := el.allEntries(0, 100, 10000)
	require.Equal(t, 1, len(entries))
	require.Equal(t, uint64(1), entries[0].Index)
	require.Equal(t, uint64(1), entries[0].Term)
	require.Equal(t, "abc", string(entries[0].Data))
}

func TestStorageBig(t *testing.T) {
	config.EntryFileRWType = 1
	defer func() { config.EntryFileRWType = 2 }()
	test := func(t *testing.T) {
		dir := t.TempDir()

		rds, err := Init(dir, 0)
		require.NoError(t, err)
		defer rds.Close()

		ent := raftpb.Entry{
			Term: 1,
			Type: raftpb.EntryNormal,
		}

		addEntries := func(start, end uint64) {
			t.Logf("adding entries: %d -> %d\n", start, end)
			for idx := start; idx <= end; idx++ {
				ent.Index = idx
				err = rds.entryLog.AddEntries([]raftpb.Entry{ent})
				require.NoError(t, err)
				li, err := rds.LastIndex()
				require.NoError(t, err)
				require.Equal(t, idx, li)
			}
		}

		N := uint64(100000) // 3 times maxNumEntries
		addEntries(1, N)
		err = rds.TrySync()
		require.NoError(t, err)

		num := rds.NumEntries()
		require.Equal(t, int(N), num)

		check := func(start, end uint64) {
			ents, err := rds.Entries(start, end, math.MaxInt64)
			require.NoError(t, err)
			require.Equal(t, int(end-start), len(ents))
			for i, e := range ents {
				require.Equal(t, start+uint64(i), e.Index)
			}
		}
		_, err = rds.Entries(0, 1, math.MaxInt64)
		require.Equal(t, raft.ErrCompacted, err)

		check(3, N)
		check(10000, 20000)
		check(20000, 33000)
		check(33000, 45000)
		check(45000, N)

		// Around file boundaries.
		check(1, N)
		check(30000, N)
		check(30001, N)
		check(60000, N)
		check(60001, N)
		check(60000, 90000)
		check(N, N+1)

		_, err = rds.Entries(N+1, N+10, math.MaxInt64)
		require.Error(t, raft.ErrUnavailable, err)

		// Jump back a few files.
		addEntries(N/3, N)
		err = rds.TrySync()
		require.NoError(t, err)
		check(3, N)
		check(10000, 20000)
		check(20000, 33000)
		check(33000, 45000)
		check(45000, N)
		check(N, N+1)

		buf := make([]byte, 128)
		_, _ = rand.Read(buf)

		cs := &raftpb.ConfState{}
		require.NoError(t, rds.CreateSnapshot(N-100, cs, buf))
		rds.DeleteBefore(N - 100)
		fi, err := rds.FirstIndexWithSnap()
		require.NoError(t, err)
		require.Equal(t, N-100+1, fi)

		snap, err := rds.Snapshot()
		require.NoError(t, err)
		require.Equal(t, N-100, snap.Metadata.Index)
		require.Equal(t, buf, snap.Data)

		require.Equal(t, 0, len(rds.entryLog.files))

		files, err := getLogFiles(filepath.Join(dir, raftEntriesDir))
		require.NoError(t, err)
		require.Equal(t, 1, len(files))

		// Jumping back.
		ent.Index = N - 50
		require.NoError(t, rds.entryLog.AddEntries([]raftpb.Entry{ent}))

		start := N - 100 + 1
		ents := rds.entryLog.allEntries(start, math.MaxInt64, math.MaxInt64)
		require.Equal(t, 50, len(ents))
		for idx, ent := range ents {
			require.Equal(t, int(start)+idx, int(ent.Index))
		}

		ent.Index = N
		require.NoError(t, rds.entryLog.AddEntries([]raftpb.Entry{ent}))
		ents = rds.entryLog.allEntries(start, math.MaxInt64, math.MaxInt64)
		require.Equal(t, 51, len(ents))
		for idx, ent := range ents {
			if idx == 50 {
				require.Equal(t, N, ent.Index)
			} else {
				require.Equal(t, int(start)+idx, int(ent.Index))
			}
		}

		ks, err := Init(dir, 0)
		require.NoError(t, err)
		defer ks.Close()
		ents = ks.entryLog.allEntries(start, math.MaxInt64, math.MaxInt64)
		require.Equal(t, 51, len(ents))
		for idx, ent := range ents {
			if idx == 50 {
				require.Equal(t, N, ent.Index)
			} else {
				require.Equal(t, int(start)+idx, int(ent.Index))
			}
		}
	}
	t.Run("with more entries", func(t *testing.T) { test(t) })
}

func getEntryForData(index uint64) raftpb.Entry {
	data := []byte("test data")
	return raftpb.Entry{Index: index, Term: 1, Type: raftpb.EntryNormal, Data: data}
}

func TestRaftDiskStorage_InitialState(t *testing.T) {
	temp := t.TempDir()
	rds, err := Init(temp, 0)
	require.NoError(t, err)
	defer rds.Close()

	entries := []raftpb.Entry{getEntryForData(1), getEntryForData(2), getEntryForData(3)}
	err = rds.Save(&raftpb.HardState{Term: 1}, entries, &raftpb.Snapshot{})
	require.NoError(t, err)

	hs, cs, err := rds.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(1), hs.Term)
	require.Equal(t, 2, cs.Size())
}

func TestRaftDiskStorage_FirstIndex_LastIndex(t *testing.T) {
	temp := t.TempDir()
	rds, err := Init(temp, 0)
	require.NoError(t, err)
	defer rds.Close()

	entries := []raftpb.Entry{getEntryForData(1), getEntryForData(2), getEntryForData(3)}
	err = rds.Save(&raftpb.HardState{}, entries, &raftpb.Snapshot{})
	require.NoError(t, err)

	idx, err := rds.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx)

	idx, err = rds.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), idx)
}

func TestRaftDiskStorage_AddEntries(t *testing.T) {
	temp := t.TempDir()
	rds, err := Init(temp, 0)
	require.NoError(t, err)
	defer rds.Close()

	entries := []raftpb.Entry{getEntryForData(1), getEntryForData(2), getEntryForData(3)}
	err = rds.Save(&raftpb.HardState{}, entries, &raftpb.Snapshot{})
	require.NoError(t, err)

	idx, err := rds.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx)

	idx, err = rds.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), idx)

	entries = []raftpb.Entry{getEntryForData(4)}
	err = rds.Save(&raftpb.HardState{}, entries, &raftpb.Snapshot{})
	idx, err = rds.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), idx)

	entries = []raftpb.Entry{getEntryForData(4)}
	err = rds.Save(&raftpb.HardState{}, entries, &raftpb.Snapshot{})
	idx, err = rds.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), idx)

	entries = []raftpb.Entry{getEntryForData(5)}
	err = rds.Save(&raftpb.HardState{}, entries, &raftpb.Snapshot{})
	idx, err = rds.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), idx)
}

func TestStorageEntrySize(t *testing.T) {
	config.EntryFileRWType = 1
	defer func() { config.EntryFileRWType = 2 }()
	temp := t.TempDir()
	rds, err := Init(temp, 0)
	require.NoError(t, err)
	defer rds.Close()

	entries := []raftpb.Entry{getEntryForData(1), getEntryForData(2), getEntryForData(3)}
	err = rds.Save(&raftpb.HardState{}, entries, &raftpb.Snapshot{})

	size := rds.EntrySize()
	require.Equal(t, 1048615, size)

	ent := raftpb.Entry{
		Term: 1,
		Type: raftpb.EntryNormal,
	}

	addEntries := func(start, end uint64) {
		t.Logf("adding entries: %d -> %d\n", start, end)
		for idx := start; idx <= end; idx++ {
			ent.Index = idx
			err = rds.entryLog.AddEntries([]raftpb.Entry{ent})
			require.NoError(t, err)
			li, err := rds.LastIndex()
			require.NoError(t, err)
			require.Equal(t, idx, li)
		}
	}

	N := uint64(100000) // 3 times maxNumEntries
	addEntries(1, N)

	size = rds.EntrySize()
	require.Equal(t, 4594304, size)
}

func TestStorageBackSync(t *testing.T) {
	temp := t.TempDir()
	rds, err := Init(temp, 1)
	require.NoError(t, err)
	defer rds.Close()

	rds.firstSync = false
	rds.TrySync()
	time.Sleep(1 * time.Second)

	rds.syncTaskCount.Store(1)
	err = rds.TrySync()
	require.NoError(t, err)
}

func TestStorageBackSyncErr(t *testing.T) {
	config.EntryFileRWType = 1
	defer func() { config.EntryFileRWType = 2 }()
	temp := t.TempDir()
	rds, err := Init(temp, 0)
	require.NoError(t, err)

	rds.entryLog.current.entry.Close()
	if err = rds.TrySync(); err != nil {
		t.Fatal("TestStorageBackSyncErr fail")
	}
	rds.meta.meta.Close()
	if err = rds.TrySync(); err != nil {
		t.Fatal("TestStorageBackSyncErr fail")
	}
}
