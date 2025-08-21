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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestEntryLogs_allEntries(t *testing.T) {
	tmpDir := t.TempDir()
	el, err := openEntryLogs(tmpDir)
	require.NoError(t, err)
	defer el.Close()

	err = el.AddEntries([]raftpb.Entry{{Index: 2, Term: 4}, {Index: 3, Term: 4}})
	require.NoError(t, err)

	t.Run("before the range", func(t *testing.T) {
		entries := el.allEntries(1, 2, maxNumEntries)
		require.Equal(t, 0, len(entries))

	})

	t.Run("within the range", func(t *testing.T) {
		entries := el.allEntries(2, 4, maxNumEntries)
		require.Equal(t, 2, len(entries))
		assert.Equal(t, 2, int(entries[0].Index))
		assert.Equal(t, 4, int(entries[0].Term))

	})

	t.Run("after the range", func(t *testing.T) {
		entries := el.allEntries(4, 6, maxNumEntries)
		require.Equal(t, 0, len(entries))

	})
}

func TestEntryLogs_Term(t *testing.T) {
	tmpDir := t.TempDir()
	el, err := openEntryLogs(tmpDir)
	require.NoError(t, err)
	defer el.Close()

	err = el.AddEntries([]raftpb.Entry{{Index: 2, Term: 4}, {Index: 3, Term: 4}})
	require.NoError(t, err)

	t.Run("compacted index", func(t *testing.T) {
		_, err = el.Term(1)
		require.EqualError(t, err, "requested index is unavailable due to compaction")
	})

	t.Run("specify existed index", func(t *testing.T) {
		term, err := el.Term(2)
		require.NoError(t, err)
		assert.Equal(t, 4, int(term))

	})

	t.Run("unavailable index", func(t *testing.T) {
		_, err = el.Term(4)
		require.EqualError(t, err, "requested entry at index is unavailable")
	})
}

func TestEntryLogs_firstIndex(t *testing.T) {
	tmpDir := t.TempDir()
	el, err := openEntryLogs(tmpDir)
	require.NoError(t, err)
	defer el.Close()

	t.Run("firstIndex for empty entry", func(t *testing.T) {
		idx := el.firstIndex()
		assert.Equal(t, 1, int(idx))

	})

	t.Run("firstIndex", func(t *testing.T) {
		err = el.AddEntries([]raftpb.Entry{{Index: 2, Term: 4}, {Index: 3, Term: 4}})
		require.NoError(t, err)

		idx := el.firstIndex()
		assert.Equal(t, 2, int(idx))

	})
}

func TestEntryLogs_lastIndex(t *testing.T) {
	tmpDir := t.TempDir()
	el, err := openEntryLogs(tmpDir)
	require.NoError(t, err)
	defer el.Close()

	t.Run("lastIndex for empty entry", func(t *testing.T) {
		idx := el.lastIndex()
		assert.Equal(t, 0, int(idx))

	})

	t.Run("lastIndex", func(t *testing.T) {
		err = el.AddEntries([]raftpb.Entry{{Index: 2, Term: 4}, {Index: 3, Term: 4}})
		require.NoError(t, err)

		idx := el.lastIndex()
		assert.Equal(t, 3, int(idx))
	})
}

func TestEntryLogs_allEntriesErr(t *testing.T) {
	tmpDir := t.TempDir()
	el, err := openEntryLogs(tmpDir)
	require.NoError(t, err)
	defer el.Close()
	el.current.entry.WriteSlice(0, 0, 1048576, nil, false, false)
	buf := el.current.getEntry(0)
	marshalEntry(buf, 4, 2, 0, 1148576)
	el.current.entry.WriteAt(0, 0, buf, false)
	entries := el.allEntries(2, 3, maxNumEntries)
	require.Equal(t, 0, len(entries))
}
