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
