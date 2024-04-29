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

package engine

import (
	"os"
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/numberenc"
	Assert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardMoveFileInfo_marshal_unmarshal(t *testing.T) {
	info := &ShardMoveFileInfo{
		Name:       "test",
		LocalFile:  []string{"local1", "local2"},
		RemoteFile: []string{"remote1", "remote2"},
	}

	dst := info.marshal([]byte{})

	require.NotNil(t, dst)
	require.Equal(t, len(dst), 48)
	resInfo := &ShardMoveFileInfo{}

	err := resInfo.unmarshal(dst)
	if err != nil {
		t.Fatal(err)
	}
	Assert.Equal(t, info.Name, resInfo.Name)
	for i := range info.LocalFile {
		Assert.Equal(t, info.LocalFile[i], resInfo.LocalFile[i])
		Assert.Equal(t, info.RemoteFile[i], resInfo.RemoteFile[i])
	}
}

func TestReadShardMoveLogFile(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	info := &ShardMoveFileInfo{
		Name:       "test",
		LocalFile:  []string{"local1", "local2"},
		RemoteFile: []string{"remote1", "remote2"},
	}

	dst := info.marshal([]byte{})

	require.NotNil(t, dst)
	require.Equal(t, len(dst), 48)

	fileName := testDir + "test_read_shard_move_log"
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_ULTRA_HIGH)
	fd, err := fileops.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fd.Write(dst)
	resInfo := &ShardMoveFileInfo{}
	err = readShardMoveLogFile(fileName, resInfo)
	if err != nil {
		t.Fatal(err)
	}
	Assert.Equal(t, info.Name, resInfo.Name)
	for i := range info.LocalFile {
		Assert.Equal(t, info.LocalFile[i], resInfo.LocalFile[i])
		Assert.Equal(t, info.RemoteFile[i], resInfo.RemoteFile[i])
	}

	fileName2 := testDir + "wrong_path"
	resInfo.reset()
	err = readShardMoveLogFile(fileName2, resInfo)
	Assert.NotNil(t, err)

	buf := make([]byte, 4)
	fd1, err := fileops.OpenFile(fileName2, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		t.Fatal(err)
	}
	defer fd1.Close()
	_, _ = fd.Write(buf)
	err = readShardMoveLogFile(fileName2, resInfo)
	Assert.NotNil(t, err)
}

func TestShardMoveRecoverReplaceFilesError(t *testing.T) {
	info := &ShardMoveFileInfo{
		Name:       "test",
		LocalFile:  []string{"local1", "local2", "local3"},
		RemoteFile: []string{"remote1", "remote2.init", fileops.ObsPrefix + "remote3.init"},
	}
	lockPath := ""
	err := shardMoveRecoverReplaceFiles(info, &lockPath)
	Assert.NotNil(t, err)
}

func TestUnmarshalError(t *testing.T) {
	info := &ShardMoveFileInfo{
		Name:       "test",
		LocalFile:  []string{"local1", "local2", "local3"},
		RemoteFile: []string{"remote1", "remote2.init", fileops.ObsPrefix + "remote3.init"},
	}
	dst := info.marshal([]byte{})

	dst1 := dst[:1]
	err := info.unmarshal(dst1)
	Assert.NotNil(t, err)

	dst2 := dst[:3]
	err = info.unmarshal(dst2)
	Assert.NotNil(t, err)

	l := int(numberenc.UnmarshalUint16(dst))
	dst3 := dst[:l+3]
	err = info.unmarshal(dst3)
	Assert.NotNil(t, err)

	dst4 := dst[:l+5]
	err = info.unmarshal(dst4)
	Assert.NotNil(t, err)

	dst8 := dst[:l+29]
	err = info.unmarshal(dst8)
	Assert.NotNil(t, err)

	dst9 := dst[:l+31]
	err = info.unmarshal(dst9)
	Assert.NotNil(t, err)

	dst10 := dst[:l+45]
	err = info.unmarshal(dst10)
	Assert.NotNil(t, err)
}
