/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package obs

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/stretchr/testify/assert"
)

func TestNewStoreReader(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	fn := tmpDir + "/" + "test.data"

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}
	bytes := make([]byte, 48)
	binary.LittleEndian.PutUint64(bytes[0:8], 1)
	binary.LittleEndian.PutUint64(bytes[8:16], 0)
	binary.LittleEndian.PutUint64(bytes[16:24], 1)
	binary.LittleEndian.PutUint64(bytes[24:32], 0)
	binary.LittleEndian.PutUint64(bytes[32:40], 0)
	binary.LittleEndian.PutUint32(bytes[40:44], 4)
	binary.LittleEndian.PutUint32(bytes[44:48], 1)
	for i := 0; i < 1000; i++ {
		fd.Write(bytes)
	}
	fd.Sync()
	_, err = NewReader(&LogPath{}, "test.data.err")
	assert.Equal(t, false, IsInterfaceNil(err))

	reader, _ := NewReader(&LogPath{}, tmpDir+"/test.data")
	defer reader.Close()
	result := make(map[int64][]byte)
	err = reader.Read([]int64{0}, []int64{48}, 48, result, 8)
	reader.StartSpan(nil)
	reader.EndSpan()
	assert.Equal(t, true, IsInterfaceNil(err))
	assert.Equal(t, 1, len(result))
	result2 := make([]byte, 48000)
	err = reader.ReadTo(0, result2)
	assert.Equal(t, true, IsInterfaceNil(err))
	assert.Equal(t, 48000, len(result2))
	c := make(chan *request.StreamReader, 3)
	v, _ := reader.Len()
	assert.Equal(t, 48000, int(v))
	go reader.StreamRead([]int64{0}, []int64{48}, 48, c, 8)
	count := 0
	for {
		select {
		case r, ok := <-c:
			if !ok {
				assert.Equal(t, 1, 1)
				return
			}
			if count >= 100 {
				t.Errorf("get wrong data")
				return
			}
			count += 1
			assert.Equal(t, 48, len(r.Content))
		}
	}
}

func TestNewOBSReader(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	fn := tmpDir + "/" + "test.data"

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}
	bytes := make([]byte, 48)
	binary.LittleEndian.PutUint64(bytes[0:8], 1)
	binary.LittleEndian.PutUint64(bytes[8:16], 0)
	binary.LittleEndian.PutUint64(bytes[16:24], 1)
	binary.LittleEndian.PutUint64(bytes[24:32], 0)
	binary.LittleEndian.PutUint64(bytes[32:40], 0)
	binary.LittleEndian.PutUint32(bytes[40:44], 4)
	binary.LittleEndian.PutUint32(bytes[44:48], 1)
	for i := 0; i < 1000; i++ {
		fd.Write(bytes)
	}
	fd.Sync()
	_, err = NewObsReader(&LogPath{}, "test.data")
	assert.Equal(t, false, IsInterfaceNil(err))
	str := Join("test", "test1", "test2")
	assert.Equal(t, "test/test1/test2", str)
	reader := &ObsReader{}
	reader.StartSpan(nil)
	defer reader.EndSpan()
	defer reader.Close()
}
