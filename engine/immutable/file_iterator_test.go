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

package immutable_test

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/require"
)

func TestBufferReader(t *testing.T) {
	saveDir = t.TempDir()
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(1e12, defaultInterval, true)

	for i := 0; i < 10; i++ {
		mh.addRecord(uint64(100+i), rg.generate(getDefaultSchemas(), 5000))
	}
	require.NoError(t, mh.saveToOrder())

	files, ok := mh.store.Order["mst"]
	require.True(t, ok)
	require.Equal(t, 1, files.Len())

	file := files.Files()[0]
	reader := immutable.NewBufferReader(64 * 1024)
	reader.Reset(file)

	fmt.Println("file size", file.FileSize())

	var offset int64 = 100
	var size uint32 = 3 * 1888
	var buf []byte
	_ = buf

	for i := 0; i < 50; i++ {
		buf1, err1 := reader.Read(offset, size)

		buf = buf[:0]
		buf2, err2 := file.ReadData(offset, size, &buf, fileops.IO_PRIORITY_LOW_READ)

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.Equal(t, len(buf2), len(buf1))
		require.Equal(t, buf2, buf1)
		offset += int64(size)
	}

	for i := 0; i < 50; i++ {
		offset -= 10
		buf1, err1 := reader.Read(offset, size)
		buf2, err2 := file.ReadData(offset, size, &buf, fileops.IO_PRIORITY_LOW_READ)

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.Equal(t, buf2, buf1)
	}
}

func TestColumnIterator_Error(t *testing.T) {
	defer beforeTest(t, 0)

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())

	rg := newRecordGenerator(1e15, defaultInterval, true)
	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	files, ok := mh.store.GetTSSPFiles("mst", true)
	require.True(t, ok)

	file := files.Files()[0]

	fi := immutable.NewFileIterator(file, logger.NewLogger(errno.ModuleMerge))
	require.NoError(t, file.Close())
	ci := immutable.NewColumnIterator(fi)

	p := &MockPerformer{}
	require.NotEmpty(t, ci.IterCurrentChunk(p))
	ci.Close()
}
