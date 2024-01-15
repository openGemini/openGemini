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

package immutable_test

import (
	"errors"
	"io"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/stretchr/testify/require"
)

func TestIndexCompressWriter_error(t *testing.T) {
	dir := t.TempDir()

	writer := &immutable.IndexCompressWriter{}
	size, err := writer.SwitchMetaBuffer()
	require.NoError(t, err)
	require.Equal(t, 0, size)

	require.Equal(t, 0, len(writer.MetaDataBlocks(nil)))

	lock := ""
	writer.Init(dir, &lock, false, false)
	_, _ = writer.Write([]byte{1, 2, 3})

	_, err = writer.SwitchMetaBuffer()
	require.NotEmpty(t, err)

	writer = &immutable.IndexCompressWriter{}
	writer.Init(dir+"/index.init", &lock, false, false)
	defer writer.Close()

	_, _ = writer.Write(make([]byte, 1024*1500))
	_, err = writer.SwitchMetaBuffer()
	require.NoError(t, err)

	writeErr := errors.New("mock write failed")
	mw := &MockWriter{n: 1024, writeErr: writeErr}
	bw := writer.GetWriter()
	bw.Reset(mw)

	_, _ = writer.Write(make([]byte, 1024*1500))
	_, err = writer.SwitchMetaBuffer()
	require.EqualError(t, err, writeErr.Error())

	_, err = writer.CopyTo(io.Discard)
	require.EqualError(t, err, writeErr.Error())
}

func TestStreamIterators_SwitchChunkMeta_error(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 10; i++ {
		rg.incrBegin(15)
		mh.addRecord(uint64(100+i), rg.generate(getDefaultSchemas(), 10))
		require.NoError(t, mh.saveToOrder())
	}

	plans := mh.store.ImmTable.LevelPlan(mh.store, 0)
	fi, err := mh.store.ImmTable.NewFileIterators(mh.store, plans[0])
	require.NoError(t, err)

	itrs := mh.store.NewStreamIterators(fi)

	writer := &mockFileWriter{n: 10}
	itrs.WithLog(logger.NewLogger(errno.ModuleCompact))
	itrs.SetWriter(writer)

	cm := &immutable.ChunkMeta{}
	cm.AllocColMeta(&record.Field{
		Type: 0,
		Name: "foo",
	})
	_, err = itrs.WriteChunkMeta(cm)
	require.NotEmpty(t, err)

	writer.writeErr = errors.New("mock write failed")
	_, err = itrs.WriteChunkMeta(cm)
	require.ErrorContains(t, err, writer.writeErr.Error())

	writer.n = 0
	require.ErrorContains(t, itrs.SwitchChunkMeta(), writer.writeErr.Error())

	writer.writeErr = nil
	writer.switchErr = errors.New("mock switch failed")
	require.ErrorContains(t, itrs.SwitchChunkMeta(), writer.switchErr.Error())
}

type MockWriter struct {
	n        int
	writeErr error
	closeErr error
}

func (w *MockWriter) Write(p []byte) (int, error) {
	return w.n, w.writeErr
}

func (w *MockWriter) Close() error {
	return w.closeErr
}

type mockFileWriter struct {
	fileops.FileWriter

	n         int
	writeErr  error
	switchErr error
}

func (w *mockFileWriter) WriteChunkMeta(p []byte) (int, error) {
	n := w.n
	if n == 0 {
		n = len(p)
	}

	return n, w.writeErr
}

func (w *mockFileWriter) Write(p []byte) (int, error) {
	n := w.n
	if n == 0 {
		n = len(p)
	}

	return n, w.writeErr
}

func (w *mockFileWriter) Name() string {
	return "name"
}

func (w *mockFileWriter) SwitchMetaBuffer() (int, error) {
	return w.n, w.switchErr
}
