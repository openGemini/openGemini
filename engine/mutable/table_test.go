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

package mutable_test

import (
	"os"
	"path"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSizeLimit(t *testing.T) {
	mutable.SetSizeLimit(0)
	require.Equal(t, int64(1024*1024), mutable.GetSizeLimit())

	var size int64 = 1024 * 1024 * 64
	mutable.SetSizeLimit(size)
	require.Equal(t, size, mutable.GetSizeLimit())
}

func TestSidsPool(t *testing.T) {
	mutable.InitMutablePool(cpu.GetCpuNum())

	sids := mutable.GetSidsImpl(4)
	mutable.PutSidsImpl(sids)

	sids = mutable.GetSidsImpl(5)
	mutable.PutSidsImpl(sids)

	sids = mutable.GetSidsImpl(1)
	mutable.PutSidsImpl(sids)
}

func TestMemTable_GetMaxTimeBySidNoLock(t *testing.T) {
	tbl := mutable.NewMemTable(config.TSSTORE)
	row := &influx.Row{}
	row.Fields = append(row.Fields, influx.Field{
		Key:      "foo",
		NumValue: 1,
		StrValue: "",
		Type:     influx.Field_Type_Int,
	})
	msInfo := tbl.CreateMsInfo("mst", row, nil)
	chunk, _ := msInfo.CreateChunk(100)
	chunk.OrderWriteRec.SetLastAppendTime(100)
	chunk.UnOrderWriteRec.SetLastAppendTime(200)

	require.Equal(t, int64(200), tbl.GetMaxTimeBySidNoLock("mst", 100))
}

func TestStoreLoadMstRowCount(t *testing.T) {
	dir := t.TempDir()
	err := os.MkdirAll(path.Join(dir, immutable.ColumnStoreDirName, "mst_0001"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	countFile := path.Join(dir, immutable.ColumnStoreDirName, "mst_0001", immutable.CountBinFile)
	rowCount := 8192
	err = mutable.StoreMstRowCount(countFile, rowCount)
	if err != nil {
		t.Fatal(err)
	}
	count, err := mutable.LoadMstRowCount(countFile)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, count, 8192)

	countFile = path.Join(dir, immutable.ColumnStoreDirName, "mst_0001", "count.bin")
	_, err = mutable.LoadMstRowCount(countFile)
	assert.Equal(t, err != nil, true)

	countFile = path.Join(dir, immutable.ColumnStoreDirName, "mst_0001", immutable.CountBinFile)
	file, _ := os.OpenFile(countFile, os.O_WRONLY|os.O_TRUNC, 0640)
	_, _ = file.WriteString("")
	_, err = mutable.LoadMstRowCount(countFile)
	assert.Equal(t, err != nil, true)
}
