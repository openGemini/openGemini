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
package immutable

import (
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/stretchr/testify/assert"
)

func TestReadPKData(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()
	mstName := "mst"
	err := writeData(testCompDir, mstName)
	if err != nil {
		t.Errorf(err.Error())
	}
	dataPath := path.Join(testCompDir, mstName)
	var opt *obs.ObsOptions

	pkRecNum := 2

	pkInfoReader, err := NewDetachedPKMetaInfoReader(dataPath, opt)
	if err != nil {
		t.Fatal(err)
	}
	pkInfo, err := pkInfoReader.Read()
	if err != nil {
		t.Fatal(err)
	}
	pkMetaReader, err := NewDetachedPKMetaReader(dataPath, opt)
	if err != nil {
		t.Fatal(err)
	}
	pmOffsets, pmLengths := make([]int64, 0, pkRecNum), make([]int64, 0, pkRecNum)
	for i := 0; i < pkRecNum; i++ {
		offset, length := GetPKMetaOffsetLengthByChunkId(pkInfo, i)
		pmOffsets, pmLengths = append(pmOffsets, offset), append(pmLengths, length)
	}
	pkMetas, err := pkMetaReader.Read(pmOffsets, pmLengths)
	if err != nil {
		t.Fatalf(fmt.Sprintf("read pk meta err: %s", err.Error()))
	}

	reader, err := NewDetachedPKDataReader(dataPath, opt)
	if err != nil {
		t.Fatal(err)
	}
	reader.SetPkMetaInfo(pkInfo)
	pdOffset, pdLength := make([]int64, 0, len(pkMetas)), make([]int64, 0, len(pkMetas))
	for i := range pkMetas {
		pdOffset = append(pdOffset, int64(pkMetas[i].Offset))
		pdLength = append(pdLength, int64(pkMetas[i].Length))
	}
	pkDatas, err := reader.Read(pdOffset, pdLength, pkMetas)
	if err != nil {
		t.Fatalf(fmt.Sprintf("read pk data err: %s", err.Error()))
	}
	pkItems := make([]*colstore.DetachedPKInfo, 0, len(pkDatas))
	for i := range pkDatas {
		pkItems = append(pkItems, colstore.GetPKInfoByPKMetaData(pkMetas[i], pkDatas[i], pkInfo.TCLocation))
	}
	assert.Equal(t, pkItems[0].Data.RowNums(), 385)
	assert.Equal(t, pkItems[0].StartBlockId, uint64(0))
	assert.Equal(t, pkItems[0].EndBlockId, uint64(384))
	assert.Equal(t, pkItems[1].Data.RowNums(), 257)
	assert.Equal(t, pkItems[1].StartBlockId, uint64(384))
	assert.Equal(t, pkItems[1].EndBlockId, uint64(640))

	_, err = NewDetachedPKMetaInfoReader(dataPath, &obs.ObsOptions{})
	assert.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
	_, err = NewDetachedPKMetaReader(dataPath, &obs.ObsOptions{})
	assert.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
	_, err = NewDetachedPKDataReader(dataPath, &obs.ObsOptions{})
	assert.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
}
