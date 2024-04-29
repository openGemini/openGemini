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
	"sort"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLastMergeTime(t *testing.T) {
	lmt := immutable.NewLastMergeTime()
	assert.False(t, lmt.Nearly("mst_0", time.Second*10))

	lmt.Update("mst_1")
	time.Sleep(time.Millisecond * 10)
	assert.False(t, lmt.Nearly("mst_0", time.Millisecond))
	assert.True(t, lmt.Nearly("mst_1", time.Second*10))
}

func TestMergeContext(t *testing.T) {
	ctx := immutable.NewMergeContext("mst")
	ctx.Release()
	ctx = immutable.NewMergeContext("mst")

	beforeTest(t, 0)
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()

	rg := newRecordGenerator(1e15, defaultInterval, true)
	for i := 0; i < 5; i++ {
		mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
		require.NoError(t, mh.saveToUnordered())
	}

	files := mh.store.OutOfOrder["mst"].Files()
	require.Equal(t, 5, len(files))

	sort.Slice(files, func(i, j int) bool {
		return i == 2
	})

	for _, f := range files {
		ctx.AddUnordered(f)
	}

	ctx.Sort()
	ctx.Release()
}

func TestColumnIterator(t *testing.T) {
	defer beforeTest(t, 0)

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()

	rg := newRecordGenerator(1e15, defaultInterval, true)
	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	files, ok := mh.store.GetTSSPFiles("mst", true)
	require.True(t, ok)

	fi := immutable.NewFileIterator(files.Files()[0], logger.NewLogger(errno.ModuleMerge))
	ci := immutable.NewColumnIterator(fi)

	p := &MockPerformer{}
	require.NoError(t, ci.Run(p))
	ci.Close()
}

func TestColumnIterator_Close(t *testing.T) {
	defer beforeTest(t, 0)

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()

	rg := newRecordGenerator(1e15, defaultInterval, true)
	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	mh.addRecord(101, rg.generate(getDefaultSchemas(), 10))
	mh.addRecord(102, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	files, ok := mh.store.GetTSSPFiles("mst", true)
	require.True(t, ok)

	for i := 0; i < 3; i++ {
		fi := immutable.NewFileIterator(files.Files()[0], logger.NewLogger(errno.ModuleMerge))
		ci := immutable.NewColumnIterator(fi)
		p := &MockPerformer{}
		if i == 0 {
			p.HandleHook = func() {
				ci.Close()
			}
		} else if i == 1 {
			p.ColumnChangedHook = func() {
				ci.Close()
			}
		} else {
			p.SeriesChangedHook = func() {
				ci.Close()
			}
		}

		require.EqualError(t, ci.Run(p), "column iterator closed")
	}
}

type MockPerformer struct {
	err error

	SeriesChangedHook func()
	FinishHook        func()
	ColumnChangedHook func()
	HandleHook        func()
	WriteOriginalHook func()
}

func (p *MockPerformer) Handle(col *record.ColVal, times []int64, lastSeg bool) error {
	if p.HandleHook != nil {
		p.HandleHook()
	}
	return p.err
}

func (p *MockPerformer) ColumnChanged(*record.Field) error {
	if p.ColumnChangedHook != nil {
		p.ColumnChangedHook()
	}
	return p.err
}

func (p *MockPerformer) SeriesChanged(uint64, []int64) error {
	if p.SeriesChangedHook != nil {
		p.SeriesChangedHook()
	}

	return p.err
}

func (p *MockPerformer) HasSeries(sid uint64) bool {
	return true
}

func (p *MockPerformer) WriteOriginal(fi *immutable.FileIterator) error {
	if p.WriteOriginalHook != nil {
		p.WriteOriginalHook()
	}
	return p.err
}

func (p *MockPerformer) Finish() error {
	if p.FinishHook != nil {
		p.FinishHook()
	}
	return p.err
}

func TestMergeTimes(t *testing.T) {
	a := []int64{1, 2, 8}
	b := []int64{2, 6, 7}

	require.Equal(t, a, immutable.MergeTimes(a, nil, nil))
	require.Equal(t, b, immutable.MergeTimes(nil, b, nil))
	require.Equal(t, []int64{1, 2, 6, 7, 8}, immutable.MergeTimes(a, b, nil))
}

func TestFillNilCol(t *testing.T) {
	col := &record.ColVal{}
	col.AppendIntegers(1, 2, 3, 4, 5)
	ref := &record.Field{
		Type: influx.Field_Type_String,
		Name: "foo",
	}

	immutable.FillNilCol(col, 0, ref)
	require.Equal(t, 0, col.Len)

	immutable.FillNilCol(col, 10, ref)
	require.Equal(t, 10, col.Len)
	require.Equal(t, 10, col.NilCount)
	require.Equal(t, 10, len(col.Offset))

	ref.Type = influx.Field_Type_Int
	immutable.FillNilCol(col, 10, ref)
	require.Equal(t, 10, col.Len)
	require.Equal(t, 10, col.NilCount)
	require.Equal(t, 0, len(col.Offset))
}

func TestBuildMergeContext(t *testing.T) {
	files := &immutable.TSSPFiles{}
	require.Empty(t, immutable.BuildMergeContext("mst", files))
	require.Empty(t, immutable.BuildFullMergeContext("mst", files))
}
