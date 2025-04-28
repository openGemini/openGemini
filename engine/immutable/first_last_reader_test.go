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

package immutable_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestFirstLastReader_ReadMinMaxFromPreAgg(t *testing.T) {
	setCompressModeSelf()
	defer setCompressModeNone()
	defer beforeTest(t, 0)()

	file, release, err := createOneTsspFile()
	require.NoError(t, err)
	defer release()

	reader := &immutable.FirstLastReader{}

	iterateChunkMeta(file, func(cm *immutable.ChunkMeta) {
		cm.Validation()
		cols := cm.GetColMeta()
		_, _, ok := reader.ReadMinFromPreAgg(&cols[0])
		require.False(t, ok)

		_, _, ok = reader.ReadMaxFromPreAgg(&cols[0])
		require.False(t, ok)

		_, _, ok = reader.ReadMinFromPreAgg(&cols[1])
		require.True(t, ok)

		_, _, ok = reader.ReadMaxFromPreAgg(&cols[2])
		require.True(t, ok)

		buf := cols[2].GetPreAgg()
		buf[0], buf[1] = 100, 100
		_, _, ok = reader.ReadMaxFromPreAgg(&cols[2])
		require.False(t, ok)
	})
}

func TestReadFirstOrLast(t *testing.T) {
	setCompressModeSelf()
	defer setCompressModeNone()
	defer beforeTest(t, 0)()

	file, release, err := createOneTsspFile()
	require.NoError(t, err)
	defer release()

	dst := record.NewRecord(
		[]record.Field{
			{Name: "time", Type: influx.Field_Type_Int},
			{Name: "time", Type: influx.Field_Type_Int},
		},
		true)
	ref := &record.Field{Name: record.TimeField, Type: influx.Field_Type_Int}
	ctx := immutable.NewReadContext(true)
	lockPath := ""
	cr, err := immutable.NewTSSPFileReader(file.Path(), &lockPath)
	if err != nil {
		t.Fatal(err)
	}
	reader := &immutable.FirstLastReader{}

	fi := immutable.NewFileIterator(file, immutable.CLog)
	itr := immutable.NewChunkIterator(fi)
	itr.NextChunkMeta()
	cm := itr.GetCurtChunkMeta()
	itr.IncrChunkUsed()
	cm.Validation()

	// first: the first row
	reader.Init(cm, cr, ref, dst, true)
	err = reader.ReadTime(ctx, true, 2)
	require.NoError(t, err)
	firstValue, _ := dst.RecMeta.ColMeta[0].First()
	require.Equal(t, firstValue.(int64), int64(1000000000000))

	// last: the last row
	reader.Init(cm, cr, ref, dst, false)
	err = reader.ReadTime(ctx, true, 2)
	require.NoError(t, err)
	lastValue, _ := dst.RecMeta.ColMeta[0].Last()
	require.Equal(t, lastValue.(int64), int64(1999000000000))

	// last: the middle row
	reader.Release()
	reader.Init(cm, cr, ref, dst, false)
	ctx.SetTr(util.TimeRange{Min: 1000000000000, Max: 1990000000000})
	err = reader.ReadTime(ctx, true, 2)
	require.NoError(t, err)
	lastValue, _ = dst.RecMeta.ColMeta[0].Last()
	require.Equal(t, lastValue.(int64), int64(1990000000000))

	// last: empty row
	ctx.SetTr(util.TimeRange{Min: 0, Max: 0})
	err = reader.ReadTime(ctx, true, 2)
	require.NoError(t, err)
}
