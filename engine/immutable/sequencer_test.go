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
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type idTime struct {
	id uint64
	tm int64
}

func TestSingleMeasurement(t *testing.T) {
	idTimes := []idTime{
		{id: 1660001, tm: 1626710820000000000},
		{id: 1660002, tm: 1626710820001000000},
		{id: 1660003, tm: 1626710820002000000},
		{id: 1660004, tm: 1626710820003000000},
		{id: 1660005, tm: 1626710820004000000},
		{id: 1660006, tm: 1626710820005000000},
		{id: 1660007, tm: 1626710820006000000},
		{id: 1660008, tm: 1626710820007000000},
		{id: 1660009, tm: 1626710820008000000},
		{id: 1660010, tm: 1626710820009000000},
		{id: 1660011, tm: 1626710820010000000},
		{id: 1660012, tm: 1626710820011000000},
		{id: 1660013, tm: 1626710820012000000},
		{id: 1660014, tm: 1626710820013000000},
		{id: 1660015, tm: 1626710820014000000},
		{id: 1660016, tm: 1626710820015000000},
		{id: 1660017, tm: 1626710820016000000},
		{id: 1660018, tm: 1626710820017000000},
		{id: 1660019, tm: 1626710820018000000},
		{id: 1660020, tm: 1626710820019000000},
	}

	seq := immutable.NewSequencer()

	idInfo := immutable.GetIDTimePairs("mst1")
	defer immutable.PutIDTimePairs(idInfo)
	for i := range idTimes {
		idInfo.Add(idTimes[i].id, idTimes[i].tm)
	}

	seq.BatchUpdateCheckTime(idInfo, false)

	checkFun := func(seq *immutable.Sequencer) {
		for i := range idTimes {
			it := &idTimes[i]
			lastFlushTm, _ := seq.Get("mst1", it.id)
			if it.tm != lastFlushTm {
				t.Fatalf("exp:%v, get:%v", it.tm, lastFlushTm)
			}
		}
	}

	checkFun(seq)
}

func TestMultiMeasurement(t *testing.T) {
	idTimess := map[string][]idTime{
		"mst1": {
			{id: 166000101, tm: 1626710820000000000}, {id: 166000201, tm: 1626710820001000000},
			{id: 166000301, tm: 1626710820002000000}, {id: 166000401, tm: 1626710820003000000},
			{id: 166000501, tm: 1626710820004000000}, {id: 166000601, tm: 1626710820005000000},
			{id: 166000701, tm: 1626710820006000000}, {id: 166000801, tm: 1626710820007000000},
			{id: 166000901, tm: 1626710820008000000}, {id: 166001001, tm: 1626710820009000000},
			{id: 166001101, tm: 1626710820010000000}, {id: 166001201, tm: 1626710820011000000},
			{id: 166001301, tm: 1626710820012000000}, {id: 166001401, tm: 1626710820013000000},
			{id: 166001501, tm: 1626710820014000000}, {id: 166001601, tm: 1626710820015000000},
			{id: 166001701, tm: 1626710820016000000}, {id: 166001801, tm: 1626710820017000000},
			{id: 166001901, tm: 1626710820018000000}, {id: 166002001, tm: 1626710820019000000},
		},

		"mst2": {
			{id: 166000102, tm: 1626710830000000000}, {id: 166000202, tm: 1626710830001000000},
			{id: 166000302, tm: 1626710830002000000}, {id: 166000402, tm: 1626710830003000000},
			{id: 166000502, tm: 1626710830004000000}, {id: 166000602, tm: 1626710830005000000},
			{id: 166000702, tm: 1626710830006000000}, {id: 166000802, tm: 1626710830007000000},
			{id: 166000902, tm: 1626710830008000000}, {id: 166001002, tm: 1626710830009000000},
			{id: 166001102, tm: 1626710830010000000}, {id: 166001202, tm: 1626710830011000000},
			{id: 166001302, tm: 1626710830012000000}, {id: 166001402, tm: 1626710830013000000},
			{id: 166001502, tm: 1626710830014000000}, {id: 166001602, tm: 1626710830015000000},
			{id: 166001702, tm: 1626710830016000000}, {id: 166001802, tm: 1626710830017000000},
			{id: 166001902, tm: 1626710830018000000}, {id: 166002002, tm: 1626710830019000000},
		},

		"mst3": {
			{id: 166000103, tm: 1626710840000000000}, {id: 166000203, tm: 1626710840001000000},
			{id: 166000303, tm: 1626710840002000000}, {id: 166000403, tm: 1626710840003000000},
			{id: 166000503, tm: 1626710840004000000}, {id: 166000603, tm: 1626710840005000000},
			{id: 166000703, tm: 1626710840006000000}, {id: 166000803, tm: 1626710840007000000},
			{id: 166000903, tm: 1626710840008000000}, {id: 166001003, tm: 1626710840009000000},
			{id: 166001103, tm: 1626710840010000000}, {id: 166001203, tm: 1626710840011000000},
			{id: 166001303, tm: 1626710840012000000}, {id: 166001403, tm: 1626710840013000000},
			{id: 166001503, tm: 1626710840014000000}, {id: 166001603, tm: 1626710840015000000},
			{id: 166001703, tm: 1626710840016000000}, {id: 166001803, tm: 1626710840017000000},
			{id: 166001903, tm: 1626710840018000000}, {id: 166002003, tm: 1626710840019000000},
		},
	}
	seq := immutable.NewSequencer()

	for k, v := range idTimess {
		idInfo := immutable.GetIDTimePairs(k)
		for i := range v {
			idInfo.Add(v[i].id, v[i].tm)
		}
		seq.BatchUpdateCheckTime(idInfo, false)
		immutable.PutIDTimePairs(idInfo)
	}

	checkFun := func(seq *immutable.Sequencer, name string) {
		idTimes := idTimess[name]
		for i := range idTimes {
			it := &idTimes[i]
			lastFlushTm, _ := seq.Get(name, it.id)
			if it.tm != lastFlushTm {
				t.Fatalf("exp:%v, get:%v", it.tm, lastFlushTm)
			}
		}
	}

	for k, _ := range idTimess {
		checkFun(seq, k)
	}
}

func TestMarshalIdTime(t *testing.T) {
	idTimes := immutable.GetIDTimePairs("mst")
	defer immutable.PutIDTimePairs(idTimes)

	id := uint64(1)
	tm := time.Now().Truncate(time.Second).UnixNano()
	count := 4000 + 10
	for i := 0; i < count; i++ {
		idTimes.Add(id, tm)
		idTimes.AddRowCounts(int64(i))
		id++
		tm += time.Millisecond.Nanoseconds()
	}

	ctx := encoding.NewCoderContext()
	defer func() {
		ctx.Release()
	}()

	enc := idTimes.Marshal(true, nil, ctx)
	if len(enc) <= 0 {
		t.Fatalf("encode id time fail")
	}

	encIdTime := immutable.GetIDTimePairs("mst")
	defer immutable.PutIDTimePairs(encIdTime)

	_, err := encIdTime.Unmarshal(true, enc)
	if err != nil {
		t.Fatal(err)
	}

	if encIdTime.Len() != idTimes.Len() {
		t.Fatalf("umarshal id time fail")
	}

	if !reflect.DeepEqual(idTimes.Tms, encIdTime.Tms) {
		t.Fatalf("exp:%v, get:%v", idTimes.Tms, encIdTime.Tms)
	}

	if !reflect.DeepEqual(idTimes.Ids, encIdTime.Ids) {
		t.Fatalf("exp:%v, get:%v", idTimes.Ids, encIdTime.Ids)
	}

	if !reflect.DeepEqual(idTimes.Rows, encIdTime.Rows) {
		t.Fatalf("exp:%v, get:%v", idTimes.Ids, encIdTime.Ids)
	}
}

func TestMarshalIdTime_error(t *testing.T) {
	idTimes := immutable.GetIDTimePairs("mst")
	_, err := idTimes.Unmarshal(true, []byte{0, 0, 0, 0})
	require.EqualError(t, err, "too small data for id time, 4")

	idTimes.Add(1, 1)
	idTimes.AddRowCounts(10)

	buf := idTimes.Marshal(true, nil, encoding.NewCoderContext())
	buf[49] = 20
	_, err = idTimes.Unmarshal(true, buf)
	require.EqualError(t, err, "block smaller (20) data (17) for time length")

	buf[15] = 200
	_, err = idTimes.Unmarshal(true, buf)
	require.EqualError(t, err, "block smaller (200) data (51) for time length")
}

func TestBatchUpdateCheckTime(t *testing.T) {
	seq := immutable.NewSequencer()

	p1 := &immutable.IdTimePairs{
		Name: "mst_1",
		Ids:  []uint64{1, 2},
		Tms:  []int64{10, 20},
		Rows: []int64{1, 2},
	}
	p2 := &immutable.IdTimePairs{
		Name: "mst_1",
		Ids:  []uint64{1, 2},
		Tms:  []int64{15, 18},
		Rows: []int64{1, 2},
	}

	seq.BatchUpdateCheckTime(p1, true)
	for i := range p1.Ids {
		lastTime, rowCount := seq.Get("mst_1", p1.Ids[i])
		assert.Equal(t, lastTime, p1.Tms[i])
		assert.Equal(t, rowCount, p1.Rows[i])
	}

	seq.BatchUpdateCheckTime(p2, true)
	for i := range p1.Ids {
		lastTime, rowCount := seq.Get("mst_1", p1.Ids[i])
		assert.Equal(t, lastTime, maxInt64(p1.Tms[i], p2.Tms[i]))
		assert.Equal(t, rowCount, p1.Rows[i]+p2.Rows[i])
	}

	seq = immutable.NewSequencer()
	config.GetStoreConfig().UnorderedOnly = true
	defer func() {
		config.GetStoreConfig().UnorderedOnly = false
	}()
	seq.BatchUpdateCheckTime(p1, true)
	for i := range p1.Ids {
		lastTime, rowCount := seq.Get("mst_1", p1.Ids[i])
		assert.Equal(t, int64(math.MinInt64), lastTime)
		assert.Equal(t, int64(0), rowCount)
	}
}

func TestDelMmsIdTime(t *testing.T) {
	seq := immutable.NewSequencer()

	p := &immutable.IdTimePairs{
		Name: "mst_1",
		Ids:  []uint64{1, 2},
		Tms:  []int64{10, 20},
		Rows: []int64{1, 2},
	}
	seq.BatchUpdateCheckTime(p, true)
	p.Name = "mst_2"
	seq.BatchUpdateCheckTime(p, true)

	seq.DelMmsIdTime("mst_1")
	seq.DelMmsIdTime("mst_not_exists")

	tm, rows := seq.Get("mst_1", 1)
	require.Equal(t, int64(math.MinInt64), tm)
	require.Equal(t, int64(0), rows)
	require.Equal(t, uint64(2), seq.SeriesTotal())
}

func TestSeriesCounter(t *testing.T) {
	sc := &immutable.SeriesCounter{}

	n := 1000
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			sc.Incr()
			wg.Done()
		}()
	}
	wg.Wait()

	require.Equal(t, uint64(n), sc.Get())
	sc.DecrN(0)
	sc.DecrN(1)
	require.Equal(t, uint64(n-1), sc.Get())
	sc.DecrN(999)
	require.Equal(t, uint64(0), sc.Get())
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
