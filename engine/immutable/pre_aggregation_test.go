// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/stretchr/testify/require"
)

func TestIntegerPreAgg(t *testing.T) {
	agg := NewIntegerPreAgg()
	col := &record.ColVal{}
	var times []int64

	for i := 0; i < 10; i++ {
		if i > 5 {
			col.AppendInteger(int64(i))
		} else {
			col.AppendIntegerNull()
		}
		times = append(times, int64(i))
	}

	agg.addValues(col, times)
	val, time := agg.max()
	require.Equal(t, int64(9), val.(int64))
	require.Equal(t, int64(9), time)

	val, time = agg.min()
	require.Equal(t, int64(6), val.(int64))
	require.Equal(t, int64(6), time)
}

func TestFloatPreAgg(t *testing.T) {
	agg := NewFloatPreAgg()
	col := &record.ColVal{}
	var times []int64

	for i := 0; i < 10; i++ {
		if i > 5 {
			col.AppendFloat(float64(i))
		} else {
			col.AppendFloatNull()
		}
		times = append(times, int64(i))
	}

	agg.addValues(col, times)
	val, time := agg.max()
	require.Equal(t, float64(9), val.(float64))
	require.Equal(t, int64(9), time)

	val, time = agg.min()
	require.Equal(t, float64(6), val.(float64))
	require.Equal(t, int64(6), time)
}

type PreAggCodec interface {
	marshal(dst []byte) []byte
	unmarshal(src []byte) ([]byte, error)
}

func assertPreAggCodec(t *testing.T, a, b PreAggCodec) {
	require.NotEmpty(t, a)
	require.NotEmpty(t, b)

	buf := a.marshal(nil)
	_, err := b.unmarshal(buf)
	require.NoError(t, err)

	require.Equal(t, a, b)
}

func TestPreAggOnlyOneLine(t *testing.T) {
	var times []int64 = []int64{100}
	col := &record.ColVal{}

	floatAgg := NewFloatPreAgg()
	col.Init()
	col.AppendFloat(1.1)
	floatAgg.addValues(col, times)
	assertPreAggCodec(t, floatAgg, NewFloatPreAgg())

	intAgg := NewIntegerPreAgg()
	col.Init()
	col.AppendInteger(111)
	intAgg.addValues(col, times)
	assertPreAggCodec(t, intAgg, NewIntegerPreAgg())
}

func TestConvert(t *testing.T) {
	builder := &PreAggBuilders{}
	require.NotNil(t, builder.FloatBuilder())
	require.NotNil(t, builder.IntegerBuilder())
}

func TestFloatPreAgg_add(t *testing.T) {
	agg := NewFloatPreAgg()
	var assertMin = func(v float64, tm int64) {
		require.Equal(t, v, agg.minV)
		require.Equal(t, tm, agg.minTime)
	}
	var assertMax = func(v float64, tm int64) {
		require.Equal(t, v, agg.maxV)
		require.Equal(t, tm, agg.maxTime)
	}

	agg.addMin(1, 10)
	agg.addMin(2, 11)
	agg.addMin(1, 20)
	assertMin(1.0, 10)

	agg.addMin(0.1, 30)
	assertMin(0.1, 30)

	agg.addMax(1, 10)
	agg.addMax(0.1, 20)
	agg.addMax(1, 20)
	assertMax(1.0, 10)

	agg.addMax(2, 30)
	assertMax(2.0, 30)
}

func TestFloatPreAggVLC(t *testing.T) {
	var assert = func(agg *FloatPreAgg) {
		buf := agg.marshal(nil)
		require.True(t, len(buf) != 16)
		require.True(t, len(buf) > 0)
		require.True(t, len(buf) <= 48)

		other := NewFloatPreAgg()
		_, err := other.unmarshal(buf)
		require.NoError(t, err)
		require.Equal(t, agg, other)
	}
	SetChunkMetaCompressMode(ChunkMetaCompressSelf)
	defer SetChunkMetaCompressMode(ChunkMetaCompressNone)

	now := time.Now().UnixNano() + 1
	minValues := []float64{0, 1, 1.11, 0}
	maxValues := []float64{0, 2, 2.22, 0}
	sumValues := []float64{0, 333, 33.333, 0}
	minTimes := []int64{10, 10000, now, now}
	maxTimes := []int64{11, 11000, now - time.Hour.Nanoseconds()*12, now}
	countValues := []int64{10, 10, now, 1 << 22}

	agg := NewFloatPreAgg()
	for i := range minValues {
		agg.reset()
		agg.addMin(minValues[i], minTimes[i])
		agg.addMax(maxValues[i], maxTimes[i])
		agg.addCount(countValues[i])
		agg.addSum(sumValues[i])
		assert(agg)
	}
}

func TestIntegerPreAggVLC(t *testing.T) {
	var assert = func(agg *IntegerPreAgg) {
		buf := agg.marshal(nil)
		require.True(t, len(buf) != 16)
		require.True(t, len(buf) > 0)
		require.True(t, len(buf) <= 48)

		other := NewIntegerPreAgg()
		_, err := other.unmarshal(buf)
		require.NoError(t, err)
		require.Equal(t, agg, other)
	}
	SetChunkMetaCompressMode(ChunkMetaCompressSelf)
	defer SetChunkMetaCompressMode(ChunkMetaCompressNone)

	now := time.Now().UnixNano() + 1
	minValues := []float64{0, 1, 1 << 60, 1}
	maxValues := []float64{0, 2, 1 << 60, 3}
	sumValues := []float64{0, 333, 1 << 60, 333}
	minTimes := []int64{10, 10000, now, now}
	maxTimes := []int64{11, 11000, now - time.Hour.Nanoseconds()*12, now + 101}
	countValues := []int64{10, 10, now, 12}

	agg := NewIntegerPreAgg()
	for i := range minValues {
		agg.reset()
		agg.addMin(minValues[i], minTimes[i])
		agg.addMax(maxValues[i], maxTimes[i])
		agg.addCount(countValues[i])
		agg.addSum(sumValues[i])
		assert(agg)
	}
}

func TestDecodeAggTimes(t *testing.T) {
	minTime := int64(1725345354123000000)
	maxTime := int64(1725355355123000000)

	var buf []byte
	buf = codec.AppendInt64WithScale(buf, minTime)
	buf = codec.AppendInt64WithScale(buf, maxTime-minTime)

	_, minTime1, maxTime1, err := DecodeAggTimes(buf)
	require.NoError(t, err)
	require.Equal(t, minTime, minTime1)
	require.Equal(t, maxTime, maxTime1)

	buf[0] = 8
	_, _, _, err = DecodeAggTimes(buf)
	require.NotEmpty(t, err)

	fmt.Println(buf)
	buf[0] = 2
	buf[7] = 8
	_, _, _, err = DecodeAggTimes(buf)
	require.NotEmpty(t, err)
}
