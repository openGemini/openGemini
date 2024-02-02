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
	"testing"

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
