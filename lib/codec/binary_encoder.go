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

package codec

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/util"
)

func AppendInt(b []byte, i int) []byte {
	return AppendInt64(b, int64(i))
}

func AppendUint8(b []byte, i uint8) []byte {
	b = append(b, i)
	return b
}

func AppendBool(b []byte, v bool) []byte {
	if v {
		return AppendUint8(b, 1)
	}
	return AppendUint8(b, 0)
}

func AppendUint16(b []byte, i uint16) []byte {
	return encoding.MarshalUint16(b, i)
}

func AppendUint32(b []byte, i uint32) []byte {
	return encoding.MarshalUint32(b, i)
}

func AppendUint64(b []byte, i uint64) []byte {
	return encoding.MarshalUint64(b, i)
}

func AppendInt16(b []byte, i int16) []byte {
	return encoding.MarshalInt16(b, i)
}

func AppendInt32(b []byte, i int32) []byte {
	i = (i << 1) ^ (i >> 31)

	return AppendUint32(b, uint32(i))
}

func AppendInt64(b []byte, i int64) []byte {
	return encoding.MarshalInt64(b, i)
}

func AppendFloat32(b []byte, f float32) []byte {
	return AppendFloat64(b, float64(f))
}

func AppendFloat64(b []byte, f float64) []byte {
	i := util.Float64ToUint64(f)
	return AppendUint64(b, i)
}

func AppendIntSlice(b []byte, a []int) []byte {
	if len(a) == 0 {
		return AppendUint32(b, 0)
	}

	a64 := make([]int64, len(a))
	for i := range a {
		a64[i] = int64(a[i])
	}

	return AppendInt64Slice(b, a64)
}

func AppendInt16Slice(b []byte, a []int16) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, util.Int16Slice2byte(a)...)
	return b
}

func AppendInt32Slice(b []byte, a []int32) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, util.Int32Slice2byte(a)...)
	return b
}

func AppendInt64Slice(b []byte, a []int64) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, util.Int64Slice2byte(a)...)
	return b
}

func AppendUint16Slice(b []byte, a []uint16) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, util.Uint16Slice2byte(a)...)
	return b
}

func AppendUint32Slice(b []byte, a []uint32) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, util.Uint32Slice2byte(a)...)
	return b
}

func AppendUint64Slice(b []byte, a []uint64) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, util.Uint64Slice2byte(a)...)
	return b
}

func AppendFloat32Slice(b []byte, a []float32) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, util.Float32Slice2byte(a)...)
	return b
}

func AppendFloat64Slice(b []byte, a []float64) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, util.Float64Slice2byte(a)...)
	return b
}

func AppendBoolSlice(b []byte, a []bool) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, util.BooleanSlice2byte(a)...)
	return b
}

func AppendStringSlice(b []byte, a []string) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	total := 0
	for i := range a {
		l := len(a[i])
		// Little Endian
		b = append(b, byte(l&0xff), byte(l>>8))
		total += l
	}
	b = AppendUint32(b, uint32(total))

	for i := range a {
		b = append(b, a[i]...)
	}
	return b
}

func AppendBytes(b []byte, buf []byte) []byte {
	b = AppendUint32(b, uint32(len(buf)))
	b = append(b, buf...)
	return b
}

func AppendString(b []byte, s string) []byte {
	b = AppendUint16(b, uint16(len(s)))
	b = append(b, s...)
	return b
}

func AppendMapStringString(b []byte, m map[string]string) []byte {
	b = AppendUint32(b, uint32(len(m)))
	if len(m) == 0 {
		return b
	}

	for k, v := range m {
		b = AppendString(b, k)
		b = AppendString(b, v)
	}
	return b
}
