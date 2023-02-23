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

package record

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

const (
	BooleanSizeBytes = int(unsafe.Sizeof(false))
	Uint32SizeBytes  = int(unsafe.Sizeof(uint32(0)))
	Uint16SizeBytes  = int(unsafe.Sizeof(uint16(0)))
	Uint64SizeBytes  = int(unsafe.Sizeof(uint64(0)))
	Int16SizeBytes   = int(unsafe.Sizeof(int16(0)))
	Int32SizeBytes   = int(unsafe.Sizeof(int32(0)))
	Int64SizeBytes   = int(unsafe.Sizeof(int64(0)))
	Float32SizeBytes = int(unsafe.Sizeof(float32(0)))
	Float64SizeBytes = int(unsafe.Sizeof(float64(0)))
	Int8SizeBytes    = int(unsafe.Sizeof(int8(0)))
)

var MinMaxTimeRange = TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime}
var typeSize map[int]int

func init() {
	typeSize = map[int]int{
		influx.Field_Type_Int:     Int64SizeBytes,
		influx.Field_Type_Float:   Float64SizeBytes,
		influx.Field_Type_Boolean: BooleanSizeBytes,
	}
}

type TimeRange struct {
	Min, Max int64
}

func (t TimeRange) Overlaps(min, max int64) bool {
	return t.Min <= max && t.Max >= min
}

func Str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func Bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func ToModelTypes(ty influxql.DataType) int {
	switch ty {
	case influxql.Tag:
		return influx.Field_Type_String
	case influxql.Integer:
		return influx.Field_Type_Int
	case influxql.Float:
		return influx.Field_Type_Float
	case influxql.Boolean:
		return influx.Field_Type_Boolean
	case influxql.String:
		return influx.Field_Type_String
	case influxql.Time:
		return influx.Field_Type_Int
	default:
		panic(fmt.Sprintf("unknown field type:%v", ty))
	}
}

func ToInfluxqlTypes(ty int) influxql.DataType {
	switch ty {
	case influx.Field_Type_Tag:
		return influxql.Tag
	case influx.Field_Type_Int:
		return influxql.Integer
	case influx.Field_Type_Float:
		return influxql.Float
	case influx.Field_Type_Boolean:
		return influxql.Boolean
	case influx.Field_Type_String:
		return influxql.String
	default:
		panic(fmt.Sprintf("unknown field type:%v", ty))
	}
}

func Bytes2Uint16Slice(b []byte) []uint16 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []uint16
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Uint16SizeBytes
	s.Cap = h.Cap / Uint16SizeBytes
	return res
}

func Uint16Slice2byte(b []uint16) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Uint16SizeBytes
	s.Cap = h.Cap * Uint16SizeBytes
	return res
}

func Bytes2Uint32Slice(b []byte) []uint32 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []uint32
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Uint32SizeBytes
	s.Cap = h.Cap / Uint32SizeBytes
	return res
}

func Bytes2Uint32SliceBigEndian(b []byte) []uint32 {
	ret := make([]uint32, 0, len(b)/Uint32SizeBytes)
	for pos := 0; pos <= len(b)-Uint32SizeBytes; pos += Uint32SizeBytes {
		ret = append(ret, numberenc.UnmarshalUint32(b[pos:pos+Uint32SizeBytes]))
	}
	return ret
}

func Uint32Slice2byte(b []uint32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Uint32SizeBytes
	s.Cap = h.Cap * Uint32SizeBytes
	return res
}

func Uint32Slice2ByteBigEndian(b []uint32) []byte {
	ret := make([]byte, 0, len(b)*Uint32SizeBytes)
	for _, val := range b {
		ret = numberenc.MarshalUint32Append(ret, val)
	}
	return ret
}

func BooleanSlice2byte(b []bool) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * BooleanSizeBytes
	s.Cap = h.Cap * BooleanSizeBytes
	return res
}

func Bytes2BooleanSlice(b []byte) []bool {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []bool
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / BooleanSizeBytes
	s.Cap = h.Cap / BooleanSizeBytes
	return res
}

func Int64Slice2byte(b []int64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Int64SizeBytes
	s.Cap = h.Cap * Int64SizeBytes
	return res
}

func Int64Slice2ByteBigEndian(b []int64) []byte {
	ret := make([]byte, 0, len(b)*Int64SizeBytes)
	for _, val := range b {
		ret = numberenc.MarshalInt64Append(ret, val)
	}
	return ret
}

func Uint64Slice2byte(b []uint64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Uint64SizeBytes
	s.Cap = h.Cap * Uint64SizeBytes
	return res
}

func Uint64Slice2ByteBigEndian(b []uint64) []byte {
	ret := make([]byte, 0, len(b)*Uint64SizeBytes)
	for _, val := range b {
		ret = numberenc.MarshalUint64Append(ret, val)
	}
	return ret
}

func Bytes2Uint64Slice(b []byte) []uint64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []uint64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Uint64SizeBytes
	s.Cap = h.Cap / Uint64SizeBytes
	return res
}

func Bytes2Uint64SliceBigEndian(b []byte) []uint64 {
	ret := make([]uint64, 0, len(b)/Uint64SizeBytes)
	for pos := 0; pos <= len(b)-Uint64SizeBytes; pos += Uint64SizeBytes {
		ret = append(ret, numberenc.UnmarshalUint64(b[pos:pos+Uint64SizeBytes]))
	}
	return ret
}

func Bytes2Int16Slice(b []byte) []int16 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []int16
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int16SizeBytes
	s.Cap = h.Cap / Int16SizeBytes
	return res
}

func Bytes2Int32Slice(b []byte) []int32 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []int32
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int32SizeBytes
	s.Cap = h.Cap / Int32SizeBytes
	return res
}

func Bytes2Int64Slice(b []byte) []int64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []int64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int64SizeBytes
	s.Cap = h.Cap / Int64SizeBytes
	return res
}

func Bytes2Int64SliceBigEndian(b []byte) []int64 {
	ret := make([]int64, 0, len(b)/Int64SizeBytes)
	for pos := 0; pos <= len(b)-Int64SizeBytes; pos += Int64SizeBytes {
		ret = append(ret, numberenc.UnmarshalInt64(b[pos:pos+Int64SizeBytes]))
	}
	return ret
}

func Float32Slice2byte(b []float32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Float32SizeBytes
	s.Cap = h.Cap * Float32SizeBytes
	return res
}

func Bytes2Float32Slice(b []byte) []float32 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []float32
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Float32SizeBytes
	s.Cap = h.Cap / Float32SizeBytes
	return res
}

func Float64Slice2byte(b []float64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Float64SizeBytes
	s.Cap = h.Cap * Float64SizeBytes
	return res
}

func Bytes2Float64Slice(b []byte) []float64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []float64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Float64SizeBytes
	s.Cap = h.Cap / Float64SizeBytes
	return res
}

func Bytes2Int8Slice(b []byte) []int8 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []int8
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int8SizeBytes
	s.Cap = h.Cap / Int8SizeBytes

	return res
}

// GetTimeRangeStartIndex return first time index in times >= startTime
func GetTimeRangeStartIndex(times []int64, startPos int, startTime int64) int {
	start := startPos
	end := len(times) - 1
	for start <= end {
		mid := (start + end) / 2
		if times[mid] == startTime {
			return mid
		} else if times[mid] < startTime {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}
	return start
}

func GetTimeRangeStartIndexDescend(times []int64, startPos int, startTime int64) int {
	start := startPos
	end := len(times) - 1
	for start <= end {
		mid := (start + end) / 2
		if times[mid] == startTime {
			return mid
		} else if times[mid] < startTime {
			end = mid - 1
		} else {
			start = mid + 1
		}
	}
	return start
}

// GetTimeRangeEndIndex return last time index in times <= endTime
func GetTimeRangeEndIndex(times []int64, startPos int, endTime int64) int {
	start := startPos
	end := len(times) - 1
	for start <= end {
		mid := (start + end) / 2
		if times[mid] == endTime {
			return mid
		} else if times[mid] < endTime {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}
	return start - 1
}

func GetTimeRangeEndIndexDescend(times []int64, startPos int, endTime int64) int {
	start := startPos
	end := len(times) - 1
	for start <= end {
		mid := (start + end) / 2
		if times[mid] == endTime {
			return mid
		} else if times[mid] < endTime {
			end = mid - 1
		} else {
			start = mid + 1
		}
	}
	return start - 1
}

func Int16Slice2byte(b []int16) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Int16SizeBytes
	s.Cap = h.Cap * Int16SizeBytes
	return res
}

func Int32Slice2byte(b []int32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Int32SizeBytes
	s.Cap = h.Cap * Int32SizeBytes
	return res
}

func Uint64ToBytesUnsafe(id uint64) []byte {
	return (*(*[8]byte)(unsafe.Pointer(&id)))[:]
}

func Uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func Float64ToUint64(v float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&v))
}

func Uint64ToFloat64(v uint64) float64 {
	return *(*float64)(unsafe.Pointer(&v))
}

func MemorySet(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

func RemoveDuplicationInt(array []uint32) []uint32 {
	swap := func(list []uint32, a, b int) {
		list[a], list[b] = list[b], list[a]
	}

	length := len(array)
	if length == 0 {
		return array
	}

	j := 0
	for i := 1; i < length; i++ {
		if array[i] != array[j] {
			j++
			if j < i {
				swap(array, i, j)
			}
		}
	}
	return array[:j+1]
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
