// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package record

import (
	"fmt"
	"math/bits"
	"unsafe"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var MinMaxTimeRange = util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime}
var typeSize = make([]int, influx.Field_Type_Last)
var zeroBuf = make([]byte, 1024)

func init() {
	typeSize[influx.Field_Type_Int] = util.Int64SizeBytes
	typeSize[influx.Field_Type_Float] = util.Float64SizeBytes
	typeSize[influx.Field_Type_Boolean] = util.BooleanSizeBytes
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

// ToPrimitiveType convert the tag to the primitive data type. Integer is supported later.
func ToPrimitiveType(ty int32) int {
	switch ty {
	case influx.Field_Type_Tag:
		return influx.Field_Type_String
	default:
		return int(ty)
	}
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

func Uint64ToBytesUnsafe(id uint64) []byte {
	return (*(*[8]byte)(unsafe.Pointer(&id)))[:]
}

func reserveBytes(b []byte, size int) []byte {
	valCap := cap(b)
	if valCap == 0 {
		return make([]byte, size)
	}

	valLen := len(b)
	remain := valCap - valLen
	if delta := size - remain; delta > 0 {
		if delta <= len(zeroBuf) {
			b = append(b[:valCap], zeroBuf[:delta]...)
		} else {
			b = append(b[:valCap], make([]byte, delta)...)
		}
	}
	return b[:valLen+size]
}

func ReserveInt64Slice(b []int64, size int) []int64 {
	bCap := cap(b)
	if bCap == 0 {
		return make([]int64, size)
	}
	bLen := len(b)
	remain := bCap - bLen
	if delta := size - remain; delta > 0 {
		b = append(b[:bCap], make([]int64, delta)...)
	}
	return b[:bLen+size]
}

func valueIndexRange(bitMap []byte, bitOffset int, bmStart, bmEnd int, pos int, posValidCount int) (int, int) {
	var start, end int
	firstIndex := 0
	if bmStart >= pos {
		start = posValidCount
		firstIndex = pos
	}
	start += valueIndexRangeWithSingle(bitMap, firstIndex+bitOffset, bmStart+bitOffset)
	end += start + valueIndexRangeWithSingle(bitMap, bmStart+bitOffset, bmEnd+bitOffset)

	return start, end
}

func ValueIndexRange(bitMap []byte, bitOffset int, bmStart, bmEnd int, pos int, posValidCount int) (int, int) {
	return valueIndexRange(bitMap, bitOffset, bmStart, bmEnd, pos, posValidCount)
}

func valueIndexRangeWithSingle(bitMap []byte, bitStart int, bitEnd int) int {
	bm := bitMap[bitStart>>3 : (bitEnd+7)>>3]
	size := len(bm)
	total := 0

	// Bitmap is stored in little-endian mode
	// In the subsequent logic, the lower N bits are repeatedly calculated.
	// Therefore, the result needs to be corrected in advance.
	// For example: bm[0] = 0 0 1 0 0 1 1 1; bitStart = 3
	//                                ^ ^ ^
	// Indicates that the lower three bits have been calculated
	if n := bitStart & 0x07; n > 0 {
		total -= bits.OnesCount8(bm[0] << (8 - n))
	}

	if n := bitEnd & 0x07; n > 0 {
		total -= bits.OnesCount8(bm[size-1] >> n)
	}

	if size >= 8 {
		pos := size - size&0x07
		// Because only the number of 1s is calculated,
		// the type can be forcibly converted,
		// regardless of the difference between big-endian and little-endian
		uints := util.Bytes2Uint64Slice(bm[:pos])
		for i := range uints {
			total += bits.OnesCount64(uints[i])
		}
		bm = bm[pos:]
	}

	// calculate the tail part less than 8 bytes
	for i := range bm {
		total += bits.OnesCount8(bm[i])
	}

	return total
}
