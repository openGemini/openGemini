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

package util

import (
	"io"
	"reflect"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/cpu"
	"go.uber.org/zap"
)

const (
	TierBegin = 0
	Hot       = 1
	Warm      = 2
	Cold      = 3
	Moving    = 4
	TierEnd   = 5
	True      = "true"
	False     = "false"
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

	RowsNumPerFragment                int = 8192
	DefaultMaxRowsPerSegment4TsStore      = 1000
	DefaultMaxRowsPerSegment4ColStore     = RowsNumPerFragment // should be the same as RowsNumPerFragment@colstore
	DefaultMaxSegmentLimit4ColStore       = 256 * 1024
	DefaultMaxChunkMetaItemSize           = 256 * 1024
	DefaultMaxChunkMetaItemCount          = 512
	CompressModMaxChunkMetaItemCount      = 16

	NonStreamingCompact               = 2
	StreamingCompact                  = 1
	AutoCompact                       = 0
	DefaultExpectedSegmentSize uint32 = 8 * 1024 * 1024
	DefaultFileSizeLimit              = 8 * 1024 * 1024 * 1024
)

const MaxMeasurementLengthWithVersion = 255

// eg, version is:_0000
const MeasurementVersionLength = 5

// the measurement name length should consider MeasurementVersionLength
const MaxMeasurementLength = MaxMeasurementLengthWithVersion - MeasurementVersionLength

type BasicType interface {
	int64 | float64 | bool | string
}

type NumberOnly interface {
	int64 | float64
}

type ExceptString interface {
	int64 | float64 | bool
}

type ExceptBool interface {
	int64 | float64 | string
}

var logger *zap.Logger

func SetLogger(lg *zap.Logger) {
	logger = lg
}

func MustClose(obj io.Closer) {
	if obj == nil || IsObjectNil(obj) {
		return
	}

	err := obj.Close()
	if err != nil && logger != nil {
		logger.WithOptions(zap.AddCallerSkip(1)).
			Error("failed to close", zap.Error(err))
	}
}

func MustRun(fn func() error) {
	if fn == nil {
		return
	}

	err := fn()
	if err != nil && logger != nil {
		logger.WithOptions(zap.AddCallerSkip(1)).
			Error("failed", zap.Error(err))
	}
}

func IsObjectNil(obj interface{}) bool {
	val := reflect.ValueOf(obj)
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map,
		reflect.UnsafePointer, reflect.Ptr,
		reflect.Interface, reflect.Slice:

		return val.IsNil()
	}

	return false
}

type Corrector struct {
	intMin   int64
	floatMin float64
}

func NewCorrector(intMin int64, floatMin float64) *Corrector {
	return &Corrector{intMin: intMin, floatMin: floatMin}
}

func (c *Corrector) Int(v *int, def int) {
	if int64(*v) <= c.intMin {
		*v = def
	}
}

func (c *Corrector) Uint64(v *uint64, def uint64) {
	if *v <= uint64(c.intMin) {
		*v = def
	}
}

func (c *Corrector) Float64(v *float64, def float64) {
	if *v <= c.floatMin {
		*v = def
	}
}

func (c *Corrector) String(v *string, def string) {
	if *v == "" {
		*v = def
	}
}

func (c *Corrector) TomlDuration(v *toml.Duration, def toml.Duration) {
	if int64(*v) <= c.intMin {
		*v = def
	}
}

func (c *Corrector) TomlSize(v *toml.Size, def toml.Size) {
	if int64(*v) <= c.intMin {
		*v = def
	}
}

func TimeCost(option string) func() {
	start := time.Now()
	return func() {
		tc := time.Since(start)
		logger.Debug(option, zap.Duration("time cost", tc))
	}
}

func CeilToPower2(num uint64) uint64 {
	if num > (1 << 63) {
		return 1 << 63
	}
	num--
	num |= num >> 1
	num |= num >> 2
	num |= num >> 4
	num |= num >> 8
	num |= num >> 16
	num |= num >> 32
	return num + 1
}

func NumberOfTrailingZeros(num uint64) int {
	if num == 0 {
		return 64
	}
	n := 63
	num1 := num << 32
	if num1 != 0 {
		n -= 32
		num = num1
	}

	num1 = num << 16
	if num1 != 0 {
		n -= 16
		num = num1
	}

	num1 = num << 8
	if num1 != 0 {
		n -= 8
		num = num1
	}

	num1 = num << 4
	if num1 != 0 {
		n -= 4
		num = num1
	}

	num1 = num << 2
	if num1 != 0 {
		n -= 2
		num = num1
	}

	return n - int((num<<1)>>63)
}

func IntLimit(min, max int, v int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
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

func Uint32Slice2byte(b []uint32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Uint32SizeBytes
	s.Cap = h.Cap * Uint32SizeBytes
	return res
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

func Uint64Slice2byte(b []uint64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Uint64SizeBytes
	s.Cap = h.Cap * Uint64SizeBytes
	return res
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

func Bytes2Uint64Slice(b []byte) []uint64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []uint64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Uint64SizeBytes
	s.Cap = h.Cap / Uint64SizeBytes
	return res
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

func Bool2str(b bool) string {
	if b {
		return True
	} else {
		return False
	}
}

func MemorySet(buf []byte, val byte) {
	for i := range buf {
		buf[i] = val
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func MaxUint32(x, y uint32) uint32 {
	if x < y {
		return y
	}
	return x
}

func MinUint32(x, y uint32) uint32 {
	if x < y {
		return x
	}
	return y
}

func Float64ToUint64(v float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&v))
}

func Uint64ToFloat64(v uint64) float64 {
	return *(*float64)(unsafe.Pointer(&v))
}

type allocItem interface {
	byte | models.Row | interface{}
}

func AllocSlice[T allocItem](data []T, size int) ([]T, []T) {
	start := len(data)
	end := start + size

	if end > cap(data) {
		data = make([]T, 0, size*cpu.GetCpuNum())
		end = size
		start = 0
	}

	return data[:end], data[start:end]
}

func DivisionCeil(dividend, divisor int) int {
	if divisor == 0 {
		return 0
	}
	return (dividend + divisor - 1) / divisor
}

var zeroBuf = make([]byte, 8000)

func PaddingZeroBuffer(out []byte, size int) []byte {
	if size > len(zeroBuf) {
		out = append(out, make([]byte, size)...)
	} else {
		out = append(out, zeroBuf[:size]...)
	}
	return out
}
