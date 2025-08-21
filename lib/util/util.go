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
	"errors"
	"io"
	"reflect"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/cpu"
	"go.uber.org/zap"
)

const (
	TierBegin = 0
	Hot       = 1
	Warm      = 2
	Cold      = 3
	Moving    = 4
	Cleared   = 5
	NoClear   = 6
	Clearing  = 7
	TierEnd   = 8
	True      = "true"
	False     = "false"
)

type Item struct {
	Name      string
	Key       string
	TagValue  string
	SeriesKey string
	Tsid      uint64
}

func ClearingWhileReading(startTier, endTier uint64) bool {
	return startTier == Clearing || endTier == Clearing || (endTier == Cleared && endTier != startTier)
}

func IsHot(tier uint64) bool {
	return tier == Hot
}

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

	DefaultEntryLogSizeLimit = 20 * 1024 * 1024 * 1024

	MaxMeasurementLengthWithVersion = 255
	// eg, version is:_0000
	MeasurementVersionLength = 5
	// the measurement name length should consider MeasurementVersionLength
	MaxMeasurementLength = MaxMeasurementLengthWithVersion - MeasurementVersionLength

	MaxTagNameLength   = 255
	MaxTagValueLength  = 64 * 1024
	MaxFieldNameLength = 255
	// No explicit field value length
)

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

type NumberInt interface {
	int | int8 | int32 | int64 | uint8 | uint32 | uint64
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
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*uint16)(unsafe.Pointer(&b[0])), cap(b)/Uint16SizeBytes)[:len(b)/Uint16SizeBytes]
}

func Uint16Slice2byte(b []uint16) []byte {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b[0])), cap(b)*Uint16SizeBytes)[:len(b)*Uint16SizeBytes]
}

func Bytes2Uint32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(&b[0])), cap(b)/Uint32SizeBytes)[:len(b)/Uint32SizeBytes]
}

func Uint32Slice2byte(b []uint32) []byte {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b[0])), cap(b)*Uint32SizeBytes)[:len(b)*Uint32SizeBytes]
}

func Bytes2Uint64Slice(b []byte) []uint64 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*uint64)(unsafe.Pointer(&b[0])), cap(b)/Uint64SizeBytes)[:len(b)/Uint64SizeBytes]
}

func Uint64Slice2byte(b []uint64) []byte {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b[0])), cap(b)*Uint64SizeBytes)[:len(b)*Uint64SizeBytes]
}

func Bytes2BooleanSlice(b []byte) []bool {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*bool)(unsafe.Pointer(&b[0])), cap(b)/BooleanSizeBytes)[:len(b)/BooleanSizeBytes]
}

func BooleanSlice2byte(b []bool) []byte {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b[0])), cap(b)*BooleanSizeBytes)[:len(b)*BooleanSizeBytes]
}
func Bytes2Int64Slice(b []byte) []int64 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(&b[0])), cap(b)/Int64SizeBytes)[:len(b)/Int64SizeBytes]
}

func Int64Slice2byte(b []int64) []byte {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b[0])), cap(b)*Int64SizeBytes)[:len(b)*Int64SizeBytes]
}

func Bytes2Int32Slice(b []byte) []int32 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*int32)(unsafe.Pointer(&b[0])), cap(b)/Int32SizeBytes)[:len(b)/Int32SizeBytes]
}

func Int32Slice2byte(b []int32) []byte {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b[0])), cap(b)*Int32SizeBytes)[:len(b)*Int32SizeBytes]
}

func Bytes2Int16Slice(b []byte) []int16 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*int16)(unsafe.Pointer(&b[0])), cap(b)/Int16SizeBytes)[:len(b)/Int16SizeBytes]
}

func Int16Slice2byte(b []int16) []byte {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b[0])), cap(b)*Int16SizeBytes)[:len(b)*Int16SizeBytes]
}

func Bytes2Int8Slice(b []byte) []int8 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*int8)(unsafe.Pointer(&b[0])), cap(b)/Int8SizeBytes)[:len(b)/Int8SizeBytes]
}

func Bytes2Float32Slice(b []byte) []float32 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), cap(b)/Float32SizeBytes)[:len(b)/Float32SizeBytes]
}

func Float32Slice2byte(b []float32) []byte {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b[0])), cap(b)*Float32SizeBytes)[:len(b)*Float32SizeBytes]
}

func Bytes2Float64Slice(b []byte) []float64 {
	if cap(b) == 0 {
		return nil
	}
	size := len(b)
	if size == 0 {
		b = b[:1]
	}
	return unsafe.Slice((*float64)(unsafe.Pointer(&b[0])), cap(b)/Float64SizeBytes)[:size/Float64SizeBytes]
}

func Float64Slice2byte(b []float64) []byte {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b[0])), cap(b)*Float64SizeBytes)[:len(b)*Float64SizeBytes]
}

type TimeRange struct {
	Min, Max int64
}

func (t TimeRange) Overlaps(min, max int64) bool {
	return t.Min <= max && t.Max >= min
}

func (t TimeRange) Contains(min, max int64) bool {
	return t.Min <= min && t.Max >= max
}

func Bytes2Value[T any](b []byte, v *T) {
	*v = *(*T)(unsafe.Pointer(&b[0]))
}

func Str2bytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func Bytes2str(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

func Bool2str(b bool) string {
	if b {
		return True
	} else {
		return False
	}
}

func Bool2Byte(b bool) byte {
	if b {
		return 1
	} else {
		return 0
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

func Max[T NumberInt](x, y T) T {
	if x > y {
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

func SliceSplitFunc[E any](s []E, filter func(*E) bool) ([]E, []E) {
	i, j := 0, len(s)
	for {
		if i == j {
			break
		}

		if filter(&s[i]) {
			j--
			s[i], s[j] = s[j], s[i]
			continue
		}

		i++
	}
	return s[j:], s[:j]
}

func TickerRun(d time.Duration, stopSignal <-chan struct{}, onTick func(), onStop func()) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			onTick()
		case <-stopSignal:
			onStop()
			return
		}
	}
}

func IntersectSortedSliceInt(slice1, slice2 []int) []int {
	var i, j int
	intersection := make([]int, 0, len(slice1)+len(slice2))
	for i < len(slice1) && j < len(slice2) {
		if slice1[i] == slice2[j] {
			intersection = append(intersection, slice1[i])
			i++
			j++
		} else if slice1[i] < slice2[j] {
			i++
		} else {
			j++
		}
	}
	return intersection
}

func UnionSortedSliceInt(slice1, slice2 []int) []int {
	union := make([]int, 0, len(slice1)+len(slice2))
	var i, j int
	for i < len(slice1) && j < len(slice2) {
		if slice1[i] == slice2[j] {
			union = append(union, slice1[i])
			i++
			j++
		} else if slice1[i] < slice2[j] {
			union = append(union, slice1[i])
			i++
		} else {
			union = append(union, slice2[j])
			j++
		}
	}
	for i < len(slice1) {
		union = append(union, slice1[i])
		i++
	}
	for j < len(slice2) {
		union = append(union, slice2[j])
		j++
	}
	return union
}

// FindIntersectionIndex finds the intersection indices of two sorted slices based on a custom comparison function.
func FindIntersectionIndex[T1, T2 any](slice1 []T1, slice2 []T2,
	compareFunc func(T1, T2) int, callbackFunc func(i, j int) error) error {
	if compareFunc == nil || callbackFunc == nil {
		return errors.New("compareFunc or callbackFunc cannot be nil")
	}

	i, j := 0, 0
	s1Len := len(slice1)
	s2Len := len(slice2)
	for i < s1Len && j < s2Len {
		switch compareFunc(slice1[i], slice2[j]) {
		case 0:
			err := callbackFunc(i, j)
			if err != nil {
				return err
			}
			i++
			j++
		case -1:
			i++
		case 1:
			j++
		default:
			return errors.New("invalid compareFunc result")
		}
	}
	return nil
}

func SplitUint64(original uint64) (high, low uint32) {
	low = uint32(original & 0xFFFFFFFF)
	high = uint32(original >> 32)
	return
}
