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
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime/debug"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/cpu"
	"go.uber.org/zap"
)

const (
	TierBegin          = 0
	Hot                = 1
	Warm               = 2
	Cold               = 3
	Moving             = 4
	Cleared            = 5
	NoClear            = 6
	Clearing           = 7
	TierEnd            = 8
	True               = "true"
	False              = "false"
	PrecisionThreshold = 1e-9
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

func Bool2Int64(b bool) int64 {
	if b {
		return 1
	} else {
		return 0
	}
}

func MemorySet[T any](buf []T, val T) {
	size := len(buf)
	if size == 0 {
		return
	}
	buf[0] = val
	for i := 1; i < size; i *= 2 {
		copy(buf[i:], buf[:i])
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

// Median calculates the median value of a numeric slice.
// It returns the median as a float64 and an error if the input slice is empty.
//
// Note: This implementation uses the QuickSelect algorithm, which modifies the order
// of elements in the input slice during execution (through in-place partitioning).
// The original element positions will not be preserved.
//
// The median is defined as:
// - For an odd-length slice: the middle element when the slice is sorted.
// - For an even-length slice: the average of the two middle elements when sorted.
//
// QuickSelect is used here for efficient median calculation (average O(n) time complexity)
// without full sorting, making it suitable for large datasets.
func Median[T NumberOnly](data []T) (float64, error) {
	if len(data) == 0 {
		return 0, errors.New("empty data")
	}
	n := len(data)

	if n%2 == 1 {
		// Odd length: median is the middle element
		mid := n / 2
		return quickSelect(data, mid), nil
	} else {
		// Even length: median is the average of the two middle elements
		mid1 := (n / 2) - 1
		val1 := quickSelect(data, mid1)
		// Use the first partition of quickSelect partition: elements after mid1 are ≥ val1.
		// The 1st smallest in this subSlice is the (mid1+1)th smallest in the original slice.
		val2 := quickSelect(data[mid1:], 1)
		return (val1 + val2) / 2, nil
	}
}

// TrimMean calculates the trimmed mean of a numeric slice by removing a specified proportion of elements
// from both the lower and upper ends, then averaging the remaining elements.
//
// Note: This implementation uses the QuickSelect algorithm, which modifies the order
// of elements in the input slice during execution (through in-place partitioning).
// The original element positions will not be preserved.
func TrimMean[T NumberOnly](data []T, proportionToCut float64) (float64, error) {
	if len(data) == 0 {
		return 0, errors.New("empty data")
	}
	if proportionToCut < 0 || proportionToCut >= 0.5 {
		return 0, errors.New("proportionToCut must be in [0, 0.5)")
	}

	n := len(data)
	lowerCut := int(proportionToCut * float64(n))
	upperCut := n - lowerCut

	// 1. Ensure elements after upperCut-1 are larger than the retained set
	quickSelect(data, upperCut-1)
	// 2. Ensure elements before lowerCut in the trimmed range are smaller than the retained set
	quickSelect(data[:upperCut], lowerCut)

	sum := 0.0
	for i := lowerCut; i < upperCut; i++ {
		sum += float64(data[i])
	}
	return sum / float64(upperCut-lowerCut), nil
}

// Quantile computes the empirical quantile of a dataset using linear interpolation,
// consistent with the behavior of numpy.quantile (method="linear").
//
// Note: This implementation uses the QuickSelect algorithm, which modifies the order
// of elements in the input slice during execution (through in-place partitioning).
// The original element positions will not be preserved.
func Quantile[T NumberOnly](data []T, q float64) (float64, error) {
	if len(data) == 0 {
		return 0, errors.New("empty data")
	}
	if q < 0 || q > 1 {
		return 0, fmt.Errorf("q must be in [0, 1], got %.1f", q)
	}

	n := len(data)
	if n == 1 {
		return float64(data[0]), nil
	}

	index := q * float64(n-1)
	k := int(math.Floor(index))
	d := index - float64(k)

	lowerVal := quickSelect(data, k)
	upperVal := 0.0
	if k+1 < n {
		upperVal = quickSelect(data, k+1)
	} else {
		upperVal = float64(data[n-1])
	}

	return (1-d)*lowerVal + d*upperVal, nil
}

// quickSelect: Find the k-th smallest element (0-based index) in nums, modifies the array in-place
func quickSelect[T NumberOnly](nums []T, k int) float64 {
	left, right := 0, len(nums)-1
	for left < right {
		pivotIdx := partition(nums, left, right)
		if pivotIdx == k {
			return float64(nums[k])
		} else if pivotIdx < k {
			left = pivotIdx + 1
		} else {
			right = pivotIdx - 1
		}
	}
	return float64(nums[left])
}

// partition function: Uses nums[right] as pivot, returns the pivot's final position
func partition[T NumberOnly](nums []T, left, right int) int {
	pivot := nums[right]
	i := left
	for j := left; j < right; j++ {
		if nums[j] <= pivot {
			nums[i], nums[j] = nums[j], nums[i]
			i++
		}
	}
	nums[i], nums[right] = nums[right], nums[i]
	return i
}

// IsInternalDatabase returns true if the database is "_internal".
func IsInternalDatabase(dbName string) bool {
	return dbName == "_internal"
}

func WaitTimeOut(wait func(), done func(), timeout time.Duration) {
	var rcv = func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			logger.Error("wait crashed", zap.Any("recover", err),
				zap.String("stack", string(debug.Stack())))
		}
	}
	defer rcv()

	ch := make(chan struct{})
	go func() {
		defer rcv()
		TickerRun(timeout, ch, func() {
			done()
		}, func() {})
	}()

	wait()
	close(ch)
}

// MergeMaps merges two maps into a new map following specific rules:
// 1. For keys present in both maps, the value from newMap is used
// 2. For keys present only in oldMap, the value from oldMap is preserved
// 3. For keys present only in newMap, the value from newMap is included
func MergeMaps[K comparable, V any](oldMap, newMap map[K]V) map[K]V {
	// Handle nil input maps to avoid panics during iteration
	safeOldMap := oldMap
	if safeOldMap == nil {
		safeOldMap = make(map[K]V)
	}

	safeNewMap := newMap
	if safeNewMap == nil {
		safeNewMap = make(map[K]V)
	}

	result := make(map[K]V, len(safeOldMap)+len(safeNewMap))
	for k, v := range safeOldMap {
		result[k] = v
	}
	for k, v := range safeNewMap {
		result[k] = v
	}

	return result
}

func EpochDivisor(epoch string) int64 {
	divisor := int64(1)

	switch epoch {
	case "u", "us", "µs", "μs": // see time.unitMap
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}

	return divisor
}

// CalculateSum calculates the sum of all elements in a numeric slice using NumberOnly constraint.
// Returns 0 if the slice is empty or nil.
func CalculateSum[T NumberOnly](values []T) T {
	var sum T
	for _, v := range values {
		sum += v
	}
	return sum
}

// CalculateMax calculates the maximum value in a numeric slice using NumberOnly constraint.
// Returns type-specific zero value for empty/nil slices: defaultMax
func CalculateMax[T NumberOnly](values []T, defaultMax T) T {
	if len(values) == 0 {
		return defaultMax
	}

	_max := values[0]
	for _, v := range values[1:] {
		if v > _max {
			_max = v
		}
	}
	return _max
}

// CalculateMin calculates the minimum value in a numeric slice using NumberOnly constraint.
// Returns type-specific zero value for empty/nil slices: defaultMin
func CalculateMin[T NumberOnly](values []T, defaultMin T) T {
	if len(values) == 0 {
		return defaultMin
	}

	_min := values[0]
	for _, v := range values[1:] {
		if v < _min {
			_min = v
		}
	}
	return _min
}

func CalculateMinMax[T NumberOnly](values []T, def T) (T, T) {
	if len(values) == 0 {
		return def, def
	}

	minV, maxV := values[0], values[0]
	for _, v := range values[1:] {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	return minV, maxV
}

func SplitInt64(n int64) (low32 uint32, high32 uint32) {
	u := uint64(n)
	low32 = uint32(u & 0xFFFFFFFF)
	high32 = uint32((u >> 32) & 0xFFFFFFFF)
	return
}

func MergeToInt64(low32, high32 uint32) int64 {
	high64 := uint64(high32) << 32
	low64 := uint64(low32)
	return int64(high64 | low64)
}
