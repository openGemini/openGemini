// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package logstore

import (
	"math"
	"time"
)

const (
	Year int = iota
	Month
	Day
	Hour
	Minute
	Second
)

const (
	SecCountPerMinute int = 1 * 60
	SecCountPerHour       = 1 * 60 * SecCountPerMinute
	SecCountPerDay        = 1 * 24 * SecCountPerHour
	SecCountPerMonth      = 1 * 30 * SecCountPerDay
	SecCountPerYear       = 1 * 12 * SecCountPerMonth
)

var AggLogBucketCountList = []int{
	60, 24, 36, 48, 60, 36, 42, 48, 54, 60,
	22, 24, 26, 28, 30, 32, 34, 36, 38, 40,
	42, 44, 46, 48, 50, 52, 54, 56, 58, 60,
	31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
	51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
}
var AggLogIntervalList = GetAggLogIntervalList()

func GetAggLogIntervalList() []int {
	var res []int
	// Second
	for i := 0; i <= 60; i += 5 {
		if i == 0 {
			res = append(res, 1)
		} else {
			res = append(res, i)
		}
	}
	// Minute
	for i := 0; i <= 60; i += 5 {
		if i == 0 {
			res = append(res, SecCountPerMinute)
		} else {
			res = append(res, i*SecCountPerMinute)
		}
	}

	// Hour
	for i := 1; i <= 24; i++ {
		res = append(res, i*SecCountPerHour)
	}

	// Day
	for i := 1; i <= 30; i++ {
		res = append(res, i*SecCountPerDay)
	}

	// Month
	for i := 1; i <= 12; i++ {
		res = append(res, i*SecCountPerMonth)
	}

	// Year
	for i := 1; i <= 100; i++ {
		res = append(res, i*SecCountPerYear)
	}
	return res
}

func getAdaptiveTimeBucketCount(startTime, endTime time.Time, ascending bool) (int, float64) {
	if !ascending {
		startTime, endTime = endTime, startTime
	}

	var delta int
	diff := endTime.Sub(startTime).Seconds()
	timeSlot := make([]int, 6) // 2023-04-25 09:50:00
	// yy
	timeSlot[Year] = int(math.Floor(diff / float64((60 * 60 * 24 * 365))))
	// MM
	timeSlot[Month] = int(math.Floor(diff / float64((60 * 60 * 24 * 30))))
	// dd
	timeSlot[Day] = int(math.Floor(diff / float64((60 * 60 * 24))))
	// hh
	timeSlot[Hour] = int(math.Floor(diff / float64((60 * 60))))
	// mm
	timeSlot[Minute] = int(math.Floor(diff / float64((60))))
	// ss
	timeSlot[Second] = int(diff)

	for i := range timeSlot {
		if i < len(timeSlot)-1 && timeSlot[i] > 0 && timeSlot[i+1] > 0 {
			switch i {
			case Year:
				delta = timeSlot[i]*12 + timeSlot[i+1]
			case Month:
				delta = timeSlot[i]*30 + timeSlot[i+1]
			case Day:
				delta = timeSlot[i]*24 + timeSlot[i+1]
			case Hour:
				delta = timeSlot[i]*60 + timeSlot[i+1]
			case Minute:
				delta = timeSlot[i]*60 + timeSlot[i+1]
			}
			break
		} else if (i == len(timeSlot)-1) || (i < len(timeSlot)-1 && timeSlot[i] > 0 && timeSlot[i+1] == 0) {
			delta = timeSlot[i]
			break
		}
	}

	var bucketCount int
	if delta > 60 {
		bucketCount = int(math.Ceil(float64(delta) / math.Ceil(float64(delta)/60)))
	} else {
		bucketCount = AggLogBucketCountList[delta-1]
	}
	return bucketCount, diff
}

func getAdaptiveTimeInterval(startTime, endTime time.Time, bucketCount int, diff float64) time.Duration {
	interval := time.Duration(math.Ceil(diff/float64(bucketCount))) * time.Second

	prev, next := binarySearch(AggLogIntervalList, int(interval.Seconds()))
	if next == -1 {
		return interval
	}

	if prev < 0 {
		prev = 0
	}
	if prev >= len(AggLogIntervalList) {
		prev = len(AggLogIntervalList) - 1
	}

	if next < 0 {
		next = 0
	}
	if next >= len(AggLogIntervalList) {
		next = len(AggLogIntervalList) - 1
	}

	start := startTime.UnixNano() / 1e9
	end := endTime.UnixNano() / 1e9
	bucket1 := math.Ceil(float64((end - start)) / float64(AggLogIntervalList[prev]))
	bucket2 := math.Ceil(float64((end - start)) / float64(AggLogIntervalList[next]))

	if math.Abs(bucket1-30) < math.Abs(bucket2-30) {
		return time.Duration(AggLogIntervalList[prev]) * time.Second
	}
	return time.Duration(AggLogIntervalList[next]) * time.Second
}

func binarySearch(arr []int, target int) (int, int) {
	left, right := 0, len(arr)-1
	for left <= right {
		mid := left + (right-left)/2
		if arr[mid] == target {
			return mid, -1
		} else if arr[mid] < target {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return right, left
}

func GetAdaptiveTimeBucket(startTime, endTime time.Time, ascending bool) time.Duration {
	bucketCount, diff := getAdaptiveTimeBucketCount(startTime, endTime, ascending)
	return getAdaptiveTimeInterval(startTime, endTime, bucketCount, diff)
}
