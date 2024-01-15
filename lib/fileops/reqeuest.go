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

package fileops

import (
	"fmt"
	"sort"
	"strings"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/request"
)

var ObsSingleRequestSize int64 = 9 * 1024 * 1024
var defaultObsRangeSize = 32

type Range struct {
	start int64
	end   int64
}

func NewRange(start, end int64) *Range {
	return &Range{
		start,
		end,
	}
}

func (r Range) String() string {
	return fmt.Sprintf("%d-%d", r.start, r.end)
}

type RangeRequest struct {
	ranges  []*Range
	length  int64
	readMap map[int64][]*request.StreamReader
}

func NewRangeRequest() *RangeRequest {
	return &RangeRequest{
		make([]*Range, 0),
		0,
		make(map[int64][]*request.StreamReader),
	}
}

func (r *RangeRequest) GetRangeString() string {
	rangeStrs := make([]string, len(r.ranges))
	for i, v := range r.ranges {
		rangeStrs[i] = v.String()
	}
	return strings.Join(rangeStrs, ",")
}

func (r *RangeRequest) AddReadAction(offset int64, length int64) {
	if len(r.ranges) > 0 && r.ranges[len(r.ranges)-1].end == offset-1 {
		r.ranges[len(r.ranges)-1].end = offset + length - 1
		bytes := make([]byte, length)
		r.readMap[r.ranges[len(r.ranges)-1].start] = append(r.readMap[r.ranges[len(r.ranges)-1].start], &request.StreamReader{
			Offset:  offset,
			Content: bytes,
		})
	} else {
		r.ranges = append(r.ranges, NewRange(offset, offset+length-1))
		bytes := make([]byte, length)
		r.readMap[offset] = append(r.readMap[offset], &request.StreamReader{
			Offset:  offset,
			Content: bytes,
		})
	}
	r.length += length
}

func NewObsReadRequest(offs []int64, sizes []int64, minBlockSize int64, obsRangeSize int) ([]*RangeRequest, error) {
	if obsRangeSize == -1 {
		obsRangeSize = defaultObsRangeSize
	}
	realObsSingleRequestSize := ObsSingleRequestSize - ObsSingleRequestSize%minBlockSize
	rangeRequests := make([]*RangeRequest, 0)
	rangeRequest := NewRangeRequest()
	for i := 0; i < len(offs); i++ {
		if minBlockSize != -1 && sizes[i]%minBlockSize != 0 {
			return nil, errno.NewError(errno.OBSClientRead, fmt.Sprintf("read size [%d] is not a multiple of min blocksize [%d]", sizes[i], minBlockSize))
		}
		if minBlockSize != -1 && sizes[i] > realObsSingleRequestSize {
			loopCount := sizes[i] / realObsSingleRequestSize
			var loop int64
			for loop = 0; loop < loopCount; loop++ {
				rangeRequest.AddReadAction(offs[i]+loop*realObsSingleRequestSize, realObsSingleRequestSize)
				rangeRequests = append(rangeRequests, rangeRequest)
				rangeRequest = NewRangeRequest()
			}
			if loop*realObsSingleRequestSize < sizes[i] {
				rangeRequest.AddReadAction(offs[i]+loop*realObsSingleRequestSize, sizes[i]-loop*realObsSingleRequestSize)
				rangeRequests = append(rangeRequests, rangeRequest)
				rangeRequest = NewRangeRequest()
			}
		} else if len(rangeRequest.ranges) >= obsRangeSize || rangeRequest.length >= realObsSingleRequestSize {
			rangeRequests = append(rangeRequests, rangeRequest)
			rangeRequest = NewRangeRequest()
			rangeRequest.AddReadAction(offs[i], sizes[i])
		} else {
			rangeRequest.AddReadAction(offs[i], sizes[i])
		}
	}
	if rangeRequest.length != 0 {
		rangeRequests = append(rangeRequests, rangeRequest)
	}
	return rangeRequests, nil
}

func NewObsRetryReadRequest(retryRange map[int64]int64, obsRangeSize int) []*RangeRequest {
	offs := make([]int64, 0, len(retryRange))
	sizes := make([]int64, len(retryRange))
	for k := range retryRange {
		offs = append(offs, k)
	}
	sort.Slice(offs, func(i, j int) bool {
		return offs[i] < offs[j]
	})
	for i, off := range offs {
		sizes[i] = retryRange[off]
	}
	rangeRequests, err := NewObsReadRequest(offs, sizes, -1, obsRangeSize)
	if err != nil {
		return nil
	}
	return rangeRequests
}
