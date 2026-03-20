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

package mergeindex

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/encoding"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/uint64set"
	"github.com/stretchr/testify/require"
)

func TestBasicRowParser_FirstAndLastTSIDs_logger(t *testing.T) {
	brp := &BasicRowParser{
		tail: []byte{1, 2, 3, 4, 5, 6, 7},
	}
	_, _ = brp.FirstAndLastTSIDs()
}

func TestParseTSIDsAndGetFilteredLen(t *testing.T) {
	totalLen := 200
	deletedLen := 100
	deleted := genDeleted(deletedLen)
	brp := genBrp(totalLen)
	a := []uint64{}
	a = deleted.AppendTo(a)
	filteredLen := brp.ParseTSIDsAndGetFilteredLen(deleted)
	require.True(t, filteredLen == uint64(totalLen-deletedLen))

	deletedLen = 0
	deleted = genDeleted(deletedLen)
	brp = genBrp(totalLen)
	filteredLen = brp.ParseTSIDsAndGetFilteredLen(deleted)
	require.True(t, filteredLen == uint64(totalLen-deletedLen))
}

func genBrp(length int) *BasicRowParser {
	tail := make([]byte, 0, 8*length)
	for i := 1; i <= length; i++ {
		tail = encoding.MarshalUint64(tail, uint64(i))
	}
	brp := &BasicRowParser{
		tail: tail,
	}
	return brp
}

func genDeleted(length int) *uint64set.Set {
	deleted := &uint64set.Set{}
	for i := 1; i <= length; i++ {
		deleted.Add(uint64(i))
	}
	return deleted
}

func BenchmarkParseTSIDsAndGetFilteredLen(b *testing.B) {
	totalLenArray := []int{5, 100, 500, 1000, 5000}
	deletedLen := 100
	deleted := genDeleted(100)

	for _, totalLen := range totalLenArray {
		brp := genBrp(totalLen)
		runTest(b, totalLen, deletedLen, brp, deleted)
	}

	deletedLenArray := []int{0, 1, 5, 10, 100, 500, 1000, 5000, 10000, 20000}
	totalLen := 1000
	brp := genBrp(totalLen)
	for _, deletedLen := range deletedLenArray {
		deleted := genDeleted(deletedLen)
		runTest(b, totalLen, deletedLen, brp, deleted)
	}
}

func runTest(b *testing.B, totalLen int, deletedLen int, brp *BasicRowParser, deleted *uint64set.Set) {
	seriesCount := uint64(0)
	originName := fmt.Sprintf("GetParseTSIDsLen-Total=%d-deleted=%d-origin", totalLen, deletedLen)
	filterName := fmt.Sprintf("GetParseTSIDsLen-Total=%d-deleted=%d-filter", totalLen, deletedLen)
	b.Run(originName, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			seriesCount += uint64(brp.TSIDsLen())
			seriesCount = 0
		}
	})
	seriesCount = 0
	b.Run(filterName, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			seriesCount += brp.ParseTSIDsAndGetFilteredLen(deleted)
			seriesCount = 0
		}
	})
}
