// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalWithColumns(t *testing.T) {
	var err error
	cm := buildChunkMeta()

	buf := cm.marshal(nil)
	other := &ChunkMeta{}

	other = &ChunkMeta{}
	_, err = other.UnmarshalWithColumns(buf, []string{"col_0009", "col_0088", "x_0011", "time"})
	require.NoError(t, err)
	require.Equal(t, 4, len(other.colMeta))
	require.Equal(t, record.TimeField, other.timeMeta().name)

	other = &ChunkMeta{}
	_, err = other.UnmarshalWithColumns(buf, []string{"col_0009", "z_0088", "time"})
	require.NoError(t, err)
	require.Equal(t, 2, len(other.colMeta))

	other = &ChunkMeta{}
	_, err = other.UnmarshalWithColumns(buf, []string{"time", "time"})
	require.NoError(t, err)
	require.Equal(t, int(other.columnCount), len(other.colMeta))
}

// The time column of SELECT count(time) is sorted together with the other columns
func TestUnmarshalWithColumns_CountTime(t *testing.T) {
	cm := buildChunkMeta()
	ctx := GetChunkMetaCodecCtx()
	defer ctx.Release()
	SetChunkMetaCompressMode(ChunkMetaCompressSelf)
	defer SetChunkMetaCompressMode(ChunkMetaCompressNone)
	selfBuf, err := MarshalChunkMeta(ctx, cm, nil)
	require.NoError(t, err)
	trailer := &Trailer{}
	trailer.ChunkMetaHeader = ctx.GetHeader()
	ctx.SetTrailer(trailer)

	tests := []struct {
		columns  []string
		expected []string
	}{
		// SELECT count(time) FROM mst WHERE col_0009 > 0
		{[]string{"col_0009", "time", "time"}, []string{"col_0009", "time"}},
		// SELECT count(time) FROM mst WHERE col_0009 > 0 AND x_0011 > 0
		{[]string{"col_0009", "time", "x_0011", "time"}, []string{"col_0009", "x_0011", "time"}},
	}

	assertColumns := func(other *ChunkMeta, expected []string) {
		require.Equal(t, len(expected), int(other.columnCount))
		require.Equal(t, len(expected), len(other.colMeta))
		for i := range expected {
			require.Equal(t, expected[i], other.colMeta[i].name)
		}
	}

	for _, tt := range tests {
		other := &ChunkMeta{}
		_, err = other.UnmarshalWithColumns(cm.marshal(nil), tt.columns)
		require.NoError(t, err)
		assertColumns(other, tt.expected)

		other = &ChunkMeta{}
		_, err = UnmarshalChunkMetaWithColumns(ctx, other, tt.columns, selfBuf)
		require.NoError(t, err)
		assertColumns(other, tt.expected)
	}
}

func TestUnmarshalWithColumns_error(t *testing.T) {
	var err error
	cm := buildChunkMeta()

	buf := cm.marshal(nil)
	other := &ChunkMeta{}

	other = &ChunkMeta{}
	cols := []string{"col_0009", "col_0088", "time"}

	_, err = other.UnmarshalWithColumns(buf[:100], cols)
	require.Contains(t, err.Error(), "too smaller data for column")

	_, err = other.UnmarshalWithColumns(buf[:629], cols)
	require.Contains(t, err.Error(), "too smaller data for column name length")

	_, err = other.UnmarshalWithColumns(buf[:640], cols)
	require.Contains(t, err.Error(), "too smaller data for column name")

	_, err = other.UnmarshalWithColumns(buf[:648], cols)
	require.Contains(t, err.Error(), "too smaller data for preagg")

	_, err = other.UnmarshalWithColumns(buf[:692], cols)
	require.Contains(t, err.Error(), "too smaller data for segment meta")
}

func buildChunkMeta() *ChunkMeta {
	cm := &ChunkMeta{
		sid:         200,
		offset:      100,
		size:        1000,
		columnCount: 301,
		segCount:    1,
		timeRange:   []SegmentRange{{1, 10}},
		colMeta:     []ColumnMeta{},
	}

	for i := 0; i < 200; i++ {
		cm.colMeta = append(cm.colMeta, ColumnMeta{
			name:    fmt.Sprintf("col_%04d", i+1),
			ty:      0,
			preAgg:  make([]byte, 48),
			entries: []Segment{{100, 64}},
		})
	}
	for i := 0; i < 100; i++ {
		cm.colMeta = append(cm.colMeta, ColumnMeta{
			name:    fmt.Sprintf("x_%04d", i+1),
			ty:      0,
			preAgg:  make([]byte, 48),
			entries: []Segment{{100, 64}},
		})
	}
	cm.colMeta = append(cm.colMeta, ColumnMeta{
		name:    record.TimeField,
		ty:      0,
		preAgg:  make([]byte, 48),
		entries: []Segment{{100, 64}},
	})
	return cm
}

func TestPreAggEnable(t *testing.T) {
	config.GetCommon().PreAggEnabled = false
	defer func() {
		config.GetCommon().PreAggEnabled = true
	}()

	cm := buildChunkMeta()
	buf := cm.marshal(nil)
	other := &ChunkMeta{}
	_, err := other.unmarshal(buf)
	require.NoError(t, err)

	require.Equal(t, zeroPreAgg, other.colMeta[0].preAgg)
}
