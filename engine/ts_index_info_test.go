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

package engine

import (
	"testing"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/mutable"
	assert2 "github.com/stretchr/testify/assert"
)

func TestTsIndexInfo(t *testing.T) {
	type args struct {
		immTables *immutable.MmsReaders
		memTables MemDataReader
		cursors   []comm.KeyCursor
	}
	tests := []struct {
		name string
		args args
		want comm.TSIndexInfo
	}{
		{
			name: "TsIndexInfo Ref/Unref",
			args: args{
				immTables: &immutable.MmsReaders{
					Orders:      []immutable.TSSPFile{&MocTsspFile{}},
					OutOfOrders: []immutable.TSSPFile{&MocTsspFile{}},
				},
				memTables: &mutable.MemTables{},
				cursors:   []comm.KeyCursor{&groupCursor{}},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			immutable.InitQueryFileCache(2, true)
			defer immutable.ResetQueryFileCache()
			info := NewTsIndexInfo([]*immutable.MmsReaders{tt.args.immTables}, []MemDataReader{tt.args.memTables}, tt.args.cursors)
			info.Ref()
			info.Unref()
			assert2.Equal(t, len(info.GetCursors()), 1)
		})
	}
}
