// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package tsreader

import (
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
)

type MockTsspFile struct {
	immutable.TSSPFile
}

func NewMockTsspFile() *MockTsspFile {
	return &MockTsspFile{}
}

func (m *MockTsspFile) Unref() {}

func (m *MockTsspFile) UnrefFileReader() {}

func (m *MockTsspFile) GetFileReaderRef() int64 {
	return 1
}

func TestUnrefTSSPFile(t *testing.T) {
	type args struct {
		enable bool
		files  []immutable.TSSPFile
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "enable query file cache",
			args: args{
				enable: true,
				files:  []immutable.TSSPFile{NewMockTsspFile(), NewMockTsspFile()},
			},
		},
		{
			name: "disable query file cache",
			args: args{
				enable: false,
				files:  []immutable.TSSPFile{NewMockTsspFile(), NewMockTsspFile()},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			immutable.InitQueryFileCache(3, tt.args.enable)
			UnrefTSSPFile(tt.args.files...)
		})
	}
}
