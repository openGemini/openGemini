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

	"github.com/openGemini/openGemini/engine/index/tsi"
)

func Test_topNLinkedList_Close(t *testing.T) {
	type fields struct {
		maxLength int
		ascending bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "close",
			fields: fields{
				maxLength: 2,
				ascending: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &topNLinkedList{
				maxLength: tt.fields.maxLength,
				ascending: tt.fields.ascending,
			}
			m.Insert(&seriesCursor{tsmCursor: &tsmMergeCursor{}, tagSetRef: &tsi.TagSetInfo{}})
			m.Insert(&seriesCursor{tsmCursor: &tsmMergeCursor{}, tagSetRef: &tsi.TagSetInfo{}})
			m.Close()
		})
	}
}
