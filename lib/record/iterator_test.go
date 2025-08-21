/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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

package record_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/stretchr/testify/require"
)

func TestConsumeRecord_Marshal(t *testing.T) {
	tags := []*record.Tag{
		{Key: "tag1", Value: "val1"},
		{Key: "tag2", Value: "val2"},
	}
	var tagsMarshal []byte
	for i := range tags {
		tagsMarshal = tags[i].Marshal(tagsMarshal)
	}

	recSchema := record.Schemas{
		record.Field{Type: 0, Name: "schema1"},
	}
	rec := record.NewRecord(recSchema, false)
	var recMarshal []byte
	recMarshal = rec.Marshal(recMarshal)

	tests := []struct {
		name       string
		record     *record.ConsumeRecord
		wantBuffer []byte
	}{
		{
			name: "no tags, non-nil record",
			record: &record.ConsumeRecord{
				Tags: nil,
				Rec:  rec,
			},
			wantBuffer: func() []byte {
				var buf []byte
				buf = codec.AppendUint32(buf, 0)
				buf = append(buf, recMarshal...)
				return buf
			}(),
		},
		{
			name: "with tags and record",
			record: &record.ConsumeRecord{
				Tags: tags,
				Rec:  rec,
			},
			wantBuffer: func() []byte {
				var buf []byte
				buf = codec.AppendUint32(buf, 2)
				buf = append(buf, tagsMarshal...)
				buf = append(buf, recMarshal...)
				return buf
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf []byte
			result := tt.record.Marshal(buf)
			require.Equal(t, tt.wantBuffer, result)
		})
	}
}
