// Copyright 2025 openGemini Authors
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

package core

import (
	"reflect"
	"testing"
)

func TestLineProtocolParser_Parse(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantErr    bool
		wantMst    string
		wantTags   map[string]string
		wantFields map[string]interface{}
	}{
		{
			name:       "ok",
			raw:        "mst,t1=1 v1=1 123",
			wantErr:    false,
			wantMst:    "mst",
			wantTags:   map[string]string{"t1": "1"},
			wantFields: map[string]interface{}{"v1": "1"},
		},
		{
			name:       "escape mst",
			raw:        `mst\,1,t1=1 v1=1 123`,
			wantErr:    false,
			wantMst:    `mst,1`,
			wantTags:   map[string]string{"t1": "1"},
			wantFields: map[string]interface{}{"v1": "1"},
		},
		{
			name:       "tag array",
			raw:        `mst,t1=[t1,t2] v1=1 123`,
			wantErr:    false,
			wantMst:    "mst",
			wantTags:   map[string]string{"t1": "[t1,t2]"},
			wantFields: map[string]interface{}{"v1": "1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewLineProtocolParser(tt.raw)
			got, err := p.Parse(1)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got[0].Tags, tt.wantTags) {
				t.Errorf("parse() got = %v, want %v", got[0].Tags, tt.wantTags)
				return
			}
			if !reflect.DeepEqual(got[0].Fields, tt.wantFields) {
				t.Errorf("parse() got = %v, want %v", got[0].Fields, tt.wantFields)
				return
			}
		})
	}
}
