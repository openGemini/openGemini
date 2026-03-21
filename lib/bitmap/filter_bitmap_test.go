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

package bitmap

import "testing"

func TestReinit(t *testing.T) {
	tests := []struct {
		name    string
		initLen int
		newLen  int
		wantLen int
	}{
		{
			name:    "test reinit with shorter length",
			initLen: 5,
			newLen:  3,
			wantLen: 3,
		},
		{
			name:    "test reinit with longer length",
			initLen: 3,
			newLen:  5,
			wantLen: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize FilterBitmap with initial length
			f := NewFilterBitmap(tt.initLen)
			// Call Reinit with new length
			f.Reinit(tt.newLen)
			// Check if the bitmap length matches expected
			if len(f.Bitmap) != tt.wantLen {
				t.Errorf("Reinit() = %v, want %v", len(f.Bitmap), tt.wantLen)
			}
			// Check if all bitmap elements are initialized properly
			for i := range f.Bitmap {
				if f.Bitmap[i] == nil {
					t.Errorf("Bitmap element at index %d is nil", i)
				}
			}
		})
	}
}
