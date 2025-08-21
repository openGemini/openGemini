// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package colstore

import (
	"testing"
)

func TestPkMetaBlockMarshal(t *testing.T) {
	meta := make([]byte, 0)
	meta = MarshalPkMetaBlock(0, 1, 0, 100, meta, nil)
	_, err := UnmarshalPkMetaBlock(meta[:0])
	if err == nil {
		t.Error("expected unmarshal failure ")
	}
	_, err = UnmarshalPkMetaBlock(meta)
	if err != nil {
		t.Errorf("expected unmarshal success: %+v", err)
	}
}
