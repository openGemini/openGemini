// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package tsi

import "testing"

func TestInitTagToValuesRowParserError(t *testing.T) {
	tmm := getTagToValuesRowsMerger()
	defer putTagToValuesRowsMerger(tmm)

	mp := tmm.GetMergeParser()
	err := mp.Init(nil, nsPrefixTagKeysToTagValues)
	if err == nil {
		t.Fatal("should unmarshalCommonPrefix error")
	}
	mp.Reset()

	var tmpB, tmp []byte
	mp = tmm.GetMergeParser()
	tmpB = append(tmpB, nsPrefixTagKeysToTagValues)
	err = mp.Init(tmpB, nsPrefixTagKeysToTagValues)
	if err == nil {
		t.Fatal("should unmarshal tag key error")
	}
	mp.Reset()

	mp = tmm.GetMergeParser()
	key := "key"
	name := "cpu"
	tmpB = append(tmpB[:0], nsPrefixTagKeysToTagValues)
	tmp = marshalCompositeTagKey(tmp[:0], []byte(name), []byte(key))
	tmpB = marshalTagValue(tmpB, tmp)
	tmpB = append(tmpB, "err"...)
	err = mp.Init(tmpB, nsPrefixTagKeysToTagValues)
	if err == nil {
		t.Fatal("should unmarshal tag value error")
	}
}
