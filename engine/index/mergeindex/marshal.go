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

package mergeindex

import "fmt"

const commonPrefixLen = 1

func unmarshalCommonPrefix(src []byte) ([]byte, byte, error) {
	if len(src) < commonPrefixLen {
		return nil, 0, fmt.Errorf("cannot unmarshal common prefix from %d bytes; need at least %d bytes; data=%X", len(src), commonPrefixLen, src)
	}
	prefix := src[0]
	return src[commonPrefixLen:], prefix, nil
}

func MarshalCommonPrefix(dst []byte, nsPrefix byte) []byte {
	dst = append(dst, nsPrefix)
	return dst
}
