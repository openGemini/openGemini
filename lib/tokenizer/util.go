/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package tokenizer

import "math/bits"

const (
	Prime_64 uint64 = 0x9E3779B185EBCA87
)

func Hash(bytes []byte) uint64 {
	var hash uint64 = 0
	for _, b := range bytes {
		hash ^= bits.RotateLeft64(hash, 11) ^ (uint64(b) * Prime_64)
	}
	return hash
}
