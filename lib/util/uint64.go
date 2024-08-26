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

package util

// IndexOf returns index position in slice from given array
// If value is -1, the string does not found.
func IndexOf(slice []uint64, s uint64) int {
	for i, v := range slice {
		if v == s {
			return i
		}
	}

	return -1
}

// Include returns true or false if given array is in slice.
func Include(slice []uint64, s uint64) bool {
	return IndexOf(slice, s) != -1
}
