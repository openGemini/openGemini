/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
package gramRegexQuery

func Replacei(old string, pos int, new uint8) string {
	return old[:pos] + string(new) + old[pos+1:]
}

func CanMerge(start []uint16, before []uint16, after []uint16) (bool, []uint16, []uint16) {
	newafter := make([]uint16, 0)
	newstart := make([]uint16, 0)
	for i := 0; i < len(after); i++ {
		a := after[i]
		for j := 0; j < len(before); j++ {
			if before[j] == a-1 {
				newstart = append(newstart, start[j])
				newafter = append(newafter, after[i])
				break
			}
		}
	}
	if len(newafter) > 0 {
		return true, newstart, newafter
	}
	return false, newstart, newafter
}

func AddToInOrderList(list []uint16, num uint16) []uint16 {
	index := 0
	isin := false
	for index < len(list) && num >= list[index] {
		if num == list[index] {
			isin = true
			break
		}
		index++
	}
	if !isin {
		list = append(list, 0)
		copy(list[index+1:], list[index:])
		list[index] = num
	}
	return list
}
