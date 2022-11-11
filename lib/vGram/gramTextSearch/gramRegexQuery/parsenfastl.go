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

type SubPathStack struct {
	list []*SubPath
}

func InitializeSubPathStack() *SubPathStack {
	return &SubPathStack{
		list: make([]*SubPath, 0),
	}
}

func (s *SubPathStack) Push(ele *SubPath) {
	s.list = append(s.list, ele)

}

func (s *SubPathStack) Pop() *SubPath {
	if len(s.list) < 0 {
		return nil
	} else {
		result := s.list[len(s.list)-1]
		s.list = s.list[:len(s.list)-1]
		return result
	}
}

func (s *SubPathStack) Top() *SubPath {
	if len(s.list) < 0 {
		return nil
	} else {
		return s.list[len(s.list)-1]
	}
}
