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
package comm

import (
	"container/list"
)

type TopNList struct {
	maxLength   int
	len         int
	l           *list.List
	compareFunc func(interface{}, interface{}) bool
}

func NewTopNList(n int, compare func(interface{}, interface{}) bool) *TopNList {
	return &TopNList{
		maxLength:   n,
		l:           list.New(),
		compareFunc: compare,
	}
}

func (m *TopNList) GetList() *list.List {
	return m.l
}

func (m *TopNList) Back() *list.Element {
	return m.l.Back()
}

func (m *TopNList) Insert(node interface{}) {
	if m.len < m.maxLength {
		nowNode := m.l.Front()
		for {
			if m.len == 0 {
				m.l.PushBack(node)
				break
			}
			if m.compareFunc(node, nowNode.Value) {
				m.l.InsertBefore(node, nowNode)
				break
			}
			if nowNode == m.l.Back() {
				m.l.InsertAfter(node, nowNode)
				break
			}
			nowNode = nowNode.Next()
		}
		m.len += 1
	}
	if m.compareFunc(node, m.l.Front().Value) {
		return
	}
	nowNode := m.l.Front()
	for {
		if m.compareFunc(node, nowNode.Value) {
			m.l.InsertBefore(node, nowNode)
			m.l.Remove(m.l.Front())
			break
		}
		if nowNode == m.l.Back() {
			m.l.InsertAfter(node, nowNode)
			m.l.Remove(m.l.Front())
			break
		}
		nowNode = nowNode.Next()
	}
}
