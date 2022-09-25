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

package engine

import "github.com/openGemini/openGemini/lib/util"

type topNLinkedList struct {
	maxLength int
	len       int
	head      *listNode
	ascending bool
}

type listNode struct {
	pre  *listNode
	next *listNode
	item *seriesCursor
}

func NewTopNLinkedList(n int, ascending bool) *topNLinkedList {
	return &topNLinkedList{
		maxLength: n,
		ascending: ascending,
	}
}

func (m *topNLinkedList) insertNode(nowNode, newNode *listNode) {
	if nowNode == m.head {
		newNode.next = nowNode
		nowNode.pre = newNode
		m.head = newNode
	} else {
		nowNode.pre.next = newNode
		newNode.pre = nowNode.pre
		newNode.next = nowNode
		nowNode.pre = newNode
	}
}

func (m *topNLinkedList) replaceFirstNode() {
	newFirs := m.head.next
	util.MustClose(m.head.item)
	m.head = nil
	m.head = newFirs
	m.head.pre = nil
}

func (m *topNLinkedList) Insert(cursor *seriesCursor) {
	if m.len == 0 {
		m.head = &listNode{item: cursor}
		m.len += 1
		return
	}
	newNode := &listNode{item: cursor}
	if m.len < m.maxLength {
		nowNode := m.head
		for i := 0; i < m.len; i++ {
			if (cursor.limitFirstTime >= nowNode.item.limitFirstTime && m.ascending) || (cursor.limitFirstTime <= nowNode.item.limitFirstTime && !m.ascending) {
				m.insertNode(nowNode, newNode)
				break
			} else if i == m.len-1 {
				nowNode.next = newNode
				newNode.pre = nowNode
			}
			nowNode = nowNode.next
		}
		m.len += 1
	} else if (m.head.item.limitFirstTime <= cursor.limitFirstTime && m.ascending) || (m.head.item.limitFirstTime >= cursor.limitFirstTime && !m.ascending) {
		util.MustClose(cursor)
		return
	} else {
		nowNode := m.head
		i := 0
		for {
			i += 1
			if (cursor.limitFirstTime >= nowNode.item.limitFirstTime && m.ascending) || (cursor.limitFirstTime <= nowNode.item.limitFirstTime && !m.ascending) {
				nowNode.pre.next = newNode
				newNode.pre = nowNode.pre
				newNode.next = nowNode
				nowNode.pre = newNode
				m.replaceFirstNode()
				break
			} else if i == m.len {
				nowNode.next = newNode
				newNode.pre = nowNode
				m.replaceFirstNode()
				break
			} else {
				nowNode = nowNode.next
			}
		}
	}
}
