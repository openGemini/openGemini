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

package tracing

import (
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/xlab/treeprint"
)

type mergeVisitor struct {
	subs map[uint64]*Trace
}

func newMergeVisitor(subs map[uint64]*Trace) *mergeVisitor {
	return &mergeVisitor{subs: subs}
}

func (v *mergeVisitor) Visit(n *tracing.TreeNode) tracing.Visitor {
	pid := n.Raw.Context.SpanID
	if t, ok := v.subs[pid]; ok {
		n.Children = append(n.Children, t.Tree())
	}

	for _, cn := range n.Children {
		tracing.Walk(v, cn)
	}

	return nil
}

type treeVisitor struct {
	root  treeprint.Tree
	trees []treeprint.Tree
}

func newTreeVisitor() *treeVisitor {
	t := treeprint.New()
	return &treeVisitor{root: t, trees: []treeprint.Tree{t}}
}

func (v *treeVisitor) Visit(n *tracing.TreeNode) tracing.Visitor {
	name := n.Raw.Name
	fs := n.Raw.Fields

	i := 0
	for i < len(fs) {
		if strings.HasPrefix(fs[i].Key(), nameValuePrefix) {
			name += fmt.Sprintf(":%v", fs[i].Value())
			fs = append(fs[:i], fs[i+1:]...)
			continue
		}

		i++
	}

	t := v.trees[len(v.trees)-1].AddBranch(name)
	v.trees = append(v.trees, t)

	if labels := n.Raw.Labels; len(labels) > 0 {
		l := t.AddBranch("labels")
		for _, ll := range n.Raw.Labels {
			l.AddNode(ll.Key + ": " + ll.Value)
		}
	}

	for _, k := range fs {
		t.AddNode(k.String())
	}

	for _, cn := range n.Children {
		tracing.Walk(v, cn)
	}

	v.trees[len(v.trees)-1] = nil
	v.trees = v.trees[:len(v.trees)-1]

	return nil
}
