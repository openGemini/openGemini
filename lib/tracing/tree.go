// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package tracing

import (
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/pkg/tracing/fields"
	"github.com/influxdata/influxdb/pkg/tracing/labels"
	"github.com/xlab/treeprint"
)

// mergeVisitor merges sub-trace information into the main trace tree
type mergeVisitor struct {
	subTraces map[uint64]*Trace // Stores sub-traces with SpanID as the key
}

// newMergeVisitor creates a new instance of mergeVisitor
func newMergeVisitor(subTraces map[uint64]*Trace) *mergeVisitor {
	return &mergeVisitor{subTraces: subTraces}
}

// Visit implements the tracing.Visitor interface to merge sub-trace nodes
func (v *mergeVisitor) Visit(n *tracing.TreeNode) tracing.Visitor {
	spanID := n.Raw.Context.SpanID
	// Merge corresponding sub-trace into child nodes if exists
	if subTrace, exists := v.subTraces[spanID]; exists {
		n.Children = append(n.Children, subTrace.Tree())
	}

	// Recursively process all child nodes
	for _, childNode := range n.Children {
		tracing.Walk(v, childNode)
	}

	return nil
}

// treeVisitor converts trace tree into a visual tree structure
type treeVisitor struct {
	root  treeprint.Tree   // Root node of the visual tree
	stack []treeprint.Tree // Stack for recursively building the tree structure
}

// newTreeVisitor creates a new instance of treeVisitor
func newTreeVisitor() *treeVisitor {
	root := treeprint.New()
	return &treeVisitor{
		root:  root,
		stack: []treeprint.Tree{root},
	}
}

// Visit implements the tracing.Visitor interface to build visual trace tree
func (v *treeVisitor) Visit(n *tracing.TreeNode) tracing.Visitor {
	// Process node name (including name prefix fields)
	nodeName := n.Raw.Name
	fields := n.Raw.Fields
	nodeName = v.enrichNodeName(nodeName, &fields)

	// Create current node and push to stack
	currentNode := v.stack[len(v.stack)-1].AddBranch(nodeName)
	v.stack = append(v.stack, currentNode)

	// Add label information
	v.addLabels(currentNode, n.Raw.Labels)

	// Add field information
	v.addFields(currentNode, fields)

	// Recursively process child nodes
	for _, childNode := range n.Children {
		tracing.Walk(v, childNode)
	}

	// Pop the stack to return to parent node
	v.stack = v.stack[:len(v.stack)-1]

	return nil
}

// enrichNodeName extracts name prefix information from fields to enrich node name
func (v *treeVisitor) enrichNodeName(baseName string, fields *fields.Fields) string {
	for i := 0; i < len(*fields); {
		if strings.HasPrefix((*fields)[i].Key(), nameValuePrefix) {
			baseName += fmt.Sprintf(":%v", (*fields)[i].Value())
			// Remove processed field
			*fields = append((*fields)[:i], (*fields)[i+1:]...)
			continue
		}
		i++
	}
	return baseName
}

// addLabels adds label information to current node
func (v *treeVisitor) addLabels(node treeprint.Tree, labels []labels.Label) {
	if len(labels) == 0 {
		return
	}

	labelNode := node.AddBranch("labels")
	for _, label := range labels {
		labelNode.AddNode(fmt.Sprintf("%s: %s", label.Key, label.Value))
	}
}

// addFields adds field information to current node
func (v *treeVisitor) addFields(node treeprint.Tree, fields []fields.Field) {
	for _, field := range fields {
		node.AddNode(field.String())
	}
}
