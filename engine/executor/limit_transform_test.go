// Copyright 2026 openGemini Authors.
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

package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLimitRemoteAbortScope(t *testing.T) {
	dag := NewTransformDag()
	limit, projection, merge, sibling := &TransformVertex{}, &TransformVertex{}, &TransformVertex{}, &TransformVertex{}
	for _, vertex := range []*TransformVertex{limit, projection, merge, sibling} {
		dag.AddVertex(vertex)
	}
	dag.AddEdge(limit, projection)
	dag.AddEdge(projection, merge)
	require.True(t, dag.canAbortFromLimit(limit))

	dag.AddEdge(sibling, merge)
	require.False(t, dag.canAbortFromLimit(limit))

	outerLimit := &TransformVertex{}
	dag.AddVertex(outerLimit)
	dag.AddEdge(merge, outerLimit)
	require.True(t, dag.canAbortFromLimit(outerLimit))
}
