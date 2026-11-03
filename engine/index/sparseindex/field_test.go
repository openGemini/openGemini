// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package sparseindex_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/stretchr/testify/require"
)

func TestFieldRefInfinity(t *testing.T) {
	field := sparseindex.NewFieldRef(nil, 0, 0)
	finite := sparseindex.NewFieldRef(nil, 0, 0)
	require.False(t, field.IsPositiveInfinity())
	require.False(t, field.IsNegativeInfinity())

	field.SetPositiveInfinity()
	require.True(t, field.IsPositiveInfinity())
	require.False(t, field.IsNegativeInfinity())
	require.True(t, field.Equals(sparseindex.POSITIVE_INFINITY))
	require.False(t, field.Equals(sparseindex.NEGATIVE_INFINITY))
	require.True(t, finite.Less(field))
	require.False(t, field.Less(finite))

	field.SetNegativeInfinity()
	require.True(t, field.IsNegativeInfinity())
	require.False(t, field.IsPositiveInfinity())
	require.True(t, field.Equals(sparseindex.NEGATIVE_INFINITY))
	require.False(t, field.Equals(sparseindex.POSITIVE_INFINITY))
	require.True(t, field.Less(finite))
	require.False(t, finite.Less(field))

	field.Set(nil, 0, 0)
	require.False(t, field.IsPositiveInfinity())
	require.False(t, field.IsNegativeInfinity())
}
