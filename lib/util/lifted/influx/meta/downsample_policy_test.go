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

package meta_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
)

func TestDownSamplePolicyInfo_GetTypes(t *testing.T) {
	policy := &meta.DownSamplePolicyInfo{}
	policy.Calls = []*meta.DownSampleOperators{{
		AggOps:   []string{"min"},
		DataType: 3,
	}, {
		AggOps:   []string{"max"},
		DataType: 3,
	}, {
		AggOps:   []string{"min"},
		DataType: 1,
	}, {
		AggOps:   []string{"min"},
		DataType: 1,
	}}

	require.Equal(t, 2, len(policy.GetTypes()))
}
