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

package executor_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
)

func TestHashAggTransUnusefulPromFn(t *testing.T) {
	countPromOp := executor.NewCountPromOperator()
	countPromOp.SetNullFill(nil, 0, 0)
	countPromOp.SetNumFill(nil, 0, nil, 0)
	countPromOp.GetTime()
	minPromOp := executor.NewMinPromOperator()
	minPromOp.SetNullFill(nil, 0, 0)
	minPromOp.SetNumFill(nil, 0, nil, 0)
	minPromOp.GetTime()
	maxPromOp := executor.NewMaxPromOperator()
	maxPromOp.SetNullFill(nil, 0, 0)
	maxPromOp.SetNumFill(nil, 0, nil, 0)
	maxPromOp.GetTime()
}
