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

package executor_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/stretchr/testify/assert"
)

func TestTargetTable(t *testing.T) {
	rowCap := 1
	tupleCap := 1
	table := executor.NewTargetTable(rowCap, tupleCap)
	table.Reset()

	checkAndAllocate := func(expect bool) {
		_, _, ok := table.CheckAndAllocate()
		assert.Equal(t, ok, expect)

		if ok {
			table.Commit()
		}
	}

	onlyAllocate := func() {
		row, tuple := table.Allocate()
		assert.NotEqual(t, row, nil)
		assert.NotEqual(t, tuple, nil)
		table.Commit()
	}

	checkAndAllocate(true)
	checkAndAllocate(false)
	onlyAllocate()
	onlyAllocate()
	checkAndAllocate(true)
	checkAndAllocate(false)
}
