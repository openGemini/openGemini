// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package handler

import (
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCrashProcessor_Handle(t *testing.T) {
	w := &MockNewResponser{}
	data := &executor.Crash{}
	p := NewCrashProcessor()
	require.NoError(t, p.Handle(w, data))

	data1 := &executor.Abort{}
	err := p.Handle(w, data1)
	assert.True(t, strings.Contains(err.Error(), "Crash"))

	w = NewMockNewResponser(errno.NewError(errno.QueryAborted))
	data = &executor.Crash{}
	p = NewCrashProcessor()
	err = p.Handle(w, data)
	require.NoError(t, p.Handle(w, data))
}
