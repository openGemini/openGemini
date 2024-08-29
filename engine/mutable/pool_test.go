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

package mutable_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/require"
)

func TestMemTablePool(t *testing.T) {
	pm := mutable.NewMemTablePoolManager()
	pm.Init()

	pool := pm.Alloc("db0/rp0")
	pool.SetExpire(10)

	tbl := pool.Get(config.TSSTORE)
	require.NotEmpty(t, tbl)

	tbl.UnRef()
	require.Equal(t, 1, pool.Size())

	tbl = pool.Get(config.TSSTORE)
	require.NotEmpty(t, tbl)
	require.Equal(t, 0, pool.Size())
	tbl.UnRef()

	pool.SetExpire(1)
	time.Sleep(time.Second * 3)
	pm.Free()

	require.Equal(t, 0, pm.Size())
	pm.Close()
}
